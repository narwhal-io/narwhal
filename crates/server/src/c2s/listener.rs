// SPDX-License-Identifier: BSD-3-Clause

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::anyhow;
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsAcceptor;
use tracing::{info, trace, warn};

use narwhal_common::conn::{ConnWorkerPool, DispatcherFactory};
use narwhal_common::service::{C2sService, Service};

use crate::c2s::MaybeKtlsStream;
use crate::c2s::config::ListenerConfig;
use crate::c2s::conn::{C2sConnManager, C2sConnWorkerPool, C2sDispatcherFactory};
use crate::util;
use crate::util::tls::{create_tls_config, generate_self_signed_cert};

const LOCALHOST_DOMAIN: &str = "localhost";

/// A TLS-enabled TCP listener for client-to-server (C2S) connections.
///
/// The `Listener` manages incoming client connections, handling TLS negotiation
/// and connection management. It supports both self-signed certificates for
/// localhost development and proper TLS certificates for production use.
///
/// The listener works in conjunction with a connection manager to handle
/// individual client connections after they are established.
pub struct C2sListener {
  /// The configuration for the C2S listener.
  config: ListenerConfig,

  /// The connection manager.
  conn_mng: C2sConnManager,

  /// The dispatcher factory.
  dispatcher_factory: C2sDispatcherFactory,

  /// The connection worker pool.
  worker_pool: Option<C2sConnWorkerPool>,

  /// The channel to signal the listener to stop.
  done_tx: Option<mpsc::Sender<()>>,

  /// The local address of the listener.
  local_address: Option<SocketAddr>,

  /// The number of worker threads for the connection pool.
  conn_worker_threads: usize,
}

// ===== impl C2sListener =====

impl C2sListener {
  /// Creates a new C2S listener with the given configuration and connection manager.
  ///
  /// # Arguments
  ///
  /// * `config` - The configuration for the C2S listener
  /// * `conn_mng` - The connection manager that will handle established connections
  /// * `dispatcher_factory` - The dispatcher factory that will create new dispatchers
  /// * `conn_worker_threads` - The number of worker threads for the connection pool
  ///
  /// # Returns
  ///
  /// Returns a new `C2sListener` instance that is ready to be bootstrapped.
  pub fn new(
    config: ListenerConfig,
    conn_mng: C2sConnManager,
    dispatcher_factory: C2sDispatcherFactory,
    conn_worker_threads: usize,
  ) -> Self {
    Self {
      config,
      conn_mng,
      dispatcher_factory,
      worker_pool: None,
      conn_worker_threads,
      done_tx: None,
      local_address: None,
    }
  }

  /// Bootstraps the listener, starting to accept incoming connections.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the listener was successfully started.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The connection manager fails to bootstrap
  /// * TLS configuration fails (invalid certificates or keys)
  /// * Unable to bind to the configured address and port
  pub async fn bootstrap(&mut self) -> anyhow::Result<()> {
    assert!(self.done_tx.is_none());
    assert!(self.worker_pool.is_none());

    // Bootstrap the connection manager.
    self.dispatcher_factory.bootstrap().await?;
    self.conn_mng.bootstrap().await?;

    // Create the connection worker pool.
    let worker_pool =
      ConnWorkerPool::new(self.conn_worker_threads, self.conn_mng.clone(), self.dispatcher_factory.clone())?;
    self.worker_pool = Some(worker_pool.clone());

    let (done_tx, mut done_rx) = mpsc::channel(1);
    self.done_tx = Some(done_tx);

    // Auto-detect kTLS support (Linux)
    #[cfg(target_os = "linux")]
    let (enable_ktls, ktls_compat_opt) = match ktls::CompatibleCiphers::new().await {
      Ok(compat) => {
        // Check if at least one cipher is supported
        let has_support = compat.tls13.aes_gcm_128
          || compat.tls13.aes_gcm_256
          || compat.tls13.chacha20_poly1305
          || compat.tls12.aes_gcm_128
          || compat.tls12.aes_gcm_256
          || compat.tls12.chacha20_poly1305;

        if has_support {
          info!(service_type = C2sService::NAME, "kTLS support detected");
          (true, Some(compat))
        } else {
          warn!(service_type = C2sService::NAME, "kTLS not available (no compatible ciphers), using userspace TLS");
          (false, None)
        }
      },
      Err(e) => {
        warn!(
          service_type = C2sService::NAME,
          error = e.to_string(),
          "kTLS capability detection failed, using userspace TLS"
        );
        (false, None)
      },
    };

    #[cfg(not(target_os = "linux"))]
    let (enable_ktls, ktls_compat_opt): (bool, Option<ktls::CompatibleCiphers>) = (false, None);

    // Create TLS config
    let tls_config = self.load_tls_config(ktls_compat_opt.as_ref())?;
    let acceptor = TlsAcceptor::from(tls_config);

    let mut listener = TcpListener::bind(self.get_address()).await?;

    self.local_address = Some(listener.local_addr()?);

    let (running_tx, running_rx) = oneshot::channel();

    tokio::spawn(async move {
      let _ = running_tx.send(());

      loop {
        tokio::select! {
          _ = Self::accept_connection(&mut listener, acceptor.clone(), worker_pool.clone(), enable_ktls) => {}
          _ = done_rx.recv() => {
            break;
          }
        }
      }
    });

    // Wait for the listener to start.
    running_rx.await?;

    info!(
      address = self.get_address(),
      domain = self.config.domain,
      service_type = C2sService::NAME,
      "accepting socket connections"
    );
    Ok(())
  }

  /// Gracefully shuts down the listener.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the shutdown was successful.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * Unable to send the shutdown signal
  /// * The connection manager fails to shut down properly
  pub async fn shutdown(&mut self) -> anyhow::Result<()> {
    assert!(self.done_tx.is_some());

    self.done_tx.take().unwrap().send(()).await?;

    info!(
      address = self.get_address(),
      domain = self.config.domain,
      service_type = C2sService::NAME,
      "stopped accepting socket connections"
    );

    self.worker_pool.take();

    // Wait for the connection manager to stop.
    self.conn_mng.shutdown().await?;
    self.dispatcher_factory.shutdown().await?;

    Ok(())
  }

  /// Returns the local address that the listener is bound to.
  ///
  /// This method will return `None` if called before `bootstrap()` or if
  /// the listener failed to bind to an address.
  ///
  /// # Returns
  ///
  /// Returns the socket address that the listener is bound to, if available.
  pub fn local_address(&self) -> Option<SocketAddr> {
    self.local_address
  }

  fn load_tls_config(&self, ktls_compat: Option<&ktls::CompatibleCiphers>) -> anyhow::Result<Arc<ServerConfig>> {
    let is_localhost = self.config.domain == LOCALHOST_DOMAIN;

    if self.config.cert_file.is_empty() || self.config.key_file.is_empty() {
      if !is_localhost {
        return Err(anyhow!("certificate and key files must be specified for non-localhost domains"));
      }
      warn!(domain = "localhost", service_type = C2sService::NAME, "using self-signed certificate");

      let (certs, key) = generate_self_signed_cert(vec![LOCALHOST_DOMAIN.to_string()])?;

      return create_tls_config(certs, key, ktls_compat);
    }
    info!(
      domain = self.config.domain,
      cert_file = self.config.cert_file,
      key_file = self.config.key_file,
      service_type = C2sService::NAME,
      "loading certificate and key files"
    );

    let certs = util::tls::load_certs(&self.config.cert_file)?;
    let key = util::tls::load_private_key(&self.config.key_file)?;

    create_tls_config(certs, key, ktls_compat)
  }

  fn get_address(&self) -> String {
    format!("{}:{}", self.config.bind_address, self.config.port)
  }

  async fn accept_connection(
    listener: &mut TcpListener,
    acceptor: TlsAcceptor,
    worker_pool: C2sConnWorkerPool,
    enable_ktls: bool,
  ) -> anyhow::Result<()> {
    let (tcp_stream, addr) = listener.accept().await?;

    trace!(local_address = format!("{:?}", addr), service_type = C2sService::NAME, "accepted connection");

    worker_pool
      .submit(move || async move {
        // Try kTLS path
        #[cfg(target_os = "linux")]
        if enable_ktls {
          let cork_stream = ktls::CorkStream::new(tcp_stream);
          let tls_stream = acceptor.accept(cork_stream).await?;

          // Attempt kTLS
          match ktls::config_ktls_server(tls_stream).await {
            Ok(ktls_stream) => {
              trace!(service_type = C2sService::NAME, "kTLS enabled for connection");
              return Ok(MaybeKtlsStream::from_ktls(ktls_stream));
            },
            Err(e) => {
              warn!(
                service_type = C2sService::NAME,
                error = ?e,
                "kTLS configuration failed unexpectedly, connection dropped"
              );
              return Err(anyhow::anyhow!("kTLS configuration failed: {:?}", e));
            },
          }
        }

        // Regular TLS path
        let tls_stream = acceptor.accept(tcp_stream).await?;
        Ok(MaybeKtlsStream::from_tls(tls_stream))
      })
      .await;

    Ok(())
  }
}
