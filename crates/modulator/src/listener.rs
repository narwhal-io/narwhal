// SPDX-License-Identifier: BSD-3-Clause

use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::anyhow;
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, trace};

use narwhal_common::conn::{ConnManager, ConnWorkerPool, Dispatcher, DispatcherFactory};
use narwhal_common::service::{M2sService, S2mService, Service};
use narwhal_util::conn::Stream;

use crate::config::*;

/// Type alias for S2M listener.
pub type S2mListener<M> = Listener<crate::conn::S2mDispatcher<M>, crate::conn::S2mDispatcherFactory<M>, S2mService>;

/// Type alias for M2S listener.
pub type M2sListener = Listener<crate::conn::M2sDispatcher, crate::conn::M2sDispatcherFactory, M2sService>;

/// Modulator server listener.
pub struct Listener<D, DF, ST>
where
  D: Dispatcher,
  DF: DispatcherFactory<D>,
  ST: Service,
{
  /// The listener configuration.
  config: ListenerConfig,

  /// The connection manager.
  conn_mng: ConnManager<D, DF, ST>,

  /// The connection worker pool.
  worker_pool: Option<ConnWorkerPool<Stream, D, DF, ST>>,

  /// The channel to signal the listener to stop.
  done_tx: Option<mpsc::Sender<()>>,

  /// The local address of the listener (only for TCP).
  local_address: Option<SocketAddr>,

  /// The number of worker threads for the connection pool.
  conn_worker_threads: usize,

  /// The maximum number of connections allowed.
  max_connections: usize,
}

// ===== impl Listener =====

impl<D, DF, ST> Listener<D, DF, ST>
where
  D: Dispatcher,
  DF: DispatcherFactory<D>,
  ST: Service,
{
  /// Creates a new listener.
  ///
  /// # Arguments
  ///
  /// * `config` - The configuration for the listener
  /// * `conn_mng` - The connection manager that will handle established connections
  /// * `conn_worker_threads` - The number of worker threads for the connection pool
  ///
  /// # Returns
  ///
  /// Returns a new `Listener` instance that is ready to be bootstrapped.
  pub fn new(
    config: ListenerConfig,
    conn_mng: ConnManager<D, DF, ST>,
    conn_worker_threads: usize,
    max_connections: usize,
  ) -> Self {
    Listener {
      config,
      conn_mng,
      worker_pool: None,
      conn_worker_threads,
      done_tx: None,
      local_address: None,
      max_connections,
    }
  }

  /// Starts the listener.
  pub async fn bootstrap(&mut self) -> anyhow::Result<()> {
    assert!(self.done_tx.is_none());
    assert!(self.worker_pool.is_none());

    let (done_tx, done_rx) = mpsc::channel(1);
    self.done_tx = Some(done_tx);

    let conn_mng = self.conn_mng.clone();
    conn_mng.bootstrap().await?;

    // Create the connection worker pool.
    let worker_pool = ConnWorkerPool::new(self.conn_worker_threads, conn_mng.clone(), self.max_connections)?;
    self.worker_pool = Some(worker_pool.clone());

    match self.config.network.as_str() {
      TCP_NETWORK => {
        self.listen_tcp(worker_pool, done_rx).await?;
      },
      UNIX_NETWORK => {
        self.listen_unix(worker_pool, done_rx).await?;
      },
      _ => {
        anyhow::bail!("unsupported network type: {}", self.config.network);
      },
    }
    Ok(())
  }

  /// Returns the local address that the listener is bound to.
  pub fn local_address(&self) -> Option<SocketAddr> {
    self.local_address
  }

  async fn listen_tcp(
    &mut self,
    worker_pool: ConnWorkerPool<Stream, D, DF, ST>,
    mut done_rx: Receiver<()>,
  ) -> anyhow::Result<()> {
    let config = self.config.clone();

    let mut listener = TcpListener::bind(&config.bind_address)
      .await
      .map_err(|e| anyhow!("failed to bind to TCP socket {}: {}", config.bind_address, e))?;

    let (running_tx, running_rx) = oneshot::channel();

    self.local_address = Some(listener.local_addr()?);

    tokio::spawn(async move {
      let _ = running_tx.send(());

      loop {
        tokio::select! {
            _ = Self::accept_tcp_connection(&mut listener, worker_pool.clone()) => {},
            _ = done_rx.recv() => {
                break;
            }
        }
      }
    });

    // Wait for the listener to start.
    running_rx.await?;

    info!(
      network = TCP_NETWORK,
      address = config.bind_address,
      service_type = ST::NAME,
      "accepting socket connections"
    );
    Ok(())
  }

  async fn listen_unix(
    &self,
    worker_pool: ConnWorkerPool<Stream, D, DF, ST>,
    mut done_rx: Receiver<()>,
  ) -> anyhow::Result<()> {
    let config = self.config.clone();

    // Validate the socket path is set.
    if config.socket_path.is_empty() {
      anyhow::bail!("unix network selected but socket_path is not set");
    }

    // Check if the socket file already exists and remove it.
    let socket_path = Path::new(&config.socket_path);
    if socket_path.exists() {
      fs::remove_file(socket_path)
        .map_err(|e| anyhow!("failed to remove socket file {}: {}", config.socket_path, e))?;
    }

    // Listen on the Unix socket.
    let mut listener = UnixListener::bind(&config.socket_path)
      .map_err(|e| anyhow!("failed to bind to Unix socket {}: {}", config.socket_path, e))?;

    let (running_tx, running_rx) = oneshot::channel();

    tokio::spawn(async move {
      let _ = running_tx.send(());

      loop {
        tokio::select! {
            _ = Self::accept_unix_connection(&mut listener, worker_pool.clone()) => {},
            _ = done_rx.recv() => {
                break;
            }
        }
      }
    });

    // Wait for the listener to start.
    running_rx.await?;

    info!(network = UNIX_NETWORK, socket_path = config.socket_path, "accepting socket connections");
    Ok(())
  }

  /// Stops the listener.
  pub async fn shutdown(&mut self) -> anyhow::Result<()> {
    assert!(self.done_tx.is_some());

    self.done_tx.take().unwrap().send(()).await?;

    match self.config.network.as_str() {
      TCP_NETWORK => {
        info!(
          network = TCP_NETWORK,
          address = self.config.bind_address,
          service_type = ST::NAME,
          "stopped accepting socket connections",
        );
      },
      UNIX_NETWORK => {
        // Remove the socket file.
        let socket_path = Path::new(&self.config.socket_path);
        if socket_path.exists() {
          fs::remove_file(socket_path)
            .map_err(|e| anyhow!("failed to remove socket file {}: {}", self.config.socket_path, e))?;
        }
        info!(
          network = UNIX_NETWORK,
          socket_path = self.config.socket_path,
          service_type = ST::NAME,
          "stopped accepting socket connections",
        );
      },
      _ => {
        unreachable!("unsupported network type: {}", self.config.network);
      },
    }

    self.worker_pool.take();

    // Wait for the connection manager to stop.
    self.conn_mng.shutdown().await?;

    Ok(())
  }

  async fn accept_tcp_connection(
    listener: &mut TcpListener,
    worker_pool: ConnWorkerPool<Stream, D, DF, ST>,
  ) -> anyhow::Result<()> {
    let (tcp_stream, addr) = listener.accept().await?;

    trace!(
      network = TCP_NETWORK,
      local_address = format!("{:?}", addr),
      service_type = ST::NAME,
      "accepted connection"
    );

    worker_pool.submit(move || async move { Ok(Stream::Tcp(tcp_stream)) }).await;

    Ok(())
  }

  async fn accept_unix_connection(
    listener: &mut UnixListener,
    worker_pool: ConnWorkerPool<Stream, D, DF, ST>,
  ) -> anyhow::Result<()> {
    let (unix_stream, _) = listener.accept().await?;

    trace!(network = UNIX_NETWORK, service_type = ST::NAME, "accepted connection");

    worker_pool.submit(move || async move { Ok(Stream::Unix(unix_stream)) }).await;

    Ok(())
  }
}
