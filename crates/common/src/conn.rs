// SPDX-License-Identifier: BSD-3-Clause

use core::fmt::Debug;
use std::cell::UnsafeCell;
use std::future::Future;
use std::io::{Cursor, IoSlice};
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use core_affinity::CoreId;

use rand::random;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, trace, warn};

use narwhal_protocol::ErrorReason::{
  BadRequest, InternalServerError, OutboundQueueIsFull, PolicyViolation, ResponseTooLarge, ServerShuttingDown, Timeout,
};
use narwhal_protocol::{ErrorParameters, Message, PingParameters, SerializeError, deserialize, serialize};

use narwhal_util::codec::{StreamReader, StreamReaderError};
use narwhal_util::io::write_all_vectored;
use narwhal_util::pool::{BucketedPool, MutablePoolBuffer, Pool, PoolBuffer};
use narwhal_util::string_atom::StringAtom;

use crate::service::Service;

const SERVER_OVERLOADED_ERROR: &[u8] = b"ERROR reason=SERVER_OVERLOADED detail=\\\"max connections reached\\\"\n";

const MAX_IOVS: usize = 128;

/// Represents the current state of a client connection in the protocol flow.
///
/// Client connections progress through these states in a strictly forward-only manner.
/// Once a connection reaches the `Authenticated` state, it remains in that state
/// until the connection is closed.
///
/// The implementation enforces that state transitions can only advance forward
/// and never regress to a previous state.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub enum State {
  /// Initial state when a client first connects.
  /// In this state, only initial handshake messages should be accepted.
  Connecting,

  /// Client has established the connection but has not yet authenticated.
  /// In this state, only authentication-related messages are accepted.
  /// A timeout will disconnect the client if authentication is not completed.
  Connected,

  /// Terminal state: client has successfully authenticated and can perform all operations.
  /// Once a connection reaches this state, it cannot transition to any other state.
  /// The negotiated heartbeat interval is provided and used for connection health monitoring.
  /// All protocol messages should be accepted in this state.
  Authenticated { heartbeat_interval: Duration },
}

/// A trait for handling messages received by a connection.
///
/// Implementors of this trait are responsible for processing incoming protocol messages,
/// managing connection state transitions, and executing appropriate business logic.
///
/// The `Dispatcher` trait is designed to work within the connection management system
/// and follows a state-based approach to protocol handling, where different message
/// types are allowed in different connection states.
///
/// # State Transitions
///
/// Dispatchers enforce a forward-only state progression through the `State` enum:
/// - `State::Connecting` → `State::Connected` → `State::Authenticated`
///
/// Implementations must ensure state transitions only move forward, never backward.
#[async_trait]
pub trait Dispatcher: Send + Sync + 'static {
  /// Processes an incoming message based on the current connection state.
  ///
  /// This method is the core of the message handling logic. It receives a message,
  /// an optional payload buffer, and the current connection state, and is responsible
  /// for executing the appropriate business logic.
  ///
  /// # State Transitions
  ///
  /// This method may return a new state to transition the connection to. The
  /// connection manager will enforce that transitions only move forward in the
  /// state progression.
  ///
  /// # Parameters
  ///
  /// * `msg` - The protocol message to process
  /// * `payload` - Optional payload data associated with the message,
  ///               typically present for content-bearing messages like broadcasts
  /// * `state` - The current connection state
  ///
  /// # Returns
  ///
  /// * `Ok(Some(new_state))` - Processing succeeded and the connection should
  ///                           transition to the new state
  /// * `Ok(None)` - Processing succeeded with no state change
  /// * `Err(e)` - An error occurred during processing
  ///
  /// # Errors
  ///
  /// Errors are typically wrapped in `ConnError` to provide structured error
  /// information to the client, including whether the error is recoverable.
  async fn dispatch_message(
    &mut self,
    msg: Message,
    payload: Option<PoolBuffer>,
    state: State,
  ) -> anyhow::Result<Option<State>>;

  /// Initializes the dispatcher.
  ///
  /// This method is called once when a new connection is established, before
  /// any messages are processed. It allows the dispatcher to set up its initial
  /// state, register with other system components, or perform other setup tasks.
  ///
  /// # Returns
  ///
  /// * `Ok(())` - Bootstrapping succeeded
  /// * `Err(e)` - An error occurred during bootstrapping
  ///
  /// # Errors
  ///
  /// If bootstrapping fails, the connection will be closed immediately.
  async fn bootstrap(&mut self) -> anyhow::Result<()>;

  /// Cleans up the dispatcher's resources.
  ///
  /// This method is called when a connection is closed, either due to a client
  /// disconnect, an error, or server shutdown. It allows the dispatcher to
  /// clean up any resources, unregister from other components, or perform
  /// other teardown tasks.
  ///
  /// # Returns
  ///
  /// * `Ok(())` - Shutdown succeeded
  /// * `Err(e)` - An error occurred during shutdown
  ///
  /// # Errors
  ///
  /// Errors during shutdown are logged but generally do not affect the
  /// connection close process.
  async fn shutdown(&mut self) -> anyhow::Result<()>;
}

/// A factory for creating new `Dispatcher` instances.
///
/// This trait separates the creation of dispatchers from their usage,
/// allowing for dependency injection and better testability.
///
/// Implementors typically hold configuration data and references to
/// shared resources that new dispatchers will need access to.
#[async_trait]
pub trait DispatcherFactory<D: Dispatcher>: Clone + Send + Sync + 'static {
  /// Creates a new instance of a dispatcher factory.
  ///
  /// This method is called whenever a new connection is established
  /// and needs a dispatcher to handle its messages.
  ///
  /// # Returns
  ///
  /// A dispatcher instance, ready to handle messages for the new connection.
  async fn create(&mut self, handler: usize, tx: ConnTx) -> D;

  /// Bootstraps the dispatcher factory with initial configuration and resources.
  ///
  /// This method is called once during initialization to set up any shared
  /// resources, establish connections, or perform other one-time setup tasks
  /// that the factory needs before it can start creating dispatchers.
  ///
  /// Implementations might use this to:
  /// * Initialize connection pools
  /// * Set up background tasks
  /// * Load configuration from external sources
  /// * Establish connections to external services
  ///
  /// # Returns
  ///
  /// * `Ok(())` - Bootstrap succeeded and the factory is ready to create dispatchers
  /// * `Err(e)` - An error occurred during bootstrap, preventing factory initialization
  async fn bootstrap(&mut self) -> anyhow::Result<()>;

  /// Shuts down the dispatcher factory and cleans up resources.
  ///
  /// This method is called when the connection manager is shutting down,
  /// allowing the factory to clean up any resources it holds.
  ///
  /// # Returns
  ///
  /// * `Ok(())` - Shutdown succeeded
  /// * `Err(e)` - An error occurred during shutdown
  async fn shutdown(&mut self) -> anyhow::Result<()>;
}

/// The connection configuration.
#[derive(Debug, Default)]
pub struct Config {
  /// The maximum number of connections that the manager can handle.
  pub max_connections: u32,

  /// The maximum message size allowed.
  pub max_message_size: u32,

  /// The maximum payload size allowed.
  pub max_payload_size: u32,

  /// The timeout for the connection phase.
  pub connect_timeout: Duration,

  /// The timeout for the authentication phase.
  pub authenticate_timeout: Duration,

  /// The timeout for reading a broadcast payload.
  pub payload_read_timeout: Duration,

  /// Total memory budget in bytes for the payload buffer pool.
  /// The pool will allocate buffers of varying sizes up to this total.
  pub payload_pool_memory_budget: u64,

  /// The maximum number of outbound messages that can be enqueued
  /// before disconnecting the client.
  pub outbound_message_queue_size: u32,

  /// The timeout for the request.
  pub request_timeout: Duration,

  /// The connection maximum number of inflight requests.
  pub max_inflight_requests: u32,

  /// The maximum number of bytes that can be read per second.
  pub rate_limit: u32,
}

/// The connection manager inner state.
struct ConnManagerInner<ST: Service> {
  /// The connection manager configuration.
  config: Arc<Config>,

  /// The next handler ID to assign.
  next_handler: Arc<AtomicUsize>,

  /// The number of active connections.
  active_connections: Arc<AtomicU32>,

  /// The message buffer pool.
  message_buffer_pool: Pool,

  /// The payload buffer pool.
  payload_buffer_pool: BucketedPool,

  /// Connection task tracker.
  task_tracker: TaskTracker,

  /// The shutdown cancellation token.
  shutdown_token: CancellationToken,

  /// Phantom data for the service type.
  _phantom: PhantomData<ST>,
}

impl<ST: Service> std::fmt::Debug for ConnManagerInner<ST> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ConnManagerInner")
      .field("config", &self.config)
      .field("next_handler", &self.next_handler)
      .field("active_connections", &self.active_connections)
      .field("message_buffer_pool", &self.message_buffer_pool)
      .field("payload_buffer_pool", &self.payload_buffer_pool)
      .field("task_tracker", &self.task_tracker)
      .field("shutdown_token", &self.shutdown_token)
      .finish()
  }
}

/// The connection manager.
#[derive(Debug)]
pub struct ConnManager<ST: Service>(Arc<RwLock<ConnManagerInner<ST>>>);

// ===== impl ConnManager =====

impl<ST: Service> Clone for ConnManager<ST> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<ST: Service> ConnManager<ST> {
  /// Creates a new connection manager.
  pub fn new(config: impl Into<Config>) -> Self {
    let conn_cfg = config.into();

    let max_connections = conn_cfg.max_connections as usize;

    // Account for the fact that each connection has two message buffers (read and write)
    let max_message_pool_buffers = max_connections * 2 + MAX_IOVS;

    let max_payload_buffers_per_bucket = max_connections + (max_connections * MAX_IOVS);

    // Create message buffer pool
    let message_buffer_pool = Pool::new(max_message_pool_buffers, conn_cfg.max_message_size as usize);

    // Create the bucketed pool with the configured memory budget.
    // The pool will distribute the budget across different size buckets.
    let payload_buffer_pool = BucketedPool::new_with_memory_budget(
      256,                                          // min buffer size
      conn_cfg.max_payload_size as usize,           // max buffer size
      conn_cfg.payload_pool_memory_budget as usize, // total memory budget
      max_payload_buffers_per_bucket,               // max buffers per bucket
      2,                                            // 2x growth between buckets
      0.5,                                          // 50% decay
    );

    let task_tracker = TaskTracker::new();
    let shutdown_token = CancellationToken::new();

    let inner = ConnManagerInner {
      config: Arc::new(conn_cfg),
      next_handler: Arc::new(AtomicUsize::new(1)),
      active_connections: Arc::new(AtomicU32::new(0)),
      message_buffer_pool,
      payload_buffer_pool,
      task_tracker,
      shutdown_token,
      _phantom: PhantomData,
    };

    Self(Arc::new(RwLock::new(inner)))
  }

  /// Bootstraps the connection manager.
  pub async fn bootstrap(&self) -> anyhow::Result<()> {
    let inner = self.0.read().await;

    info!(max_conns = inner.config.max_connections, service_type = ST::NAME, "connection manager started");

    Ok(())
  }

  /// Shuts down the connection manager.
  pub async fn shutdown(&self) -> anyhow::Result<()> {
    let inner = self.0.read().await;

    // Notify the shutdown to all active connections.
    inner.shutdown_token.cancel();

    // Wait for all connections to finish.
    inner.task_tracker.close();
    inner.task_tracker.wait().await;

    info!(service_type = ST::NAME, "connection manager stopped");

    Ok(())
  }

  pub async fn run_connection<S, D: Dispatcher, DF: DispatcherFactory<D>>(
    &self,
    mut stream: S,
    mut dispatcher_factory: DF,
  ) where
    S: AsyncRead + AsyncWrite + Unpin,
  {
    let inner = self.0.read().await;

    let config = inner.config.clone();
    let message_buffer_pool = inner.message_buffer_pool.clone();
    let payload_buffer_pool = inner.payload_buffer_pool.clone();
    let next_handler = inner.next_handler.clone();
    let active_connections = inner.active_connections.clone();
    let shutdown_token = inner.shutdown_token.clone();

    drop(inner);

    // Check if we've reached the maximum number of connections
    let current_connections = active_connections.fetch_add(1, Ordering::SeqCst);

    if current_connections >= config.max_connections {
      active_connections.fetch_sub(1, Ordering::SeqCst);

      let _ = stream.write_all(SERVER_OVERLOADED_ERROR).await;
      let _ = stream.flush().await;
      let _ = stream.shutdown().await;

      let max_conns = config.max_connections;
      warn!(max_conns, service_type = ST::NAME, "max connections limit reached");

      return;
    }

    // Create a new connection.
    let send_msg_channel_size = config.outbound_message_queue_size as usize;

    let (send_msg_tx, send_msg_rx) = channel(send_msg_channel_size);
    let (close_tx, close_rx) = channel(1);

    // Assign a unique handler
    let handler = next_handler.fetch_add(1, Ordering::SeqCst);
    let tx = ConnTx { send_msg_tx, close_tx };

    let dispatcher = dispatcher_factory.create(handler, tx.clone()).await;

    let mut conn = Box::new(Conn {
      handler,
      config: config.clone(),
      state: State::Connecting,
      dispatcher: Rc::new(UnsafeCell::new(dispatcher)),
      task_tracker: TaskTracker::new(),
      cancellation_token: CancellationToken::new(),
      activity_counter: Arc::new(AtomicU64::new(0)),
      pong_notifier: None,
      scheduled_task: None,
      inflight_requests: Arc::new(AtomicU32::new(0)),
      tx,
    });

    conn.schedule_timeout(config.connect_timeout, Some(StringAtom::from("connection timeout")));

    trace!(handler = handler, service_type = ST::NAME, "connection registered");

    // Run loop until the connection is closed.
    let payload_read_timeout = config.payload_read_timeout;

    let max_payload_size = config.max_payload_size as usize;

    let rate_limit = config.rate_limit;

    match Conn::<D>::run::<S, ST>(
      stream,
      &mut conn,
      handler,
      send_msg_rx,
      close_rx,
      shutdown_token,
      message_buffer_pool,
      payload_buffer_pool,
      payload_read_timeout,
      max_payload_size,
      rate_limit,
    )
    .await
    {
      Ok(_) => {},
      Err(e) => {
        warn!(handler = handler, service_type = ST::NAME, "connection error: {}", e.to_string());
      },
    }

    // Decrement the active connections counter
    active_connections.fetch_sub(1, Ordering::SeqCst);

    trace!(handler = handler, service_type = ST::NAME, "connection deregistered");
  }
}

/// A client connection.
#[derive(Debug)]
pub struct Conn<D: Dispatcher> {
  /// The connection configuration.
  config: Arc<Config>,

  /// The connection handler.
  handler: usize,

  /// The connection state.
  state: State,

  /// The connection dispatcher.
  dispatcher: Rc<UnsafeCell<D>>,

  /// The transmitter channels.
  tx: ConnTx,

  /// Current number of inflight requests.
  inflight_requests: Arc<AtomicU32>,

  // Increments on every received message
  activity_counter: Arc<AtomicU64>,

  // Channel to send PONG notifications to ping loop
  pong_notifier: Option<tokio::sync::mpsc::Sender<u32>>,

  /// Current scheduled task (ping or timeout).
  scheduled_task: Option<tokio::task::JoinHandle<()>>,

  /// Track tasks associated with connection requests.
  task_tracker: TaskTracker,

  /// Token used to signal request cancellation.
  cancellation_token: CancellationToken,
}

// ===== impl Conn =====

impl<D: Dispatcher> Conn<D> {
  async fn dispatch_message<ST: Service>(&mut self, msg: Message, payload: Option<PoolBuffer>) -> anyhow::Result<()> {
    let dispatcher = self.dispatcher.clone();

    match self.state {
      State::Authenticated { .. } => {
        // Handle pong message
        if let Message::Pong(params) = &msg {
          if let Some(notifier) = &self.pong_notifier {
            notifier.send(params.id).await?;
          }
          return Ok(());
        }

        // Submit the request to the dispatcher asynchronously.
        let handler = self.handler;
        let state = self.state;
        let tx = self.tx.clone();

        self.submit_request::<_, ST>(async move {
          let dispatcher_ref = unsafe { &mut *dispatcher.get() };

          match dispatcher_ref.dispatch_message(msg, payload, state).await {
            Ok(_) => {},
            Err(e) => {
              Self::notify_error::<ST>(e, tx, handler)?;
            },
          }
          Ok(())
        })?;

        // Increment activity counter
        self.activity_counter.fetch_add(1, Ordering::Relaxed);
      },
      _ => {
        let dispatcher_ref = unsafe { &mut *dispatcher.get() };

        match dispatcher_ref.dispatch_message(msg, payload, self.state).await {
          Ok(Some(new_state)) => {
            assert!(new_state >= self.state, "invalid state transition");

            let old_state = self.state;
            self.state = new_state;

            // Schedule timeout according to state transition.
            if old_state != self.state {
              // Cancel any previous timeout.
              self.cancel_scheduled_task();

              match self.state {
                State::Connected => {
                  self.schedule_timeout(
                    self.config.authenticate_timeout,
                    Some(StringAtom::from("authentication timeout")),
                  );
                },
                State::Authenticated { heartbeat_interval } => {
                  self.run_ping_loop::<ST>(heartbeat_interval);
                },
                _ => {},
              }
            }
          },
          Ok(None) => {},
          Err(e) => {
            Self::notify_error::<ST>(e, self.tx.clone(), self.handler)?;
          },
        }
      },
    }

    Ok(())
  }

  fn submit_request<F, ST>(&mut self, future: F) -> anyhow::Result<()>
  where
    F: Future<Output = anyhow::Result<()>> + 'static,
    ST: Service,
  {
    // First check if the maximum number of inflight requests has been reached,
    // and if not, increment the counter.
    let max_inflight_requests = self.config.max_inflight_requests;
    let inflight_requests = self.inflight_requests.clone();

    let inc_res = inflight_requests.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
      if v <= max_inflight_requests { Some(v + 1) } else { None }
    });
    if inc_res.is_err() {
      return Err(
        narwhal_protocol::Error {
          id: None,
          reason: PolicyViolation,
          detail: Some(StringAtom::from("max inflight requests reached")),
        }
        .into(),
      );
    }

    // Spawn the request task.
    let task_tracker = self.task_tracker.clone();

    let handler = self.handler;
    let request_timeout = self.config.request_timeout;

    let tx = self.tx.clone();

    let cancellation_token = self.cancellation_token.clone();

    task_tracker.spawn_local(async move {
      tokio::select! {
        res = future => {
            if let Err(e) = res && let Err(e) = Self::notify_error::<ST>(e, tx, handler) {
                warn!(handler = handler, service_type = ST::NAME, "failed to notify request error: {}", e.to_string());
            }
        },
        _ = tokio::time::sleep(request_timeout) => {
          error!(handler = handler, service_type = ST::NAME, "request timeout");
        },
        _ = cancellation_token.cancelled() => {
          trace!(handler = handler, service_type = ST::NAME, "request cancelled");
        },
      }

      // Decrement the inflight requests counter.
      inflight_requests.fetch_sub(1, Ordering::Relaxed);
    });

    Ok(())
  }

  /// Schedules a timeout task.
  fn schedule_timeout(&mut self, timeout: Duration, detail: Option<StringAtom>) {
    let tx = self.tx.clone();

    let timeout_task = tokio::task::spawn_local(async move {
      tokio::time::sleep(timeout).await;

      tx.close(Message::Error(ErrorParameters { id: None, reason: Timeout.into(), detail }));
    });
    self.scheduled_task = Some(timeout_task);
  }

  /// Runs the ping/pong monitoring loop
  fn run_ping_loop<ST: Service>(&mut self, heartbeat_interval: Duration) {
    let tx = self.tx.clone();
    let handler = self.handler;

    // A 3x heartbeat interval is used for the timeout.
    let heartbeat_timeout = heartbeat_interval * 3;

    let activity_counter = self.activity_counter.clone();

    // Create PONG notification channel
    let (pong_tx, mut pong_rx) = tokio::sync::mpsc::channel::<u32>(1);
    self.pong_notifier = Some(pong_tx);

    let ping_task = tokio::task::spawn_local(async move {
      let mut last_check_counter = activity_counter.load(Ordering::Relaxed);

      loop {
        tokio::time::sleep(heartbeat_interval).await;

        let current_counter = activity_counter.load(Ordering::Relaxed);

        if current_counter != last_check_counter {
          last_check_counter = current_counter;
          continue; // Activity detected, skip ping
        }

        let ping_id: u32 = random();
        tx.send_message(Message::Ping(PingParameters { id: ping_id }));
        trace!(id = ping_id, handler, service_type = ST::NAME, "sent ping");

        // Wait for PONG or timeout
        match tokio::time::timeout(heartbeat_timeout, pong_rx.recv()).await {
          Ok(Some(pong_id)) if pong_id == ping_id => {
            trace!(id = ping_id, "pong received");

            last_check_counter = activity_counter.load(Ordering::Relaxed);
          },

          // Wrong pong id
          Ok(Some(_)) => {
            tx.close(Message::Error(ErrorParameters {
              id: None,
              reason: BadRequest.into(),
              detail: Some(StringAtom::from("wrong pong id")),
            }));
            break;
          },

          // Connection closing...
          Ok(None) => {
            break;
          },

          // Timeout - no PONG received
          Err(_) => {
            tx.close(Message::Error(ErrorParameters {
              id: None,
              reason: Timeout.into(),
              detail: Some(StringAtom::from("ping timeout")),
            }));
            break;
          },
        }
      }
    });

    self.scheduled_task = Some(ping_task);
  }

  /// Cancels currently scheduled task.
  fn cancel_scheduled_task(&mut self) {
    if let Some(task) = self.scheduled_task.take() {
      task.abort()
    }
  }

  /// Bootstraps the connection.
  async fn bootstrap(&mut self) -> anyhow::Result<()> {
    unsafe { &mut *self.dispatcher.get() }.bootstrap().await?;
    Ok(())
  }

  /// Shuts down the connection.
  async fn shutdown(&mut self) -> anyhow::Result<()> {
    // Cancel any pending ping or timeout task.
    self.cancel_scheduled_task();

    // Signal cancellation and wait for all request tasks to finish.
    self.cancellation_token.cancel();

    self.task_tracker.close();
    self.task_tracker.wait().await;

    // Shutdown the dispatcher.
    unsafe { &mut *self.dispatcher.get() }.shutdown().await?;

    Ok(())
  }

  fn notify_error<ST: Service>(e: anyhow::Error, tx: ConnTx, handler: usize) -> anyhow::Result<()> {
    // Notify the client about the error, or disconnect the connection in case it's an internal
    // or non-recoverable error.
    if let Some(conn_err) = e.downcast_ref::<narwhal_protocol::Error>() {
      if conn_err.is_recoverable() {
        tx.send_message(conn_err.into());
      } else {
        tx.close(conn_err.into());
      }
    } else {
      error!(handler = handler, service_type = ST::NAME, "internal server error: {}", e.to_string());
      tx.close(Message::Error(ErrorParameters { id: None, reason: InternalServerError.into(), detail: None }));
    }
    Ok(())
  }

  #[allow(clippy::too_many_arguments)]
  async fn run<T, ST>(
    stream: T,
    conn: &mut Conn<D>,
    handler: usize,
    send_msg_rx: Receiver<(Message, Option<PoolBuffer>)>,
    close_rx: Receiver<Message>,
    shutdown_token: CancellationToken,
    message_buffer_pool: Pool,
    payload_buffer_pool: BucketedPool,
    payload_read_timeout: Duration,
    max_payload_size: usize,
    rate_limit: u32,
  ) -> anyhow::Result<()>
  where
    T: AsyncRead + AsyncWrite + Unpin,
    ST: Service,
  {
    let (reader, mut writer) = tokio::io::split(stream);

    // Bootstrap the connection.
    conn.bootstrap().await?;

    let loop_result = async {
      Self::run_connection_loop::<_, _, ST>(
        reader,
        &mut writer,
        conn,
        handler,
        send_msg_rx,
        close_rx,
        &shutdown_token,
        message_buffer_pool.clone(),
        payload_buffer_pool.clone(),
        payload_read_timeout,
        max_payload_size,
        rate_limit,
      )
      .await?;

      Ok::<(), anyhow::Error>(())
    }
    .await;

    // Shutdown the connection.
    let shutdown_result = conn.shutdown().await;

    match writer.shutdown().await {
      Ok(_) => {},
      Err(e) => {
        // Ignore expected socket disconnection errors that occur when client disconnects abruptly
        use std::io::ErrorKind::*;
        match e.kind() {
          NotConnected | BrokenPipe | ConnectionAborted | ConnectionReset | UnexpectedEof => {},
          _ => return Err(e.into()),
        }
      },
    }

    // Return any error from the main loop first, then any error from shutdown.
    loop_result?;
    shutdown_result?;

    Ok(())
  }

  #[allow(clippy::too_many_arguments)]
  async fn run_connection_loop<T, W, ST>(
    reader: ReadHalf<T>,
    writer: &mut W,
    conn: &mut Conn<D>,
    handler: usize,
    mut send_msg_rx: Receiver<(Message, Option<PoolBuffer>)>,
    mut close_rx: Receiver<Message>,
    shutdown_token: &CancellationToken,
    message_buffer_pool: Pool,
    payload_buffer_pool: BucketedPool,
    payload_read_timeout: Duration,
    max_payload_size: usize,
    rate_limit: u32,
  ) -> anyhow::Result<()>
  where
    T: AsyncRead + AsyncWrite + Unpin,
    W: AsyncWrite + Unpin,
    ST: Service,
  {
    // Initialize stream reader
    let read_pool_buffer = message_buffer_pool.acquire_buffer().await;
    let mut stream_reader = StreamReader::with_pool_buffer(reader, read_pool_buffer);

    // Initialize connection loop state
    let mut rate_limit_counter = 0;
    let mut rate_limit_last_check = tokio::time::Instant::now();

    let mut message_buffers_batch: Vec<PoolBuffer> = Vec::with_capacity(MAX_IOVS);
    let mut payload_buffers_batch: Vec<Option<PoolBuffer>> = Vec::with_capacity(MAX_IOVS);

    let mut iovs = vec![IoSlice::new(&[]); MAX_IOVS * 3].into_boxed_slice();

    let cancelled = shutdown_token.cancelled();
    tokio::pin!(cancelled);

    'connection_loop: loop {
      tokio::select! {
        // Read the next line from the stream.
        res = {
          stream_reader.next()
        } => {
          match res {
            Ok(true) => {
              let line_bytes = stream_reader.get_line().unwrap();

              let message_length = line_bytes.len() as u32;

              // Deserialize the message and handle it.
              match deserialize(Cursor::new(line_bytes)) {
                Ok(msg) => {
                  // Dispatch the message to the connection.
                  let mut payload_opt: Option<PoolBuffer> = None;

                  // Check if the message has an associated payload, and if so,
                  // read it from the connection.
                  let mut payload_length: u32 = 0;

                  if let Some(payload_info) = msg.payload_info() {
                    if payload_info.length > max_payload_size {
                      let err_message = Message::Error(ErrorParameters{id: payload_info.id, reason: PolicyViolation.into(), detail: Some("payload too large".into())});
                      Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await).await?;
                      break 'connection_loop;
                    }
                    payload_length = payload_info.length as u32;

                    let mut pool_buff = payload_buffer_pool.acquire_buffer(payload_info.length).await.unwrap();

                    let payload = &mut pool_buff.as_mut_slice()[..payload_info.length];

                    match tokio::time::timeout(payload_read_timeout, Self::read_payload(payload, &mut stream_reader, payload_info.id)).await {
                      Ok(res) => {
                        match res {
                          Ok(_) => {
                            payload_opt = Some(pool_buff.freeze(payload_info.length));
                          },
                          Err(e) => {
                            let err_message: Message = {
                                if let Some(e) = e.downcast_ref::<narwhal_protocol::Error>() {
                                    e.into()
                                } else {
                                    warn!(handler = handler, service_type = ST::NAME, "failed to read payload: {}", e);
                                    Message::Error(ErrorParameters{id: None, reason: InternalServerError.into(), detail: None})
                                }
                            };

                            Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await).await?;
                            break 'connection_loop;
                          },
                        }
                      },
                      Err(_) => {
                        let err_message = Message::Error(ErrorParameters{id: None, reason: Timeout.into(), detail: Some("payload read timeout".into())});
                        Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await).await?;
                        break 'connection_loop;
                      },
                    }
                  }

                  // Check if the rate limit is exceeded.
                  if rate_limit > 0 {
                    let now = tokio::time::Instant::now();
                    let elapsed = now.duration_since(rate_limit_last_check);
                    if elapsed.as_secs() > 1 {
                        rate_limit_counter = 0;
                        rate_limit_last_check = now;
                    }

                    rate_limit_counter += message_length + payload_length;

                    if rate_limit_counter > rate_limit {
                      let err_message = Message::Error(ErrorParameters{id: msg.correlation_id(), reason: PolicyViolation.into(), detail: Some("rate limit exceeded".into())});
                      Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await).await?;
                      break 'connection_loop;
                    }
                  }

                  // Dispatch the message to the connection handler.
                  match conn.dispatch_message::<ST>(msg, payload_opt).await {
                    Ok(_) => {},
                    Err(e) => {
                      warn!(handler = handler, service_type = ST::NAME, "failed to dispatch message: {}", e.to_string());
                      break 'connection_loop;
                    },
                  }
                },
                Err(e) => {
                  let err_detail = format!("{}", e);
                  let err_message = Message::Error(ErrorParameters{id: None, reason: BadRequest.into(), detail: Some(StringAtom::from(err_detail))});
                  Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await).await?;
                  break 'connection_loop;
                },
              }
            },
            Ok(false) => {
              // Stream closed by the client.
              trace!(handler = handler, service_type = ST::NAME, "connection closed by peer");
              break 'connection_loop;
            }
            Err(e) => {
              match e {
                StreamReaderError::MaxLineLengthExceeded => {
                  let err_message = Message::Error(ErrorParameters{ id: None, reason: PolicyViolation.into(), detail: Some("max message size exceeded".into()) });
                  Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await).await?;
                  break 'connection_loop;
                },
                StreamReaderError::IoError(e) => {
                  if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    error!(handler = handler, service_type = ST::NAME, "failed to read from connection: {}", e.to_string());
                  }
                  break 'connection_loop;
                },
              }
            }
          }
        },

        // Write the message to the stream.
        res = send_msg_rx.recv() => {
          const MESSAGE_CHANNEL_CLOSED_LOG: &str = "message channel closed";

          match res {
            Some((message, payload_opt)) => {
                let message_buff = Self::serialize_message(&message, message_buffer_pool.acquire_buffer().await)?;

                message_buffers_batch.push(message_buff);
                payload_buffers_batch.push(payload_opt);
            }
            None => {
              error!(handler = handler, service_type = ST::NAME, MESSAGE_CHANNEL_CLOSED_LOG);
              break 'connection_loop;
            }
          };

          loop {
            if message_buffers_batch.len() == message_buffers_batch.capacity() {
                break;
            }

            match send_msg_rx.try_recv() {
              Ok((message, payload_opt)) => {
                  let message_buff = Self::serialize_message(&message, message_buffer_pool.acquire_buffer().await)?;

                  message_buffers_batch.push(message_buff);
                  payload_buffers_batch.push(payload_opt);
              },
              Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
              Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                error!(handler = handler, service_type = ST::NAME, MESSAGE_CHANNEL_CLOSED_LOG);
                break 'connection_loop;
              }
            }
          }

          let iovs_count = Self::prepare_iovs(
              message_buffers_batch.as_ptr(),
              payload_buffers_batch.as_ptr(),
              message_buffers_batch.len(),
              iovs.as_mut_ptr(),
          );

          Self::write_iovs(&mut iovs[..iovs_count], writer).await?;

          message_buffer_pool.release_buffers(&mut message_buffers_batch);
          payload_buffers_batch.clear();
        },

        // Close the connection.
        res = close_rx.recv() => {
          let err_message = res.unwrap();
          Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await).await?;
          trace!(handler = handler, service_type = ST::NAME, "closed connection");
          break 'connection_loop;
        },

        // Close the connection on shutdown.
        _ = &mut cancelled => {
          let err_message = Message::Error(ErrorParameters{id: None, reason: ServerShuttingDown.into(), detail: None});
          Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await).await?;
          trace!(handler = handler, service_type = ST::NAME, "closed connection");
          break 'connection_loop;
        },
      }
    }

    Ok(())
  }

  async fn write_message<W>(
    message: &Message,
    payload_opt: Option<PoolBuffer>,
    writer: &mut W,
    message_buffer: MutablePoolBuffer,
  ) -> anyhow::Result<()>
  where
    W: AsyncWrite + Unpin,
  {
    let message_buff = Self::serialize_message(message, message_buffer)?;

    let message_buffer_batch = [message_buff];
    let payload_buffer_batch = [payload_opt];

    let mut iovs = [IoSlice::new(&[]); 3];
    let iovs_count =
      Self::prepare_iovs(message_buffer_batch.as_ptr(), payload_buffer_batch.as_ptr(), 1, iovs.as_mut_ptr());

    Self::write_iovs(&mut iovs[..iovs_count], writer).await?;

    Ok(())
  }

  fn prepare_iovs<'a>(
    message_buffer_batch_ptr: *const PoolBuffer,
    payload_buffer_batch_ptr: *const Option<PoolBuffer>,
    batch_len: usize,
    iovs: *mut IoSlice<'a>,
  ) -> usize {
    let mut iovs_count = 0;

    unsafe {
      for i in 0..batch_len {
        let message_buffer_batch_item = &*message_buffer_batch_ptr.add(i);
        let payload_buffer_batch_item = &*payload_buffer_batch_ptr.add(i);

        let message_buff = &message_buffer_batch_item;
        let payload_opt = &payload_buffer_batch_item;

        *iovs.add(iovs_count) = IoSlice::new(message_buff.as_slice());
        iovs_count += 1;

        if let Some(payload) = payload_opt {
          *iovs.add(iovs_count) = IoSlice::new(payload.as_slice());
          iovs_count += 1;
          *iovs.add(iovs_count) = IoSlice::new(b"\n");
          iovs_count += 1;
        }
      }
    }

    iovs_count
  }

  async fn write_iovs<W>(iovs: &mut [IoSlice<'_>], writer: &mut W) -> anyhow::Result<()>
  where
    W: AsyncWrite + Unpin,
  {
    write_all_vectored(iovs, writer).await?;
    writer.flush().await?;

    Ok(())
  }

  fn serialize_message(message: &Message, message_buffer: MutablePoolBuffer) -> anyhow::Result<PoolBuffer> {
    Self::serialize_message_inner(message, message_buffer, true)
      .map_err(|e| anyhow!("failed to serialize message: {}", e))
  }

  fn serialize_message_inner(
    message: &Message,
    mut message_buffer: MutablePoolBuffer,
    handle_too_large: bool,
  ) -> anyhow::Result<PoolBuffer> {
    let write_buffer = message_buffer.as_mut_slice();

    match serialize(message, write_buffer) {
      Ok(n) => Ok(message_buffer.freeze(n)),
      Err(SerializeError::MessageTooLarge) if handle_too_large => match message.correlation_id() {
        Some(correlation_id) => {
          let error_msg = narwhal_protocol::Error::new(ResponseTooLarge)
            .with_id(correlation_id)
            .with_detail(StringAtom::from("response exceeded maximum message size"));

          Self::serialize_message_inner(&error_msg.into(), message_buffer, false)
        },
        None => Err(anyhow!(SerializeError::MessageTooLarge)),
      },
      Err(e) => Err(e.into()),
    }
  }

  async fn read_payload<T>(
    buffer: &mut [u8],
    stream_reader: &mut StreamReader<ReadHalf<T>>,
    correlation_id: Option<u32>,
  ) -> anyhow::Result<()>
  where
    T: AsyncRead + Unpin,
  {
    stream_reader.read_raw(buffer).await?;

    // Read last byte, and ensure it's a newline.
    let mut cr: [u8; 1] = [0; 1];
    stream_reader.read_raw(&mut cr).await?;

    // Verify it's actually a newline
    if cr[0] != b'\n' {
      let mut error = narwhal_protocol::Error::new(BadRequest).with_detail(StringAtom::from("invalid payload format"));
      if let Some(id) = correlation_id {
        error = error.with_id(id);
      }
      return Err(error.into());
    }

    Ok(())
  }
}

/// The connection transmitter.
#[derive(Clone, Debug)]
pub struct ConnTx {
  /// The send message channel.
  send_msg_tx: Sender<(Message, Option<PoolBuffer>)>,

  /// The connection close channel.
  close_tx: Sender<Message>,
}

// ===== impl ConnTx =====

impl ConnTx {
  /// Creates a new connection transmitter.
  ///
  /// # Arguments
  ///
  /// * `send_msg_tx` - The send message channel.
  /// * `close_tx` - The connection close channel.
  pub fn new(send_msg_tx: Sender<(Message, Option<PoolBuffer>)>, close_tx: Sender<Message>) -> Self {
    Self { send_msg_tx, close_tx }
  }

  /// Sends a message without a payload to the connection.
  ///
  /// # Arguments
  ///
  /// * `message` - The message to send.
  pub fn send_message(&self, message: Message) {
    self.send_message_with_payload(message, None);
  }

  /// Sends a message with an optional payload to the connection.
  ///
  /// # Arguments
  ///
  /// * `message` - The message to send.
  /// * `payload_opt` - An optional payload to send with the message.
  ///
  /// # Panics
  ///
  /// Panics if the message is not a `Message::Message` or `Message::S2mForwardPayloadAck`.
  pub fn send_message_with_payload(&self, message: Message, payload_opt: Option<PoolBuffer>) {
    assert!(
      payload_opt.is_none()
        || matches!(
          message,
          Message::Message { .. } | Message::ModDirect { .. } | Message::S2mForwardBroadcastPayloadAck { .. }
        ),
      "a Message::Message, Message::ModDirect or Message::S2mForwardBroadcastPayloadAck variant is expected when payload is present"
    );

    match self.send_msg_tx.try_send((message, payload_opt)) {
      Ok(_) => {},
      Err(_) => {
        // The send channel is full, so most likely the client is either not reading
        // or is too slow to process incoming messages. In this case, we close the connection.
        self.close(Message::Error(ErrorParameters { id: None, reason: OutboundQueueIsFull.into(), detail: None }));
      },
    }
  }

  /// Closes the connection.
  ///
  /// # Arguments
  ///
  /// * `message` - The message to send to the connection before closing.
  ///
  /// # Panics
  ///
  /// Panics if the message is not a `Message::Error`.
  pub fn close(&self, message: Message) {
    assert!(matches!(message, Message::Error { .. }), "a Message::Error message is expected");
    let _ = self.close_tx.try_send(message);
  }
}

const CONN_QUEUE_SIZE: usize = 1024;

/// A trait representing a connection stream that can be read from and written to.
pub trait ConnStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

impl<T> ConnStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

/// A type alias for an async function that provides a connection stream when invoked.
pub type ConnProvider<S> =
  Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = anyhow::Result<S>> + Send>> + Send + 'static>;

/// A pool of connection workers that distributes streams across multiple workers.
///
/// The pool manages multiple `ConnWorker` instances, each bound to a different CPU core.
/// When streams are submitted, they are automatically routed to the worker with the
/// fewest active streams, providing automatic load balancing.
///
/// # Type Parameters
///
/// * `S` - The connection stream type (must implement `ConnStream`)
/// * `D` - The dispatcher type for handling messages
/// * `DF` - The dispatcher factory for creating dispatchers
/// * `ST` - The service type being served
pub struct ConnWorkerPool<S, D: Dispatcher, DF: DispatcherFactory<D>, ST: Service>
where
  S: ConnStream,
{
  /// The collection of workers in the pool.
  workers: Arc<[ConnWorker<S, D, DF, ST>]>,
}

impl<S, D: Dispatcher, DF: DispatcherFactory<D>, ST: Service> Clone for ConnWorkerPool<S, D, DF, ST>
where
  S: ConnStream,
{
  fn clone(&self) -> Self {
    Self { workers: self.workers.clone() }
  }
}

impl<S, D: Dispatcher, DF: DispatcherFactory<D>, ST: Service> ConnWorkerPool<S, D, DF, ST>
where
  S: ConnStream,
{
  /// Creates a new connection worker pool.
  ///
  /// Each worker is assigned to a different CPU core in a round-robin fashion based
  /// on the available cores on the system. If `worker_count` exceeds the number of
  /// available cores, workers will be assigned to cores cyclically.
  ///
  /// # Arguments
  ///
  /// * `worker_count` - The number of workers to create in the pool
  /// * `conn_manager` - The connection manager to use for processing streams
  /// * `dispatcher_factory` - The dispatcher factory to use for creating dispatchers
  ///
  /// # Returns
  ///
  /// Returns a `ConnWorkerPool` instance or an error if worker creation fails.
  pub fn new(worker_count: usize, conn_manager: ConnManager<ST>, dispatcher_factory: DF) -> anyhow::Result<Self> {
    // Get available CPU cores
    let core_ids = core_affinity::get_core_ids().ok_or_else(|| anyhow::anyhow!("failed to get core IDs"))?;

    if core_ids.is_empty() {
      return Err(anyhow::anyhow!("no CPU cores available"));
    }

    // Create workers, assigning each to a core in round-robin fashion
    let mut workers = Vec::with_capacity(worker_count);

    for i in 0..worker_count {
      let core_id = core_ids[i % core_ids.len()];
      let worker = ConnWorker::new(conn_manager.clone(), dispatcher_factory.clone(), core_id)?;
      workers.push(worker);
    }

    Ok(ConnWorkerPool { workers: workers.into() })
  }

  /// Submits a new stream provider to be processed by a worker.
  ///
  /// This method performs automatic load balancing by finding the worker with the
  /// lowest number of active streams and submitting the provided stream to that worker.
  ///
  /// If multiple workers have the same minimum load, the first one encountered is chosen.
  ///
  /// # Arguments
  ///
  /// * `stream_provider` - A function that provides the connection stream when invoked
  pub async fn submit<F, Fut>(&self, stream_provider: F)
  where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<S>> + Send + 'static,
  {
    if let Some(worker) = self.workers.iter().min_by_key(|w| w.active_streams()) {
      worker.submit(stream_provider).await;
    }
  }

  /// Returns the total number of active streams across all workers.
  ///
  /// # Returns
  ///
  /// The sum of active streams from all workers in the pool.
  pub fn active_streams(&self) -> usize {
    self.workers.iter().map(|w| w.active_streams()).sum()
  }

  /// Returns the number of workers in the pool.
  ///
  /// # Returns
  ///
  /// The number of workers as a `usize`.
  pub fn worker_count(&self) -> usize {
    self.workers.len()
  }
}

/// A worker that processes connection streams on a dedicated thread.
///
/// # Type Parameters
///
/// * `S` - The connection stream type (must implement `ConnStream`)
/// * `D` - The dispatcher type for handling messages
/// * `DF` - The dispatcher factory for creating dispatchers
/// * `ST` - The service type being served
#[derive(Debug)]
struct ConnWorker<S, D: Dispatcher, DF: DispatcherFactory<D>, ST: Service>
where
  S: ConnStream,
{
  /// Channel sender for submitting stream providers to the worker thread.
  tx: Option<tokio::sync::mpsc::Sender<ConnProvider<S>>>,

  /// Handle to the worker thread. Used to join the thread on shutdown.
  handle: Option<thread::JoinHandle<()>>,

  /// Atomic counter tracking the number of currently active stream tasks.
  active_streams: Arc<AtomicUsize>,

  /// Phantom data to maintain type parameters in the struct.
  _phantom: std::marker::PhantomData<(D, DF, ST)>,
}

impl<S, D: Dispatcher, DF: DispatcherFactory<D>, ST: Service> ConnWorker<S, D, DF, ST>
where
  S: ConnStream,
{
  /// Creates a new connection worker bound to a specific CPU core.
  ///
  /// This spawns a new OS thread with the specified CPU affinity and creates a
  /// single-threaded Tokio runtime on that thread. The worker will process streams
  /// submitted via the `submit()` method.
  ///
  /// # Arguments
  ///
  /// * `conn_manager` - The connection manager to use for processing streams
  /// * `dispatcher_factory` - The dispatcher factory to use for creating dispatchers
  /// * `affinity` - The CPU core ID to bind this worker thread to
  ///
  /// # Returns
  ///
  /// Returns a `ConnWorker` instance or an error if thread creation fails.
  ///
  /// # Example
  ///
  /// ```ignore
  /// let worker = ConnWorker::new(conn_manager, dispatcher_factory, CoreId { id: 0 })?;
  /// ```
  fn new(conn_manager: ConnManager<ST>, dispatcher_factory: DF, affinity: CoreId) -> anyhow::Result<Self> {
    let (tx, rx) = tokio::sync::mpsc::channel::<ConnProvider<S>>(CONN_QUEUE_SIZE);

    let active_streams = Arc::new(AtomicUsize::new(0));
    let active_streams_clone = active_streams.clone();

    let handle = thread::spawn(move || {
      let _ = core_affinity::set_for_current(affinity);

      Self::spawn_runtime(conn_manager, dispatcher_factory, rx, active_streams_clone);
    });

    Ok(ConnWorker { tx: Some(tx), handle: Some(handle), active_streams, _phantom: PhantomData })
  }

  /// Returns the current number of active stream tasks being processed.
  ///
  /// # Returns
  ///
  /// The number of active streams as a `usize`.
  fn active_streams(&self) -> usize {
    self.active_streams.load(Ordering::Relaxed)
  }

  /// Submits a new stream provider to be processed by the worker thread.
  ///
  /// If the worker has been stopped, the provider is silently ignored.
  ///
  /// # Arguments
  ///
  /// * `stream_provider` - A function that provides the connection stream when invoked
  async fn submit<F, Fut>(&self, stream_provider: F)
  where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<S>> + Send + 'static,
  {
    if let Some(tx) = &self.tx {
      let _ = tx.send(Box::new(|| Box::pin(stream_provider()))).await;
    }
  }

  /// Spawns a single-threaded async runtime associated to the worker thread.
  fn spawn_runtime(
    conn_manager: ConnManager<ST>,
    dispatcher_factory: DF,
    mut rx: tokio::sync::mpsc::Receiver<ConnProvider<S>>,
    active_streams: Arc<AtomicUsize>,
  ) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    rt.block_on(async move {
      let local = tokio::task::LocalSet::new();

      local
        .run_until(async move {
          while let Some(stream_provider) = rx.recv().await {
            let conn_manager = conn_manager.clone();
            let dispatcher_factory = dispatcher_factory.clone();

            let worker_active_conns = active_streams.clone();

            tokio::task::spawn_local(async move {
              let stream = match stream_provider().await {
                Ok(s) => s,
                Err(e) => {
                  warn!("connection provider failed: {}", e.to_string());
                  return;
                },
              };

              worker_active_conns.fetch_add(1, Ordering::Relaxed);

              conn_manager.run_connection(stream, dispatcher_factory).await;

              worker_active_conns.fetch_sub(1, Ordering::Relaxed);
            });
          }
        })
        .await;
    });
  }
}

impl<S, D: Dispatcher, DF: DispatcherFactory<D>, ST: Service> Drop for ConnWorker<S, D, DF, ST>
where
  S: ConnStream,
{
  fn drop(&mut self) {
    // Drop the sender to signal the worker thread to stop
    if let Some(tx) = self.tx.take() {
      drop(tx);

      // Wait for the worker thread to terminate
      if let Some(handle) = self.handle.take() {
        let _ = handle.join();
      }
    }
  }
}
