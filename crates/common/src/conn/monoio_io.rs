// SPDX-License-Identifier: BSD-3-Clause

use std::io::Cursor;
use std::time::Duration;

use anyhow::anyhow;
use async_channel::Receiver;
use futures::FutureExt;
use monoio::BufResult;
use monoio::buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, trace, warn};

use narwhal_protocol::ErrorReason::{BadRequest, InternalServerError, PolicyViolation, ServerShuttingDown, Timeout};
use narwhal_protocol::{ErrorParameters, Message, deserialize};
use narwhal_util::codec_monoio::{StreamReader, StreamReaderError};
use narwhal_util::pool::{BucketedPool, MutablePoolBuffer, Pool, PoolBuffer};
use narwhal_util::string_atom::StringAtom;

use async_channel::{Sender, TryRecvError, bounded};

use crate::runtime;
use crate::service::Service;

use super::{
  Conn, ConnRuntime, ConnTx, Dispatcher, DispatcherFactory, LocalStream, MAX_BUFFERS_PER_BATCH, READ_CHANNEL_CAPACITY,
  ReadResult, SERVER_OVERLOADED_ERROR,
};

impl<S: AsyncReadRent> AsyncReadRent for LocalStream<S> {
  async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
    // SAFETY: single-threaded and cooperative – only one task
    // executes at any point, so no concurrent mutable access can occur.
    let stream = unsafe { &mut *self.0.get() };
    stream.read(buf).await
  }

  async fn readv<B: IoVecBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
    let stream = unsafe { &mut *self.0.get() };
    stream.readv(buf).await
  }
}

impl<S: AsyncWriteRent> AsyncWriteRent for LocalStream<S> {
  async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
    let stream = unsafe { &mut *self.0.get() };
    stream.write(buf).await
  }

  async fn writev<B: IoVecBuf>(&mut self, buf: B) -> BufResult<usize, B> {
    let stream = unsafe { &mut *self.0.get() };
    stream.writev(buf).await
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    let stream = unsafe { &mut *self.0.get() };
    stream.flush().await
  }

  async fn shutdown(&mut self) -> std::io::Result<()> {
    let stream = unsafe { &mut *self.0.get() };
    stream.shutdown().await
  }
}

impl<ST: Service> ConnRuntime<ST> {
  /// Runs a single client connection to completion.
  pub async fn run_connection<S, D: Dispatcher, DF: DispatcherFactory<D>>(
    &self,
    mut stream: S,
    mut dispatcher_factory: DF,
  ) where
    S: AsyncReadRent + AsyncWriteRent + 'static,
  {
    // Assign a unique handler
    let handler = self.next_handler();
    let config = self.config();
    let message_buffer_pool = self.message_buffer_pool();
    let payload_buffer_pool = self.payload_buffer_pool();
    let newline_buffer = self.newline_buffer();
    let active_connections = self.active_connections();
    let shutdown_token = self.shutdown_token();
    let metrics = self.metrics();

    // Check if we've reached the maximum number of connections
    let current_connections = active_connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if current_connections >= config.max_connections {
      active_connections.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
      metrics.connections_rejected.inc();

      let _ = stream.write_all(SERVER_OVERLOADED_ERROR.to_vec()).await;
      let _ = stream.flush().await;
      let _ = stream.shutdown().await;

      let max_conns = config.max_connections;
      warn!(max_conns, service_type = ST::NAME, "max connections limit reached");

      return;
    }

    metrics.connections_active.inc();

    // Create a new connection.
    let send_msg_channel_size = config.outbound_message_queue_size as usize;

    let (send_msg_tx, send_msg_rx) = bounded(send_msg_channel_size);
    let (close_tx, close_rx) = bounded(1);

    let tx = ConnTx::new(send_msg_tx, close_tx, metrics.outbound_queue_full.clone());

    let dispatcher = dispatcher_factory.create(handler, tx.clone()).await;

    let mut conn = Conn::new(handler, config.clone(), dispatcher, tx);

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
      newline_buffer,
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
    active_connections.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    metrics.connections_active.dec();

    trace!(handler = handler, service_type = ST::NAME, "connection deregistered");
  }
}

impl<D: Dispatcher> Conn<D> {
  #[allow(clippy::too_many_arguments)]
  async fn run<S, ST>(
    stream: S,
    conn: &mut Conn<D>,
    handler: usize,
    send_msg_rx: Receiver<(Message, Option<PoolBuffer>)>,
    close_rx: Receiver<Message>,
    shutdown_token: CancellationToken,
    message_buffer_pool: Pool,
    payload_buffer_pool: BucketedPool,
    newline_buffer: PoolBuffer,
    payload_read_timeout: Duration,
    max_payload_size: usize,
    rate_limit: u32,
  ) -> anyhow::Result<()>
  where
    S: AsyncReadRent + AsyncWriteRent + 'static,
    ST: Service,
  {
    let local_stream = LocalStream::new(stream);

    let reader = local_stream.clone();
    let mut writer = local_stream;

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
        newline_buffer.clone(),
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

  /// Dedicated read loop spawned as a separate task.
  ///
  /// Sequentially reads from the stream, deserializes messages, reads payloads, and enforces rate limiting.
  /// Results are sent to the main connection loop to be processed.
  #[allow(clippy::too_many_arguments)]
  async fn run_read_loop<T>(
    mut stream_reader: StreamReader<LocalStream<T>>,
    read_tx: Sender<ReadResult>,
    payload_buffer_pool: BucketedPool,
    payload_read_timeout: Duration,
    max_payload_size: usize,
    rate_limit: u32,
    handler: usize,
    service_name: &'static str,
  ) where
    T: AsyncReadRent + AsyncWriteRent + 'static,
  {
    let mut rate_limit_counter: u32 = 0;
    let mut rate_limit_last_check = std::time::Instant::now();

    let mut delimiter_buf = Box::new([0u8; 1]);

    loop {
      match stream_reader.next().await {
        Ok(true) => {
          let line_bytes = stream_reader.get_line().unwrap();
          let message_length = line_bytes.len() as u32;

          match deserialize(Cursor::new(line_bytes)) {
            Ok(msg) => {
              let mut payload_opt: Option<PoolBuffer> = None;
              let mut payload_length: u32 = 0;

              // Read associated payload if present.
              if let Some(payload_info) = msg.payload_info() {
                if payload_info.length > max_payload_size {
                  let err_message = Message::Error(ErrorParameters {
                    id: payload_info.id,
                    reason: PolicyViolation.into(),
                    detail: Some("payload too large".into()),
                  });
                  let _ = read_tx.send(ReadResult::Error(err_message)).await;
                  break;
                }
                payload_length = payload_info.length as u32;

                let pool_buff = payload_buffer_pool.acquire_buffer(payload_info.length).await.unwrap();

                match runtime::timeout(
                  payload_read_timeout,
                  Self::read_payload(
                    pool_buff,
                    delimiter_buf,
                    &mut stream_reader,
                    payload_info.length,
                    payload_info.id,
                  ),
                )
                .await
                {
                  Ok(Ok(mut result)) => {
                    payload_opt = Some(result.data.freeze(payload_info.length));
                    delimiter_buf = result.delimiter;
                  },
                  Ok(Err(e)) => {
                    let err_message: Message = if let Some(e) = e.downcast_ref::<narwhal_protocol::Error>() {
                      e.into()
                    } else {
                      warn!(handler = handler, service_type = service_name, "failed to read payload: {}", e);
                      Message::Error(ErrorParameters { id: None, reason: InternalServerError.into(), detail: None })
                    };
                    let _ = read_tx.send(ReadResult::Error(err_message)).await;
                    break;
                  },
                  Err(_) => {
                    let err_message = Message::Error(ErrorParameters {
                      id: None,
                      reason: Timeout.into(),
                      detail: Some("payload read timeout".into()),
                    });
                    let _ = read_tx.send(ReadResult::Error(err_message)).await;
                    break;
                  },
                }
              }

              // Check if the rate limit is exceeded.
              if rate_limit > 0 {
                let now = std::time::Instant::now();
                let elapsed = now.duration_since(rate_limit_last_check);
                if elapsed.as_secs() > 1 {
                  rate_limit_counter = 0;
                  rate_limit_last_check = now;
                }

                rate_limit_counter += message_length + payload_length;

                if rate_limit_counter > rate_limit {
                  let err_message = Message::Error(ErrorParameters {
                    id: msg.correlation_id(),
                    reason: PolicyViolation.into(),
                    detail: Some("rate limit exceeded".into()),
                  });
                  let _ = read_tx.send(ReadResult::Error(err_message)).await;
                  break;
                }
              }

              // Send parsed message to the main connection loop.
              if read_tx.send(ReadResult::Message { message: msg, payload: payload_opt }).await.is_err() {
                break;
              }
            },
            Err(e) => {
              let err_detail = format!("{}", e);
              let err_message = Message::Error(ErrorParameters {
                id: None,
                reason: BadRequest.into(),
                detail: Some(StringAtom::from(err_detail)),
              });
              let _ = read_tx.send(ReadResult::Error(err_message)).await;
              break;
            },
          }
        },
        Ok(false) => {
          // Stream closed by the client.
          let _ = read_tx.send(ReadResult::Eof).await;
          break;
        },
        Err(e) => {
          match e {
            StreamReaderError::MaxLineLengthExceeded => {
              let err_message = Message::Error(ErrorParameters {
                id: None,
                reason: PolicyViolation.into(),
                detail: Some("max message size exceeded".into()),
              });
              let _ = read_tx.send(ReadResult::Error(err_message)).await;
            },
            StreamReaderError::IoError(e) => {
              if e.kind() != std::io::ErrorKind::UnexpectedEof {
                error!(
                  handler = handler,
                  service_type = service_name,
                  "failed to read from connection: {}",
                  e.to_string()
                );
              }
            },
          }
          break;
        },
      }
    }
  }

  #[allow(clippy::too_many_arguments)]
  async fn run_connection_loop<T, W, ST>(
    reader: LocalStream<T>,
    writer: &mut W,
    conn: &mut Conn<D>,
    handler: usize,
    send_msg_rx: Receiver<(Message, Option<PoolBuffer>)>,
    close_rx: Receiver<Message>,
    shutdown_token: &CancellationToken,
    message_buffer_pool: Pool,
    payload_buffer_pool: BucketedPool,
    newline_buffer: PoolBuffer,
    payload_read_timeout: Duration,
    max_payload_size: usize,
    rate_limit: u32,
  ) -> anyhow::Result<()>
  where
    T: AsyncReadRent + AsyncWriteRent + 'static,
    W: AsyncWriteRent,
    ST: Service,
  {
    // Initialize stream reader and hand it off to the read loop.
    let read_pool_buffer = message_buffer_pool.acquire_buffer().await;
    let stream_reader = StreamReader::with_pool_buffer(reader, read_pool_buffer);

    let (read_tx, read_rx) = bounded::<ReadResult>(READ_CHANNEL_CAPACITY);

    // Spawn the read loop as a separate task.
    // The task handle is dropped at scope exit to stop polling the reader.
    let reader_task = runtime::spawn(Self::run_read_loop(
      stream_reader,
      read_tx,
      payload_buffer_pool,
      payload_read_timeout,
      max_payload_size,
      rate_limit,
      handler,
      ST::NAME,
    ));

    let mut pool_buffer_batch = Vec::<PoolBuffer>::with_capacity(MAX_BUFFERS_PER_BATCH);
    let mut iovec_batch = Vec::<libc::iovec>::with_capacity(MAX_BUFFERS_PER_BATCH);

    let mut cancelled = std::pin::pin!(shutdown_token.cancelled().fuse());

    'connection_loop: loop {
      let mut read_res = std::pin::pin!(read_rx.recv().fuse());
      let mut send_msg_res = std::pin::pin!(send_msg_rx.recv().fuse());
      let mut close_res = std::pin::pin!(close_rx.recv().fuse());

      futures::select! {
        // Receive parsed messages from the read loop.
        res = read_res => {
          match res {
            Ok(ReadResult::Message { message, payload }) => {
              match conn.dispatch_message::<ST>(message, payload).await {
                Ok(_) => {},
                Err(e) => {
                  warn!(handler = handler, service_type = ST::NAME, "failed to dispatch message: {}", e.to_string());
                  break 'connection_loop;
                },
              }
            }
            Ok(ReadResult::Eof) => {
              trace!(handler = handler, service_type = ST::NAME, "connection closed by peer");
              break 'connection_loop;
            }
            Ok(ReadResult::Error(err_message)) => {
              let _  = Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await, &newline_buffer, pool_buffer_batch, iovec_batch).await?;
              break 'connection_loop;
            }
            Err(_) => {
              // Read task exited unexpectedly.
              break 'connection_loop;
            }
          }
        },

        // Write outbound messages to the stream.
        res = send_msg_res => {
          const MESSAGE_CHANNEL_CLOSED_LOG: &str = "message channel closed";

          match res {
            Ok((message, payload_opt)) => {
              Self::add_message_to_batch(&message, payload_opt, &mut pool_buffer_batch, &message_buffer_pool, &newline_buffer).await?;
            }
            Err(_) => {
              error!(handler = handler, service_type = ST::NAME, MESSAGE_CHANNEL_CLOSED_LOG);
              break 'connection_loop;
            }
          };

          loop {
            if pool_buffer_batch.len() >= MAX_BUFFERS_PER_BATCH {
                break;
            }

            match send_msg_rx.try_recv() {
              Ok((message, payload_opt)) => {
                Self::add_message_to_batch(&message, payload_opt, &mut pool_buffer_batch, &message_buffer_pool, &newline_buffer).await?;
              },
              Err(TryRecvError::Empty) => break,
              Err(TryRecvError::Closed) => {
                error!(handler = handler, service_type = ST::NAME, MESSAGE_CHANNEL_CLOSED_LOG);
                break 'connection_loop;
              }
            }
          }

          (pool_buffer_batch, iovec_batch) = Self::write_batch(pool_buffer_batch, iovec_batch, writer).await?;
        },

        // Close the connection.
        res = close_res => {
          let err_message = res.unwrap();
          let _  = Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await, &newline_buffer, pool_buffer_batch, iovec_batch).await?;
          trace!(handler = handler, service_type = ST::NAME, "closed connection");
          break 'connection_loop;
        },

        // Close the connection on shutdown.
        _ = cancelled => {
          let err_message = Message::Error(ErrorParameters{id: None, reason: ServerShuttingDown.into(), detail: None});
          let _ = Self::write_message(&err_message, None, writer, message_buffer_pool.acquire_buffer().await, &newline_buffer, pool_buffer_batch, iovec_batch).await?;
          trace!(handler = handler, service_type = ST::NAME, "closed connection on shutdown");
          break 'connection_loop;
        },
      }
    }

    drop(reader_task);

    Ok(())
  }

  async fn write_message<W>(
    message: &Message,
    payload_opt: Option<PoolBuffer>,
    writer: &mut W,
    message_buffer: MutablePoolBuffer,
    newline_buffer: &PoolBuffer,
    mut batch: Vec<PoolBuffer>,
    iovec_batch: Vec<libc::iovec>,
  ) -> anyhow::Result<(Vec<PoolBuffer>, Vec<libc::iovec>)>
  where
    W: AsyncWriteRent,
  {
    let message_buff = Self::serialize_message(message, message_buffer)?;
    batch.push(message_buff);

    if let Some(payload_buf) = payload_opt {
      batch.push(payload_buf);
      batch.push(newline_buffer.clone());
    };

    Self::write_batch(batch, iovec_batch, writer).await
  }

  async fn write_batch<W>(
    mut batch: Vec<PoolBuffer>,
    mut iovec_batch: Vec<libc::iovec>,
    writer: &mut W,
  ) -> anyhow::Result<(Vec<PoolBuffer>, Vec<libc::iovec>)>
  where
    W: AsyncWriteRent,
  {
    // Populate the pre-allocated iovec batch from the pool buffers.
    iovec_batch.extend(batch.iter().map(|b| libc::iovec { iov_base: b.read_ptr() as *mut _, iov_len: b.bytes_init() }));

    let mut remaining: usize = iovec_batch.iter().map(|v| v.iov_len).sum();

    // Loop writev until every byte is out.
    while remaining > 0 {
      let (result, returned_iovecs) = writer.writev(iovec_batch).await;
      let written = result?;

      iovec_batch = returned_iovecs;

      if written == 0 {
        return Err(anyhow!("writev unable to make progress"));
      }
      remaining -= written;

      if remaining == 0 {
        break;
      }

      // Advance iovecs past the bytes just written.
      let mut consumed = 0usize;
      let mut to_skip = written;

      for iovec in iovec_batch.iter_mut() {
        if to_skip >= iovec.iov_len {
          to_skip -= iovec.iov_len;
          consumed += 1;
        } else {
          // SAFETY: we advance the pointer within the bounds of the
          // original PoolBuffer, which is kept alive in `batch`.
          iovec.iov_base = unsafe { (iovec.iov_base as *mut u8).add(to_skip) as *mut _ };
          iovec.iov_len -= to_skip;
          break;
        }
      }

      // Remove fully-consumed entries so writev never sees leading
      // zero-length iovecs (which some implementations return 0 for).
      if consumed > 0 {
        iovec_batch.drain(..consumed);
      }
    }

    batch.clear();
    iovec_batch.clear();

    writer.flush().await?;

    Ok((batch, iovec_batch))
  }

  async fn read_payload<B, T>(
    mut buf: B,
    mut delimiter_buf: Box<[u8; 1]>,
    stream_reader: &mut StreamReader<LocalStream<T>>,
    len: usize,
    correlation_id: Option<u32>,
  ) -> anyhow::Result<PayloadReadResult<B>>
  where
    B: IoBuf + IoBufMut,
    T: AsyncReadRent + 'static,
  {
    buf = stream_reader.read_raw(buf, len).await?;

    // Read last byte, and ensure it's a newline
    delimiter_buf = stream_reader.read_raw(delimiter_buf, 1).await?;

    if delimiter_buf[0] != b'\n' {
      let mut error = narwhal_protocol::Error::new(BadRequest).with_detail(StringAtom::from("invalid payload format"));
      if let Some(id) = correlation_id {
        error = error.with_id(id);
      }
      return Err(error.into());
    }

    Ok(PayloadReadResult { data: buf, delimiter: delimiter_buf })
  }
}

/// The result of reading a payload and its trailing delimiter.
struct PayloadReadResult<B> {
  /// The payload data buffer.
  data: B,

  /// The delimiter buffer.
  delimiter: Box<[u8; 1]>,
}
