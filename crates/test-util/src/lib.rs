// SPDX-License-Identifier: BSD-3-Clause

pub mod c2s_suite;
pub mod m2s_suite;
pub mod modulator;
pub mod s2m_suite;
pub mod tls;

pub use c2s_suite::{C2sSuite, default_c2s_config};
pub use m2s_suite::{M2sSuite, default_m2s_config};
pub use modulator::{TestModulator, default_s2m_config};
pub use s2m_suite::{S2mSuite, default_s2m_config_with_secret};

use std::io::Cursor;
use std::time::Duration;

use anyhow::anyhow;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use narwhal_protocol::{Message, deserialize, serialize};
use narwhal_util::{codec::StreamReader, pool::MutablePoolBuffer};

/// A testing macro for asserting that a message matches an expected type and parameters.
///
/// This macro is designed to simplify testing of message-based protocols where messages
/// are typically represented as enums with associated data. It performs two levels of
/// validation:
/// 1. Ensures the message is of the expected variant/type
/// 2. Validates that the message parameters match the expected values
///
/// If either check fails, the macro will panic with a descriptive error message,
/// making it ideal for use in unit tests and integration tests.
///
/// # Parameters
///
/// * `$msg` - An expression that evaluates to the message to be tested
/// * `$message_type` - The path to the expected message variant (e.g., `Message::Connect`)
/// * `$expected_params` - The expected parameters/data associated with the message
///
/// # Panics
///
/// This macro will panic if:
/// - The message is not of the expected type/variant
/// - The message parameters don't match the expected values (using `assert_eq!`)
#[macro_export]
macro_rules! assert_message {
  ($msg:expr, $message_type:path, $expected_params:expr) => {
    match $msg {
      $message_type(params) => {
        assert_eq!(params, $expected_params);
      },
      _ => panic!("expected {} message", stringify!($message_type)),
    }
  };
}

/// A test connection wrapper for async streams that provides message-based communication.
///
/// # Type Parameters
///
/// * `T` - Any type that implements both `AsyncRead` and `AsyncWrite`, typically
///   network streams like `TcpStream` or `UnixStream`
pub struct TestConn<T: AsyncRead + AsyncWrite> {
  /// the writer handle for the stream.
  pub writer: futures::io::WriteHalf<T>,

  /// the reader handle for the stream.
  pub reader: StreamReader<futures::io::ReadHalf<T>>,

  /// buffer for writing messages.
  write_buffer: Vec<u8>,
}

// ===== impl TestConn =====

impl<T: AsyncRead + AsyncWrite> TestConn<T> {
  /// Creates a new `TestConn` instance from an async stream.
  ///
  /// # Arguments
  ///
  /// * `stream` - An async stream that implements both `AsyncRead` and `AsyncWrite`
  /// * `read_buffer` - Buffer for reading messages.
  /// * `max_message_size` - Maximum size in bytes for messages that can be sent/received
  ///
  /// # Returns
  ///
  /// A new `TestConn` instance ready for message communication.
  pub fn new(stream: T, read_buffer: MutablePoolBuffer, max_message_size: usize) -> Self {
    let (rh, wh) = AsyncReadExt::split(stream);
    let write_buffer = vec![0u8; max_message_size];

    let stream_reader = StreamReader::with_pool_buffer(rh, read_buffer);

    Self { writer: wh, reader: stream_reader, write_buffer }
  }

  /// Serializes and sends a message over the connection.
  ///
  /// # Arguments
  ///
  /// * `message` - The message to be sent, implementing the `Message` trait
  ///
  /// # Returns
  ///
  /// * `Ok(())` - If the message was successfully sent
  /// * `Err(anyhow::Error)` - If serialization failed or there was an I/O error
  pub async fn write_message(&mut self, message: Message) -> anyhow::Result<()> {
    let n = serialize(&message, &mut self.write_buffer)?;

    AsyncWriteExt::write_all(&mut self.writer, &self.write_buffer[..n]).await?;
    AsyncWriteExt::flush(&mut self.writer).await?;

    Ok(())
  }

  /// Reads and deserializes a message from the connection with a timeout.
  ///
  /// # Returns
  ///
  /// * `Ok(Message)` - If a message was successfully received and deserialized
  /// * `Err(anyhow::Error)` - If the operation failed
  ///
  /// # Timeout
  ///
  /// The method uses a hardcoded 10-seconds timeout. If no message is received
  /// within this timeframe, a timeout error is returned.
  pub async fn read_message(&mut self) -> anyhow::Result<Message> {
    // Set a timeout for reading the message
    let read_timeout = Duration::from_secs(10);

    let reader = &mut self.reader;

    let line = match tokio::time::timeout(read_timeout, reader.next()).await {
      Ok(Ok(true)) => reader.get_line().unwrap(),
      Ok(Ok(false)) => return Err(anyhow!("no message received")),
      Ok(Err(e)) => return Err(anyhow!("error reading from stream: {}", e)),
      Err(_) => return Err(anyhow!("timeout waiting for message")),
    };

    let msg = deserialize(Cursor::new(line))?;

    Ok(msg)
  }

  /// Writes raw bytes directly to the connection without any encoding or framing.
  ///
  /// This method bypasses the message serialization system and writes the provided
  /// byte slice directly to the underlying stream.
  ///
  /// # Arguments
  ///
  /// * `data` - A byte slice containing the raw data to be sent
  ///
  /// # Returns
  ///
  /// * `Ok(())` - If the data was successfully written and flushed
  /// * `Err(anyhow::Error)` - If there was an I/O error during writing or flushing
  pub async fn write_raw_bytes(&mut self, data: &[u8]) -> anyhow::Result<()> {
    self.writer.write_all(data).await?;
    self.writer.flush().await?;
    Ok(())
  }

  /// Reads raw bytes directly from the connection into the provided buffer.
  ///
  /// This method bypasses the message deserialization system and reads raw bytes
  /// directly from the underlying stream into the provided mutable byte slice.
  /// The buffer will be filled exactly to its capacity.
  ///
  /// # Arguments
  ///
  /// * `data` - A mutable byte slice that will be filled with the read data
  ///
  /// # Returns
  ///
  /// * `Ok(())` - If the buffer was successfully filled with data from the stream
  /// * `Err(anyhow::Error)` - If there was an I/O error during reading
  pub async fn read_raw_bytes(&mut self, data: &mut [u8]) -> anyhow::Result<()> {
    self.reader.read_raw(data).await?;
    Ok(())
  }

  /// Gracefully shuts down the connection's write half.
  ///
  /// This method closes the write side of the connection, signaling to the remote
  /// peer that no more data will be sent. This is typically used as part of a
  /// clean connection termination process.
  ///
  /// # Returns
  ///
  /// * `Ok(())` - If the shutdown was successful
  /// * `Err(anyhow::Error)` - If there was an error during the shutdown process
  pub async fn shutdown(&mut self) -> anyhow::Result<()> {
    self.writer.close().await?;
    Ok(())
  }
}
