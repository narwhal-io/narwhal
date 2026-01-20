// SPDX-License-Identifier: BSD-3-Clause

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

/// A wrapper enum that can hold either a regular TLS stream or a kTLS-enabled stream.
///
/// This allows the connection handling code to work with both types transparently,
/// enabling kTLS optimization when available while falling back to userspace TLS
/// when required.
pub enum MaybeKtlsStream {
  /// Regular userspace TLS stream using rustls
  Regular(TlsStream<TcpStream>),

  /// Kernel TLS stream with encryption offloaded to the kernel
  Ktls(ktls::KtlsStream<TcpStream>),
}

impl MaybeKtlsStream {
  /// Creates a new MaybeKtlsStream from a regular TLS stream
  pub fn from_tls(stream: TlsStream<TcpStream>) -> Self {
    MaybeKtlsStream::Regular(stream)
  }

  /// Creates a new MaybeKtlsStream from a kTLS stream
  pub fn from_ktls(stream: ktls::KtlsStream<TcpStream>) -> Self {
    MaybeKtlsStream::Ktls(stream)
  }

  /// Returns true if this stream is using kTLS
  pub fn is_ktls(&self) -> bool {
    matches!(self, MaybeKtlsStream::Ktls(_))
  }
}

impl AsyncRead for MaybeKtlsStream {
  fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
    match &mut *self {
      MaybeKtlsStream::Regular(stream) => Pin::new(stream).poll_read(cx, buf),
      MaybeKtlsStream::Ktls(stream) => Pin::new(stream).poll_read(cx, buf),
    }
  }
}

impl AsyncWrite for MaybeKtlsStream {
  fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match &mut *self {
      MaybeKtlsStream::Regular(stream) => Pin::new(stream).poll_write(cx, buf),
      MaybeKtlsStream::Ktls(stream) => Pin::new(stream).poll_write(cx, buf),
    }
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match &mut *self {
      MaybeKtlsStream::Regular(stream) => Pin::new(stream).poll_flush(cx),
      MaybeKtlsStream::Ktls(stream) => Pin::new(stream).poll_flush(cx),
    }
  }

  fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match &mut *self {
      MaybeKtlsStream::Regular(stream) => Pin::new(stream).poll_shutdown(cx),
      MaybeKtlsStream::Ktls(stream) => Pin::new(stream).poll_shutdown(cx),
    }
  }

  fn poll_write_vectored(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    bufs: &[io::IoSlice<'_>],
  ) -> Poll<io::Result<usize>> {
    match &mut *self {
      MaybeKtlsStream::Regular(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
      MaybeKtlsStream::Ktls(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
    }
  }

  fn is_write_vectored(&self) -> bool {
    match self {
      MaybeKtlsStream::Regular(stream) => stream.is_write_vectored(),
      MaybeKtlsStream::Ktls(stream) => stream.is_write_vectored(),
    }
  }
}
