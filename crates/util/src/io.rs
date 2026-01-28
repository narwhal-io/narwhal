// SPDX-License-Identifier: BSD-3-Clause

use std::io::IoSlice;

use anyhow;
use futures::io::{AsyncWrite, AsyncWriteExt};

/// Writes all the provided buffers to an asynchronous writer using vectored I/O.
///
/// This function efficiently writes multiple buffers (gathered in the `bufs` slice) to
/// the provided writer using vectored I/O operations. It continues writing until all
/// buffers are completely written or an error occurs.
///
/// # Parameters
///
/// * `bufs` - A mutable slice of IoSlice references containing the data to write
/// * `writer` - A mutable reference to the writer implementing AsyncWrite and AsyncWriteExt
///
/// # Returns
///
/// * `Ok(())` - When all buffers have been successfully written
/// * `Err(_)` - When a write operation fails or returns zero bytes written
///
/// # Errors
///
/// This function returns an error if:
/// - Any write operation returns an underlying I/O error
/// - A write operation returns 0 bytes written (indicating a closed connection)
pub async fn write_all_vectored<W>(bufs: &mut [IoSlice<'_>], writer: &mut W) -> anyhow::Result<()>
where
  W: AsyncWrite + AsyncWriteExt + Unpin,
{
  let mut bufs_remaining = bufs;

  while !bufs_remaining.is_empty() {
    let n = writer.write_vectored(bufs_remaining).await?;
    if n == 0 {
      return Err(anyhow::anyhow!("failed to write to stream"));
    }

    IoSlice::advance_slices(&mut bufs_remaining, n);
  }

  Ok(())
}
