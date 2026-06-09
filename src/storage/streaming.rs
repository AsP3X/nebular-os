use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::Stream;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio_util::io::ReaderStream;

use super::compressibility::CompressionContext;
use super::compression::{
    compress_file_to_storage, pump_block_blob_full, pump_block_blob_range, BLOB_MAGIC,
    FIXED_HEADER_LEN,
};
use super::error::{internal, map_io_error, StorageError};

/// Human: AsyncRead wrapper that skips an offset and stops after a byte budget (HTTP Range on raw files).
/// Agent: WRAPS inner AsyncRead; poll_read skips until `skip` consumed then caps total bytes at `limit`.
pub struct LimitedAsyncRead<R> {
    inner: R,
    skip: u64,
    remaining: u64,
}

// Human: Constructor binds the byte window for a Range response (skip logical offset, cap length).
// Agent: new(inner, skip, limit); remaining=limit; skip consumed in poll_read before user buffer fills.
impl<R: AsyncRead + Unpin> LimitedAsyncRead<R> {
    pub fn new(inner: R, skip: u64, limit: u64) -> Self {
        Self {
            inner,
            skip,
            remaining: limit,
        }
    }
}

// Human: AsyncRead that seeks past `skip` bytes then returns at most `remaining` bytes to the caller.
// Agent: poll_read phases: (1) drain skip via discard buffer (2) read min(remaining, buf) into caller.
impl<R: AsyncRead + Unpin> AsyncRead for LimitedAsyncRead<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.skip > 0 {
            let mut discard = [0u8; 8192];
            while self.skip > 0 {
                let chunk = (self.skip as usize).min(discard.len());
                let mut rb = ReadBuf::new(&mut discard[..chunk]);
                match Pin::new(&mut self.inner).poll_read(cx, &mut rb) {
                    Poll::Ready(Ok(())) => {
                        let n = rb.filled().len();
                        if n == 0 {
                            return Poll::Ready(Ok(()));
                        }
                        self.skip -= n as u64;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
        let max = (self.remaining as usize).min(buf.remaining());
        if max == 0 {
            return Poll::Ready(Ok(()));
        }
        let unfilled = buf.initialize_unfilled_to(max);
        let mut sub = ReadBuf::new(unfilled);
        match Pin::new(&mut self.inner).poll_read(cx, &mut sub) {
            Poll::Ready(Ok(())) => {
                let n = sub.filled().len();
                unsafe {
                    buf.assume_init(n);
                    buf.advance(n);
                }
                self.remaining -= n as u64;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// Human: HTTP response body stream that can come from disk or a channel pump.
/// Agent: ENUM FileLimited|Channel; Stream<Item=Result<Bytes,io::Error>> for axum Body::from_stream.
pub enum ObjectBodyStream {
    FileLimited(ReaderStream<LimitedAsyncRead<File>>),
    Channel(tokio_stream::wrappers::ReceiverStream<Result<Bytes, std::io::Error>>),
    Http(Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>),
}

impl Stream for ObjectBodyStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            ObjectBodyStream::FileLimited(s) => Pin::new(s).poll_next(cx),
            ObjectBodyStream::Channel(s) => Pin::new(s).poll_next(cx),
            ObjectBodyStream::Http(s) => Pin::new(s).poll_next(cx),
        }
    }
}

/// Human: Wraps an object body stream for axum GET responses.
/// Agent: WRAPS ObjectBodyStream; no spill file needed for block-compressed range reads.
pub struct GuardedObjectBodyStream {
    pub stream: ObjectBodyStream,
}

impl GuardedObjectBodyStream {
    pub fn from_http_stream(stream: ObjectBodyStream) -> Self {
        Self { stream }
    }
}

impl Stream for GuardedObjectBodyStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// Human: Build a streaming body for GET, honoring Range on raw and NOSB block-compressed blobs.
/// Agent: raw=>LimitedAsyncRead; NOSB+full=>pump_block_blob_full; NOSB+range=>pump_block_blob_range.
pub async fn open_object_body_stream(
    blob_path: &Path,
    logical_size: u64,
    range_start: u64,
    content_length: u64,
    _data_dir: &str,
) -> Result<GuardedObjectBodyStream, StorageError> {
    // Human: Peek only the fixed header to detect NOSB; never buffer the whole blob here.
    // Agent: READS first FIXED_HEADER_LEN bytes; raw blobs stream directly from disk.
    let mut peek = [0u8; FIXED_HEADER_LEN];
    let mut file = File::open(blob_path).await.map_err(map_io_error)?;
    let read = file.read(&mut peek).await.map_err(map_io_error)?;
    if read < FIXED_HEADER_LEN || !peek.starts_with(BLOB_MAGIC) {
        drop(file);
        let stream = open_raw_file_stream(blob_path, range_start, content_length).await?;
        return Ok(GuardedObjectBodyStream { stream });
    }

    let path = blob_path.to_path_buf();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    if range_start == 0 && content_length == logical_size {
        tokio::task::spawn_blocking(move || pump_block_blob_full(path, logical_size, tx));
    } else {
        tokio::task::spawn_blocking(move || {
            pump_block_blob_range(path, logical_size, range_start, content_length, tx)
        });
    }

    Ok(GuardedObjectBodyStream {
        stream: ObjectBodyStream::Channel(tokio_stream::wrappers::ReceiverStream::new(rx)),
    })
}

async fn open_raw_file_stream(
    blob_path: &Path,
    range_start: u64,
    content_length: u64,
) -> Result<ObjectBodyStream, StorageError> {
    let file = File::open(blob_path).await.map_err(map_io_error)?;
    let limited = LimitedAsyncRead::new(file, range_start, content_length);
    Ok(ObjectBodyStream::FileLimited(ReaderStream::new(limited)))
}

/// Human: Stream upload body to a temp file while hashing; no in-memory payload buffer.
/// Agent: WRITES tmp_path; RETURNS (logical_size, xxh3 digest hex).
pub async fn stream_body_to_temp(
    body: &mut (impl AsyncRead + Unpin),
    tmp_path: &Path,
    buffer_size: usize,
) -> Result<(u64, String), StorageError> {
    let mut file = fs::File::create(tmp_path).await.map_err(map_io_error)?;
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = vec![0u8; buffer_size.max(4096)];
    let mut size: u64 = 0;

    loop {
        let n = body.read(&mut buf).await.map_err(map_io_error)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).await.map_err(map_io_error)?;
        size += n as u64;
    }
    file.flush().await.map_err(map_io_error)?;
    let etag = format!("{:016x}", hasher.digest());
    Ok((size, etag))
}

/// Human: Hash an on-disk temp file (used after multipart part concatenation).
/// Agent: READS tmp_path in chunks; RETURNS (size, xxh3 hex etag).
pub fn hash_temp_file(tmp_path: &Path, buffer_size: usize) -> Result<(u64, String), StorageError> {
    use std::io::Read;

    let total_size = std::fs::metadata(tmp_path)
        .map_err(|e| internal(anyhow::anyhow!(e)))?
        .len();
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut f = std::fs::File::open(tmp_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    let mut buf = vec![0u8; buffer_size.max(4096)];
    loop {
        let n = f.read(&mut buf).map_err(|e| internal(anyhow::anyhow!(e)))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok((total_size, format!("{:016x}", hasher.digest())))
}

/// Human: After temp upload, block-compress-or-store to final blob path without full-RAM buffering.
/// Agent: CALLS compress_file_to_storage with block_size; may copy raw when NOSB does not shrink.
pub async fn finalize_temp_to_blob(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    zstd_level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
) -> Result<(), StorageError> {
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent).await.map_err(internal)?;
    }
    if final_path.exists() {
        fs::remove_file(final_path).await.map_err(map_io_error)?;
    }
    let tmp = tmp_path.to_path_buf();
    let fin = final_path.to_path_buf();
    let owned_key = ctx.object_key.map(str::to_string);
    let owned_ct = ctx.content_type.map(str::to_string);
    let min_size = ctx.min_size;
    tokio::task::spawn_blocking(move || {
        let ctx = CompressionContext::new(
            owned_key.as_deref(),
            owned_ct.as_deref(),
            logical_size,
            min_size,
        );
        compress_file_to_storage(&tmp, &fin, logical_size, zstd_level, block_size, ctx)
    })
    .await
    .map_err(internal)??;
    Ok(())
}
