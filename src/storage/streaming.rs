use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::Stream;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio_util::io::ReaderStream;

use super::compression::{
    compress_file_to_storage, decompress_file_to_temp, read_blob_header_size, BLOB_MAGIC, HEADER_LEN,
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
        // Human: Range fully satisfied — signal EOF without reading more from disk.
        // Agent: remaining==0 => Ready(Ok(())) with empty buf (HTTP body complete).
        if self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        // Human: Skip bytes before the range start by reading into a throwaway buffer (no copy to client).
        // Agent: WHILE skip>0 poll inner into 8KiB discard; EOF on inner ends poll early.
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
        // Human: Read only the slice of the range that still fits in this response chunk.
        // Agent: max=min(remaining, buf.remaining()); sub-read then decrement remaining.
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

/// Human: Deletes a spill file when the response body is dropped (range reads on compressed blobs).
/// Agent: Drop impl remove_file on spill_path; held inside GuardedObjectBodyStream until stream ends.
pub struct SpillFileGuard {
    pub path: PathBuf,
}

impl Drop for SpillFileGuard {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
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

/// Human: Keeps a spill-file guard alive until the client finishes reading the object body.
/// Agent: WRAPS ObjectBodyStream; Drop order deletes spill after stream exhausted.
pub struct GuardedObjectBodyStream {
    pub stream: ObjectBodyStream,
    _spill_guard: Option<SpillFileGuard>,
}

impl GuardedObjectBodyStream {
    pub fn from_http_stream(stream: ObjectBodyStream) -> Self {
        Self {
            stream,
            _spill_guard: None,
        }
    }
}

impl Stream for GuardedObjectBodyStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// Human: Build a streaming body for GET, honoring Range on both raw and zstd-wrapped blobs.
/// Agent: READS blob_path+logical_size; raw=>LimitedAsyncRead on File; compressed+range=>temp decompress; compressed+full=>channel pump.
pub async fn open_object_body_stream(
    blob_path: &Path,
    logical_size: u64,
    range_start: u64,
    content_length: u64,
    data_dir: &str,
) -> Result<GuardedObjectBodyStream, StorageError> {
    let header = fs::read(blob_path)
        .await
        .map_err(map_io_error)?;
    if header.len() < HEADER_LEN || !header.starts_with(BLOB_MAGIC) {
        let stream = open_raw_file_stream(blob_path, range_start, content_length).await?;
        return Ok(GuardedObjectBodyStream {
            stream,
            _spill_guard: None,
        });
    }

    if range_start == 0 && content_length == logical_size {
        let stream = open_full_compressed_stream(blob_path, logical_size).await?;
        return Ok(GuardedObjectBodyStream {
            stream,
            _spill_guard: None,
        });
    }

    let spill = format!(
        "{}/.tmp/decompress-{}.bin",
        data_dir,
        uuid::Uuid::new_v4()
    );
    let blob_path_owned = blob_path.to_path_buf();
    let spill_path = spill.clone();
    tokio::task::spawn_blocking(move || {
        decompress_file_to_temp(&blob_path_owned, logical_size, Path::new(&spill_path))
    })
    .await
    .map_err(internal)??;

    let guard = SpillFileGuard {
        path: PathBuf::from(&spill),
    };
    let file = File::open(&spill).await.map_err(map_io_error)?;
    let limited = LimitedAsyncRead::new(file, range_start, content_length);
    Ok(GuardedObjectBodyStream {
        stream: ObjectBodyStream::FileLimited(ReaderStream::new(limited)),
        _spill_guard: Some(guard),
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

async fn open_full_compressed_stream(
    blob_path: &Path,
    logical_size: u64,
) -> Result<ObjectBodyStream, StorageError> {
    let path = blob_path.to_path_buf();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    tokio::task::spawn_blocking(move || pump_zstd_decode(path, logical_size, tx));
    Ok(ObjectBodyStream::Channel(
        tokio_stream::wrappers::ReceiverStream::new(rx),
    ))
}

fn pump_zstd_decode(
    blob_path: PathBuf,
    logical_size: u64,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let mut file = match std::fs::File::open(&blob_path) {
        Ok(f) => f,
        Err(e) => {
            let _ = tx.blocking_send(Err(e));
            return;
        }
    };
    let stored = match read_blob_header_size(
        file.try_clone()
            .unwrap_or_else(|_| std::fs::File::open(&blob_path).expect("reopen blob")),
    ) {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.blocking_send(Err(std::io::Error::other(e.to_string())));
            return;
        }
    };
    if stored != logical_size {
        let _ = tx.blocking_send(Err(std::io::Error::other("blob header size mismatch")));
        return;
    }
    if file.seek(std::io::SeekFrom::Start(HEADER_LEN as u64)).is_err() {
        let _ = tx.blocking_send(Err(std::io::Error::other("seek past header failed")));
        return;
    }
    let mut decoder = match zstd::stream::read::Decoder::new(file) {
        Ok(d) => d,
        Err(e) => {
            let _ = tx.blocking_send(Err(e));
            return;
        }
    };
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        match decoder.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if tx.blocking_send(Ok(Bytes::copy_from_slice(&buf[..n]))).is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                return;
            }
        }
    }
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

/// Human: After temp upload, compress-or-store to final blob path without loading the whole object into RAM.
/// Agent: CALLS compress_file_to_storage; compares on-disk sizes; may copy raw when compression does not shrink.
pub async fn finalize_temp_to_blob(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    zstd_level: i32,
) -> Result<(), StorageError> {
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent).await.map_err(internal)?;
    }
    if final_path.exists() {
        fs::remove_file(final_path).await.map_err(map_io_error)?;
    }
    let tmp = tmp_path.to_path_buf();
    let fin = final_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        compress_file_to_storage(&tmp, &fin, logical_size, zstd_level)
    })
    .await
    .map_err(internal)??;
    Ok(())
}
