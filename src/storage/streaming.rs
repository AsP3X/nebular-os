use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::Stream;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio_util::io::ReaderStream;

use super::buffer_pool::BufferPool;
use super::compression::{
    decompress_file_to_temp, pump_block_blob_full, pump_block_blob_range, read_blob_header_size,
    BlobFormat, IndexedReadContext, FIXED_HEADER_LEN_V1, HEADER_LEN, HEADER_LEN_V2,
};
use super::blocks::BlockStore;
use super::blob_finalize::blob_format_from_header;
use super::error::{internal, map_io_error, StorageError};

/// Human: AsyncRead wrapper that skips an offset and stops after a byte budget (HTTP Range on raw files).
/// Agent: WRAPS inner AsyncRead; poll_read skips until `skip` consumed then caps total bytes at `limit`.
pub struct LimitedAsyncRead<R> {
    inner: R,
    skip: u64,
    remaining: u64,
}

impl<R: AsyncRead + Unpin> LimitedAsyncRead<R> {
    pub fn new(inner: R, skip: u64, limit: u64) -> Self {
        Self {
            inner,
            skip,
            remaining: limit,
        }
    }
}

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

/// Human: Deletes a spill file when the response body is dropped (range reads on legacy zstd blobs).
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

pub enum ObjectBodyStream {
    FileLimited(ReaderStream<LimitedAsyncRead<File>>),
    PooledLimited(ReaderStream<LimitedAsyncRead<PooledFileRead>>),
    Channel(tokio_stream::wrappers::ReceiverStream<Result<Bytes, std::io::Error>>),
    Http(Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>),
}

impl Stream for ObjectBodyStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            ObjectBodyStream::FileLimited(s) => Pin::new(s).poll_next(cx),
            ObjectBodyStream::PooledLimited(s) => Pin::new(s).poll_next(cx),
            ObjectBodyStream::Channel(s) => Pin::new(s).poll_next(cx),
            ObjectBodyStream::Http(s) => Pin::new(s).poll_next(cx),
        }
    }
}

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

/// Human: Build a streaming body for GET, honoring Range on raw, NOSB, legacy zstd, and dedup blobs.
pub async fn open_object_body_stream(
    blob_path: &Path,
    logical_size: u64,
    range_start: u64,
    content_length: u64,
    ctx: &super::blob_finalize::ReadContext,
) -> Result<GuardedObjectBodyStream, StorageError> {
    let mut peek = [0u8; FIXED_HEADER_LEN_V1];
    let mut file = File::open(blob_path).await.map_err(map_io_error)?;
    let read = file.read(&mut peek).await.map_err(map_io_error)?;
    let format = blob_format_from_header(&peek[..read]);

    if format == BlobFormat::Nosd {
        return open_dedup_object_stream(
            blob_path,
            logical_size,
            range_start,
            content_length,
            ctx,
        )
        .await;
    }

    if format == BlobFormat::Raw {
        drop(file);
        if ctx.verify_on_read
            && range_start == 0
            && content_length == logical_size
            && let Some(expected) = ctx.expected_etag.as_deref()
        {
            let path = blob_path.to_path_buf();
            let expected = expected.to_string();
            tokio::task::spawn_blocking(move || verify_raw_file_etag(&path, &expected))
                .await
                .map_err(internal)??;
        }
        let stream = open_raw_file_stream(
            blob_path,
            range_start,
            content_length,
            &ctx.buffer_pool,
            ctx.read_buffer_size,
        )
        .await?;
        return Ok(GuardedObjectBodyStream {
            stream,
            _spill_guard: None,
        });
    }

    if matches!(format, BlobFormat::Nosb | BlobFormat::Nosi) {
        let path = blob_path.to_path_buf();
        let read_ctx = IndexedReadContext {
            dict: ctx.dict.as_ref().map(|d| d.to_vec()),
            data_dir: ctx.data_dir.clone(),
            block_cache: ctx.block_cache.clone(),
        };
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
        if range_start == 0 && content_length == logical_size {
            tokio::task::spawn_blocking(move || {
                pump_block_blob_full(path, logical_size, read_ctx, tx)
            });
        } else {
            tokio::task::spawn_blocking(move || {
                pump_block_blob_range(path, logical_size, range_start, content_length, read_ctx, tx)
            });
        }
        return Ok(GuardedObjectBodyStream {
            stream: ObjectBodyStream::Channel(tokio_stream::wrappers::ReceiverStream::new(rx)),
            _spill_guard: None,
        });
    }

    let dict = ctx.dict_bytes();

    if range_start == 0 && content_length == logical_size {
        let stream = open_full_zstd_stream(blob_path, logical_size, format, dict).await?;
        return Ok(GuardedObjectBodyStream {
            stream,
            _spill_guard: None,
        });
    }

    let spill = format!(
        "{}/.tmp/decompress-{}.bin",
        ctx.data_dir,
        uuid::Uuid::new_v4()
    );
    let blob_path_owned = blob_path.to_path_buf();
    let spill_path = spill.clone();
    let dict_owned = ctx.dict.clone();
    let data_dir = ctx.data_dir.clone();
    tokio::task::spawn_blocking(move || {
        decompress_file_to_temp(
            &blob_path_owned,
            logical_size,
            Path::new(&spill_path),
            dict_owned.as_deref().map(|v| v.as_slice()),
            Some(data_dir.as_str()),
        )
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

async fn open_dedup_object_stream(
    blob_path: &Path,
    logical_size: u64,
    range_start: u64,
    content_length: u64,
    ctx: &super::blob_finalize::ReadContext,
) -> Result<GuardedObjectBodyStream, StorageError> {
    let spill = format!(
        "{}/.tmp/dedup-{}.bin",
        ctx.data_dir,
        uuid::Uuid::new_v4()
    );
    let blob_path_owned = blob_path.to_path_buf();
    let spill_path = spill.clone();
    let data_dir = ctx.data_dir.clone();
    tokio::task::spawn_blocking(move || {
        let store = BlockStore::new(&data_dir);
        store.assemble_to_file(&blob_path_owned, Path::new(&spill_path), logical_size)
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

/// Human: xxh3 hex digest of on-disk file bytes (used for scrub and wire checksum).
/// Agent: Blocking read with configurable buffer; returns 16-char lowercase hex.
pub fn hash_file_xxh3_hex(path: &Path, buffer_size: usize) -> Result<String, StorageError> {
    use std::io::Read;
    use xxhash_rust::xxh3::Xxh3;

    let mut f = std::fs::File::open(path).map_err(map_io_error)?;
    let mut hasher = Xxh3::new();
    let mut buf = vec![0u8; buffer_size.max(4096)];
    loop {
        let n = f.read(&mut buf).map_err(map_io_error)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:016x}", hasher.digest()))
}

fn verify_raw_file_etag(path: &Path, expected: &str) -> Result<(), StorageError> {
    let actual = hash_file_xxh3_hex(path, 256 * 1024)?;
    if actual != expected {
        return Err(internal(anyhow::anyhow!("raw blob etag mismatch on read")));
    }
    Ok(())
}

/// Human: AsyncRead that serves file bytes using a pooled buffer (fewer allocations on GET).
/// Agent: WRAPS File; refills pooled cache; copies into caller ReadBuf per poll.
pub(crate) struct PooledFileRead {
    file: File,
    pool: BufferPool,
    cache: Vec<u8>,
    cache_pos: usize,
    cache_len: usize,
}

impl PooledFileRead {
    fn new(file: File, pool: BufferPool) -> Self {
        let cache = pool.acquire();
        Self {
            file,
            pool,
            cache,
            cache_pos: 0,
            cache_len: 0,
        }
    }
}

impl AsyncRead for PooledFileRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.cache_pos < self.cache_len {
            let avail = self.cache_len - self.cache_pos;
            let take = avail.min(out.remaining());
            out.put_slice(&self.cache[self.cache_pos..self.cache_pos + take]);
            self.cache_pos += take;
            return Poll::Ready(Ok(()));
        }
        let cap = self.pool.buffer_capacity();
        let mut chunk = self.pool.acquire();
        chunk.resize(cap, 0);
        let mut rb = ReadBuf::new(&mut chunk[..cap]);
        match Pin::new(&mut self.file).poll_read(cx, &mut rb) {
            Poll::Ready(Ok(())) => {
                let n = rb.filled().len();
                if n == 0 {
                    self.pool.release(chunk);
                    return Poll::Ready(Ok(()));
                }
                let old = std::mem::replace(&mut self.cache, chunk);
                self.pool.release(old);
                self.cache_len = n;
                self.cache_pos = 0;
                let take = n.min(out.remaining());
                out.put_slice(&self.cache[..take]);
                self.cache_pos = take;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                self.pool.release(chunk);
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                self.pool.release(chunk);
                Poll::Pending
            }
        }
    }
}

impl Drop for PooledFileRead {
    fn drop(&mut self) {
        let mut buf = Vec::new();
        std::mem::swap(&mut buf, &mut self.cache);
        self.pool.release(buf);
    }
}

async fn open_raw_file_stream(
    blob_path: &Path,
    range_start: u64,
    content_length: u64,
    pool: &BufferPool,
    _read_buffer_size: usize,
) -> Result<ObjectBodyStream, StorageError> {
    let file = File::open(blob_path).await.map_err(map_io_error)?;
    let pooled = PooledFileRead::new(file, pool.clone());
    let limited = LimitedAsyncRead::new(pooled, range_start, content_length);
    Ok(ObjectBodyStream::PooledLimited(ReaderStream::new(limited)))
}

async fn open_full_zstd_stream(
    blob_path: &Path,
    logical_size: u64,
    format: BlobFormat,
    dict: Option<&[u8]>,
) -> Result<ObjectBodyStream, StorageError> {
    let path = blob_path.to_path_buf();
    let dict_vec = dict.map(|d| d.to_vec());
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        pump_zstd_decode(path, logical_size, format, dict_vec, tx)
    });
    Ok(ObjectBodyStream::Channel(
        tokio_stream::wrappers::ReceiverStream::new(rx),
    ))
}

fn pump_zstd_decode(
    blob_path: PathBuf,
    logical_size: u64,
    format: BlobFormat,
    dict: Option<Vec<u8>>,
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
    let header_len = match format {
        BlobFormat::Nosz => HEADER_LEN,
        BlobFormat::Nos2 => HEADER_LEN_V2,
        _ => {
            let _ = tx.blocking_send(Err(std::io::Error::other("not a zstd blob")));
            return;
        }
    };
    if file
        .seek(std::io::SeekFrom::Start(header_len as u64))
        .is_err()
    {
        let _ = tx.blocking_send(Err(std::io::Error::other("seek past header failed")));
        return;
    }
    let reader = std::io::BufReader::new(file);
    let mut decoder = if let Some(ref d) = dict.filter(|d| !d.is_empty()) {
        match zstd::stream::read::Decoder::with_dictionary(reader, d) {
            Ok(d) => d,
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                return;
            }
        }
    } else {
        match zstd::stream::read::Decoder::with_buffer(reader) {
            Ok(d) => d,
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                return;
            }
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

/// Human: Read a multipart blob field in chunks while computing xxh3 wire checksum.
/// Agent: Used by POST /_cluster/replicate; enforces max_size; returns (bytes, checksum hex).
pub async fn read_multipart_blob_field(
    field: axum::extract::multipart::Field<'_>,
    max_size: usize,
) -> Result<(Vec<u8>, String), StorageError> {
    use futures_util::{StreamExt, TryStreamExt};
    use xxhash_rust::xxh3::Xxh3;

    let mut data = Vec::new();
    let mut hasher = Xxh3::new();
    let mut stream = field.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| internal(anyhow::anyhow!(e)))?;
        if data.len().saturating_add(chunk.len()) > max_size {
            return Err(StorageError::PayloadTooLarge);
        }
        hasher.update(&chunk);
        data.extend_from_slice(&chunk);
    }
    Ok((data, format!("{:016x}", hasher.digest())))
}

pub fn verify_wire_checksum(bytes: &[u8], expected: &str) -> Result<(), StorageError> {
    use xxhash_rust::xxh3::xxh3_64;
    let actual = format!("{:016x}", xxh3_64(bytes));
    if actual != expected {
        return Err(internal(anyhow::anyhow!("replication wire checksum mismatch")));
    }
    Ok(())
}

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

pub use super::blob_finalize::{finalize_temp_to_blob, BlobFinalizeOptions, ReadContext};
