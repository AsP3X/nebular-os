use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::Stream;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, ReadBuf};
use tokio_util::io::ReaderStream;

use super::buffer_pool::BufferPool;
use super::compression::{
    read_blob_header_size, stored_blob_format, BlobFormat, IndexedBlobReader, IndexedReadContext,
    FIXED_HEADER_LEN_V1, HEADER_LEN, HEADER_LEN_V2,
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
    // Human: Everything below reads through this one handle, so an overwrite or maintenance swap that renames a
    // new blob over the path mid-request can't pair the format sniffed here with another file's bytes (a raw
    // GET used to reopen the path and could stream a freshly recompressed container as the object).
    let mut file = File::open(blob_path).await.map_err(map_io_error)?;
    let mut peek = [0u8; FIXED_HEADER_LEN_V1];
    let read = read_up_to(&mut file, &mut peek).await.map_err(map_io_error)?;
    let mut format = blob_format_from_header(&peek[..read]);
    if format != BlobFormat::Raw {
        let file_len = file.metadata().await.map_err(map_io_error)?.len();
        format = stored_blob_format(&peek[..read], file_len, logical_size);
    }
    let whole = range_start == 0 && content_length == logical_size;

    let stream = match format {
        BlobFormat::Nosd => {
            let file = file.into_std().await;
            let data_dir = ctx.data_dir.clone();
            let reader = tokio::task::spawn_blocking(move || {
                DedupBlobReader::from_file(file, &data_dir, logical_size, range_start, content_length)
            })
            .await
            .map_err(internal)??;
            stream_chunks(reader)
        }
        BlobFormat::Raw => {
            if ctx.verify_on_read
                && whole
                && let Some(expected) = ctx.expected_etag.as_deref()
            {
                let copy = file.try_clone().await.map_err(map_io_error)?.into_std().await;
                let expected = expected.to_string();
                tokio::task::spawn_blocking(move || verify_raw_etag(copy, &expected))
                    .await
                    .map_err(internal)??;
            }
            file.seek(std::io::SeekFrom::Start(range_start))
                .await
                .map_err(map_io_error)?;
            let pooled = PooledFileRead::new(file, ctx.buffer_pool.clone());
            ObjectBodyStream::PooledLimited(ReaderStream::new(LimitedAsyncRead::new(
                pooled,
                0,
                content_length,
            )))
        }
        BlobFormat::Nosb | BlobFormat::Nosi => {
            let file = file.into_std().await;
            let path = blob_path.to_path_buf();
            let read_ctx = IndexedReadContext {
                dict: ctx.dict.clone(),
                data_dir: ctx.data_dir.clone(),
                block_cache: ctx.block_cache.clone(),
            };
            let reader = tokio::task::spawn_blocking(move || {
                IndexedBlobReader::from_file(file, &path, logical_size, range_start, content_length, read_ctx)
            })
            .await
            .map_err(internal)??;
            stream_chunks(reader)
        }
        BlobFormat::Nosz | BlobFormat::Nos2 => {
            let file = file.into_std().await;
            let dict = ctx.dict.clone();
            let reader = tokio::task::spawn_blocking(move || {
                ZstdBlobReader::from_file(file, logical_size, format, dict.as_deref().map(|d| d.as_slice()))
            })
            .await
            .map_err(internal)?
            .map_err(map_io_error)?;
            if whole {
                stream_chunks(reader)
            } else {
                // Human: A legacy zstd stream can't seek, so decode up to the range and drop what precedes it —
                // this used to decode the whole object into memory (plus a spill file) for every range request.
                stream_chunks(RangeOf {
                    inner: reader,
                    skip: range_start,
                    remaining: content_length,
                })
            }
        }
    };
    Ok(GuardedObjectBodyStream {
        stream,
        _spill_guard: None,
    })
}

/// Read until `buf` is full or the file ends (a single read may return fewer bytes).
async fn read_up_to(file: &mut File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = file.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

/// Human: xxh3 hex digest of on-disk file bytes (used for scrub and wire checksum).
/// Agent: Blocking read with configurable buffer; returns 16-char lowercase hex.
pub fn hash_file_xxh3_hex(path: &Path, buffer_size: usize) -> Result<String, StorageError> {
    let file = std::fs::File::open(path).map_err(map_io_error)?;
    hash_reader_xxh3_hex(file, buffer_size)
}

/// xxh3 hex digest of everything `reader` yields.
pub fn hash_reader_xxh3_hex(mut reader: impl Read, buffer_size: usize) -> Result<String, StorageError> {
    use xxhash_rust::xxh3::Xxh3;

    let mut hasher = Xxh3::new();
    let mut buf = vec![0u8; buffer_size.max(4096)];
    loop {
        let n = reader.read(&mut buf).map_err(map_io_error)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:016x}", hasher.digest()))
}

/// Check a raw blob, read through an open handle from its start, against its ETag.
fn verify_raw_etag(mut file: std::fs::File, expected: &str) -> Result<(), StorageError> {
    file.seek(std::io::SeekFrom::Start(0)).map_err(map_io_error)?;
    let actual = hash_reader_xxh3_hex(file, 256 * 1024)?;
    if actual != expected {
        return Err(internal(anyhow::anyhow!("raw blob etag mismatch on read")));
    }
    Ok(())
}

/// Human: AsyncRead that serves file bytes using a pooled buffer (fewer allocations on GET).
/// Agent: WRAPS File; refills pooled cache; copies into caller ReadBuf per poll.
pub struct PooledFileRead {
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

/// Human: A blocking reader of an object's bytes that can be stepped one chunk at a time.
trait ChunkSource: Send + 'static {
    /// The next chunk, or `None` at the end.
    fn read_chunk(&mut self) -> std::io::Result<Option<Bytes>>;
}

impl ChunkSource for IndexedBlobReader {
    fn read_chunk(&mut self) -> std::io::Result<Option<Bytes>> {
        self.next_chunk()
            .map_err(|e| std::io::Error::other(e.to_string()))
    }
}

/// Decoded chunks (blocks, up to 1 MiB by default) a download may read ahead of its client — the memory a
/// stalled download holds here, on top of the chunk the HTTP layer is writing.
const READ_AHEAD: usize = 2;

/// Chunks one blocking step may read before handing its thread back, so a download that keeps pace with the
/// decoder doesn't hold a pool thread for its whole transfer while fsyncs and uploads queue behind it.
const CHUNKS_PER_STEP: usize = 16;

type ChunkSender = tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>;
type ChunkPermit = tokio::sync::mpsc::OwnedPermit<Result<Bytes, std::io::Error>>;

/// Human: Stream a blocking source without tying a blocking-pool thread to the client. A blocking step reads
/// while the channel has room and hands its thread back once the channel is full; the pump then waits for the
/// client asynchronously. The old pumps parked a thread per download until the client had read everything, so
/// a few hundred slow downloads exhausted the pool and stalled all file I/O in the process.
/// Agent: fast clients keep one step running continuously (no per-chunk handoff); the pump ends when the
/// response body (receiver) is dropped.
fn stream_chunks(source: impl ChunkSource) -> ObjectBodyStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(READ_AHEAD);
    tokio::spawn(async move {
        let errors = tx.clone();
        let (mut source, mut tx) = (source, tx);
        loop {
            let Ok(permit) = tx.reserve_owned().await else {
                return;
            };
            match tokio::task::spawn_blocking(move || fill_while_room(source, permit)).await {
                Ok((returned, Some(sender))) => (source, tx) = (returned, sender),
                Ok((_, None)) => return,
                Err(e) => {
                    let _ = errors.send(Err(std::io::Error::other(e))).await;
                    return;
                }
            }
        }
    });
    ObjectBodyStream::Channel(tokio_stream::wrappers::ReceiverStream::new(rx))
}

/// Human: Read chunks into the channel until it is full (or CHUNKS_PER_STEP were read), then give the thread back.
/// Agent: RETURNS the sender to wait for room with, or None once the stream is over (end, error, or the
/// receiver was dropped).
fn fill_while_room<S: ChunkSource>(mut source: S, mut permit: ChunkPermit) -> (S, Option<ChunkSender>) {
    use tokio::sync::mpsc::error::TrySendError;
    for _ in 0..CHUNKS_PER_STEP {
        let sender = match source.read_chunk() {
            Ok(Some(chunk)) => permit.send(Ok(chunk)),
            Ok(None) => return (source, None),
            Err(e) => {
                permit.send(Err(e));
                return (source, None);
            }
        };
        permit = match sender.try_reserve_owned() {
            Ok(permit) => permit,
            Err(TrySendError::Full(sender)) => return (source, Some(sender)),
            Err(TrySendError::Closed(_)) => return (source, None),
        };
    }
    // Human: Dropping the unused permit frees its slot; the pump reserves again before the next step.
    let sender = permit.release();
    (source, Some(sender))
}

/// Human: `remaining` bytes of a source after dropping its first `skip` bytes (ranges of unseekable streams).
struct RangeOf<S> {
    inner: S,
    skip: u64,
    remaining: u64,
}

impl<S: ChunkSource> ChunkSource for RangeOf<S> {
    fn read_chunk(&mut self) -> std::io::Result<Option<Bytes>> {
        while self.remaining > 0 {
            let Some(mut chunk) = self.inner.read_chunk()? else {
                return Err(std::io::Error::other("stream ended before the requested range"));
            };
            if self.skip >= chunk.len() as u64 {
                self.skip -= chunk.len() as u64;
                continue;
            }
            let mut tail = chunk.split_off(self.skip as usize);
            self.skip = 0;
            tail.truncate(tail.len().min(self.remaining as usize));
            self.remaining -= tail.len() as u64;
            return Ok(Some(tail));
        }
        Ok(None)
    }
}

/// Human: A legacy NOSD dedup manifest, served one referenced block at a time; a range starts at the block that
/// holds its first byte. This used to rebuild the whole object in a spill file for every GET, ranges included.
struct DedupBlobReader {
    store: BlockStore,
    blocks: std::vec::IntoIter<(u64, u32)>,
    skip: usize,
    remaining: u64,
}

impl DedupBlobReader {
    fn from_file(
        mut file: std::fs::File,
        data_dir: &str,
        logical_size: u64,
        range_start: u64,
        length: u64,
    ) -> Result<Self, StorageError> {
        file.seek(std::io::SeekFrom::Start(0)).map_err(map_io_error)?;
        let mut manifest = Vec::new();
        file.read_to_end(&mut manifest).map_err(map_io_error)?;
        let mut blocks = super::compression::parse_dedup_manifest(&manifest, logical_size)?;
        let mut offset = 0u64;
        let first = blocks
            .iter()
            .position(|(_, size)| {
                let end = offset + u64::from(*size);
                let holds_start = range_start < end;
                if !holds_start {
                    offset = end;
                }
                holds_start
            })
            .unwrap_or(blocks.len());
        Ok(Self {
            store: BlockStore::new(data_dir),
            blocks: {
                blocks.drain(..first);
                blocks.into_iter()
            },
            skip: (range_start - offset) as usize,
            remaining: length,
        })
    }
}

impl ChunkSource for DedupBlobReader {
    fn read_chunk(&mut self) -> std::io::Result<Option<Bytes>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let (hash, size) = self
            .blocks
            .next()
            .ok_or_else(|| std::io::Error::other("dedup manifest ends before the requested range"))?;
        let block = self
            .store
            .read_logical_block(hash, size as usize)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let start = std::mem::take(&mut self.skip).min(block.len());
        let take = ((block.len() - start) as u64).min(self.remaining) as usize;
        self.remaining -= take as u64;
        Ok(Some(if start == 0 && take == block.len() {
            Bytes::from(block)
        } else {
            Bytes::copy_from_slice(&block[start..start + take])
        }))
    }
}

/// Human: A legacy NOSZ/NOS2 blob, decoded as one zstd stream in 256 KiB steps.
/// Agent: Errors if the stream ends short of, or runs past, the object's logical size.
struct ZstdBlobReader {
    decoder: zstd::stream::read::Decoder<'static, std::io::BufReader<std::fs::File>>,
    buf: Vec<u8>,
    remaining: u64,
}

impl ZstdBlobReader {
    fn from_file(
        mut file: std::fs::File,
        logical_size: u64,
        format: BlobFormat,
        dict: Option<&[u8]>,
    ) -> std::io::Result<Self> {
        file.seek(std::io::SeekFrom::Start(0))?;
        let stored = read_blob_header_size(file.try_clone()?)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        if stored != logical_size {
            return Err(std::io::Error::other("blob header size mismatch"));
        }
        let header_len = match format {
            BlobFormat::Nosz => HEADER_LEN,
            BlobFormat::Nos2 => HEADER_LEN_V2,
            _ => return Err(std::io::Error::other("not a zstd blob")),
        };
        // Human: The frame header names the dictionary it was compressed with (18 bytes is its longest form).
        file.seek(std::io::SeekFrom::Start(header_len as u64))?;
        let mut frame_head = Vec::with_capacity(18);
        (&mut file).take(18).read_to_end(&mut frame_head)?;
        file.seek(std::io::SeekFrom::Start(header_len as u64))?;
        let dict = crate::storage::dict_store::dictionary_for_frame(&frame_head, dict)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let reader = std::io::BufReader::new(file);
        let decoder = match dict {
            Some(d) => zstd::stream::read::Decoder::with_dictionary(reader, &d)?,
            None => zstd::stream::read::Decoder::with_buffer(reader)?,
        };
        Ok(Self {
            decoder,
            buf: vec![0u8; 256 * 1024],
            remaining: logical_size,
        })
    }
}

impl ChunkSource for ZstdBlobReader {
    fn read_chunk(&mut self) -> std::io::Result<Option<Bytes>> {
        let n = self.decoder.read(&mut self.buf)?;
        if n == 0 {
            return if self.remaining == 0 {
                Ok(None)
            } else {
                Err(std::io::Error::other("zstd stream ended before the object's size"))
            };
        }
        if n as u64 > self.remaining {
            return Err(std::io::Error::other("zstd stream is longer than the object"));
        }
        self.remaining -= n as u64;
        Ok(Some(Bytes::copy_from_slice(&self.buf[..n])))
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

/// Human: Stream a multipart file field into `dest` while hashing it, so replicated objects of any size never
/// sit in memory. `max_len` (the event's declared size, when known first) stops oversized bodies early.
/// Agent: RETURNS (bytes written, xxh3 hex); caller owns `dest` cleanup.
pub async fn receive_multipart_blob_field(
    field: axum::extract::multipart::Field<'_>,
    dest: &Path,
    max_len: Option<u64>,
) -> Result<(u64, String), StorageError> {
    use futures_util::{StreamExt, TryStreamExt};
    use xxhash_rust::xxh3::Xxh3;

    let mut file = fs::File::create(dest).await.map_err(map_io_error)?;
    let mut hasher = Xxh3::new();
    let mut written = 0u64;
    let mut stream = field.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| internal(anyhow::anyhow!(e)))?;
        written += chunk.len() as u64;
        if max_len.is_some_and(|max| written > max) {
            return Err(internal(anyhow::anyhow!("replication payload larger than its event size")));
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(map_io_error)?;
    }
    file.flush().await.map_err(map_io_error)?;
    Ok((written, format!("{:016x}", hasher.digest())))
}

pub fn verify_wire_checksum(bytes: &[u8], expected: &str) -> Result<(), StorageError> {
    use xxhash_rust::xxh3::xxh3_64;
    // Human: Objects without an ETag (e.g. copies of legacy rows) carry no checksum to compare.
    if expected.is_empty() {
        return Ok(());
    }
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

#[allow(deprecated)]
pub use super::blob_finalize::finalize_temp_to_blob;
pub use super::blob_finalize::{stage_temp_blob, BlobFinalizeOptions, ReadContext, StagedBlob};

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::StreamExt;

    use super::*;
    use crate::storage::blob_finalize::ReadContext;
    use crate::storage::compressibility::{CompressionContext, DEFAULT_MIN_COMPRESSIBLE_SIZE};
    use crate::storage::compression::{compress_blob, is_indexed_blob, EncodeOptions};

    fn legacy_zstd_blob(logical: &[u8], header_size: u64) -> Vec<u8> {
        let mut blob = crate::storage::compression::BLOB_MAGIC.to_vec();
        blob.extend_from_slice(&header_size.to_le_bytes());
        blob.extend_from_slice(&zstd::encode_all(logical, 1).unwrap());
        blob
    }

    async fn collect(mut body: GuardedObjectBodyStream) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn dedup_manifests_stream_block_by_block() {
        use crate::storage::blocks::BlockStore;
        use crate::storage::compression::DEDUP_MAGIC;

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_string_lossy().to_string();
        let store = BlockStore::new(&data_dir);
        let blocks: [&[u8]; 3] = [b"hello", b" dedup ", b"blocks"];
        let logical: Vec<u8> = blocks.concat();
        let mut manifest = DEDUP_MAGIC.to_vec();
        manifest.extend_from_slice(&(logical.len() as u64).to_le_bytes());
        manifest.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
        for block in blocks {
            let hash = store.write_logical_block(block, 3).unwrap();
            manifest.extend_from_slice(&hash.to_le_bytes());
            manifest.extend_from_slice(&(block.len() as u32).to_le_bytes());
        }
        let path = dir.path().join("manifest");
        std::fs::write(&path, &manifest).unwrap();
        let ctx = ReadContext::for_data_dir(&data_dir);
        let size = logical.len() as u64;
        for (start, len) in [(0, size), (5, 7), (3, 6), (12, 6), (size - 1, 1)] {
            let body = open_object_body_stream(&path, size, start, len, &ctx).await.unwrap();
            assert_eq!(collect(body).await, logical[start as usize..(start + len) as usize], "range {start}+{len}");
        }

        // Human: A damaged block fails the read instead of being served.
        let damaged = store.block_path(BlockStore::hash_block(b" dedup "));
        std::fs::write(&damaged, b" DEDUP ").unwrap();
        let mut body = open_object_body_stream(&path, size, 0, size, &ctx).await.unwrap();
        let mut failed = false;
        while let Some(chunk) = body.next().await {
            failed |= chunk.is_err();
        }
        assert!(failed);
    }

    #[tokio::test]
    async fn legacy_zstd_ranges_stream_without_a_spill_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".tmp")).unwrap();
        let logical: Vec<u8> = (0..700_000u32).map(|i| (i % 251) as u8).collect();
        let size = logical.len() as u64;
        let path = dir.path().join("legacy");
        std::fs::write(&path, legacy_zstd_blob(&logical, size)).unwrap();
        let ctx = ReadContext::for_data_dir(&dir.path().to_string_lossy());
        // Human: Ranges inside, across and exactly on the decoder's 256 KiB chunk boundaries, and at the end.
        for (start, len) in [(0, 10), (5, 300_000), (262_143, 2), (262_144, 262_144), (699_000, 1_000)] {
            let body = open_object_body_stream(&path, size, start, len, &ctx).await.unwrap();
            assert!(
                collect(body).await == logical[start as usize..(start + len) as usize],
                "range {start}+{len}"
            );
        }
        assert_eq!(std::fs::read_dir(dir.path().join(".tmp")).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn legacy_zstd_stream_shorter_than_its_header_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy");
        std::fs::write(&path, legacy_zstd_blob(&[7u8; 900], 1_000)).unwrap();
        let ctx = ReadContext::for_data_dir(&dir.path().to_string_lossy());
        let mut body = open_object_body_stream(&path, 1_000, 0, 1_000, &ctx).await.unwrap();
        let mut got = 0;
        let mut failed = false;
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(c) => got += c.len(),
                Err(_) => failed = true,
            }
        }
        assert!(failed, "a short stream must end in an error, not a silently short body ({got} bytes)");

        // Human: A header that disagrees with the metadata is refused before any byte is sent.
        assert!(open_object_body_stream(&path, 999, 0, 999, &ctx).await.is_err());
    }

    /// Human: Downloads whose clients stop reading must not pin blocking-pool threads: with the pool
    /// exhausted, every file operation in the process (uploads, fsync, tokio::fs) waits behind them.
    #[test]
    fn stalled_downloads_leave_the_blocking_pool_free() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let payload: Vec<u8> = (0..2_000_000u32).map(|i| (i % 251) as u8).collect();
            let ctx = CompressionContext::new(
                Some("data/log.txt"),
                Some("text/plain"),
                payload.len() as u64,
                DEFAULT_MIN_COMPRESSIBLE_SIZE,
                &[],
            );
            let blob = compress_blob(&payload, 1, 16 * 1024, ctx, EncodeOptions::default()).unwrap();
            assert!(is_indexed_blob(&blob));
            let indexed = dir.path().join("indexed");
            std::fs::write(&indexed, &blob).unwrap();
            let legacy = dir.path().join("legacy");
            std::fs::write(&legacy, legacy_zstd_blob(&payload, payload.len() as u64)).unwrap();
            let read_ctx = ReadContext::for_data_dir(&dir.path().to_string_lossy());
            let size = payload.len() as u64;

            // Human: Three downloads (full, range, legacy zstd) on a pool of two threads, none of them read.
            let mut stalled = Vec::new();
            for (path, start, len) in [(&indexed, 0, size), (&indexed, 1_000, size - 2_000), (&legacy, 0, size)] {
                let (body, first) = tokio::time::timeout(Duration::from_secs(5), async {
                    let mut body = open_object_body_stream(path, size, start, len, &read_ctx)
                        .await
                        .unwrap();
                    let first = body.next().await.unwrap().unwrap();
                    (body, first)
                })
                .await
                .expect("blocking pool exhausted by stalled downloads");
                stalled.push((body, start, first));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;

            let other = tokio::time::timeout(Duration::from_secs(5), tokio::task::spawn_blocking(|| 7)).await;
            assert_eq!(
                other.expect("blocking pool exhausted by two stalled downloads").unwrap(),
                7
            );

            for (mut body, start, first) in stalled {
                let mut got = first.to_vec();
                while let Some(chunk) = body.next().await {
                    got.extend_from_slice(&chunk.unwrap());
                }
                assert_eq!(got.len(), (size - 2 * start) as usize);
                assert_eq!(&got[..], &payload[start as usize..(size - start) as usize]);
            }
        });
    }
}
