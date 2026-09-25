use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use sqlx::Pool;
use sqlx::Sqlite;
use xxhash_rust::xxh3::xxh3_64;

use super::compression::{
    clamp_zstd_level, collect_dedup_refs, detect_blob_format, parse_dedup_manifest, BlobFormat,
    DEDUP_ENTRY_LEN, DEDUP_HEADER_LEN, DEDUP_MAGIC, FIXED_HEADER_LEN_V1, NOSI_FLAG_DEDUP,
};
use super::error::{internal, StorageError};

/// Human: Optional zstd wrapper on `.blocks/` payloads (logical bytes hashed; NOSK on disk).
pub const BLOCK_CHUNK_MAGIC: &[u8; 4] = b"NOSK";
pub const BLOCK_CHUNK_HEADER_LEN: usize = 8;

/// Stripes serializing, per block hash, a reuse check against GC deleting the same block file.
static BLOCK_FILE_LOCKS: LazyLock<Vec<Mutex<()>>> = LazyLock::new(|| (0..256).map(|_| Mutex::new(())).collect());

fn block_file_lock(hash: u64) -> MutexGuard<'static, ()> {
    BLOCK_FILE_LOCKS[(hash % 256) as usize]
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Human: Content-addressed block files under `.blocks/` with SQLite refcounts.
/// A block whose refcount drops to zero is only deleted by `gc_released_blocks`, after a grace period and only if
/// no upload reused it meanwhile — an upload counts its refs after encoding, so deleting at once could remove a
/// block it had just decided to share.
/// Agent: WRITES NOSD manifest blobs; INCREMENT/DECREMENT dedup_blocks on share/release.
#[derive(Clone)]
pub struct BlockStore {
    data_dir: String,
}

impl BlockStore {
    pub fn new(data_dir: &str) -> Self {
        Self {
            data_dir: data_dir.to_string(),
        }
    }

    pub fn block_path(&self, hash: u64) -> PathBuf {
        let hex = format!("{:016x}", hash);
        PathBuf::from(&self.data_dir)
            .join(".blocks")
            .join(&hex[..2])
            .join(hex)
    }

    pub fn hash_block(data: &[u8]) -> u64 {
        xxh3_64(data)
    }

    /// Store logical chunk under content hash; compresses when smaller than raw.
    pub fn write_logical_block(
        &self,
        chunk: &[u8],
        zstd_level: i32,
    ) -> Result<u64, StorageError> {
        self.store_block(chunk, zstd_level)?.ok_or_else(|| {
            internal(anyhow::anyhow!("a different block is stored under this chunk's hash"))
        })
    }

    /// Human: Store `chunk` under its content hash, or share the stored copy — after checking it holds exactly
    /// these bytes. `None` means a different block already has this hash (a hash collision, possibly crafted, or
    /// a damaged file): the caller then keeps the chunk in the object itself instead of sharing it.
    /// Agent: A shared block's mtime is refreshed so the zero-ref GC grace period restarts; new blocks are
    /// written to a temp name and renamed, so a crash never leaves a partial block under a hash.
    pub fn store_block(&self, chunk: &[u8], zstd_level: i32) -> Result<Option<u64>, StorageError> {
        let hash = Self::hash_block(chunk);
        let path = self.block_path(hash);
        let _guard = block_file_lock(hash);
        if path.exists() {
            let same = self
                .read_logical_block(hash, chunk.len())
                .is_ok_and(|existing| existing == chunk);
            if !same {
                return Ok(None);
            }
            let _ = File::options()
                .write(true)
                .open(&path)
                .and_then(|f| f.set_modified(SystemTime::now()));
            return Ok(Some(hash));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
        let level = clamp_zstd_level(zstd_level);
        let on_disk = if chunk.len() >= 64 {
            match zstd::encode_all(chunk, level) {
                Ok(compressed) if compressed.len() < chunk.len() => {
                    let mut out = Vec::with_capacity(BLOCK_CHUNK_HEADER_LEN + compressed.len());
                    out.extend_from_slice(BLOCK_CHUNK_MAGIC);
                    out.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
                    out.extend_from_slice(&compressed);
                    out
                }
                _ => chunk.to_vec(),
            }
        } else {
            chunk.to_vec()
        };
        let partial = path.with_extension(format!("partial-{}", uuid::Uuid::new_v4()));
        std::fs::write(&partial, &on_disk)
            .and_then(|()| std::fs::rename(&partial, &path))
            .map_err(|e| {
                let _ = std::fs::remove_file(&partial);
                internal(anyhow::anyhow!(e))
            })?;
        Ok(Some(hash))
    }

    /// Read logical bytes for a content-addressed block (raw or NOSK-wrapped).
    /// Human: A block's logical bytes, checked against the content hash it is stored under (the address is the
    /// checksum), so a damaged or swapped block file is reported instead of served.
    /// Agent: Decompression is bounded by `expected_len` (a corrupt frame can't expand without limit).
    pub fn read_logical_block(&self, hash: u64, expected_len: usize) -> Result<Vec<u8>, StorageError> {
        let path = self.block_path(hash);
        let data = std::fs::read(&path).map_err(|e| {
            internal(anyhow::anyhow!("missing dedup block {hash:016x}: {e}"))
        })?;
        let block = if data.len() >= BLOCK_CHUNK_HEADER_LEN && data.starts_with(BLOCK_CHUNK_MAGIC) {
            let logical_len =
                u32::from_le_bytes(data[4..8].try_into().map_err(|_| {
                    internal(anyhow::anyhow!("invalid block chunk header"))
                })?) as usize;
            if logical_len != expected_len {
                return Err(internal(anyhow::anyhow!(
                    "dedup block logical size mismatch for {hash:016x}"
                )));
            }
            let mut decoded = Vec::with_capacity(expected_len);
            zstd::stream::read::Decoder::new(&data[BLOCK_CHUNK_HEADER_LEN..])
                .map_err(internal)?
                .take(expected_len as u64 + 1)
                .read_to_end(&mut decoded)
                .map_err(internal)?;
            decoded
        } else {
            data
        };
        if block.len() != expected_len {
            return Err(internal(anyhow::anyhow!(
                "dedup block size mismatch for {hash:016x}"
            )));
        }
        if Self::hash_block(&block) != hash {
            return Err(internal(anyhow::anyhow!(
                "dedup block {hash:016x} does not match its content hash"
            )));
        }
        Ok(block)
    }

    /// Chunk `tmp_path` into blocks, write manifest to `final_path`.
    pub fn write_dedup_from_file(
        &self,
        tmp_path: &Path,
        final_path: &Path,
        logical_size: u64,
        block_size: usize,
    ) -> Result<Vec<(u64, u32)>, StorageError> {
        let block_size = block_size.max(4096);
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
        std::fs::create_dir_all(PathBuf::from(&self.data_dir).join(".blocks"))
            .map_err(|e| internal(anyhow::anyhow!(e)))?;

        let mut infile = File::open(tmp_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        let mut buf = vec![0u8; block_size];
        let mut entries: Vec<(u64, u32)> = Vec::new();

        loop {
            let n = infile.read(&mut buf).map_err(|e| internal(anyhow::anyhow!(e)))?;
            if n == 0 {
                break;
            }
            let chunk = &buf[..n];
            let hash = self.write_logical_block(chunk, super::compression::DEFAULT_ZSTD_LEVEL)?;
            entries.push((hash, n as u32));
        }

        let mut manifest = Vec::with_capacity(DEDUP_HEADER_LEN + entries.len() * DEDUP_ENTRY_LEN);
        manifest.extend_from_slice(DEDUP_MAGIC);
        manifest.extend_from_slice(&logical_size.to_le_bytes());
        manifest.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (hash, size) in &entries {
            manifest.extend_from_slice(&hash.to_le_bytes());
            manifest.extend_from_slice(&size.to_le_bytes());
        }

        let part = final_path.with_extension("deduppart");
        std::fs::write(&part, &manifest).map_err(|e| internal(anyhow::anyhow!(e)))?;
        std::fs::rename(&part, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        Ok(entries)
    }

    pub fn assemble_to_file(
        &self,
        manifest_path: &Path,
        out_path: &Path,
        logical_size: u64,
    ) -> Result<(), StorageError> {
        let manifest = std::fs::read(manifest_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        let entries = parse_dedup_manifest(&manifest, logical_size)?;
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
        let mut out = File::create(out_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        for (hash, size) in entries {
            let chunk = self.read_logical_block(hash, size as usize)?;
            std::io::Write::write_all(&mut out, &chunk)
                .map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
        let written = std::fs::metadata(out_path)
            .map_err(|e| internal(anyhow::anyhow!(e)))?
            .len();
        if written != logical_size {
            return Err(internal(anyhow::anyhow!(
                "dedup assemble size mismatch: got {written} expected {logical_size}"
            )));
        }
        Ok(())
    }

    /// Dedup block refs held by a blob; only NOSD manifests and dedup-flagged NOSI blobs are read in full.
    pub fn manifest_entries(blob_path: &Path) -> Result<Vec<(u64, u32)>, StorageError> {
        let mut head = Vec::with_capacity(FIXED_HEADER_LEN_V1);
        File::open(blob_path)
            .and_then(|f| f.take(FIXED_HEADER_LEN_V1 as u64).read_to_end(&mut head))
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
        let may_hold_refs = match detect_blob_format(&head) {
            BlobFormat::Nosd => true,
            BlobFormat::Nosi => {
                head.len() >= FIXED_HEADER_LEN_V1
                    && u16::from_le_bytes([head[22], head[23]]) & NOSI_FLAG_DEDUP != 0
            }
            _ => false,
        };
        if !may_hold_refs {
            return Ok(Vec::new());
        }
        let data = std::fs::read(blob_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        collect_dedup_refs(&data)
    }

    pub async fn inc_refs(
        pool: &Pool<Sqlite>,
        entries: &[(u64, u32)],
    ) -> Result<(), StorageError> {
        for (hash, size) in entries {
            let hex = format!("{hash:016x}");
            sqlx::query(
                "INSERT INTO dedup_blocks (hash, size, refcount, released_at) VALUES (?, ?, 1, NULL)
                 ON CONFLICT(hash) DO UPDATE SET refcount = refcount + 1, released_at = NULL",
            )
            .bind(&hex)
            .bind(*size as i64)
            .execute(pool)
            .await
            .map_err(internal)?;
        }
        Ok(())
    }

    /// Human: Drop one reference per entry. A block that reaches zero stays on disk until `gc_released_blocks`
    /// removes it (see the type docs); the decrement is one statement, so concurrent releases can't lose counts.
    pub async fn dec_refs(
        pool: &Pool<Sqlite>,
        _data_dir: &str,
        entries: &[(u64, u32)],
    ) -> Result<(), StorageError> {
        let now = unix_now();
        for (hash, _size) in entries {
            sqlx::query(
                "UPDATE dedup_blocks SET refcount = refcount - 1, \
                 released_at = CASE WHEN refcount <= 1 THEN ? ELSE released_at END \
                 WHERE hash = ? AND refcount > 0",
            )
            .bind(now)
            .bind(format!("{hash:016x}"))
            .execute(pool)
            .await
            .map_err(internal)?;
        }
        Ok(())
    }

    /// Human: Delete up to `limit` blocks nothing references, once they've been unreferenced for `grace` and no
    /// upload shared them since (sharing refreshes the file's mtime). RETURNS the number of blocks removed.
    /// Agent: FILE removed under the block's lock after the mtime check; the row goes only while still at zero.
    pub async fn gc_released_blocks(
        pool: &Pool<Sqlite>,
        data_dir: &str,
        grace: Duration,
        limit: i64,
    ) -> Result<u64, StorageError> {
        let cutoff = unix_now() - grace.as_secs() as i64;
        let released: Vec<(String,)> = sqlx::query_as(
            "SELECT hash FROM dedup_blocks WHERE refcount <= 0 AND released_at IS NOT NULL AND released_at < ? LIMIT ?",
        )
        .bind(cutoff)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(internal)?;
        let store = BlockStore::new(data_dir);
        let cutoff_time = SystemTime::UNIX_EPOCH + Duration::from_secs(cutoff.max(0) as u64);
        let mut removed = 0;
        for (hex,) in released {
            let Ok(hash) = u64::from_str_radix(&hex, 16) else {
                continue;
            };
            let path = store.block_path(hash);
            let deleted = {
                let _guard = block_file_lock(hash);
                let shared_since = std::fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .is_ok_and(|modified| modified > cutoff_time);
                if shared_since {
                    false
                } else {
                    match std::fs::remove_file(&path) {
                        Ok(()) => true,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                        Err(e) => {
                            tracing::warn!(block = %hex, error = %e, "cannot remove released dedup block");
                            false
                        }
                    }
                }
            };
            if deleted {
                sqlx::query("DELETE FROM dedup_blocks WHERE hash = ? AND refcount <= 0")
                    .bind(&hex)
                    .execute(pool)
                    .await
                    .map_err(internal)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub async fn release_blob(
        pool: &Pool<Sqlite>,
        data_dir: &str,
        blob_path: &Path,
    ) -> Result<(), StorageError> {
        // Human: Refs that can't be read are leaked (their blocks stay), rather than making the blob undeletable.
        let entries = match Self::manifest_entries(blob_path) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(path = %blob_path.display(), error = %e, "cannot read dedup refs; leaving them counted");
                return Ok(());
            }
        };
        if entries.is_empty() {
            return Ok(());
        }
        Self::dec_refs(pool, data_dir, &entries).await
    }

    pub async fn init_schema(pool: &Pool<Sqlite>) -> Result<(), StorageError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dedup_blocks (
                hash     TEXT PRIMARY KEY,
                size     INTEGER NOT NULL,
                refcount INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(pool)
        .await
        .map_err(internal)?;
        super::object_meta::add_column_if_missing(pool, "dedup_blocks", "released_at", "INTEGER").await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_dedup_released ON dedup_blocks(released_at) WHERE refcount <= 0",
        )
        .execute(pool)
        .await
        .map_err(internal)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn dedup_write_and_assemble() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().to_str().unwrap();
        let store = BlockStore::new(data_dir);
        let payload = b"block dedup test payload ".repeat(200);
        let src = tmp.path().join("src.bin");
        std::fs::write(&src, &payload).unwrap();
        let final_path = tmp.path().join("manifest.bin");
        store
            .write_dedup_from_file(&src, &final_path, payload.len() as u64, 4096)
            .unwrap();
        let manifest = std::fs::read(&final_path).unwrap();
        assert!(manifest.starts_with(DEDUP_MAGIC));
        let out = tmp.path().join("out.bin");
        store
            .assemble_to_file(&final_path, &out, payload.len() as u64)
            .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), payload);
    }
}
