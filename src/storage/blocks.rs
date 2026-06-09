use std::fs::File;
use std::io::{copy, Read};
use std::path::{Path, PathBuf};

use sqlx::Pool;
use sqlx::Sqlite;
use xxhash_rust::xxh3::xxh3_64;

use super::compression::{
    collect_dedup_refs, parse_dedup_manifest, DEDUP_ENTRY_LEN, DEDUP_HEADER_LEN, DEDUP_MAGIC,
};
use super::error::{internal, StorageError};

/// Human: Content-addressed block files under `.blocks/` with SQLite refcounts.
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
            let hash = Self::hash_block(chunk);
            let path = self.block_path(hash);
            if !path.exists() {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| internal(anyhow::anyhow!(e)))?;
                }
                std::fs::write(&path, chunk).map_err(|e| internal(anyhow::anyhow!(e)))?;
            }
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
            let path = self.block_path(hash);
            let mut block = File::open(&path).map_err(|e| {
                internal(anyhow::anyhow!("missing dedup block {hash:016x}: {e}"))
            })?;
            copy(&mut block, &mut out).map_err(|e| internal(anyhow::anyhow!(e)))?;
            let meta = std::fs::metadata(&path).map_err(|e| internal(anyhow::anyhow!(e)))?;
            if meta.len() != size as u64 {
                return Err(internal(anyhow::anyhow!(
                    "dedup block size mismatch for {hash:016x}"
                )));
            }
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

    pub fn manifest_entries(blob_path: &Path) -> Result<Vec<(u64, u32)>, StorageError> {
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
                "INSERT INTO dedup_blocks (hash, size, refcount) VALUES (?, ?, 1)
                 ON CONFLICT(hash) DO UPDATE SET refcount = refcount + 1",
            )
            .bind(&hex)
            .bind(*size as i64)
            .execute(pool)
            .await
            .map_err(internal)?;
        }
        Ok(())
    }

    pub async fn dec_refs(
        pool: &Pool<Sqlite>,
        data_dir: &str,
        entries: &[(u64, u32)],
    ) -> Result<(), StorageError> {
        let store = BlockStore::new(data_dir);
        for (hash, _size) in entries {
            let hex = format!("{hash:016x}");
            let row: Option<(i64,)> =
                sqlx::query_as("SELECT refcount FROM dedup_blocks WHERE hash = ?")
                    .bind(&hex)
                    .fetch_optional(pool)
                    .await
                    .map_err(internal)?;
            let Some((refcount,)) = row else {
                continue;
            };
            if refcount <= 1 {
                sqlx::query("DELETE FROM dedup_blocks WHERE hash = ?")
                    .bind(&hex)
                    .execute(pool)
                    .await
                    .map_err(internal)?;
                let path = store.block_path(*hash);
                let _ = std::fs::remove_file(path);
            } else {
                sqlx::query(
                    "UPDATE dedup_blocks SET refcount = refcount - 1 WHERE hash = ?",
                )
                .bind(&hex)
                .execute(pool)
                .await
                .map_err(internal)?;
            }
        }
        Ok(())
    }

    pub async fn release_blob(
        pool: &Pool<Sqlite>,
        data_dir: &str,
        blob_path: &Path,
    ) -> Result<(), StorageError> {
        let entries = Self::manifest_entries(blob_path)?;
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
