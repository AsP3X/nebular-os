use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use super::compression::clamp_zstd_level;
use super::error::{internal, StorageError};

/// Human: On-disk zstd training dictionary cache (`.dict/{id}.zdict`).
/// Agent: LOAD/SAVE under NOS_DATA_DIR; id 0 = global dict for NOS_ZSTD_DICT_ENABLED uploads.
pub struct DictStore {
    data_dir: String,
    cache: RwLock<Option<Arc<Vec<u8>>>>,
}

impl DictStore {
    pub fn new(data_dir: &str) -> Self {
        Self {
            data_dir: data_dir.to_string(),
            cache: RwLock::new(None),
        }
    }

    pub fn dict_path(&self, id: u16) -> PathBuf {
        PathBuf::from(&self.data_dir)
            .join(".dict")
            .join(format!("{id}.zdict"))
    }

    pub fn load(&self, id: u16) -> Result<Option<Arc<Vec<u8>>>, StorageError> {
        if id == 0
            && let Ok(guard) = self.cache.read()
            && let Some(dict) = guard.as_ref()
        {
            return Ok(Some(dict.clone()));
        }
        let path = self.dict_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let arc = Arc::new(bytes);
        if id == 0 && let Ok(mut guard) = self.cache.write() {
            *guard = Some(arc.clone());
        }
        Ok(Some(arc))
    }

    pub fn save(&self, id: u16, dict: &[u8]) -> Result<(), StorageError> {
        let path = self.dict_path(id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
        std::fs::write(&path, dict).map_err(|e| internal(anyhow::anyhow!(e)))?;
        if id == 0 && let Ok(mut guard) = self.cache.write() {
            *guard = Some(Arc::new(dict.to_vec()));
        }
        Ok(())
    }

    /// Train a dictionary from logical object samples and persist when non-empty.
    pub fn train_and_save(
        &self,
        id: u16,
        samples: &[Vec<u8>],
        max_dict_bytes: usize,
        level: i32,
    ) -> Result<bool, StorageError> {
        if samples.len() < 2 {
            return Ok(false);
        }
        let total: usize = samples.iter().map(|s| s.len()).sum();
        if total < 256 {
            return Ok(false);
        }
        let max_size = max_dict_bytes.min(total.saturating_mul(3) / 10).max(256);
        let dict = zstd::dict::from_samples(samples, max_size)
            .map_err(|e| internal(anyhow::anyhow!("dict training failed: {e}")))?;
        if dict.is_empty() {
            return Ok(false);
        }
        let _level = clamp_zstd_level(level);
        let _ = _level;
        self.save(id, &dict)?;
        Ok(true)
    }

    pub fn global_dict(&self) -> Option<Arc<Vec<u8>>> {
        self.load(0).ok().flatten()
    }

    pub fn dict_dir(&self) -> PathBuf {
        PathBuf::from(&self.data_dir).join(".dict")
    }

    pub fn exists_on_disk(&self, id: u16) -> bool {
        self.dict_path(id).exists()
    }

    pub fn remove(&self, id: u16) -> Result<(), StorageError> {
        let path = self.dict_path(id);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
        if id == 0 && let Ok(mut guard) = self.cache.write() {
            *guard = None;
        }
        Ok(())
    }
}

impl Clone for DictStore {
    fn clone(&self) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            cache: RwLock::new(
                self.cache
                    .read()
                    .ok()
                    .and_then(|g| g.as_ref().map(|d| d.clone())),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = DictStore::new(tmp.path().to_str().unwrap());
        store.save(0, b"fake-dict-bytes").unwrap();
        let loaded = store.load(0).unwrap().unwrap();
        assert_eq!(&loaded[..], b"fake-dict-bytes");
    }

    #[test]
    fn train_from_repetitive_samples() {
        let tmp = TempDir::new().unwrap();
        let store = DictStore::new(tmp.path().to_str().unwrap());
        let samples: Vec<Vec<u8>> = (0..8)
            .map(|i| format!("COMMON_PREFIX log entry {i} with shared vocabulary\n").repeat(120))
            .map(|s| s.into_bytes())
            .collect();
        assert!(store.train_and_save(0, &samples, 4096, 3).unwrap());
        assert!(store.exists_on_disk(0));
    }
}
