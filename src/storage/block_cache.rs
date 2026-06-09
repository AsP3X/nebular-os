use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;

/// Human: LRU of decoded logical blocks for range GET on indexed blobs.
#[derive(Clone)]
pub struct BlockDecodeCache {
    inner: Arc<Mutex<LruCache<(String, usize), Arc<Vec<u8>>>>>,
}

impl BlockDecodeCache {
    pub fn new(capacity: usize) -> Option<Self> {
        let cap = NonZeroUsize::new(capacity.max(1))?;
        Some(Self {
            inner: Arc::new(Mutex::new(LruCache::new(cap))),
        })
    }

    pub fn get(&self, blob_path: &str, block_idx: usize) -> Option<Arc<Vec<u8>>> {
        let mut guard = self.inner.lock().ok()?;
        guard.get(&(blob_path.to_string(), block_idx)).cloned()
    }

    pub fn insert(&self, blob_path: &str, block_idx: usize, block: Vec<u8>) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.put((blob_path.to_string(), block_idx), Arc::new(block));
        }
    }
}
