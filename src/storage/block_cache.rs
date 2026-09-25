use std::sync::{Arc, Mutex};

use hashlink::LruCache;

/// Blob path, block index, and a content tag for the block (stored checksum or payload hash).
type BlockKey = (String, usize, u64);

/// Default byte budget for decoded blocks (NOS_BLOCK_CACHE_MAX_BYTES).
pub const DEFAULT_BLOCK_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

struct Inner {
    /// Unbounded map; `capacity` and `max_bytes` are enforced on insert so evicted bytes are accounted for.
    entries: LruCache<BlockKey, Arc<Vec<u8>>>,
    capacity: usize,
    bytes: usize,
    max_bytes: usize,
}

/// Human: LRU of decoded logical blocks for range GET on indexed blobs, bounded by entries *and* bytes
/// (blocks can be many MiB, so an entry count alone doesn't bound memory).
/// Agent: KEYED by a per-block content tag as well as path+index — an overwrite at the same path
/// changes the tags, so stale decodes miss instead of being served; `0` entries disables.
#[derive(Clone)]
pub struct BlockDecodeCache {
    inner: Arc<Mutex<Inner>>,
}

impl BlockDecodeCache {
    pub fn new(capacity: usize) -> Option<Self> {
        Self::with_byte_budget(capacity, DEFAULT_BLOCK_CACHE_MAX_BYTES)
    }

    pub fn with_byte_budget(capacity: usize, max_bytes: usize) -> Option<Self> {
        if capacity == 0 || max_bytes == 0 {
            return None;
        }
        Some(Self {
            inner: Arc::new(Mutex::new(Inner {
                entries: LruCache::new_unbounded(),
                capacity,
                bytes: 0,
                max_bytes,
            })),
        })
    }

    pub fn get(&self, blob_path: &str, block_idx: usize, tag: u64) -> Option<Arc<Vec<u8>>> {
        let mut guard = self.inner.lock().ok()?;
        guard
            .entries
            .get(&(blob_path.to_string(), block_idx, tag))
            .cloned()
    }

    pub fn insert(&self, blob_path: &str, block_idx: usize, tag: u64, block: Vec<u8>) {
        let Ok(mut guard) = self.inner.lock() else {
            return;
        };
        if block.len() > guard.max_bytes {
            return;
        }
        let len = block.len();
        if let Some(replaced) = guard
            .entries
            .insert((blob_path.to_string(), block_idx, tag), Arc::new(block))
        {
            guard.bytes = guard.bytes.saturating_sub(replaced.len());
        }
        guard.bytes += len;
        while guard.bytes > guard.max_bytes || guard.entries.len() > guard.capacity {
            let Some((_, evicted)) = guard.entries.remove_lru() else {
                break;
            };
            guard.bytes = guard.bytes.saturating_sub(evicted.len());
        }
    }

    /// Bytes of decoded blocks currently held.
    pub fn bytes(&self) -> usize {
        self.inner.lock().map(|g| g.bytes).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_capacity_disables_cache() {
        assert!(BlockDecodeCache::new(0).is_none());
        assert!(BlockDecodeCache::new(1).is_some());
        assert!(BlockDecodeCache::with_byte_budget(4, 0).is_none());
    }

    #[test]
    fn content_tag_is_part_of_the_key() {
        let cache = BlockDecodeCache::new(4).unwrap();
        cache.insert("blob", 0, 111, b"old".to_vec());
        assert!(cache.get("blob", 0, 222).is_none());
        assert_eq!(cache.get("blob", 0, 111).unwrap().as_slice(), b"old");
    }

    #[test]
    fn entry_capacity_evicts_least_recently_used() {
        let cache = BlockDecodeCache::with_byte_budget(2, 1_000).unwrap();
        cache.insert("b", 0, 0, vec![0; 4]);
        cache.insert("b", 1, 0, vec![1; 4]);
        assert!(cache.get("b", 0, 0).is_some(), "touching block 0 makes block 1 the oldest");
        cache.insert("b", 2, 0, vec![2; 4]);
        assert!(cache.get("b", 1, 0).is_none());
        assert!(cache.get("b", 0, 0).is_some() && cache.get("b", 2, 0).is_some());
        assert_eq!(cache.bytes(), 8);
    }

    #[test]
    fn byte_budget_evicts_least_recently_used() {
        let cache = BlockDecodeCache::with_byte_budget(100, 10).unwrap();
        cache.insert("b", 0, 0, vec![0; 4]);
        cache.insert("b", 1, 0, vec![1; 4]);
        cache.insert("b", 2, 0, vec![2; 4]);
        assert_eq!(cache.bytes(), 8);
        assert!(cache.get("b", 0, 0).is_none(), "oldest block evicted to fit the budget");
        cache.insert("b", 3, 0, vec![3; 11]);
        assert!(cache.get("b", 3, 0).is_none(), "a block larger than the budget is not cached");
        // Human: Replacing an entry must not double-count its bytes.
        cache.insert("b", 2, 0, vec![2; 4]);
        assert_eq!(cache.bytes(), 8);
    }
}
