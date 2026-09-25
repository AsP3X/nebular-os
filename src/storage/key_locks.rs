use std::sync::Arc;

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use xxhash_rust::xxh3::Xxh3;

const STRIPES: usize = 4096;

/// Human: Serializes writers of the same object inside this process — PUT, copy, delete, multipart
/// complete, maintenance rewrites, purge and reconcile — so their check-then-act steps see a stable object.
/// Readers (GET, HEAD) take the shared side while they read the metadata and open the blob, so they never
/// pair one version's metadata with the next version's bytes.
/// Agent: STRIPED by xxh3(bucket, key); callers needing several keys MUST use lock_many (sorted stripes,
/// no deadlock); never acquire a key lock while holding the engine's capacity lock, and never read-lock a
/// key while holding its write lock.
#[derive(Clone)]
pub struct KeyLocks {
    stripes: Arc<Vec<RwLock<()>>>,
}

impl Default for KeyLocks {
    fn default() -> Self {
        Self {
            stripes: Arc::new((0..STRIPES).map(|_| RwLock::new(())).collect()),
        }
    }
}

impl KeyLocks {
    pub(crate) fn stripe(bucket: &str, key: &str) -> usize {
        let mut hasher = Xxh3::new();
        hasher.update(bucket.as_bytes());
        hasher.update(&[0]);
        hasher.update(key.as_bytes());
        (hasher.digest() % STRIPES as u64) as usize
    }

    /// Exclusive access for a writer.
    pub async fn lock(&self, bucket: &str, key: &str) -> RwLockWriteGuard<'_, ()> {
        self.stripes[Self::stripe(bucket, key)].write().await
    }

    /// Shared access for a reader; writers of the key wait until it is released.
    pub async fn read(&self, bucket: &str, key: &str) -> RwLockReadGuard<'_, ()> {
        self.stripes[Self::stripe(bucket, key)].read().await
    }

    /// Locks every listed object of `bucket`, acquiring stripes in ascending order.
    pub async fn lock_many<'a, I, K>(&self, pairs: I) -> Vec<RwLockWriteGuard<'_, ()>>
    where
        I: IntoIterator<Item = (&'a str, K)>,
        K: AsRef<str>,
    {
        let mut stripes: Vec<usize> = pairs
            .into_iter()
            .map(|(bucket, key)| Self::stripe(bucket, key.as_ref()))
            .collect();
        stripes.sort_unstable();
        stripes.dedup();
        let mut guards = Vec::with_capacity(stripes.len());
        for idx in stripes {
            guards.push(self.stripes[idx].write().await);
        }
        guards
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn same_key_is_exclusive_across_clones() {
        let locks = KeyLocks::default();
        let other = locks.clone();
        let guard = locks.lock("b", "k").await;
        let blocked = tokio::time::timeout(Duration::from_millis(50), other.lock("b", "k")).await;
        assert!(blocked.is_err(), "second writer must wait");
        drop(guard);
        assert!(tokio::time::timeout(Duration::from_millis(50), other.lock("b", "k")).await.is_ok());
    }

    #[tokio::test]
    async fn readers_share_and_exclude_writers() {
        let locks = KeyLocks::default();
        let first = locks.read("b", "k").await;
        let second = tokio::time::timeout(Duration::from_millis(50), locks.read("b", "k")).await;
        assert!(second.is_ok(), "readers share the key");
        let writer = tokio::time::timeout(Duration::from_millis(50), locks.lock("b", "k")).await;
        assert!(writer.is_err(), "a writer waits for readers");
        drop((first, second));
        assert!(tokio::time::timeout(Duration::from_millis(50), locks.lock("b", "k")).await.is_ok());
    }

    #[tokio::test]
    async fn lock_many_dedups_shared_stripes() {
        let locks = KeyLocks::default();
        let guards = locks.lock_many([("b", "k"), ("b", "k")]).await;
        assert_eq!(guards.len(), 1);
    }
}
