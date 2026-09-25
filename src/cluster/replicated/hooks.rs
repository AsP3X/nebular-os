//! Human: The commit hooks through which replication versions every change to a replicated key — inside the
//! key's write lock, so the recorded version always describes the state that was committed (see
//! `crate::storage::CommitHook` and `versions`).

use futures_util::future::BoxFuture;

use crate::cluster::config::ClusterConfig;
use crate::cluster::replication_rules;
use crate::storage::error::StorageError;
use crate::storage::{CommitHook, Committed};

use super::log::{ReplicationEvent, ReplicationLog};
use super::versions::Version;

/// Human: A change made on this node: once committed it gets the key's next version and is queued for peers.
/// Agent: after = record_local_change (version + event, one transaction); SKIPS keys NOS_REPLICATION_*PREFIXES exclude.
pub(crate) struct LocalChange<'a> {
    pub log: &'a ReplicationLog,
    pub cluster: &'a ClusterConfig,
    /// Storage class of a write (a delete uses the deleted object's class when it had one).
    pub storage_class: &'a str,
    pub replication_group: &'a str,
}

impl CommitHook for LocalChange<'_> {
    fn before<'a>(&'a self, _bucket: &'a str, _key: &'a str) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async { Ok(()) })
    }

    fn after<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        committed: Committed<'a>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            if !replication_rules::should_replicate_key(self.cluster, bucket, key) {
                return Ok(());
            }
            self.log
                .record_local_change(bucket, key, committed, self.storage_class, self.replication_group)
                .await
        })
    }
}

/// Human: A change received from a peer. It is applied only when it is newer than this node's version of the
/// key; an older one (a late retry, or a write another node's newer change already replaced) is refused.
/// Agent: `before` refuses with StorageError::PreconditionFailed.
pub(crate) struct RemoteChange<'a> {
    pub log: &'a ReplicationLog,
    pub event: &'a ReplicationEvent,
}

impl CommitHook for RemoteChange<'_> {
    fn before<'a>(&'a self, bucket: &'a str, key: &'a str) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let incoming = self.event.effective_version();
            match self.log.key_version(bucket, key).await? {
                Some(current) if current.version >= incoming => Err(StorageError::PreconditionFailed),
                _ => Ok(()),
            }
        })
    }

    fn after<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        _committed: Committed<'a>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move { self.log.record_remote_change(bucket, key, self.event).await })
    }
}

/// Human: Restoring an object that is missing here from a peer's copy (heal). Refused when this node deleted
/// the key — the peer just hasn't applied the delete yet; the copy's version, when the peer sent it, is
/// recorded unless this node already has a newer one.
/// Agent: before REFUSES (PreconditionFailed) on a tombstone; after STORES `version` only when newer than the key's.
pub(crate) struct Restore<'a> {
    pub log: &'a ReplicationLog,
    pub version: Option<Version>,
}

impl CommitHook for Restore<'_> {
    fn before<'a>(&'a self, bucket: &'a str, key: &'a str) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            match self.log.key_version(bucket, key).await? {
                Some(current) if current.deleted => Err(StorageError::PreconditionFailed),
                _ => Ok(()),
            }
        })
    }

    fn after<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        _committed: Committed<'a>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let Some(version) = self.version.clone() else {
                return Ok(());
            };
            let newer = self
                .log
                .key_version(bucket, key)
                .await?
                .is_none_or(|current| current.version < version);
            if newer {
                self.log.record_restored(bucket, key, version).await?;
            }
            Ok(())
        })
    }
}
