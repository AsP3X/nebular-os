use std::sync::Arc;

use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use crate::storage::error::StorageError;
use crate::storage::multipart::{InitMultipartResult, PartUploadResult};
use crate::storage::types::{ListResult, ObjectMetadata};

use super::assignment::{AssignmentResolution, AssignmentRules, WriteContext};
use super::config::ClusterConfig;
use super::peer::PeerRegistry;
use super::replicated::ReplicatedBackend;
use super::standalone::StandaloneBackend;

/// Human: Inner storage delegate — standalone or replicated underneath assignment gates.
/// Agent: Assigned mode uses Standalone; ReplicatedAssigned uses ReplicatedBackend.
#[derive(Clone)]
pub enum AssignedInner {
    Standalone(StandaloneBackend),
    Replicated(ReplicatedBackend),
}

/// Human: Enforces storage-class placement before delegating to standalone or replicated engine.
/// Agent: WRITE paths check AssignmentResolution; SET objects.storage_class + origin_node after commit.
#[derive(Clone)]
pub struct AssignedBackend {
    inner: AssignedInner,
    cluster: Arc<ClusterConfig>,
    rules: Arc<AssignmentRules>,
    peers: Arc<PeerRegistry>,
}

impl AssignedBackend {
    pub fn new(
        inner: AssignedInner,
        cluster: Arc<ClusterConfig>,
        rules: AssignmentRules,
        peers: PeerRegistry,
    ) -> Self {
        Self {
            inner,
            cluster,
            rules: Arc::new(rules),
            peers: Arc::new(peers),
        }
    }

    pub fn engine(&self) -> &StorageEngine {
        match &self.inner {
            AssignedInner::Standalone(b) => b.engine(),
            AssignedInner::Replicated(b) => b.engine(),
        }
    }

    pub fn resolve(
        &self,
        bucket: &str,
        key: &str,
        ctx: Option<&WriteContext>,
    ) -> AssignmentResolution {
        AssignmentResolution::resolve(&self.rules, &self.cluster, &self.peers, bucket, key, ctx)
    }

    pub async fn pending_replication_events(&self) -> Result<u64, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(_) => Ok(0),
            AssignedInner::Replicated(b) => b.pending_replication_events().await,
        }
    }

    pub fn replication_log(&self) -> Option<&super::replicated::ReplicationLog> {
        match &self.inner {
            AssignedInner::Replicated(b) => Some(b.replication_log()),
            AssignedInner::Standalone(_) => None,
        }
    }

    fn ensure_placement(
        &self,
        bucket: &str,
        key: &str,
        ctx: Option<&WriteContext>,
    ) -> Result<AssignmentResolution, StorageError> {
        let resolution = self.resolve(bucket, key, ctx);
        if resolution.accept_local {
            return Ok(resolution);
        }
        if self.cluster.assignment_forward {
            // Human: Forwarding is opt-in; Phase 3 keeps it off by default.
            // Agent: DEFERRED forward path — return NotAssigned until NOS_ASSIGNMENT_FORWARD is implemented.
        }
        Err(StorageError::NotAssigned {
            assigned_node: resolution.assigned_node.unwrap_or_else(|| "unknown".into()),
            storage_class: resolution.storage_class,
        })
    }

    async fn record_placement(&self, bucket: &str, key: &str, class: &str) -> Result<(), StorageError> {
        self.engine()
            .set_object_placement(bucket, key, class, &self.cluster.node_id)
            .await
    }

    pub async fn ensure_write_preconditions(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        self.ensure_placement(bucket, key, ctx)?;
        match &self.inner {
            AssignedInner::Standalone(b) => {
                b.ensure_write_preconditions(bucket, key, if_match, if_none_match)
                    .await
            }
            AssignedInner::Replicated(b) => {
                b.ensure_write_preconditions(bucket, key, if_match, if_none_match)
                    .await
            }
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        body: impl tokio::io::AsyncRead + Unpin,
        ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        let resolution = self.ensure_placement(bucket, key, ctx)?;
        let meta = match &self.inner {
            AssignedInner::Standalone(b) => {
                b.put_object(bucket, key, content_type, custom_meta, body)
                    .await?
            }
            AssignedInner::Replicated(b) => {
                let meta = b
                    .put_object_local(bucket, key, content_type, custom_meta, body)
                    .await?;
                b.replication_log()
                    .enqueue_put(&meta, &resolution.storage_class)
                    .await?;
                meta
            }
        };
        self.record_placement(bucket, &meta.key, &resolution.storage_class)
            .await?;
        Ok(meta)
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        let resolution = self.ensure_placement(dst_bucket, dst_key, ctx)?;
        let meta = match &self.inner {
            AssignedInner::Standalone(b) => {
                b.copy_object(
                    src_bucket,
                    src_key,
                    dst_bucket,
                    dst_key,
                    if_match,
                    if_none_match,
                )
                .await?
            }
            AssignedInner::Replicated(b) => {
                let meta = b
                    .copy_object_local(
                        src_bucket,
                        src_key,
                        dst_bucket,
                        dst_key,
                        if_match,
                        if_none_match,
                    )
                    .await?;
                b.replication_log()
                    .enqueue_put(&meta, &resolution.storage_class)
                    .await?;
                meta
            }
        };
        self.record_placement(dst_bucket, &meta.key, &resolution.storage_class)
            .await?;
        Ok(meta)
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range_header: Option<&str>,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<GetObjectOutcome, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => {
                b.get_object(
                    bucket,
                    key,
                    range_header,
                    if_none_match,
                    if_modified_since,
                )
                .await
            }
            AssignedInner::Replicated(b) => {
                b.get_object(
                    bucket,
                    key,
                    range_header,
                    if_none_match,
                    if_modified_since,
                )
                .await
            }
        }
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => {
                b.head_object(bucket, key, if_none_match, if_modified_since)
                    .await
            }
            AssignedInner::Replicated(b) => {
                b.head_object(bucket, key, if_none_match, if_modified_since)
                    .await
            }
        }
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        self.ensure_placement(bucket, key, ctx)?;
        match &self.inner {
            AssignedInner::Standalone(b) => b.delete_object(bucket, key, if_match).await,
            AssignedInner::Replicated(b) => b.delete_object(bucket, key, if_match).await,
        }
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: Option<u64>,
        start_after: Option<&str>,
    ) -> Result<ListResult, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => {
                b.list_objects(bucket, prefix, delimiter, limit, start_after)
                    .await
            }
            AssignedInner::Replicated(b) => {
                b.list_objects(bucket, prefix, delimiter, limit, start_after)
                    .await
            }
        }
    }

    pub async fn probe_readiness(&self) -> ReadinessChecks {
        match &self.inner {
            AssignedInner::Standalone(b) => b.probe_readiness().await,
            AssignedInner::Replicated(b) => b.probe_readiness().await,
        }
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => b.object_count().await,
            AssignedInner::Replicated(b) => b.object_count().await,
        }
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => b.total_bytes().await,
            AssignedInner::Replicated(b) => b.total_bytes().await,
        }
    }

    pub async fn init_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        ctx: Option<&WriteContext>,
    ) -> Result<InitMultipartResult, StorageError> {
        self.ensure_placement(bucket, key, ctx)?;
        match &self.inner {
            AssignedInner::Standalone(b) => b.init_multipart(bucket, key, content_type).await,
            AssignedInner::Replicated(b) => b.init_multipart(bucket, key, content_type).await,
        }
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<PartUploadResult, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => {
                b.upload_part(bucket, key, upload_id, part_number, body)
                    .await
            }
            AssignedInner::Replicated(b) => {
                b.upload_part(bucket, key, upload_id, part_number, body)
                    .await
            }
        }
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
        ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        let resolution = self.ensure_placement(bucket, key, ctx)?;
        let meta = match &self.inner {
            AssignedInner::Standalone(b) => {
                b.complete_multipart(bucket, key, upload_id, custom_meta)
                    .await?
            }
            AssignedInner::Replicated(b) => {
                let meta = b
                    .complete_multipart_local(bucket, key, upload_id, custom_meta)
                    .await?;
                b.replication_log()
                    .enqueue_put(&meta, &resolution.storage_class)
                    .await?;
                meta
            }
        };
        self.record_placement(bucket, &meta.key, &resolution.storage_class)
            .await?;
        Ok(meta)
    }

    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => b.abort_multipart(bucket, key, upload_id).await,
            AssignedInner::Replicated(b) => b.abort_multipart(bucket, key, upload_id).await,
        }
    }

    pub async fn multipart_key_for_upload(&self, upload_id: &str) -> Result<String, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => b.multipart_key_for_upload(upload_id).await,
            AssignedInner::Replicated(b) => b.multipart_key_for_upload(upload_id).await,
        }
    }

    pub fn multipart_part_size(&self) -> usize {
        match &self.inner {
            AssignedInner::Standalone(b) => b.multipart_part_size(),
            AssignedInner::Replicated(b) => b.multipart_part_size(),
        }
    }
}
