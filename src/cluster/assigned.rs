use std::sync::Arc;

use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use crate::storage::error::StorageError;
use crate::storage::multipart::{InitMultipartResult, PartUploadResult};
use crate::storage::types::{DeletePrefixFailure, DeletePrefixOutcome, ListCountResult, ListResult, ObjectMetadata};

use super::assignment::{AssignmentResolution, AssignmentRules, WriteContext};
use super::forward;
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

    pub async fn replication_status(
        &self,
    ) -> Result<super::replicated::ReplicationStatusReport, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(_) => {
                Ok(super::replicated::ReplicationStatusReport::default())
            }
            AssignedInner::Replicated(b) => b.replication_status().await,
        }
    }

    pub async fn backfill_replication(
        &self,
        limit: usize,
    ) -> Result<super::replicated::BackfillReport, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(_) => Ok(super::replicated::BackfillReport::default()),
            AssignedInner::Replicated(b) => b.backfill_replication(limit).await,
        }
    }

    pub async fn scrub_with_recovery(
        &self,
        opts: crate::storage::scrub::ScrubOptions,
    ) -> Result<crate::storage::maintenance::VerifyBlobsReport, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => b.engine().scrub_objects(opts).await,
            AssignedInner::Replicated(b) => b.scrub_with_recovery(opts).await,
        }
    }

    pub async fn scrub_with_defaults(
        &self,
        limit: usize,
    ) -> Result<crate::storage::maintenance::VerifyBlobsReport, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => b.engine().scrub_with_defaults(limit).await,
            AssignedInner::Replicated(b) => {
                let cursor = b.engine().get_maintenance_state("scrub_cursor").await?;
                let mode = if b.engine().scrub_mode_light() {
                    crate::storage::scrub::ScrubMode::Light
                } else {
                    crate::storage::scrub::ScrubMode::Deep
                };
                let report = b
                    .scrub_with_recovery(crate::storage::scrub::ScrubOptions {
                        limit,
                        sample_denom: b.engine().scrub_sample_denom(),
                        mode,
                        start_after: cursor,
                    })
                    .await?;
                if let Some(ref next) = report.next_start_after {
                    b.engine().set_maintenance_state("scrub_cursor", next).await?;
                }
                Ok(report)
            }
        }
    }

    pub async fn replay_dead_letter(&self, event_id: &str) -> Result<bool, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(_) => Ok(false),
            AssignedInner::Replicated(b) => b.replay_dead_letter(event_id).await,
        }
    }

    pub async fn verify_blob_integrity_with_recovery(
        &self,
        limit: usize,
    ) -> Result<crate::storage::maintenance::VerifyBlobsReport, StorageError> {
        self.scrub_with_recovery(crate::storage::scrub::ScrubOptions {
            limit,
            sample_denom: 1,
            mode: crate::storage::scrub::ScrubMode::Deep,
            start_after: None,
        })
        .await
    }

    pub fn replication_log(&self) -> Option<&super::replicated::ReplicationLog> {
        match &self.inner {
            AssignedInner::Replicated(b) => Some(b.replication_log()),
            AssignedInner::Standalone(_) => None,
        }
    }

    pub fn replication_log_arc(&self) -> Option<std::sync::Arc<super::replicated::ReplicationLog>> {
        match &self.inner {
            AssignedInner::Replicated(b) => Some(b.replication_log_arc()),
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
        if resolution.accept_local || self.cluster.assignment_forward {
            return Ok(resolution);
        }
        Err(StorageError::NotAssigned {
            assigned_node: resolution.assigned_node.unwrap_or_else(|| "unknown".into()),
            storage_class: resolution.storage_class,
        })
    }

    fn not_assigned(resolution: &AssignmentResolution) -> StorageError {
        StorageError::NotAssigned {
            assigned_node: resolution.assigned_node.clone().unwrap_or_else(|| "unknown".into()),
            storage_class: resolution.storage_class.clone(),
        }
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
        mut body: impl tokio::io::AsyncRead + Unpin,
        ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        let resolution = self.resolve(bucket, key, ctx);
        if !resolution.accept_local {
            if self.cluster.assignment_forward {
                let mut buf = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut body, &mut buf)
                    .await
                    .map_err(crate::storage::error::map_io_error)?;
                return forward::proxy_put(
                    &self.peers,
                    &resolution,
                    bucket,
                    key,
                    content_type,
                    custom_meta,
                    buf,
                    ctx,
                )
                .await;
            }
            return Err(Self::not_assigned(&resolution));
        }
        let meta = match &self.inner {
            AssignedInner::Standalone(b) => {
                b.put_object(bucket, key, content_type, custom_meta, body)
                    .await?
            }
            AssignedInner::Replicated(b) => {
                let meta = b
                    .put_object_local(bucket, key, content_type, custom_meta, body)
                    .await?;
                b.enqueue_replicated_put(&meta, &resolution.storage_class, ctx)
                    .await?;
                meta
            }
        };
        self.record_placement(bucket, &meta.key, &resolution.storage_class)
            .await?;
        Ok(meta)
    }

    #[allow(clippy::too_many_arguments)]
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
        let resolution = self.resolve(dst_bucket, dst_key, ctx);
        if !resolution.accept_local {
            if self.cluster.assignment_forward {
                return forward::proxy_copy(
                    &self.peers,
                    &resolution,
                    src_bucket,
                    src_key,
                    dst_bucket,
                    dst_key,
                    if_match,
                    if_none_match,
                    ctx,
                )
                .await;
            }
            return Err(Self::not_assigned(&resolution));
        }
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
                b.enqueue_replicated_put(&meta, &resolution.storage_class, ctx)
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
            AssignedInner::Replicated(b) => b.delete_object(bucket, key, if_match, ctx).await,
        }
    }

    pub async fn delete_objects_by_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        limit: Option<u64>,
        start_after: Option<&str>,
        ctx: Option<&WriteContext>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        let local = match &self.inner {
            AssignedInner::Standalone(b) => {
                b.delete_objects_by_prefix(bucket, prefix, limit, start_after)
                    .await
            }
            AssignedInner::Replicated(b) => {
                b.delete_objects_by_prefix(bucket, prefix, limit, start_after, ctx)
                    .await
            }
        }?;
        self.fanout_delete_prefix(local, bucket, prefix, limit, start_after, ctx)
            .await
    }

    pub async fn delete_objects_batch(
        &self,
        bucket: &str,
        keys: &[String],
        ctx: Option<&WriteContext>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        let local = match &self.inner {
            AssignedInner::Standalone(b) => b.delete_objects_batch(bucket, keys).await,
            AssignedInner::Replicated(b) => b.delete_objects_batch(bucket, keys, ctx).await,
        }?;
        self.fanout_batch_delete(local, bucket, keys, ctx).await
    }

    async fn fanout_delete_prefix(
        &self,
        mut local: DeletePrefixOutcome,
        bucket: &str,
        prefix: &str,
        limit: Option<u64>,
        start_after: Option<&str>,
        ctx: Option<&WriteContext>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        if self.peers.peers.is_empty() {
            return Ok(local);
        }
        for (peer_id, peer) in &self.peers.peers {
            if peer_id == &self.cluster.node_id {
                continue;
            }
            match forward::proxy_delete_prefix(
                &peer.url,
                bucket,
                prefix,
                limit,
                start_after,
                ctx,
            )
            .await
            {
                Ok(remote) => Self::merge_delete_outcome(&mut local, remote),
                Err(e) => local.failed.push(DeletePrefixFailure {
                    key: format!("peer:{peer_id}"),
                    error: e.to_string(),
                }),
            }
        }
        Ok(local)
    }

    async fn fanout_batch_delete(
        &self,
        mut local: DeletePrefixOutcome,
        bucket: &str,
        keys: &[String],
        ctx: Option<&WriteContext>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        if self.peers.peers.is_empty() {
            return Ok(local);
        }
        for (peer_id, peer) in &self.peers.peers {
            if peer_id == &self.cluster.node_id {
                continue;
            }
            match forward::proxy_batch_delete(&peer.url, bucket, keys, ctx).await {
                Ok(remote) => Self::merge_delete_outcome(&mut local, remote),
                Err(e) => local.failed.push(DeletePrefixFailure {
                    key: format!("peer:{peer_id}"),
                    error: e.to_string(),
                }),
            }
        }
        Ok(local)
    }

    fn merge_delete_outcome(local: &mut DeletePrefixOutcome, remote: DeletePrefixOutcome) {
        local.deleted += remote.deleted;
        local.failed.extend(remote.failed);
        local.truncated |= remote.truncated;
        local.deleted_objects.extend(remote.deleted_objects);
        if local.next_start_after.is_none() {
            local.next_start_after = remote.next_start_after;
        }
    }

    pub async fn count_objects_by_prefix(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<ListCountResult, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(b) => b.count_objects_by_prefix(bucket, prefix).await,
            AssignedInner::Replicated(b) => b.count_objects_by_prefix(bucket, prefix).await,
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
        let resolution = self.resolve(bucket, key, ctx);
        if !resolution.accept_local {
            if self.cluster.assignment_forward {
                return forward::proxy_init_multipart(
                    &self.peers,
                    &resolution,
                    bucket,
                    key,
                    content_type,
                    ctx,
                )
                .await;
            }
            return Err(Self::not_assigned(&resolution));
        }
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
        mut body: impl tokio::io::AsyncRead + Unpin,
        ctx: Option<&WriteContext>,
    ) -> Result<PartUploadResult, StorageError> {
        let resolution = self.resolve(bucket, key, ctx);
        if !resolution.accept_local {
            if self.cluster.assignment_forward {
                let mut buf = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut body, &mut buf)
                    .await
                    .map_err(crate::storage::error::map_io_error)?;
                return forward::proxy_upload_part(
                    &self.peers,
                    &resolution,
                    bucket,
                    key,
                    upload_id,
                    part_number,
                    buf,
                    ctx,
                )
                .await;
            }
            return Err(Self::not_assigned(&resolution));
        }
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
        let resolution = self.resolve(bucket, key, ctx);
        if !resolution.accept_local {
            if self.cluster.assignment_forward {
                return forward::proxy_complete_multipart(
                    &self.peers,
                    &resolution,
                    bucket,
                    key,
                    upload_id,
                    custom_meta,
                    ctx,
                )
                .await;
            }
            return Err(Self::not_assigned(&resolution));
        }
        let meta = match &self.inner {
            AssignedInner::Standalone(b) => {
                b.complete_multipart(bucket, key, upload_id, custom_meta)
                    .await?
            }
            AssignedInner::Replicated(b) => {
                let meta = b
                    .complete_multipart_local(bucket, key, upload_id, custom_meta)
                    .await?;
                b.enqueue_replicated_put(&meta, &resolution.storage_class, ctx)
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
