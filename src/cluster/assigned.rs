use std::sync::Arc;

use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use crate::storage::{sanitize_bucket, sanitize_key};
use crate::storage::multipart::CompletedPart;
use crate::storage::write_path::WriteConditions;
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

    /// Stop background tasks once a config reload replaced this backend.
    pub fn shutdown(&self) {
        if let AssignedInner::Replicated(b) = &self.inner {
            b.shutdown();
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
        start_after: Option<&str>,
    ) -> Result<super::replicated::BackfillReport, StorageError> {
        match &self.inner {
            AssignedInner::Standalone(_) => Ok(super::replicated::BackfillReport::default()),
            AssignedInner::Replicated(b) => b.backfill_replication(limit, start_after).await,
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
                let opts = b.engine().next_scrub_options(limit).await?;
                let report = b.scrub_with_recovery(opts.clone()).await?;
                b.engine().save_scrub_progress(&opts, &report).await?;
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
            ..crate::storage::scrub::ScrubOptions::default()
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
        if resolution.accept_local || self.forwards(ctx) {
            return Ok(resolution);
        }
        Err(Self::not_assigned(&resolution))
    }

    /// Human: Whether a request this node isn't assigned is forwarded to the node that is. Never one another node
    /// already forwarded here (their rules disagree; answering "not assigned" beats bouncing it around), and only
    /// with a bearer token to pass on — a presigned URL or a SigV4 signature covers this request, not the
    /// forwarded one, so the peer rejected it.
    /// Agent: NOS_ASSIGNMENT_FORWARD && !x-nd-forwarded && bearer Authorization; else callers answer 409.
    fn forwards(&self, ctx: Option<&WriteContext>) -> bool {
        self.cluster.assignment_forward && !WriteContext::is_forwarded(ctx) && carries_bearer(ctx)
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
        let resolution = self.ensure_placement(bucket, key, ctx)?;
        if !resolution.accept_local {
            // Human: The write is forwarded; the owning peer evaluates the preconditions against its copy.
            return Ok(());
        }
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

    #[allow(clippy::too_many_arguments)]
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        mut body: impl tokio::io::AsyncRead + Unpin,
        ctx: Option<&WriteContext>,
        conditions: WriteConditions<'_>,
    ) -> Result<ObjectMetadata, StorageError> {
        let resolution = self.resolve(bucket, key, ctx);
        if !resolution.accept_local {
            if self.forwards(ctx) {
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
                    conditions,
                )
                .await;
            }
            return Err(Self::not_assigned(&resolution));
        }
        let meta = match &self.inner {
            AssignedInner::Standalone(b) => {
                b.put_object(bucket, key, content_type, custom_meta, body, conditions)
                    .await?
            }
            AssignedInner::Replicated(b) => {
                b.put_object_in_class(
                    bucket,
                    key,
                    content_type,
                    custom_meta,
                    body,
                    &resolution.storage_class,
                    ctx,
                    conditions,
                )
                .await?
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
            if self.forwards(ctx) {
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
                b.copy_object_in_class(
                    src_bucket,
                    src_key,
                    dst_bucket,
                    dst_key,
                    if_match,
                    if_none_match,
                    &resolution.storage_class,
                    ctx,
                )
                .await?
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
        if !self.forwards(ctx) {
            if !WriteContext::is_forwarded(ctx) {
                self.ensure_placement(bucket, key, ctx)?;
            }
            return self.delete_local(bucket, key, if_match, ctx).await;
        }
        // Human: Validate here first — the peers run in parallel with the local delete, so a key the local delete
        // would refuse must not reach them at all.
        sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        if forward::object_path(bucket, key).is_none() {
            return Err(StorageError::InvalidRequest(
                "keys with `.` path segments can't be deleted across nodes; send the request to the node that holds the object".into(),
            ));
        }
        // Human: With forwarding on, the object may be on any node — the rules that placed it can depend on hints
        // (Content-Type, size) a DELETE doesn't carry — so the delete goes to every node. Deleting only here
        // (what this used to do) reported success while the object stayed on the node it was assigned to.
        // Agent: local delete ∥ proxy_delete_object to every peer (x-nd-forwarded); RETURNS combine_delete_results.
        let remote = futures_util::future::join_all(
            self.peers
                .peers
                .iter()
                .filter(|(peer_id, _)| **peer_id != self.cluster.node_id)
                .map(|(_, peer)| forward::proxy_delete_object(&peer.url, bucket, key, if_match, ctx)),
        );
        let (local, remote) = tokio::join!(self.delete_local(bucket, key, if_match, ctx), remote);
        combine_delete_results(if_match.is_some(), std::iter::once(local).chain(remote))
    }

    async fn delete_local(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
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
        if WriteContext::is_forwarded(ctx) {
            return Ok(local);
        }
        if !carries_bearer(ctx) {
            return Ok(self.peers_unreachable_with(local));
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
        if WriteContext::is_forwarded(ctx) {
            return Ok(local);
        }
        if !carries_bearer(ctx) {
            return Ok(self.peers_unreachable_with(local));
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

    /// Human: The local outcome, with every peer listed as failed: this request's credentials can't be passed on,
    /// so the other nodes weren't asked (deleting only here used to look like success).
    fn peers_unreachable_with(&self, mut local: DeletePrefixOutcome) -> DeletePrefixOutcome {
        for peer_id in self.peers.peers.keys().filter(|id| **id != self.cluster.node_id) {
            local.failed.push(DeletePrefixFailure {
                key: format!("peer:{peer_id}"),
                error: "not asked: only requests with a bearer token can be passed on to other nodes".into(),
            });
        }
        local
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
            if self.forwards(ctx) {
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
            if self.forwards(ctx) {
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
        parts: Option<&[CompletedPart]>,
    ) -> Result<ObjectMetadata, StorageError> {
        let resolution = self.resolve(bucket, key, ctx);
        if !resolution.accept_local {
            if self.forwards(ctx) {
                return forward::proxy_complete_multipart(
                    &self.peers,
                    &resolution,
                    bucket,
                    key,
                    upload_id,
                    custom_meta,
                    ctx,
                    parts,
                )
                .await;
            }
            return Err(Self::not_assigned(&resolution));
        }
        let meta = match &self.inner {
            AssignedInner::Standalone(b) => {
                b.complete_multipart(bucket, key, upload_id, custom_meta, parts)
                    .await?
            }
            AssignedInner::Replicated(b) => {
                b.complete_multipart_in_class(
                    bucket,
                    key,
                    upload_id,
                    custom_meta,
                    parts,
                    &resolution.storage_class,
                    ctx,
                )
                .await?
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

/// Whether the request carries a bearer token, the one credential another node accepts on its behalf.
fn carries_bearer(ctx: Option<&WriteContext>) -> bool {
    ctx.and_then(|c| c.authorization.as_deref())
        .is_some_and(forward::is_bearer)
}

/// Human: One answer for a delete sent to every node, where at most one holds the object. With If-Match, a node
/// that deleted it answers for all (the others lack that version); otherwise every node must have answered,
/// since one that couldn't be asked may be the one holding the object.
/// Agent: conditional && any Ok → Ok; else first non-412 error; else PreconditionFailed if any; else Ok.
fn combine_delete_results(
    conditional: bool,
    results: impl IntoIterator<Item = Result<(), StorageError>>,
) -> Result<(), StorageError> {
    let mut deleted = false;
    let mut precondition_failed = false;
    let mut failure = None;
    for result in results {
        match result {
            Ok(()) => deleted = true,
            Err(StorageError::PreconditionFailed) => precondition_failed = true,
            Err(e) => {
                failure.get_or_insert(e);
            }
        }
    }
    if conditional && deleted {
        return Ok(());
    }
    match failure {
        Some(e) => Err(e),
        None if precondition_failed => Err(StorageError::PreconditionFailed),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::combine_delete_results;
    use crate::storage::error::StorageError;

    fn unreachable() -> Result<(), StorageError> {
        Err(StorageError::Internal(anyhow::anyhow!("peer unreachable")))
    }

    #[test]
    fn a_conditional_delete_succeeds_where_the_version_was() {
        let pf = || Err(StorageError::PreconditionFailed);
        assert!(combine_delete_results(true, [pf(), Ok(()), unreachable()]).is_ok());
        assert!(matches!(combine_delete_results(true, [pf(), pf()]), Err(StorageError::PreconditionFailed)));
        assert!(matches!(combine_delete_results(true, [pf(), unreachable()]), Err(StorageError::Internal(_))));
    }

    #[test]
    fn an_unconditional_delete_needs_every_node() {
        assert!(combine_delete_results(false, [Ok(()), Ok(())]).is_ok());
        assert!(combine_delete_results(false, [Ok(()), unreachable()]).is_err());
    }
}
