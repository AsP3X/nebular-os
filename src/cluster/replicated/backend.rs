use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::cluster::assignment::{replication_group_for_write, WriteContext};
use crate::cluster::config::ClusterConfig;
use crate::cluster::peer::{PeerRegistry, spawn_peer_health_checks};
use crate::cluster::read_repair;
use crate::observability::NosMetrics;
use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use crate::storage::error::StorageError;
use crate::storage::multipart::{CompletedPart, InitMultipartResult, PartUploadResult};
use crate::storage::write_path::WriteConditions;
use crate::storage::types::{DeletePrefixOutcome, ListCountResult, ListResult, ObjectMetadata};

use super::hooks::LocalChange;
use super::log::{BackfillReport, ReplicationLog, ReplicationStatusReport};
use super::worker::spawn_replication_worker;
use crate::cluster::replication_recover;
use crate::cluster::replication_rules;
use crate::storage::maintenance::VerifyBlobsReport;

use crate::cluster::standalone::StandaloneBackend as InnerBackend;

/// Human: Local engine plus replication log enqueue and readonly replica enforcement.
/// Agent: Wraps StandaloneBackend; mutating ops enqueue; readonly => StorageError::ReadOnlyReplica.
#[derive(Clone)]
pub struct ReplicatedBackend {
    inner: InnerBackend,
    log: Arc<ReplicationLog>,
    cluster: Arc<ClusterConfig>,
    peers: Arc<PeerRegistry>,
    /// Stops this backend's background tasks (replication worker, peer health checks) once it is replaced.
    shutdown: CancellationToken,
    /// Human: Also stops them when the last handle goes away — e.g. a backend built for a configuration that is
    /// then rejected, which nobody would ever call `shutdown` on.
    _stop_when_dropped: Arc<tokio_util::sync::DropGuard>,
}

impl ReplicatedBackend {
    pub fn new(
        engine: StorageEngine,
        cluster: Arc<ClusterConfig>,
        peers: PeerRegistry,
        metrics: Arc<NosMetrics>,
    ) -> Self {
        let token = cluster
            .cluster_token
            .clone()
            .unwrap_or_default();
        let log = Arc::new(ReplicationLog::new(
            engine.write_pool().clone(),
            engine.data_dir().to_string(),
            cluster.node_id.clone(),
        ));
        let inner = InnerBackend::new(engine);

        let peers = Arc::new(peers);
        let shutdown = CancellationToken::new();
        spawn_replication_worker(
            log.clone(),
            peers.clone(),
            cluster.clone(),
            token.clone(),
            metrics,
            shutdown.clone(),
        );
        spawn_peer_health_checks(peers.clone(), token, cluster.node_id.clone(), shutdown.clone());

        Self {
            inner,
            log,
            cluster,
            peers,
            _stop_when_dropped: Arc::new(shutdown.clone().drop_guard()),
            shutdown,
        }
    }

    /// Human: Stop this backend's replication worker and health checks (it was replaced by a config reload).
    /// Events still queued stay in the log for the replacing backend's worker.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    fn replication_group(&self, ctx: Option<&WriteContext>) -> String {
        replication_group_for_write(ctx, &self.cluster)
    }

    fn storage_class_for_write(&self, ctx: Option<&WriteContext>) -> String {
        ctx.and_then(|c| c.storage_class_header.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.cluster.default_storage_class.clone())
    }

    /// Human: Hook that versions a change made here and queues it for peers, as `storage_class`.
    /// Agent: passed as WriteConditions.hook by every local write, copy, multipart complete and delete here.
    fn local_change<'a>(&'a self, storage_class: &'a str, replication_group: &'a str) -> LocalChange<'a> {
        LocalChange {
            log: &self.log,
            cluster: &self.cluster,
            storage_class,
            replication_group,
        }
    }

    pub async fn replication_status(&self) -> Result<ReplicationStatusReport, StorageError> {
        self.log.status_report().await
    }

    /// Human: Enqueue one batch of existing objects for replication, in key order after `start_after` (it used
    /// to take the same oldest `limit` rows every call, so objects past the first batch were never backfilled).
    pub async fn backfill_replication(
        &self,
        limit: usize,
        start_after: Option<&str>,
    ) -> Result<BackfillReport, StorageError> {
        let limit = limit.max(1) as i64;
        let page = self
            .inner
            .engine()
            .object_meta()
            .list_key_page(limit, start_after)
            .await?;
        let mut report = BackfillReport {
            next_start_after: page.last_key().map(str::to_string),
            is_truncated: page.is_truncated,
            ..BackfillReport::default()
        };
        for (bucket, key, _) in page.rows {
            report.scanned += 1;
            if !replication_rules::should_replicate_key(&self.cluster, &bucket, &key) {
                report.skipped += 1;
                continue;
            }
            let engine = self.inner.engine();
            let Some(meta) = engine.object_meta().try_fetch_active_metadata(&bucket, &key).await? else {
                report.skipped += 1;
                continue;
            };
            // Human: Nothing to send for a row whose blob is gone (scrub reports it; heal restores it).
            let variants = crate::storage::blob_path_variants(engine.data_dir(), &bucket, &key);
            if crate::storage::existing_blob_paths(&variants).is_empty() {
                report.skipped += 1;
                continue;
            }
            let class = meta
                .storage_class
                .as_deref()
                .unwrap_or("default");
            self.log
                .enqueue_put(&meta, class, &self.cluster.replication_group)
                .await?;
            report.enqueued += 1;
        }
        Ok(report)
    }

    pub async fn scrub_with_recovery(
        &self,
        opts: crate::storage::scrub::ScrubOptions,
    ) -> Result<VerifyBlobsReport, StorageError> {
        let mut report = self.inner.engine().scrub_objects(opts).await?;
        if self.cluster.replication_factor <= 1 || report.corrupted == 0 {
            return Ok(report);
        }
        let token = self
            .cluster
            .cluster_token
            .as_deref()
            .unwrap_or_default();
        let client = crate::cluster::http::client();
        for (bucket, key) in report.corrupted_keys.clone() {
            // Human: Only a peer copy of the version this node's row names may replace the damaged bytes.
            let Some(expected) = self
                .inner
                .engine()
                .object_meta()
                .try_fetch_active_metadata(&bucket, &key)
                .await?
                .and_then(|meta| meta.etag)
                .filter(|etag| !etag.is_empty())
            else {
                continue;
            };
            if replication_recover::heal_object_from_peers(
                &client,
                &self.peers,
                &self.cluster.node_id,
                token,
                self.inner.engine(),
                Some(&self.log),
                &bucket,
                &key,
                replication_recover::HealExpectation::Version(&expected),
            )
            .await?
            {
                report.recovered += 1;
                report.corrupted = report.corrupted.saturating_sub(1);
            }
        }
        Ok(report)
    }

    pub async fn replay_dead_letter(&self, event_id: &str) -> Result<bool, StorageError> {
        self.log.replay_dead_letter(event_id).await
    }

    pub async fn verify_blob_integrity_with_recovery(
        &self,
        limit: usize,
    ) -> Result<VerifyBlobsReport, StorageError> {
        self.scrub_with_recovery(crate::storage::scrub::ScrubOptions {
            limit,
            ..crate::storage::scrub::ScrubOptions::default()
        })
        .await
    }

    pub fn engine(&self) -> &StorageEngine {
        self.inner.engine()
    }

    pub fn replication_log(&self) -> &ReplicationLog {
        &self.log
    }

    pub fn replication_log_arc(&self) -> Arc<ReplicationLog> {
        self.log.clone()
    }

    pub async fn pending_replication_events(&self) -> Result<u64, StorageError> {
        self.log.count_pending().await
    }

    /// Human: This node deleted the key (a tombstone): a peer that still has the object just hasn't applied the
    /// delete yet, so read repair must neither serve nor restore it.
    /// Agent: READS replication_versions.deleted for the key; checked before read repair and heal on GET/HEAD misses.
    async fn deleted_here(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        Ok(self
            .log
            .key_version(bucket, key)
            .await?
            .is_some_and(|version| version.deleted))
    }

    fn ensure_writable(&self) -> Result<(), StorageError> {
        if self.cluster.is_readonly_replica() {
            return Err(StorageError::ReadOnlyReplica);
        }
        Ok(())
    }

    pub async fn ensure_write_preconditions(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<(), StorageError> {
        self.ensure_writable()?;
        self.inner
            .ensure_write_preconditions(bucket, key, if_match, if_none_match)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        body: impl tokio::io::AsyncRead + Unpin,
        write_ctx: Option<&WriteContext>,
        conditions: WriteConditions<'_>,
    ) -> Result<ObjectMetadata, StorageError> {
        let class = self.storage_class_for_write(write_ctx);
        self.put_object_in_class(bucket, key, content_type, custom_meta, body, &class, write_ctx, conditions)
            .await
    }

    /// Human: Write locally and queue the change for peers, placed in `storage_class`.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_object_in_class(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        body: impl tokio::io::AsyncRead + Unpin,
        storage_class: &str,
        write_ctx: Option<&WriteContext>,
        conditions: WriteConditions<'_>,
    ) -> Result<ObjectMetadata, StorageError> {
        self.ensure_writable()?;
        let group = self.replication_group(write_ctx);
        let hook = self.local_change(storage_class, &group);
        self.engine()
            .put_object_conditional(
                bucket,
                key,
                content_type,
                custom_meta,
                body,
                WriteConditions {
                    hook: Some(&hook),
                    ..conditions
                },
            )
            .await
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
        write_ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        let class = self.storage_class_for_write(write_ctx);
        self.copy_object_in_class(
            src_bucket,
            src_key,
            dst_bucket,
            dst_key,
            if_match,
            if_none_match,
            &class,
            write_ctx,
        )
        .await
    }

    /// Human: Copy locally and queue the destination's new version for peers, placed in `storage_class`.
    #[allow(clippy::too_many_arguments)]
    pub async fn copy_object_in_class(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        storage_class: &str,
        write_ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        self.ensure_writable()?;
        let group = self.replication_group(write_ctx);
        let hook = self.local_change(storage_class, &group);
        self.engine()
            .copy_object_conditional(
                src_bucket,
                src_key,
                dst_bucket,
                dst_key,
                WriteConditions {
                    if_match,
                    if_none_match,
                    hook: Some(&hook),
                },
            )
            .await
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range_header: Option<&str>,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<GetObjectOutcome, StorageError> {
        match self
            .inner
            .get_object(
                bucket,
                key,
                range_header,
                if_none_match,
                if_modified_since,
            )
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(StorageError::NotFound) if self.cluster.replication_read_repair => {
                if self.deleted_here(bucket, key).await? {
                    return Err(StorageError::NotFound);
                }
                let token = self
                    .cluster
                    .cluster_token
                    .as_deref()
                    .unwrap_or_default();
                let client = crate::cluster::http::client();
                // Human: A row whose blob is missing names the version to restore; with no row, take the
                // peer's copy unless an object is written here meanwhile.
                let local_etag = match crate::storage::sanitize_key(key) {
                    Ok(safe_key) => self
                        .engine()
                        .object_meta()
                        .try_fetch_active_metadata(bucket, &safe_key)
                        .await?
                        .map(|meta| meta.etag.unwrap_or_default()),
                    Err(_) => None,
                };
                let expect = match local_etag.as_deref() {
                    Some(etag) => replication_recover::HealExpectation::Version(etag),
                    None => replication_recover::HealExpectation::Absent,
                };
                if self.cluster.replication_heal_on_read
                    && replication_recover::heal_object_from_peers(
                        &client,
                        &self.peers,
                        &self.cluster.node_id,
                        token,
                        self.inner.engine(),
                        Some(&self.log),
                        bucket,
                        key,
                        expect,
                    )
                    .await?
                {
                    return self
                        .inner
                        .get_object(
                            bucket,
                            key,
                            range_header,
                            if_none_match,
                            if_modified_since,
                        )
                        .await;
                }
                read_repair::fetch_from_peers(
                    &client,
                    &self.peers,
                    &self.cluster.node_id,
                    token,
                    bucket,
                    key,
                    range_header,
                    if_none_match,
                    if_modified_since,
                )
                .await
            }
            Err(e) => Err(e),
        }
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        match self
            .inner
            .head_object(bucket, key, if_none_match, if_modified_since)
            .await
        {
            // Human: Answer like GET — when read repair would serve the object from a peer, HEAD must not 404.
            Err(StorageError::NotFound) if self.cluster.replication_read_repair => {
                if self.deleted_here(bucket, key).await? {
                    return Err(StorageError::NotFound);
                }
                let token = self.cluster.cluster_token.as_deref().unwrap_or_default();
                let client = crate::cluster::http::client();
                match read_repair::fetch_from_peers(
                    &client,
                    &self.peers,
                    &self.cluster.node_id,
                    token,
                    bucket,
                    key,
                    None,
                    if_none_match,
                    if_modified_since,
                )
                .await?
                {
                    GetObjectOutcome::NotModified(_) => Ok(None),
                    GetObjectOutcome::Content { meta, .. } => Ok(Some(*meta)),
                }
            }
            other => other,
        }
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        write_ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        self.ensure_writable()?;
        let class = self.storage_class_for_write(write_ctx);
        let group = self.replication_group(write_ctx);
        let hook = self.local_change(&class, &group);
        self.engine()
            .delete_object_conditional(
                bucket,
                key,
                WriteConditions {
                    if_match,
                    hook: Some(&hook),
                    ..WriteConditions::default()
                },
            )
            .await
    }

    pub async fn delete_objects_by_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        limit: Option<u64>,
        start_after: Option<&str>,
        write_ctx: Option<&WriteContext>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        self.ensure_writable()?;
        let class = self.storage_class_for_write(write_ctx);
        let group = self.replication_group(write_ctx);
        let hook = self.local_change(&class, &group);
        self.engine()
            .delete_objects_by_prefix_hooked(bucket, prefix, limit, start_after, Some(&hook))
            .await
    }

    pub async fn delete_objects_batch(
        &self,
        bucket: &str,
        keys: &[String],
        write_ctx: Option<&WriteContext>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        self.ensure_writable()?;
        let class = self.storage_class_for_write(write_ctx);
        let group = self.replication_group(write_ctx);
        let hook = self.local_change(&class, &group);
        self.engine()
            .delete_objects_batch_hooked(bucket, keys, Some(&hook))
            .await
    }

    pub async fn count_objects_by_prefix(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<ListCountResult, StorageError> {
        self.inner.count_objects_by_prefix(bucket, prefix).await
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: Option<u64>,
        start_after: Option<&str>,
    ) -> Result<ListResult, StorageError> {
        self.inner
            .list_objects(bucket, prefix, delimiter, limit, start_after)
            .await
    }

    pub async fn probe_readiness(&self) -> ReadinessChecks {
        self.inner.probe_readiness().await
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        self.inner.object_count().await
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        self.inner.total_bytes().await
    }

    pub async fn init_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> Result<InitMultipartResult, StorageError> {
        self.ensure_writable()?;
        self.inner.init_multipart(bucket, key, content_type).await
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<PartUploadResult, StorageError> {
        self.ensure_writable()?;
        self.inner
            .upload_part(bucket, key, upload_id, part_number, body)
            .await
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
        write_ctx: Option<&WriteContext>,
        parts: Option<&[CompletedPart]>,
    ) -> Result<ObjectMetadata, StorageError> {
        let class = self.storage_class_for_write(write_ctx);
        self.complete_multipart_in_class(bucket, key, upload_id, custom_meta, parts, &class, write_ctx)
            .await
    }

    /// Human: Assemble the upload locally and queue the new version for peers, placed in `storage_class`.
    #[allow(clippy::too_many_arguments)]
    pub async fn complete_multipart_in_class(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
        parts: Option<&[CompletedPart]>,
        storage_class: &str,
        write_ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        self.ensure_writable()?;
        let group = self.replication_group(write_ctx);
        let hook = self.local_change(storage_class, &group);
        self.engine()
            .complete_multipart_conditional(
                bucket,
                key,
                upload_id,
                custom_meta,
                parts,
                WriteConditions {
                    hook: Some(&hook),
                    ..WriteConditions::default()
                },
            )
            .await
    }

    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        self.ensure_writable()?;
        self.inner.abort_multipart(bucket, key, upload_id).await
    }

    pub async fn multipart_key_for_upload(&self, upload_id: &str) -> Result<String, StorageError> {
        self.inner.multipart_key_for_upload(upload_id).await
    }

    pub fn multipart_part_size(&self) -> usize {
        self.inner.multipart_part_size()
    }
}
