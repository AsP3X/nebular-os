use std::sync::Arc;

use crate::cluster::assignment::{replication_group_for_write, WriteContext};
use crate::cluster::config::ClusterConfig;
use crate::cluster::peer::{PeerRegistry, spawn_peer_health_checks};
use crate::cluster::read_repair;
use crate::observability::NosMetrics;
use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use crate::storage::error::StorageError;
use crate::storage::multipart::{InitMultipartResult, PartUploadResult};
use crate::storage::types::{DeletePrefixOutcome, ListCountResult, ListResult, ObjectMetadata};

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
        spawn_replication_worker(
            log.clone(),
            peers.clone(),
            cluster.clone(),
            token.clone(),
            metrics,
        );
        spawn_peer_health_checks(peers.clone(), token, cluster.node_id.clone());

        Self {
            inner,
            log,
            cluster,
            peers,
        }
    }

    fn replication_group(&self, ctx: Option<&WriteContext>) -> String {
        replication_group_for_write(ctx, &self.cluster)
    }

    fn storage_class_for_write(&self, ctx: Option<&WriteContext>) -> String {
        ctx.and_then(|c| c.storage_class_header.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.cluster.default_storage_class.clone())
    }

    pub async fn enqueue_replicated_put(
        &self,
        meta: &ObjectMetadata,
        storage_class: &str,
        write_ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        if !replication_rules::should_replicate_key(&self.cluster, &meta.bucket, &meta.key) {
            return Ok(());
        }
        let group = self.replication_group(write_ctx);
        self.log.enqueue_put(meta, storage_class, &group).await?;
        Ok(())
    }

    async fn maybe_enqueue_put(
        &self,
        meta: &ObjectMetadata,
        write_ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        let class = self.storage_class_for_write(write_ctx);
        self.enqueue_replicated_put(meta, &class, write_ctx).await
    }

    async fn maybe_enqueue_delete(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
        write_ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        if !replication_rules::should_replicate_key(&self.cluster, bucket, key) {
            return Ok(());
        }
        let group = self.replication_group(write_ctx);
        self.log
            .enqueue_delete(bucket, key, storage_class, &group)
            .await?;
        Ok(())
    }

    pub async fn replication_status(&self) -> Result<ReplicationStatusReport, StorageError> {
        self.log.status_report().await
    }

    pub async fn backfill_replication(&self, limit: usize) -> Result<BackfillReport, StorageError> {
        let limit = limit.max(1) as i64;
        let rows = self
            .inner
            .engine()
            .object_meta()
            .list_recompress_candidates(limit)
            .await?;
        let mut report = BackfillReport::default();
        for (bucket, key, _) in rows {
            report.scanned += 1;
            if !replication_rules::should_replicate_key(&self.cluster, &bucket, &key) {
                report.skipped += 1;
                continue;
            }
            let meta = match self.inner.engine().head_object(&bucket, &key, None, None).await? {
                Some(m) => m,
                None => {
                    report.skipped += 1;
                    continue;
                }
            };
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
        let client = reqwest::Client::new();
        for (bucket, key) in report.corrupted_keys.clone() {
            if replication_recover::heal_object_from_peers(
                &client,
                &self.peers,
                &self.cluster.node_id,
                token,
                self.inner.engine(),
                &bucket,
                &key,
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
            sample_denom: 1,
            mode: crate::storage::scrub::ScrubMode::Deep,
            start_after: None,
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

    /// Human: Local put without replication enqueue (used by AssignedBackend with explicit class).
    /// Agent: CALLS inner.put_object only; NO replication_log insert.
    pub async fn put_object_local(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<ObjectMetadata, StorageError> {
        self.ensure_writable()?;
        self.inner
            .put_object(bucket, key, content_type, custom_meta, body)
            .await
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        body: impl tokio::io::AsyncRead + Unpin,
        write_ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        let meta = self
            .put_object_local(bucket, key, content_type, custom_meta, body)
            .await?;
        self.maybe_enqueue_put(&meta, write_ctx).await?;
        Ok(meta)
    }

    pub async fn copy_object_local(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<ObjectMetadata, StorageError> {
        self.ensure_writable()?;
        self.inner
            .copy_object(
                src_bucket,
                src_key,
                dst_bucket,
                dst_key,
                if_match,
                if_none_match,
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
        let meta = self
            .copy_object_local(
                src_bucket,
                src_key,
                dst_bucket,
                dst_key,
                if_match,
                if_none_match,
            )
            .await?;
        self.maybe_enqueue_put(&meta, write_ctx).await?;
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
                let token = self
                    .cluster
                    .cluster_token
                    .as_deref()
                    .unwrap_or_default();
                let client = reqwest::Client::new();
                if self.cluster.replication_heal_on_read
                    && replication_recover::heal_object_from_peers(
                        &client,
                        &self.peers,
                        &self.cluster.node_id,
                        token,
                        self.inner.engine(),
                        bucket,
                        key,
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
        self.inner
            .head_object(bucket, key, if_none_match, if_modified_since)
            .await
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        write_ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        self.ensure_writable()?;
        let storage_class = self
            .inner
            .engine()
            .active_storage_class(bucket, key)
            .await?
            .unwrap_or_else(|| "default".into());
        self.inner.delete_object(bucket, key, if_match).await?;
        self.maybe_enqueue_delete(bucket, key, &storage_class, write_ctx)
            .await?;
        Ok(())
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
        let outcome = self
            .inner
            .delete_objects_by_prefix(bucket, prefix, limit, start_after)
            .await?;
        for obj in &outcome.deleted_objects {
            let storage_class = obj.storage_class.as_deref().unwrap_or("default");
            self.maybe_enqueue_delete(bucket, &obj.key, storage_class, write_ctx)
                .await?;
        }
        Ok(outcome)
    }

    pub async fn delete_objects_batch(
        &self,
        bucket: &str,
        keys: &[String],
        write_ctx: Option<&WriteContext>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        self.ensure_writable()?;
        let outcome = self.inner.delete_objects_batch(bucket, keys).await?;
        for obj in &outcome.deleted_objects {
            let storage_class = obj.storage_class.as_deref().unwrap_or("default");
            self.maybe_enqueue_delete(bucket, &obj.key, storage_class, write_ctx)
                .await?;
        }
        Ok(outcome)
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

    pub async fn complete_multipart_local(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
    ) -> Result<ObjectMetadata, StorageError> {
        self.ensure_writable()?;
        self.inner
            .complete_multipart(bucket, key, upload_id, custom_meta)
            .await
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
        write_ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        let meta = self
            .complete_multipart_local(bucket, key, upload_id, custom_meta)
            .await?;
        self.maybe_enqueue_put(&meta, write_ctx).await?;
        Ok(meta)
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
