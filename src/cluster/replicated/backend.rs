use std::sync::Arc;

use crate::cluster::config::ClusterConfig;
use crate::cluster::peer::PeerRegistry;
use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use crate::storage::error::StorageError;
use crate::storage::multipart::{InitMultipartResult, PartUploadResult};
use crate::storage::types::{ListResult, ObjectMetadata};

use super::log::ReplicationLog;
use super::worker::spawn_replication_worker;

use crate::cluster::standalone::StandaloneBackend as InnerBackend;

/// Human: Local engine plus replication log enqueue and readonly replica enforcement.
/// Agent: Wraps StandaloneBackend; mutating ops enqueue; readonly => StorageError::ReadOnlyReplica.
#[derive(Clone)]
pub struct ReplicatedBackend {
    inner: InnerBackend,
    log: Arc<ReplicationLog>,
    cluster: Arc<ClusterConfig>,
}

impl ReplicatedBackend {
    pub fn new(
        engine: StorageEngine,
        cluster: Arc<ClusterConfig>,
        peers: PeerRegistry,
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

        spawn_replication_worker(log.clone(), Arc::new(peers), cluster.clone(), token);

        Self {
            inner,
            log,
            cluster,
        }
    }

    pub fn engine(&self) -> &StorageEngine {
        self.inner.engine()
    }

    pub fn replication_log(&self) -> &ReplicationLog {
        &self.log
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
    ) -> Result<ObjectMetadata, StorageError> {
        let meta = self
            .put_object_local(bucket, key, content_type, custom_meta, body)
            .await?;
        self.log.enqueue_put(&meta, "default").await?;
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

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
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
        self.log.enqueue_put(&meta, "default").await?;
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
        self.inner
            .get_object(
                bucket,
                key,
                range_header,
                if_none_match,
                if_modified_since,
            )
            .await
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
    ) -> Result<(), StorageError> {
        self.ensure_writable()?;
        self.inner.delete_object(bucket, key, if_match).await?;
        self.log.enqueue_delete(bucket, key).await?;
        Ok(())
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
    ) -> Result<ObjectMetadata, StorageError> {
        let meta = self
            .complete_multipart_local(bucket, key, upload_id, custom_meta)
            .await?;
        self.log.enqueue_put(&meta, "default").await?;
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
