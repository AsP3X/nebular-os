use crate::config::NosConfig;
use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use crate::storage::error::StorageError;
use crate::storage::multipart::{InitMultipartResult, PartUploadResult};
use crate::storage::types::{ListResult, ObjectMetadata};

use super::standalone::StandaloneBackend;

/// Human: Single entry point for route handlers — standalone today, cluster variants later.
/// Agent: StorageBackend enum; routes use backend.* not StorageEngine directly.
#[derive(Clone)]
pub enum StorageBackend {
    Standalone(StandaloneBackend),
}

impl StorageBackend {
    pub fn standalone(engine: StorageEngine) -> Self {
        Self::Standalone(StandaloneBackend::new(engine))
    }

    pub fn engine(&self) -> &StorageEngine {
        match self {
            Self::Standalone(b) => b.engine(),
        }
    }

    fn as_standalone(&self) -> &StandaloneBackend {
        match self {
            Self::Standalone(b) => b,
        }
    }

    pub async fn ensure_write_preconditions(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<(), StorageError> {
        self.as_standalone()
            .ensure_write_preconditions(bucket, key, if_match, if_none_match)
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
        self.as_standalone()
            .put_object(bucket, key, content_type, custom_meta, body)
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
        self.as_standalone()
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

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range_header: Option<&str>,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<GetObjectOutcome, StorageError> {
        self.as_standalone()
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
        self.as_standalone()
            .head_object(bucket, key, if_none_match, if_modified_since)
            .await
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
    ) -> Result<(), StorageError> {
        self.as_standalone().delete_object(bucket, key, if_match).await
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: Option<u64>,
        start_after: Option<&str>,
    ) -> Result<ListResult, StorageError> {
        self.as_standalone()
            .list_objects(bucket, prefix, delimiter, limit, start_after)
            .await
    }

    pub async fn probe_readiness(&self) -> ReadinessChecks {
        self.as_standalone().probe_readiness().await
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        self.as_standalone().object_count().await
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        self.as_standalone().total_bytes().await
    }

    pub async fn init_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> Result<InitMultipartResult, StorageError> {
        self.as_standalone()
            .init_multipart(bucket, key, content_type)
            .await
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<PartUploadResult, StorageError> {
        self.as_standalone()
            .upload_part(bucket, key, upload_id, part_number, body)
            .await
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
    ) -> Result<ObjectMetadata, StorageError> {
        self.as_standalone()
            .complete_multipart(bucket, key, upload_id, custom_meta)
            .await
    }

    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        self.as_standalone()
            .abort_multipart(bucket, key, upload_id)
            .await
    }

    pub async fn multipart_key_for_upload(&self, upload_id: &str) -> Result<String, StorageError> {
        self.as_standalone().multipart_key_for_upload(upload_id).await
    }

    pub fn multipart_part_size(&self) -> usize {
        self.as_standalone().multipart_part_size()
    }
}

/// Human: Construct the storage facade from engine + config; Phase 0 always standalone passthrough.
/// Agent: READS cfg.cluster.mode; Standalone => StorageBackend::standalone(engine); cluster modes Phase 2+.
pub fn build_backend(engine: StorageEngine, cfg: &NosConfig) -> StorageBackend {
    if cfg.cluster.is_standalone() {
        StorageBackend::standalone(engine)
    } else {
        // Human: Non-standalone modes get the same local engine wrapper until replication/assignment land.
        // Agent: Phase 1 mounts /_cluster routes; Phase 2+ replaces this branch with ClusterBackend.
        StorageBackend::standalone(engine)
    }
}
