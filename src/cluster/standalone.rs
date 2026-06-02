use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use crate::storage::error::StorageError;
use crate::storage::multipart::{InitMultipartResult, PartUploadResult};
use crate::storage::types::{ListResult, ObjectMetadata};

/// Human: Phase 0 facade — every HTTP storage call delegates to the local engine unchanged.
/// Agent: StandaloneBackend wraps StorageEngine Clone; ClusterBackend variant added in later phases.
#[derive(Clone)]
pub struct StandaloneBackend(pub StorageEngine);

impl StandaloneBackend {
    pub fn new(engine: StorageEngine) -> Self {
        Self(engine)
    }

    pub fn engine(&self) -> &StorageEngine {
        &self.0
    }

    pub async fn ensure_write_preconditions(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<(), StorageError> {
        self.0
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
        self.0
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
        self.0
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
        self.0
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
        self.0
            .head_object(bucket, key, if_none_match, if_modified_since)
            .await
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
    ) -> Result<(), StorageError> {
        self.0.delete_object(bucket, key, if_match).await
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: Option<u64>,
        start_after: Option<&str>,
    ) -> Result<ListResult, StorageError> {
        self.0
            .list_objects(bucket, prefix, delimiter, limit, start_after)
            .await
    }

    pub async fn probe_readiness(&self) -> ReadinessChecks {
        self.0.probe_readiness().await
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        self.0.object_count().await
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        self.0.total_bytes().await
    }

    pub async fn init_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> Result<InitMultipartResult, StorageError> {
        self.0.init_multipart(bucket, key, content_type).await
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<PartUploadResult, StorageError> {
        self.0
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
        self.0
            .complete_multipart(bucket, key, upload_id, custom_meta)
            .await
    }

    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        self.0.abort_multipart(bucket, key, upload_id).await
    }

    pub async fn multipart_key_for_upload(&self, upload_id: &str) -> Result<String, StorageError> {
        self.0.multipart_key_for_upload(upload_id).await
    }

    pub fn multipart_part_size(&self) -> usize {
        self.0.multipart_part_size()
    }
}
