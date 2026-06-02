use std::sync::Arc;

use crate::storage::engine::{GetObjectOutcome, ReadinessChecks, StorageEngine};
use super::config::ClusterConfig;
use crate::storage::error::StorageError;
use crate::storage::multipart::{InitMultipartResult, PartUploadResult};
use crate::storage::types::{ListResult, ObjectMetadata};

use super::assigned::{AssignedBackend, AssignedInner};
use super::assignment::WriteContext;
use crate::observability::NosMetrics;
use super::replicated::ReplicatedBackend;
use super::standalone::StandaloneBackend;

/// Human: Single entry point for route handlers — standalone, replicated, or assigned placement.
/// Agent: StorageBackend enum; build_backend selects variant from ClusterMode.
#[derive(Clone)]
pub enum StorageBackend {
    Standalone(StandaloneBackend),
    Replicated(ReplicatedBackend),
    Assigned(AssignedBackend),
}

impl StorageBackend {
    pub fn standalone(engine: StorageEngine) -> Self {
        Self::Standalone(StandaloneBackend::new(engine))
    }

    pub fn engine(&self) -> &StorageEngine {
        match self {
            Self::Standalone(b) => b.engine(),
            Self::Replicated(b) => b.engine(),
            Self::Assigned(b) => b.engine(),
        }
    }

    pub async fn pending_replication_events(&self) -> Result<u64, StorageError> {
        match self {
            Self::Standalone(_) => Ok(0),
            Self::Replicated(b) => b.pending_replication_events().await,
            Self::Assigned(b) => b.pending_replication_events().await,
        }
    }

    pub async fn ensure_write_preconditions(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        write_ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        match self {
            Self::Standalone(b) => {
                b.ensure_write_preconditions(bucket, key, if_match, if_none_match)
                    .await
            }
            Self::Replicated(b) => {
                b.ensure_write_preconditions(bucket, key, if_match, if_none_match)
                    .await
            }
            Self::Assigned(b) => {
                b.ensure_write_preconditions(bucket, key, if_match, if_none_match, write_ctx)
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
        write_ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        match self {
            Self::Standalone(b) => {
                b.put_object(bucket, key, content_type, custom_meta, body)
                    .await
            }
            Self::Replicated(b) => {
                b.put_object(bucket, key, content_type, custom_meta, body, write_ctx)
                    .await
            }
            Self::Assigned(b) => {
                b.put_object(bucket, key, content_type, custom_meta, body, write_ctx)
                    .await
            }
        }
    }

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
        match self {
            Self::Standalone(b) => {
                b.copy_object(
                    src_bucket,
                    src_key,
                    dst_bucket,
                    dst_key,
                    if_match,
                    if_none_match,
                )
                .await
            }
            Self::Replicated(b) => {
                b.copy_object(
                    src_bucket,
                    src_key,
                    dst_bucket,
                    dst_key,
                    if_match,
                    if_none_match,
                    write_ctx,
                )
                .await
            }
            Self::Assigned(b) => {
                b.copy_object(
                    src_bucket,
                    src_key,
                    dst_bucket,
                    dst_key,
                    if_match,
                    if_none_match,
                    write_ctx,
                )
                .await
            }
        }
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range_header: Option<&str>,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<GetObjectOutcome, StorageError> {
        match self {
            Self::Standalone(b) => {
                b.get_object(
                    bucket,
                    key,
                    range_header,
                    if_none_match,
                    if_modified_since,
                )
                .await
            }
            Self::Replicated(b) => {
                b.get_object(
                    bucket,
                    key,
                    range_header,
                    if_none_match,
                    if_modified_since,
                )
                .await
            }
            Self::Assigned(b) => {
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
        match self {
            Self::Standalone(b) => {
                b.head_object(bucket, key, if_none_match, if_modified_since)
                    .await
            }
            Self::Replicated(b) => {
                b.head_object(bucket, key, if_none_match, if_modified_since)
                    .await
            }
            Self::Assigned(b) => {
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
        write_ctx: Option<&WriteContext>,
    ) -> Result<(), StorageError> {
        match self {
            Self::Standalone(b) => b.delete_object(bucket, key, if_match).await,
            Self::Replicated(b) => b.delete_object(bucket, key, if_match, write_ctx).await,
            Self::Assigned(b) => b.delete_object(bucket, key, if_match, write_ctx).await,
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
        match self {
            Self::Standalone(b) => {
                b.list_objects(bucket, prefix, delimiter, limit, start_after)
                    .await
            }
            Self::Replicated(b) => {
                b.list_objects(bucket, prefix, delimiter, limit, start_after)
                    .await
            }
            Self::Assigned(b) => {
                b.list_objects(bucket, prefix, delimiter, limit, start_after)
                    .await
            }
        }
    }

    pub async fn probe_readiness(&self) -> ReadinessChecks {
        match self {
            Self::Standalone(b) => b.probe_readiness().await,
            Self::Replicated(b) => b.probe_readiness().await,
            Self::Assigned(b) => b.probe_readiness().await,
        }
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        match self {
            Self::Standalone(b) => b.object_count().await,
            Self::Replicated(b) => b.object_count().await,
            Self::Assigned(b) => b.object_count().await,
        }
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        match self {
            Self::Standalone(b) => b.total_bytes().await,
            Self::Replicated(b) => b.total_bytes().await,
            Self::Assigned(b) => b.total_bytes().await,
        }
    }

    pub async fn init_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        write_ctx: Option<&WriteContext>,
    ) -> Result<InitMultipartResult, StorageError> {
        match self {
            Self::Standalone(b) => b.init_multipart(bucket, key, content_type).await,
            Self::Replicated(b) => b.init_multipart(bucket, key, content_type).await,
            Self::Assigned(b) => b.init_multipart(bucket, key, content_type, write_ctx).await,
        }
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: impl tokio::io::AsyncRead + Unpin,
        write_ctx: Option<&WriteContext>,
    ) -> Result<PartUploadResult, StorageError> {
        match self {
            Self::Standalone(b) => {
                b.upload_part(bucket, key, upload_id, part_number, body)
                    .await
            }
            Self::Replicated(b) => {
                b.upload_part(bucket, key, upload_id, part_number, body)
                    .await
            }
            Self::Assigned(b) => {
                b.upload_part(bucket, key, upload_id, part_number, body, write_ctx)
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
        write_ctx: Option<&WriteContext>,
    ) -> Result<ObjectMetadata, StorageError> {
        match self {
            Self::Standalone(b) => {
                b.complete_multipart(bucket, key, upload_id, custom_meta)
                    .await
            }
            Self::Replicated(b) => {
                b.complete_multipart(bucket, key, upload_id, custom_meta, write_ctx)
                    .await
            }
            Self::Assigned(b) => {
                b.complete_multipart(bucket, key, upload_id, custom_meta, write_ctx)
                    .await
            }
        }
    }

    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        match self {
            Self::Standalone(b) => b.abort_multipart(bucket, key, upload_id).await,
            Self::Replicated(b) => b.abort_multipart(bucket, key, upload_id).await,
            Self::Assigned(b) => b.abort_multipart(bucket, key, upload_id).await,
        }
    }

    pub async fn multipart_key_for_upload(&self, upload_id: &str) -> Result<String, StorageError> {
        match self {
            Self::Standalone(b) => b.multipart_key_for_upload(upload_id).await,
            Self::Replicated(b) => b.multipart_key_for_upload(upload_id).await,
            Self::Assigned(b) => b.multipart_key_for_upload(upload_id).await,
        }
    }

    pub fn multipart_part_size(&self) -> usize {
        match self {
            Self::Standalone(b) => b.multipart_part_size(),
            Self::Replicated(b) => b.multipart_part_size(),
            Self::Assigned(b) => b.multipart_part_size(),
        }
    }
}

/// Human: Construct the storage facade from engine + config.
/// Agent: Standalone passthrough; Replicated* / Assigned* per ClusterMode flags.
pub fn build_backend(
    engine: StorageEngine,
    cluster: &ClusterConfig,
    metrics: Arc<NosMetrics>,
) -> anyhow::Result<StorageBackend> {
    if cluster.is_standalone() {
        return Ok(StorageBackend::standalone(engine));
    }

    let cluster = Arc::new(cluster.clone());
    let peers = cluster.peer_registry()?;
    if !peers.peers.contains_key(&cluster.node_id) {
        tracing::warn!(
            node_id = %cluster.node_id,
            "NOS_NODE_ID is not listed in NOS_CLUSTER_PEERS (asymmetric peering)"
        );
    }

    let inner = if cluster.mode_includes_replication() {
        AssignedInner::Replicated(ReplicatedBackend::new(
            engine,
            cluster.clone(),
            peers.clone(),
            metrics,
        ))
    } else {
        AssignedInner::Standalone(StandaloneBackend::new(engine))
    };

    if cluster.mode_includes_assignment() {
        let rules = cluster.assignment_rules()?;
        return Ok(StorageBackend::Assigned(AssignedBackend::new(
            inner,
            cluster,
            rules,
            peers,
        )));
    }

    if let AssignedInner::Replicated(r) = inner {
        return Ok(StorageBackend::Replicated(r));
    }

    anyhow::bail!("unsupported cluster mode {:?}", cluster.mode)
}
