//! Human: Proxy object writes to the assigned peer when NOS_ASSIGNMENT_FORWARD is enabled.
//! Agent: HTTP to peer public API with caller Authorization and placement headers.

use axum::http::header;
use reqwest::StatusCode;
use crate::cluster::assignment::{AssignmentResolution, WriteContext};
use crate::cluster::peer::PeerRegistry;
use crate::storage::error::{internal, StorageError};
use crate::storage::multipart::{InitMultipartResult, PartUploadResult};
use crate::storage::types::ObjectMetadata;

fn peer_base<'a>(
    peers: &'a PeerRegistry,
    resolution: &'a AssignmentResolution,
) -> Result<(&'a str, String), StorageError> {
    let node_id = resolution
        .assigned_node
        .as_deref()
        .ok_or_else(|| internal(anyhow::anyhow!("forward requires assigned_node")))?;
    let base = peers
        .peer_url(node_id)
        .ok_or_else(|| internal(anyhow::anyhow!("unknown peer id: {node_id}")))?;
    Ok((node_id, base.trim_end_matches('/').to_string()))
}

fn auth_header(ctx: Option<&WriteContext>) -> Result<&str, StorageError> {
    ctx.and_then(|c| c.authorization.as_deref())
        .ok_or_else(|| internal(anyhow::anyhow!("forward requires Authorization header")))
}

fn apply_placement_headers(
    builder: reqwest::RequestBuilder,
    resolution: &AssignmentResolution,
    ctx: Option<&WriteContext>,
) -> reqwest::RequestBuilder {
    let mut req = builder.header("x-nd-storage-class", &resolution.storage_class);
    if let Some(group) = ctx.and_then(|c| c.replication_group_header.as_deref()) {
        req = req.header("x-nd-replication-group", group);
    }
    req
}

async fn map_forward_status(
    resp: reqwest::Response,
    resolution: &AssignmentResolution,
) -> Result<reqwest::Response, StorageError> {
    let status = resp.status();
    if status == StatusCode::CONFLICT {
        return Err(StorageError::NotAssigned {
            assigned_node: resolution.assigned_node.clone().unwrap_or_default(),
            storage_class: resolution.storage_class.clone(),
        });
    }
    if !status.is_success() {
        return Err(internal(anyhow::anyhow!("peer forward returned {status}")));
    }
    Ok(resp)
}

fn metadata_from_etag(
    bucket: &str,
    key: &str,
    content_type: Option<&str>,
    custom_meta: Option<&str>,
    etag: Option<String>,
) -> ObjectMetadata {
    ObjectMetadata {
        bucket: bucket.to_string(),
        key: key.to_string(),
        size: 0,
        mime_type: content_type.map(str::to_string),
        etag,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        custom_meta: custom_meta.map(str::to_string),
        deleted_at: None,
        storage_class: None,
        origin_node: None,
    }
}

/// Human: Forward a PUT body to the peer that owns this storage class.
pub async fn proxy_put(
    peers: &PeerRegistry,
    resolution: &AssignmentResolution,
    bucket: &str,
    key: &str,
    content_type: Option<&str>,
    custom_meta: Option<&str>,
    body: Vec<u8>,
    ctx: Option<&WriteContext>,
) -> Result<ObjectMetadata, StorageError> {
    let (_, base) = peer_base(peers, resolution)?;
    let url = format!("{base}/{bucket}/{key}");
    let client = reqwest::Client::new();
    let mut req = client
        .put(&url)
        .header(header::AUTHORIZATION, auth_header(ctx)?)
        .body(body);
    if let Some(ct) = content_type {
        req = req.header(header::CONTENT_TYPE, ct);
    }
    if let Some(meta) = custom_meta {
        req = req.header("x-nd-custom-meta", meta);
    }
    let resp = map_forward_status(
        apply_placement_headers(req, resolution, ctx).send().await.map_err(internal)?,
        resolution,
    )
    .await?;
    let etag = resp
        .json::<serde_json::Value>()
        .await
        .ok()
        .and_then(|v| v.get("etag").and_then(|e| e.as_str().map(str::to_string)));
    Ok(metadata_from_etag(bucket, key, content_type, custom_meta, etag))
}

/// Human: Forward server-side copy to the assigned peer via PUT + x-nd-copy-source.
pub async fn proxy_copy(
    peers: &PeerRegistry,
    resolution: &AssignmentResolution,
    src_bucket: &str,
    src_key: &str,
    dst_bucket: &str,
    dst_key: &str,
    if_match: Option<&str>,
    if_none_match: Option<&str>,
    ctx: Option<&WriteContext>,
) -> Result<ObjectMetadata, StorageError> {
    let (_, base) = peer_base(peers, resolution)?;
    let url = format!("{base}/{dst_bucket}/{dst_key}");
    let copy_source = format!("{src_bucket}/{src_key}");
    let client = reqwest::Client::new();
    let mut req = client
        .put(&url)
        .header(header::AUTHORIZATION, auth_header(ctx)?)
        .header("x-nd-copy-source", &copy_source);
    if let Some(v) = if_match {
        req = req.header(header::IF_MATCH, v);
    }
    if let Some(v) = if_none_match {
        req = req.header(header::IF_NONE_MATCH, v);
    }
    let resp = map_forward_status(
        apply_placement_headers(req, resolution, ctx).send().await.map_err(internal)?,
        resolution,
    )
    .await?;
    let etag = resp
        .json::<serde_json::Value>()
        .await
        .ok()
        .and_then(|v| v.get("etag").and_then(|e| e.as_str().map(str::to_string)));
    Ok(metadata_from_etag(dst_bucket, dst_key, None, None, etag))
}

/// Human: Forward multipart init to the assigned peer.
pub async fn proxy_init_multipart(
    peers: &PeerRegistry,
    resolution: &AssignmentResolution,
    bucket: &str,
    key: &str,
    content_type: Option<&str>,
    ctx: Option<&WriteContext>,
) -> Result<InitMultipartResult, StorageError> {
    let (_, base) = peer_base(peers, resolution)?;
    let url = format!("{base}/{bucket}/_multipart?key={}", urlencoding::encode(key));
    let client = reqwest::Client::new();
    let mut req = client
        .post(&url)
        .header(header::AUTHORIZATION, auth_header(ctx)?);
    if let Some(ct) = content_type {
        req = req.header(header::CONTENT_TYPE, ct);
    }
    let resp = map_forward_status(
        apply_placement_headers(req, resolution, ctx).send().await.map_err(internal)?,
        resolution,
    )
    .await?;
    resp.json::<InitMultipartResult>()
        .await
        .map_err(internal)
}

/// Human: Forward a multipart part upload to the assigned peer.
pub async fn proxy_upload_part(
    peers: &PeerRegistry,
    resolution: &AssignmentResolution,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
    ctx: Option<&WriteContext>,
) -> Result<PartUploadResult, StorageError> {
    let (_, base) = peer_base(peers, resolution)?;
    let _ = key;
    let url = format!("{base}/{bucket}/_multipart/{upload_id}/parts/{part_number}");
    let client = reqwest::Client::new();
    let req = client
        .put(&url)
        .header(header::AUTHORIZATION, auth_header(ctx)?)
        .body(body);
    let resp = map_forward_status(
        apply_placement_headers(req, resolution, ctx)
            .send()
            .await
            .map_err(internal)?,
        resolution,
    )
    .await?;
    resp.json::<PartUploadResult>().await.map_err(internal)
}

/// Human: Forward multipart complete to the assigned peer.
pub async fn proxy_complete_multipart(
    peers: &PeerRegistry,
    resolution: &AssignmentResolution,
    bucket: &str,
    key: &str,
    upload_id: &str,
    custom_meta: Option<&str>,
    ctx: Option<&WriteContext>,
) -> Result<ObjectMetadata, StorageError> {
    let (_, base) = peer_base(peers, resolution)?;
    let url = format!(
        "{base}/{bucket}/_multipart/{upload_id}/complete?key={}",
        urlencoding::encode(key)
    );
    let client = reqwest::Client::new();
    let mut req = client
        .post(&url)
        .header(header::AUTHORIZATION, auth_header(ctx)?);
    if let Some(meta) = custom_meta {
        req = req.header("x-nd-custom-meta", meta);
    }
    let resp = map_forward_status(
        apply_placement_headers(req, resolution, ctx).send().await.map_err(internal)?,
        resolution,
    )
    .await?;
    let etag = resp
        .json::<serde_json::Value>()
        .await
        .ok()
        .and_then(|v| v.get("etag").and_then(|e| e.as_str().map(str::to_string)));
    Ok(metadata_from_etag(bucket, key, None, custom_meta, etag))
}
