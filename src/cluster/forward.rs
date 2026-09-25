//! Human: Proxy object writes to the assigned peer when NOS_ASSIGNMENT_FORWARD is enabled.
//! Agent: HTTP to peer public API with caller Authorization and placement headers. Every forwarded request
//! carries FORWARDED_HEADER; a node never forwards or fans out such a request again, so nodes whose rules or
//! peer lists disagree can't bounce a request between them forever.

use axum::http::header;
use reqwest::StatusCode;
use crate::cluster::assignment::{AssignmentResolution, WriteContext};
use crate::cluster::peer::PeerRegistry;
use crate::storage::error::{internal, StorageError};
use crate::storage::multipart::{CompletedPart, InitMultipartResult, PartUploadResult};
use crate::storage::write_path::WriteConditions;
use crate::storage::types::ObjectMetadata;

/// Marks a request one node sent another on a client's behalf.
pub const FORWARDED_HEADER: &str = "x-nd-forwarded";

/// Human: The peer URL path of an object, each segment percent-encoded (keys may hold spaces, `?`, `#`, `%`...).
/// None for a key with a `.` or `..` segment: URL parsing resolves those (encoded or not), so a request for
/// `a/./b` would reach `a/b` — such keys can only be handled on the node a client sends them to.
/// Agent: RETURNS "{bucket}/{seg}/{seg}…" with `/` kept as the separator.
pub fn object_path(bucket: &str, key: &str) -> Option<String> {
    if [bucket].into_iter().chain(key.split('/')).any(|segment| matches!(segment, "." | "..")) {
        return None;
    }
    let key: Vec<_> = key.split('/').map(urlencoding::encode).collect();
    Some(format!("{}/{}", urlencoding::encode(bucket), key.join("/")))
}

/// `object_path`, or a `400` for a key another node can't be asked about.
fn forwardable_path(bucket: &str, key: &str) -> Result<String, StorageError> {
    object_path(bucket, key).ok_or_else(|| {
        StorageError::InvalidRequest(
            "keys with `.` or `..` path segments can't be forwarded to another node; send the request to the node that holds the object".into(),
        )
    })
}

/// Human: Whether an `Authorization` value can be passed on to a peer: a bearer token can, a SigV4 or `NOS`
/// signature can't — it covers this request's host and headers, which a forwarded request doesn't keep.
pub fn is_bearer(authorization: &str) -> bool {
    authorization
        .get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("bearer "))
}

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
    let mut req = builder
        .header(FORWARDED_HEADER, "1")
        .header("x-nd-storage-class", &resolution.storage_class);
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
        return Err(peer_error(resp).await);
    }
    Ok(resp)
}

/// Human: A peer's error answer as the same error here, so the client sees what the owning node said (a `412`
/// used to reach it as a `500`).
/// Agent: 400 (with the peer's message) | 404 | 408 | 412 | 413 | 507 map to their StorageError; else Internal.
async fn peer_error(resp: reqwest::Response) -> StorageError {
    let status = resp.status();
    let message = resp
        .json::<serde_json::Value>()
        .await
        .ok()
        .and_then(|body| body.get("error").and_then(|e| e.as_str()).map(str::to_string));
    match status {
        StatusCode::BAD_REQUEST => StorageError::InvalidRequest(
            message.unwrap_or_else(|| "rejected by the node that holds the object".into()),
        ),
        StatusCode::NOT_FOUND => StorageError::NotFound,
        StatusCode::REQUEST_TIMEOUT => StorageError::RequestTimeout,
        StatusCode::PRECONDITION_FAILED => StorageError::PreconditionFailed,
        StatusCode::PAYLOAD_TOO_LARGE => StorageError::PayloadTooLarge,
        StatusCode::INSUFFICIENT_STORAGE => StorageError::InsufficientStorage,
        _ => internal(anyhow::anyhow!("peer forward returned {status}")),
    }
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
#[allow(clippy::too_many_arguments)]
pub async fn proxy_put(
    peers: &PeerRegistry,
    resolution: &AssignmentResolution,
    bucket: &str,
    key: &str,
    content_type: Option<&str>,
    custom_meta: Option<&str>,
    body: Vec<u8>,
    ctx: Option<&WriteContext>,
    conditions: WriteConditions<'_>,
) -> Result<ObjectMetadata, StorageError> {
    let (_, base) = peer_base(peers, resolution)?;
    let url = format!("{base}/{}", forwardable_path(bucket, key)?);
    let mut req = crate::cluster::http::upload_client()
        .put(&url)
        .timeout(crate::cluster::http::transfer_timeout(body.len() as u64))
        .header(header::AUTHORIZATION, auth_header(ctx)?)
        .body(body);
    if let Some(ct) = content_type {
        req = req.header(header::CONTENT_TYPE, ct);
    }
    // Human: The owning peer evaluates preconditions against its own copy of the object.
    if let Some(v) = conditions.if_match {
        req = req.header(header::IF_MATCH, v);
    }
    if let Some(v) = conditions.if_none_match {
        req = req.header(header::IF_NONE_MATCH, v);
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
#[allow(clippy::too_many_arguments)]
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
    let url = format!("{base}/{}", forwardable_path(dst_bucket, dst_key)?);
    let copy_source = format!("{src_bucket}/{src_key}");
    let mut req = crate::cluster::http::client()
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
    let url = format!(
        "{base}/{}/_multipart?key={}",
        urlencoding::encode(bucket),
        urlencoding::encode(key)
    );
    let mut req = crate::cluster::http::client()
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
#[allow(clippy::too_many_arguments)]
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
    let url = format!(
        "{base}/{}/_multipart/{}/parts/{part_number}",
        urlencoding::encode(bucket),
        urlencoding::encode(upload_id)
    );
    let req = crate::cluster::http::upload_client()
        .put(&url)
        .timeout(crate::cluster::http::transfer_timeout(body.len() as u64))
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
#[allow(clippy::too_many_arguments)]
pub async fn proxy_complete_multipart(
    peers: &PeerRegistry,
    resolution: &AssignmentResolution,
    bucket: &str,
    key: &str,
    upload_id: &str,
    custom_meta: Option<&str>,
    ctx: Option<&WriteContext>,
    parts: Option<&[CompletedPart]>,
) -> Result<ObjectMetadata, StorageError> {
    let (_, base) = peer_base(peers, resolution)?;
    let url = format!(
        "{base}/{}/_multipart/{}/complete?key={}",
        urlencoding::encode(bucket),
        urlencoding::encode(upload_id),
        urlencoding::encode(key)
    );
    let mut req = crate::cluster::http::client()
        .post(&url)
        .header(header::AUTHORIZATION, auth_header(ctx)?);
    if let Some(meta) = custom_meta {
        req = req.header("x-nd-custom-meta", meta);
    }
    if let Some(parts) = parts {
        req = req.json(&serde_json::json!({ "parts": parts }));
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

fn peer_auth(ctx: Option<&WriteContext>) -> Result<String, StorageError> {
    auth_header(ctx).map(str::to_string)
}

/// Human: Forward a single-object DELETE (with its If-Match) to a peer, which deletes only its own copy.
/// Agent: DELETE with Authorization + x-nd-forwarded (+ If-Match); 2xx → Ok, 412 → PreconditionFailed, else Err.
pub async fn proxy_delete_object(
    peer_base: &str,
    bucket: &str,
    key: &str,
    if_match: Option<&str>,
    ctx: Option<&WriteContext>,
) -> Result<(), StorageError> {
    let url = format!("{}/{}", peer_base.trim_end_matches('/'), forwardable_path(bucket, key)?);
    let mut req = crate::cluster::http::client()
        .delete(&url)
        .header(header::AUTHORIZATION, auth_header(ctx)?)
        .header(FORWARDED_HEADER, "1");
    if let Some(v) = if_match {
        req = req.header(header::IF_MATCH, v);
    }
    let resp = req.send().await.map_err(internal)?;
    match resp.status() {
        status if status.is_success() => Ok(()),
        // Human: Nothing to delete there.
        StatusCode::NOT_FOUND => Ok(()),
        // Human: A read-only replica takes no direct writes; replication brings it the delete.
        StatusCode::SERVICE_UNAVAILABLE => Ok(()),
        _ => Err(peer_error(resp).await),
    }
}

/// Human: Forward prefix delete to a peer node (cluster fan-out).
pub async fn proxy_delete_prefix(
    peer_base: &str,
    bucket: &str,
    prefix: &str,
    limit: Option<u64>,
    start_after: Option<&str>,
    ctx: Option<&WriteContext>,
) -> Result<crate::storage::types::DeletePrefixOutcome, StorageError> {
    use crate::storage::types::{DeletePrefixOutcome, DeletePrefixResponse};
    let mut url = format!(
        "{}/{}?prefix={}",
        peer_base.trim_end_matches('/'),
        urlencoding::encode(bucket),
        urlencoding::encode(prefix)
    );
    if let Some(l) = limit {
        url.push_str(&format!("&limit={l}"));
    }
    if let Some(sa) = start_after {
        url.push_str(&format!("&start_after={}", urlencoding::encode(sa)));
    }
    let resp = crate::cluster::http::client()
        .delete(&url)
        .header(header::AUTHORIZATION, peer_auth(ctx)?)
        .header(FORWARDED_HEADER, "1")
        .send()
        .await
        .map_err(internal)?;
    if !resp.status().is_success() {
        return Err(internal(anyhow::anyhow!(
            "peer prefix delete returned {}",
            resp.status()
        )));
    }
    let body: DeletePrefixResponse = resp
        .json()
        .await
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    Ok(DeletePrefixOutcome {
        deleted: body.deleted,
        failed: body.failed,
        truncated: body.truncated,
        next_start_after: body.next_start_after,
        deleted_objects: Vec::new(),
    })
}

/// Human: Forward batch delete to a peer node (cluster fan-out).
pub async fn proxy_batch_delete(
    peer_base: &str,
    bucket: &str,
    keys: &[String],
    ctx: Option<&WriteContext>,
) -> Result<crate::storage::types::DeletePrefixOutcome, StorageError> {
    use crate::storage::types::{DeletePrefixOutcome, DeletePrefixResponse};
    let url = format!(
        "{}/{}/_batch_delete",
        peer_base.trim_end_matches('/'),
        urlencoding::encode(bucket)
    );
    let resp = crate::cluster::http::client()
        .post(&url)
        .header(header::AUTHORIZATION, peer_auth(ctx)?)
        .header(FORWARDED_HEADER, "1")
        .json(&serde_json::json!({ "keys": keys }))
        .send()
        .await
        .map_err(internal)?;
    if !resp.status().is_success() {
        return Err(internal(anyhow::anyhow!(
            "peer batch delete returned {}",
            resp.status()
        )));
    }
    let body: DeletePrefixResponse = resp
        .json()
        .await
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    Ok(DeletePrefixOutcome {
        deleted: body.deleted,
        failed: body.failed,
        truncated: body.truncated,
        next_start_after: body.next_start_after,
        deleted_objects: Vec::new(),
    })
}
