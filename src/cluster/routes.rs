use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::cluster::assignment::WriteContext;
use crate::cluster::backend::StorageBackend;
use crate::routes::helpers::{
    apply_object_headers, object_content_response, parse_if_modified_since, parse_if_none_match,
    range_not_satisfiable_response,
};
use crate::routes::{errors::map_storage_error, AppState};
use crate::storage::engine::GetObjectOutcome;
use crate::storage::error::StorageError;

#[derive(Serialize)]
pub struct ClusterHealthResponse {
    pub status: &'static str,
    pub cluster_mode: &'static str,
    pub node_id: String,
    pub storage_classes: Vec<String>,
    pub replication_group: String,
    pub replication_role: String,
    pub replication_pending_events: u64,
}

/// Human: Peers and operators probe cluster identity and replication backlog.
/// Agent: GET /_cluster/health; Bearer NOS_CLUSTER_TOKEN; JSON additive ops fields.
pub async fn cluster_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cluster = state.cluster();
    let pending = state
        .backend()
        .pending_replication_events()
        .await
        .unwrap_or(0);
    (
        StatusCode::OK,
        Json(ClusterHealthResponse {
            status: "ok",
            cluster_mode: cluster.mode.as_str(),
            node_id: cluster.node_id.clone(),
            storage_classes: cluster.storage_classes.clone(),
            replication_group: cluster.replication_group.clone(),
            replication_role: cluster.replication_role.clone(),
            replication_pending_events: pending,
        }),
    )
}

/// Human: Peer checks whether an object exists locally before fetch/repair.
/// Agent: HEAD /_cluster/objects/{bucket}/{key}; 200 if exists else 404 JSON error.
/// Human: Peers fetch object bytes for read-repair without user JWT.
/// Agent: GET /_cluster/objects/{bucket}/{key}; streams local blob; does not persist on caller.
pub async fn cluster_object_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((bucket, key)): axum::extract::Path<(String, String)>,
) -> Response {
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let if_none_match = parse_if_none_match(&headers);
    let if_modified_since = parse_if_modified_since(&headers);

    match state
        .backend()
        .engine()
        .get_object(
            &bucket,
            &key,
            range_header,
            if_none_match.as_deref(),
            if_modified_since,
        )
        .await
    {
        Ok(GetObjectOutcome::NotModified(meta)) => {
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::NOT_MODIFIED;
            apply_object_headers(resp.headers_mut(), &meta);
            resp
        }
        Ok(GetObjectOutcome::Content {
            stream,
            content_length,
            total_size,
            range,
            meta,
        }) => {
            let mut resp = object_content_response(stream, content_length, total_size, range, &meta);
            // Human: A peer restoring the object from this copy records the version it has here. The version is read
            // after the bytes were opened, so it is sent only if the object is still the one being served: a write
            // in between would otherwise label these bytes with the newer version, and the peer would then refuse
            // that newer version when it arrived.
            // Agent: version read first, ETag re-checked after — a write commits its metadata before its version.
            if let Some(log) = state.backend().replication_log()
                && let Ok(Some(version)) = log.key_version(&bucket, &key).await
                && let Ok(Some(current)) = state
                    .backend()
                    .engine()
                    .object_meta()
                    .try_fetch_active_metadata(&meta.bucket, &meta.key)
                    .await
                && current.etag.is_some()
                && current.etag == meta.etag
                && let Ok(value) = header::HeaderValue::from_str(&version.version.to_header())
            {
                resp.headers_mut()
                    .insert(crate::cluster::replicated::versions::HEADER, value);
            }
            resp
        }
        Err(StorageError::RangeNotSatisfiable { size }) => range_not_satisfiable_response(size),
        Err(e) => map_storage_error(e).into_response(),
    }
}

pub async fn cluster_object_head(
    State(state): State<Arc<AppState>>,
    axum::extract::Path((bucket, key)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    match state.backend().engine().object_exists(&bucket, &key).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "not found" })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, %bucket, %key, "cluster object head failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "storage error" })),
            )
                .into_response()
        }
    }
}

#[derive(Serialize)]
pub struct ClusterCapabilitiesResponse {
    pub version: &'static str,
    pub cluster_mode: &'static str,
    pub node_id: String,
    pub storage_classes: Vec<String>,
    pub replication_group: String,
    pub replication_role: String,
}

/// Human: Peers discover node capabilities without user JWT.
/// Agent: GET /_cluster/capabilities; same auth as /_cluster/health.
#[derive(serde::Deserialize)]
pub struct AssignmentResolveRequest {
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub storage_class_header: Option<String>,
    #[serde(default)]
    pub content_length: Option<u64>,
}

#[derive(serde::Serialize)]
pub struct AssignmentResolveResponse {
    pub storage_class: String,
    pub assigned_node: Option<String>,
    pub accept_local: bool,
}

/// Human: Debug endpoint for Ownly/admin to preview placement before upload.
/// Agent: POST /_cluster/assignment/resolve; Bearer cluster token; no write.
pub async fn assignment_resolve(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AssignmentResolveRequest>,
) -> axum::response::Response {
    let resolution = match &state.backend() {
        StorageBackend::Assigned(b) => {
            let ctx = WriteContext {
                storage_class_header: body.storage_class_header.clone(),
                content_type: body.content_type.clone(),
                custom_meta_storage_class: None,
                content_length: body.content_length,
                authorization: None,
                replication_group_header: None,
                forwarded: false,
            };
            b.resolve(&body.bucket, &body.key, Some(&ctx))
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "assignment resolve requires assigned cluster mode" })),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(AssignmentResolveResponse {
            storage_class: resolution.storage_class,
            assigned_node: resolution.assigned_node,
            accept_local: resolution.accept_local,
        }),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
pub struct BackfillQuery {
    pub limit: Option<u64>,
    /// Cursor from a previous response's `next_start_after`.
    pub start_after: Option<String>,
}

/// Human: Enqueue replication events for existing objects not yet pushed to peers, one batch per call.
/// Agent: POST /_cluster/replication/backfill; Bearer cluster token; optional limit (default 100) and
/// start_after; REPEAT with start_after=next_start_after while is_truncated.
pub async fn replication_backfill(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BackfillQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(100).min(10_000) as usize;
    match state
        .backend()
        .backfill_replication(limit, query.start_after.as_deref())
        .await
    {
        Ok(report) => (StatusCode::OK, Json(report)).into_response(),
        Err(e) => {
            let (status, json) = crate::routes::errors::map_storage_error(e);
            (status, json).into_response()
        }
    }
}

pub async fn cluster_capabilities(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cluster = state.cluster();
    (
        StatusCode::OK,
        Json(ClusterCapabilitiesResponse {
            version: env!("CARGO_PKG_VERSION"),
            cluster_mode: cluster.mode.as_str(),
            node_id: cluster.node_id.clone(),
            storage_classes: cluster.storage_classes.clone(),
            replication_group: cluster.replication_group.clone(),
            replication_role: cluster.replication_role.clone(),
        }),
    )
}
