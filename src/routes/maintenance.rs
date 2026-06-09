use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::auth::Claims;
use crate::routes::AppState;

#[derive(Debug, Deserialize)]
pub struct OrphanQuery {
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub limit: Option<u64>,
}

fn require_admin(claims: Option<&Claims>) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    match claims {
        Some(c) if c.role.eq_ignore_ascii_case("admin") => Ok(()),
        _ => Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "admin role required" })),
        )),
    }
}

/// Human: Inspect blob files on disk that have no active metadata row.
/// Agent: GET /_nos/maintenance/orphans; admin JWT; optional bucket/prefix/limit filters.
pub async fn list_orphans(
    State(state): State<Arc<AppState>>,
    Query(query): Query<OrphanQuery>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let claims = req.extensions().get::<Claims>();
    if let Err(resp) = require_admin(claims) {
        return resp.into_response();
    }

    let limit = query.limit.unwrap_or(1000).min(10_000) as usize;
    match state
        .engine()
        .list_orphan_blobs(query.bucket.as_deref(), query.prefix.as_deref(), limit)
        .await
    {
        Ok(result) => Json(result).into_response(),
        Err(e) => {
            let (status, json) = crate::routes::errors::map_storage_error(e);
            (status, json).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct GcOrphansQuery {
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub limit: Option<u64>,
}

/// Human: Remove orphan blobs (no metadata row) under optional bucket/prefix.
/// Agent: POST /_nos/maintenance/gc_orphans; admin JWT; returns bytes reclaimed count.
pub async fn gc_orphans(
    State(state): State<Arc<AppState>>,
    Query(query): Query<GcOrphansQuery>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let claims = req.extensions().get::<Claims>();
    if let Err(resp) = require_admin(claims) {
        return resp.into_response();
    }

    let limit = query.limit.unwrap_or(1000).min(10_000) as usize;
    match state
        .engine()
        .gc_orphan_blobs(query.bucket.as_deref(), query.prefix.as_deref(), limit)
        .await
    {
        Ok(report) => {
            state.metrics.add_orphan_gc_bytes(report.bytes_reclaimed);
            Json(report).into_response()
        }
        Err(e) => {
            let (status, json) = crate::routes::errors::map_storage_error(e);
            (status, json).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct VerifyBlobsQuery {
    pub limit: Option<u64>,
}

/// Human: Proactively verify blob block checksums without client GET traffic.
/// Agent: POST /_nos/maintenance/verify_blobs; admin JWT; optional limit (default engine batch size).
pub async fn verify_blobs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<VerifyBlobsQuery>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let claims = req.extensions().get::<Claims>();
    if let Err(resp) = require_admin(claims) {
        return resp.into_response();
    }

    let limit = query
        .limit
        .unwrap_or(state.engine().verify_batch_size() as u64)
        .min(10_000) as usize;
    match state.backend().verify_blob_integrity(limit).await {
        Ok(report) => Json(report).into_response(),
        Err(e) => {
            let (status, json) = crate::routes::errors::map_storage_error(e);
            (status, json).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct MigrateBlobsQuery {
    pub limit: Option<u64>,
    pub start_after: Option<String>,
}

/// Human: Batch-migrate legacy nested paths and compression formats to current Nebular layout.
/// Agent: POST /_nos/maintenance/migrate_blobs; admin JWT; optional limit + start_after cursor.
pub async fn migrate_blobs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<MigrateBlobsQuery>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let claims = req.extensions().get::<Claims>();
    if let Err(resp) = require_admin(claims) {
        return resp.into_response();
    }

    let limit = query
        .limit
        .unwrap_or(state.engine().recompress_batch_size() as u64)
        .min(10_000) as usize;
    match state
        .engine()
        .migrate_blobs(limit, query.start_after.as_deref())
        .await
    {
        Ok(report) => Json(report).into_response(),
        Err(e) => {
            let (status, json) = crate::routes::errors::map_storage_error(e);
            (status, json).into_response()
        }
    }
}

/// Human: Replication queue depth and dead-letter counts for cluster operators.
/// Agent: GET /_nos/maintenance/replication_status; admin JWT; zeroed in standalone mode.
pub async fn replication_status(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let claims = req.extensions().get::<Claims>();
    if let Err(resp) = require_admin(claims) {
        return resp.into_response();
    }

    match state.backend().replication_status().await {
        Ok(report) => Json(report).into_response(),
        Err(e) => {
            let (status, json) = crate::routes::errors::map_storage_error(e);
            (status, json).into_response()
        }
    }
}
