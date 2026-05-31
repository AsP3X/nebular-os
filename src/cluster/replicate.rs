use axum::{
    body::Bytes,
    extract::{FromRequest, Multipart, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::cluster::backend::StorageBackend;
use crate::cluster::replicated::apply::apply_replication_event_bytes;
use crate::cluster::replicated::ReplicationEvent;
use crate::routes::AppState;
use crate::storage::error::{internal, StorageError};

/// Human: Peers apply idempotent replication events (JSON delete or multipart put).
/// Agent: POST /_cluster/replicate; Bearer cluster token; 200 on apply or duplicate event_id.
pub async fn replicate(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
) -> Response {
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let result = if content_type.starts_with("multipart/") {
        let mut multipart = match Multipart::from_request(req, state.as_ref()).await {
            Ok(m) => m,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        };
        apply_multipart(&state, &mut multipart).await
    } else {
        let body = match axum::body::to_bytes(req.into_body(), state.max_body_size)
            .await
        {
            Ok(b) => b,
            Err(_) => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    Json(json!({ "error": "payload too large" })),
                )
                    .into_response();
            }
        };
        apply_json(&state, body).await
    };

    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "replicate apply failed");
            let status = match &e {
                StorageError::NotFound => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (
                status,
                Json(json!({ "error": e.client_message() })),
            )
                .into_response()
        }
    }
}

async fn apply_json(state: &AppState, body: Bytes) -> Result<(), StorageError> {
    let event: ReplicationEvent =
        serde_json::from_slice(&body).map_err(internal)?;
    let log = replication_log(state)?;
    let engine = state.backend.engine();
    apply_replication_event_bytes(engine, log, &event, None).await
}

async fn apply_multipart(
    state: &AppState,
    multipart: &mut Multipart,
) -> Result<(), StorageError> {
    let mut event_json: Option<String> = None;
    let mut blob: Option<Vec<u8>> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(internal)?
    {
        match field.name() {
            Some("event") => {
                event_json = Some(field.text().await.map_err(internal)?);
            }
            Some("blob") => {
                blob = Some(field.bytes().await.map_err(internal)?.to_vec());
            }
            _ => {}
        }
    }

    let event_raw = event_json.ok_or(StorageError::NotFound)?;
    let event: ReplicationEvent =
        serde_json::from_str(&event_raw).map_err(internal)?;
    let log = replication_log(state)?;
    let engine = state.backend.engine();
    apply_replication_event_bytes(engine, log, &event, blob).await
}

fn replication_log(state: &AppState) -> Result<&crate::cluster::replicated::ReplicationLog, StorageError> {
    match &state.backend {
        StorageBackend::Replicated(r) => Ok(r.replication_log()),
        StorageBackend::Assigned(b) => b
            .replication_log()
            .ok_or_else(|| internal(anyhow::anyhow!("replicate on assigned standalone inner"))),
        StorageBackend::Standalone(_) => {
            Err(internal(anyhow::anyhow!("replicate on non-replicated backend")))
        }
    }
}
