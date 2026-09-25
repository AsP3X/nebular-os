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
use crate::cluster::replicated::apply::{
    apply_replication_event_bytes, apply_replication_event_file,
};
use crate::cluster::replicated::ReplicationEvent;
use crate::routes::AppState;
use crate::storage::engine::TempFileGuard;
use crate::storage::error::{internal, StorageError};
use crate::storage::streaming::receive_multipart_blob_field;
use std::path::PathBuf;

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
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "invalid request" })),
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
                StorageError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
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
    let backend = state.backend();
    apply_replication_event_bytes(backend.engine(), log.as_ref(), &event, None).await
}

async fn apply_multipart(
    state: &AppState,
    multipart: &mut Multipart,
) -> Result<(), StorageError> {
    let mut event: Option<ReplicationEvent> = None;
    let mut received: Option<(TempFileGuard, u64, String)> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(internal)?
    {
        match field.name() {
            Some("event") => {
                let raw = field.text().await.map_err(internal)?;
                event = Some(serde_json::from_str(&raw).map_err(internal)?);
            }
            Some("blob") => {
                // Human: Spool to disk while hashing — objects of any size replicate without buffering.
                let spool = TempFileGuard {
                    path: PathBuf::from(format!(
                        "{}/.tmp/{}.repl",
                        state.engine.data_dir(),
                        uuid::Uuid::new_v4()
                    )),
                };
                let max_len = event
                    .as_ref()
                    .and_then(|e| e.size)
                    .and_then(|s| u64::try_from(s).ok());
                let (len, checksum) =
                    receive_multipart_blob_field(field, &spool.path, max_len).await?;
                received = Some((spool, len, checksum));
            }
            _ => {}
        }
    }

    let event = event.ok_or(StorageError::NotFound)?;
    let log = replication_log(state)?;
    let backend = state.backend();
    let Some((spool, len, checksum)) = received else {
        return apply_replication_event_bytes(backend.engine(), log.as_ref(), &event, None).await;
    };
    if let Some(size) = event.size
        && u64::try_from(size).ok() != Some(len)
    {
        return Err(internal(anyhow::anyhow!(
            "replication payload is {len} bytes, event says {size}"
        )));
    }
    if let Some(expected) = event.wire_checksum.as_deref().filter(|e| !e.is_empty())
        && expected != checksum
    {
        return Err(internal(anyhow::anyhow!("replication wire checksum mismatch")));
    }
    apply_replication_event_file(backend.engine(), log.as_ref(), &event, &spool.path).await
}

fn replication_log(
    state: &AppState,
) -> Result<std::sync::Arc<crate::cluster::replicated::ReplicationLog>, StorageError> {
    match state.backend() {
        StorageBackend::Replicated(r) => Ok(r.replication_log_arc()),
        StorageBackend::Assigned(b) => b
            .replication_log_arc()
            .ok_or_else(|| internal(anyhow::anyhow!("replicate on assigned standalone inner"))),
        StorageBackend::Standalone(_) => {
            Err(internal(anyhow::anyhow!("replicate on non-replicated backend")))
        }
    }
}
