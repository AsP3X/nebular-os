use axum::{
    extract::{Path, Request, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::routes::helpers::write_context_from_headers;
use crate::routes::AppState;
use crate::storage::types::DeletePrefixResponse;

#[derive(Debug, Deserialize)]
pub struct BatchDeleteRequest {
    pub keys: Vec<String>,
}

/// Human: Delete many object keys in one HTTP round-trip.
/// Agent: POST /{bucket}/_batch_delete; parallel blob drops + batch metadata txn; same response shape as prefix delete.
pub async fn batch_delete(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    req: Request,
) -> impl IntoResponse {
    let write_ctx = write_context_from_headers(req.headers(), None);
    let body = match axum::body::to_bytes(req.into_body(), state.max_body_size).await {
        Ok(b) => b,
        Err(_) => {
            return crate::routes::errors::payload_too_large_response();
        }
    };
    let parsed: BatchDeleteRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid JSON body, expected {\"keys\": [...]}" })),
            )
                .into_response();
        }
    };

    tracing::info!(
        %bucket,
        key_count = parsed.keys.len(),
        "batch_delete started"
    );

    match state
        .backend()
        .delete_objects_batch(&bucket, &parsed.keys, Some(&write_ctx))
        .await
    {
        Ok(outcome) => {
            state
                .metrics
                .add_objects_deleted(outcome.deleted);
            tracing::info!(
                %bucket,
                deleted = outcome.deleted,
                failed = outcome.failed.len(),
                "batch_delete completed"
            );
            Json(DeletePrefixResponse::from(outcome)).into_response()
        }
        Err(e) => {
            tracing::error!(%bucket, error = %e, "batch_delete failed");
            let (status, json) = crate::routes::errors::map_storage_error(e);
            (status, json).into_response()
        }
    }
}
