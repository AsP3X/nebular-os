use axum::{
    extract::{Path, Query, Request, State},
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::routes::AppState;
use crate::s3_compat::{self, xml};

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: Option<u64>,
    start_after: Option<String>,
    /// S3 ListObjectsV2 uses `list-type=2`; we accept it when `NOS_S3_COMPAT` is enabled.
    #[serde(rename = "list-type")]
    list_type: Option<String>,
}

/// Human: List object keys in a bucket; returns JSON by default or S3 ListObjectsV2 XML when compat is on.
/// Agent: GET /{bucket}; READS NOS_S3_COMPAT; CALLS StorageEngine::list_objects; EMITS JSON or application/xml.
pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(query): Query<ListQuery>,
    req: Request,
) -> impl IntoResponse {
    tracing::info!(%bucket, prefix = ?query.prefix, limit = ?query.limit, "list_objects started");

    // Human: Decide response shape before we hit storage so errors use the same format the client expects.
    // Agent: use_s3_xml IF NOS_S3_COMPAT AND (list-type=2 OR wants_s3_response Accept/query).
    let use_s3_xml = state.config.s3_compat
        && (query.list_type.as_deref() == Some("2")
            || s3_compat::wants_s3_response(
                req.headers(),
                req.uri().query(),
                state.config.s3_compat,
            ));

    match state
        .backend
        .read()
        .await
        .list_objects(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            query.limit,
            query.start_after.as_deref(),
        )
        .await
    {
        Ok(result) => {
            tracing::info!(%bucket, item_count = result.items.len(), "list_objects completed");
            if use_s3_xml {
                // Human: S3 SDKs expect ListObjectsV2 XML, not our native JSON list shape.
                // Agent: CALLS xml::list_objects_v2_xml; HTTP 200; Content-Type application/xml.
                let body = xml::list_objects_v2_xml(&bucket, &result);
                return (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/xml")],
                    body,
                )
                    .into_response();
            }
            Json(result).into_response()
        }
        Err(e) => {
            tracing::error!(%bucket, error = %e, "list_objects failed");
            let (status, json) = crate::routes::errors::map_storage_error(e);
            if use_s3_xml {
                // Human: Map our JSON error envelope to S3-style XML when the client asked for S3 compat.
                // Agent: CALLS maybe_s3_json_error; READS error string from JSON body; same status as native path.
                return s3_compat::maybe_s3_json_error(
                    status,
                    json.0["error"].as_str().unwrap_or("error"),
                    true,
                    true,
                );
            }
            (status, json).into_response()
        }
    }
}
