use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::routes::AppState;

fn bearer_token(req: &Request) -> &str {
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

/// Human: Inter-node routes use cluster token; bootstrap token allowed until cluster is configured.
/// Agent: Compares Bearer to cluster_token or NOS_CLUSTER_BOOTSTRAP_TOKEN; 401 JSON {error:unauthorized}.
pub async fn cluster_token_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let provided = bearer_token(&req);
    if provided.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    if state
        .bootstrap_token
        .as_deref()
        .is_some_and(|t| provided == t)
    {
        return next.run(req).await;
    }

    let cluster_token = state
        .cluster
        .read()
        .map(|c| c.cluster_token.clone())
        .unwrap_or(None);
    let Some(expected) = cluster_token.filter(|t| !t.is_empty()) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "cluster token not configured" })),
        )
            .into_response();
    };

    if provided != expected {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    next.run(req).await
}
