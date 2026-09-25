use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::auth::constant_time_eq;
use crate::routes::AppState;

fn bearer_token(req: &Request) -> &str {
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

/// Routes the bootstrap token may call — configuring the node and reading its identity, never object data.
const BOOTSTRAP_ROUTES: [&str; 3] = ["/_cluster/config", "/_cluster/health", "/_cluster/capabilities"];

/// Human: Inter-node routes use the cluster token. The bootstrap token only works until a cluster token is
/// configured, and only for config/health/capabilities (it used to grant every cluster route forever).
/// Agent: constant-time comparisons; 401 JSON {error:unauthorized}; 503 when no cluster token exists yet.
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

    let cluster_token = state
        .cluster
        .read()
        .map(|c| c.cluster_token.clone())
        .unwrap_or(None)
        .filter(|t| !t.is_empty());

    if cluster_token.is_none()
        && BOOTSTRAP_ROUTES.contains(&req.uri().path())
        && state
            .bootstrap_token
            .as_deref()
            .is_some_and(|t| constant_time_eq(provided, t))
    {
        return next.run(req).await;
    }

    let Some(expected) = cluster_token else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "cluster token not configured" })),
        )
            .into_response();
    };

    if !constant_time_eq(provided, &expected) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    next.run(req).await
}
