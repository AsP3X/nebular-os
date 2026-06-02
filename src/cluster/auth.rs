// Human: Inter-node routes use a shared secret, not end-user JWTs.
// Agent: Compares Authorization Bearer to config.cluster.cluster_token; 401 JSON {error:unauthorized}.

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::routes::AppState;

fn bearer_token(req: &Request) -> &str {
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

// Human: Runtime config accepts bootstrap token (first apply) or active cluster token.
// Agent: GET/PUT /_cluster/config; READS bootstrap_token + cluster RwLock; 401 when neither matches.
pub async fn runtime_config_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let provided = bearer_token(&req);
    if provided.is_empty() {
        return unauthorized();
    }

    if state
        .bootstrap_token
        .as_deref()
        .is_some_and(|expected| provided == expected)
    {
        return next.run(req).await;
    }

    if state
        .cluster
        .read()
        .ok()
        .and_then(|cluster| cluster.cluster_token.clone())
        .is_some_and(|expected| provided == expected)
    {
        return next.run(req).await;
    }

    unauthorized()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "unauthorized" })),
    )
        .into_response()
}

/// Human: Inter-node replication routes require the active cluster token.
/// Agent: Compares Authorization Bearer to cluster.cluster_token; 401 JSON {error:unauthorized}.
pub async fn cluster_token_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let expected = match state
        .cluster
        .read()
        .ok()
        .and_then(|c| c.cluster_token.clone())
    {
        Some(t) => t,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "cluster token not configured" })),
            )
                .into_response();
        }
    };

    let provided = bearer_token(&req);

    if provided != expected {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    next.run(req).await
}
