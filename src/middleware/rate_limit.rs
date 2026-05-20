use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use dashmap::DashMap;
use serde_json::json;

use crate::routes::AppState;

pub struct ClientBucket {
    tokens: f64,
    last_refill: Instant,
}

/// Per-IP token bucket limiting for protected routes.
pub async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let rps = state.config.rate_limit_rps;
    if rps == 0 {
        return next.run(req).await;
    }

    let ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let burst = state.config.rate_limit_burst as f64;
    let rate = rps as f64;
    let now = Instant::now();

    let mut entry = state
        .rate_limiters
        .entry(ip)
        .or_insert(ClientBucket {
            tokens: burst,
            last_refill: now,
        });

    let elapsed = now.duration_since(entry.last_refill).as_secs_f64();
    entry.tokens = (entry.tokens + elapsed * rate).min(burst);
    entry.last_refill = now;

    if entry.tokens < 1.0 {
        state.metrics.inc_errors();
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": "rate limit exceeded" })),
        )
            .into_response();
    }

    entry.tokens -= 1.0;
    next.run(req).await
}

pub fn new_rate_limit_map() -> Arc<DashMap<String, ClientBucket>> {
    Arc::new(DashMap::new())
}
