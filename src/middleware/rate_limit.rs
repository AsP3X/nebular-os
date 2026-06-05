use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::auth::Claims;
use crate::routes::AppState;

pub struct ClientBucket {
    pub tokens: f64,
    pub last_refill: std::time::Instant,
}

pub fn new_rate_limit_map() -> Arc<dashmap::DashMap<String, ClientBucket>> {
    Arc::new(dashmap::DashMap::new())
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

    if is_bulk_delete_exempt(&req) {
        return next.run(req).await;
    }

    if let Some(claims) = req.extensions().get::<Claims>() {
        let role = claims.role.to_ascii_lowercase();
        if state.config.rate_limit_bypass_roles.iter().any(|r| r == &role) {
            return next.run(req).await;
        }
    }

    let ip = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let burst = state.config.rate_limit_burst as f64;
    let rate = rps as f64;
    let now = std::time::Instant::now();

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
        let mut resp = (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": "rate limit exceeded" })),
        )
            .into_response();
        resp.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        return resp;
    }

    entry.tokens -= 1.0;
    next.run(req).await
}

fn is_bulk_delete_exempt(req: &Request) -> bool {
    if req.method() == Method::DELETE {
        let query = req.uri().query().unwrap_or("");
        if query.contains("prefix=") {
            return true;
        }
    }
    req.uri().path().ends_with("/_batch_delete")
}
