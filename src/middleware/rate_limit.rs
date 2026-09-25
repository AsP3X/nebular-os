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

/// Buckets tracked before idle ones are evicted (each is a few dozen bytes).
const MAX_TRACKED_CLIENTS: usize = 50_000;
/// A bucket untouched this long is full again anyway, so dropping it loses nothing.
const IDLE_EVICT_AFTER: std::time::Duration = std::time::Duration::from_secs(600);

/// Human: The client's IP (not IP:port — every connection has its own port, which reset the limit per connection).
pub fn client_ip(req: &Request) -> String {
    req.extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn refill(bucket: &mut ClientBucket, rate: f64, burst: f64, now: std::time::Instant) {
    let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
    bucket.last_refill = now;
}

fn evict_idle(map: &dashmap::DashMap<String, ClientBucket>, now: std::time::Instant) {
    if map.len() > MAX_TRACKED_CLIENTS {
        map.retain(|_, bucket| now.duration_since(bucket.last_refill) < IDLE_EVICT_AFTER);
        // Human: Still crowded with recent clients (e.g. rotating IPv6 addresses): start over instead of
        // rescanning on every request — resetting buckets is the cheap way for a rate limiter to fail.
        if map.len() > MAX_TRACKED_CLIENTS / 2 {
            map.clear();
        }
    }
}

/// Human: Spend one token from `client`'s bucket; false when it is empty.
pub fn take_token(map: &dashmap::DashMap<String, ClientBucket>, client: String, rate: f64, burst: f64) -> bool {
    let now = std::time::Instant::now();
    evict_idle(map, now);
    let mut bucket = map.entry(client).or_insert(ClientBucket {
        tokens: burst,
        last_refill: now,
    });
    refill(&mut bucket, rate, burst, now);
    if bucket.tokens < 1.0 {
        return false;
    }
    bucket.tokens -= 1.0;
    true
}

pub fn too_many_requests() -> Response {
    let mut resp = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({ "error": "rate limit exceeded" })),
    )
        .into_response();
    resp.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    resp
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

    let ip = client_ip(&req);
    if !take_token(
        &state.rate_limiters,
        ip,
        rps as f64,
        state.config.rate_limit_burst as f64,
    ) {
        state.metrics.inc_errors();
        return too_many_requests();
    }
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
