use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::routes::AppState;

/// Weighted in-flight upload budget (bytes) to avoid OOM under concurrent PUT bodies.
pub struct UploadBudget {
    max_bytes: u64,
    permit_unit: u64,
    in_flight: AtomicU64,
}

impl UploadBudget {
    pub fn new(max_bytes: u64, permit_unit: u64) -> Arc<Self> {
        Arc::new(Self {
            max_bytes: max_bytes.max(1),
            permit_unit: permit_unit.max(4096),
            in_flight: AtomicU64::new(0),
        })
    }

    pub fn permits_for(&self, content_length: u64) -> u64 {
        let units = content_length.div_ceil(self.permit_unit);
        units.saturating_mul(self.permit_unit).max(self.permit_unit)
    }

    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn try_acquire(&self, bytes: u64) -> Option<UploadPermitGuard<'_>> {
        let need = self.permits_for(bytes);
        loop {
            let current = self.in_flight.load(Ordering::Relaxed);
            if current.saturating_add(need) > self.max_bytes {
                return None;
            }
            if self
                .in_flight
                .compare_exchange_weak(current, current + need, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(UploadPermitGuard {
                    budget: self,
                    bytes: need,
                });
            }
        }
    }
}

pub struct UploadPermitGuard<'a> {
    budget: &'a UploadBudget,
    bytes: u64,
}

impl Drop for UploadPermitGuard<'_> {
    fn drop(&mut self) {
        self.budget
            .in_flight
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

fn is_upload_method(method: &Method) -> bool {
    matches!(method, &Method::PUT | &Method::POST)
}

fn is_upload_path(path: &str) -> bool {
    path.contains("/_multipart/") && path.ends_with("/complete")
        || (path.matches('/').count() >= 2 && !path.starts_with("/_nos/") && !path.starts_with("/_cluster/"))
}

/// Returns 503 + Retry-After when the aggregate upload buffer is saturated.
pub async fn upload_budget_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let Some(budget) = state.upload_budget.as_ref() else {
        return next.run(req).await;
    };

    if !is_upload_method(req.method()) || !is_upload_path(req.uri().path()) {
        return next.run(req).await;
    }

    let content_length = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(state.max_body_size as u64)
        .min(state.max_body_size as u64);

    let Some(guard) = budget.try_acquire(content_length) else {
        state.metrics.inc_upload_rejected();
        let mut resp = (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "upload capacity exceeded, retry later" })),
        )
            .into_response();
        resp.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_static("1"),
        );
        return resp;
    };

    let response = next.run(req).await;
    drop(guard);
    response
}

pub fn default_retry_after() -> Duration {
    Duration::from_secs(1)
}
