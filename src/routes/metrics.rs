use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::routes::AppState;

#[derive(serde::Serialize)]
pub struct StorageClassCount {
    pub class: String,
    pub count: i64,
}

#[derive(serde::Serialize)]
pub struct MetricsResponse {
    pub total_objects: i64,
    pub total_bytes: i64,
    pub logical_bytes: i64,
    pub max_logical_bytes: i64,
    pub metadata_backend: String,
    pub replication_pending_events: u64,
    pub replication_errors_total: u64,
    pub storage_class_counts: Vec<StorageClassCount>,
}

/// How long computed storage totals are reused. Computing them scans the metadata tables, and /metrics is open
/// to anyone unless NOS_METRICS_TOKEN is set, so scrapes share one result instead of each running the scans.
const STORAGE_STATS_TTL: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct StorageStats {
    total_objects: i64,
    total_bytes: i64,
    replication_pending_events: u64,
    storage_class_counts: Vec<(String, i64)>,
}

/// Human: The last computed storage totals. Holding the lock while computing makes concurrent scrapes wait for
/// one computation instead of starting their own.
#[derive(Default)]
pub struct StorageStatsCache(tokio::sync::Mutex<Option<(Instant, StorageStats)>>);

impl StorageStatsCache {
    async fn get(&self, state: &AppState) -> Result<StorageStats, StatusCode> {
        let mut cached = self.0.lock().await;
        if let Some((at, stats)) = cached.as_ref()
            && at.elapsed() < STORAGE_STATS_TTL
        {
            return Ok(stats.clone());
        }
        let stats = compute_storage_stats(state).await?;
        *cached = Some((Instant::now(), stats.clone()));
        Ok(stats)
    }
}

async fn compute_storage_stats(state: &AppState) -> Result<StorageStats, StatusCode> {
    let failed = |what: &'static str| {
        move |e: crate::storage::error::StorageError| {
            tracing::error!(error = %e, "{what} failed");
            state.metrics.inc_errors();
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    let backend = state.backend();
    Ok(StorageStats {
        total_objects: backend.object_count().await.map_err(failed("object_count"))?,
        total_bytes: backend.total_bytes().await.map_err(failed("total_bytes"))?,
        replication_pending_events: backend.pending_replication_events().await.unwrap_or(0),
        storage_class_counts: backend
            .engine()
            .objects_by_storage_class()
            .await
            .unwrap_or_default(),
    })
}

pub async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    state.metrics.inc_requests();
    let StorageStats {
        total_objects,
        total_bytes,
        replication_pending_events,
        storage_class_counts,
    } = state.storage_stats.get(&state).await?;

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if accept.contains("text/plain") || accept.contains("application/openmetrics-text") {
        let body = state.metrics.render_prometheus(
            total_objects,
            total_bytes,
            replication_pending_events,
            &storage_class_counts,
            state
                .metrics
                .upload_in_flight_bytes(state.upload_budget.as_deref()),
        );
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        )
            .into_response());
    }

    let storage_class_counts: Vec<StorageClassCount> = storage_class_counts
        .into_iter()
        .map(|(class, count)| StorageClassCount { class, count })
        .collect();

    Ok(Json(MetricsResponse {
        total_objects,
        total_bytes,
        logical_bytes: total_bytes,
        max_logical_bytes: state.engine.max_logical_bytes(),
        metadata_backend: state.engine.metadata_backend().as_str().to_string(),
        replication_pending_events,
        replication_errors_total: state.metrics.replication_errors_total(),
        storage_class_counts,
    })
    .into_response())
}
