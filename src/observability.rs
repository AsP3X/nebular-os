use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// In-process counters exported as Prometheus text on `/metrics`.
#[derive(Default)]
pub struct NosMetrics {
    pub http_requests_total: AtomicU64,
    pub http_errors_total: AtomicU64,
    pub bytes_uploaded_total: AtomicU64,
    pub bytes_downloaded_total: AtomicU64,
}

impl NosMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc_requests(&self) {
        self.http_requests_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_errors(&self) {
        self.http_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_uploaded(&self, n: u64) {
        self.bytes_uploaded_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_downloaded(&self, n: u64) {
        self.bytes_downloaded_total.fetch_add(n, Ordering::Relaxed);
    }

    /// Renders Prometheus exposition format for scrape targets.
    pub fn render_prometheus(&self, total_objects: i64, total_bytes: i64) -> String {
        format!(
            "# HELP nos_http_requests_total Total HTTP requests handled.\n\
             # TYPE nos_http_requests_total counter\n\
             nos_http_requests_total {}\n\
             # HELP nos_http_errors_total Total HTTP 4xx/5xx responses.\n\
             # TYPE nos_http_errors_total counter\n\
             nos_http_errors_total {}\n\
             # HELP nos_bytes_uploaded_total Total uploaded bytes.\n\
             # TYPE nos_bytes_uploaded_total counter\n\
             nos_bytes_uploaded_total {}\n\
             # HELP nos_bytes_downloaded_total Total downloaded bytes.\n\
             # TYPE nos_bytes_downloaded_total counter\n\
             nos_bytes_downloaded_total {}\n\
             # HELP nos_objects_total Live objects in metadata DB.\n\
             # TYPE nos_objects_total gauge\n\
             nos_objects_total {}\n\
             # HELP nos_storage_bytes_total Live object bytes in metadata DB.\n\
             # TYPE nos_storage_bytes_total gauge\n\
             nos_storage_bytes_total {}\n",
            self.http_requests_total.load(Ordering::Relaxed),
            self.http_errors_total.load(Ordering::Relaxed),
            self.bytes_uploaded_total.load(Ordering::Relaxed),
            self.bytes_downloaded_total.load(Ordering::Relaxed),
            total_objects,
            total_bytes,
        )
    }
}
