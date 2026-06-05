use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// In-process counters exported as Prometheus text on `/metrics`.
#[derive(Default)]
pub struct NosMetrics {
    pub http_requests_total: AtomicU64,
    pub http_errors_total: AtomicU64,
    pub bytes_uploaded_total: AtomicU64,
    pub bytes_downloaded_total: AtomicU64,
    pub replication_errors_total: AtomicU64,
    pub objects_deleted_total: AtomicU64,
    pub upload_rejected_total: AtomicU64,
    pub orphan_gc_bytes_reclaimed_total: AtomicU64,
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

    pub fn inc_replication_errors(&self) {
        self.replication_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_objects_deleted(&self, n: u64) {
        self.objects_deleted_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_upload_rejected(&self) {
        self.upload_rejected_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_orphan_gc_bytes(&self, n: u64) {
        self.orphan_gc_bytes_reclaimed_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn replication_errors_total(&self) -> u64 {
        self.replication_errors_total.load(Ordering::Relaxed)
    }

    pub fn upload_in_flight_bytes(&self, budget: Option<&super::middleware::UploadBudget>) -> u64 {
        budget.map(|b| b.in_flight_bytes()).unwrap_or(0)
    }

    /// Renders Prometheus exposition format for scrape targets.
    pub fn render_prometheus(
        &self,
        total_objects: i64,
        total_bytes: i64,
        replication_pending_events: u64,
        storage_class_counts: &[(String, i64)],
        upload_in_flight_bytes: u64,
    ) -> String {
        let mut out = format!(
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
             # HELP nos_objects_deleted_total Objects deleted via batch/prefix delete.\n\
             # TYPE nos_objects_deleted_total counter\n\
             nos_objects_deleted_total {}\n\
             # HELP nos_upload_rejected_total Upload requests rejected due to capacity.\n\
             # TYPE nos_upload_rejected_total counter\n\
             nos_upload_rejected_total {}\n\
             # HELP nos_orphan_gc_bytes_reclaimed_total Bytes reclaimed by orphan GC.\n\
             # TYPE nos_orphan_gc_bytes_reclaimed_total counter\n\
             nos_orphan_gc_bytes_reclaimed_total {}\n\
             # HELP nos_upload_in_flight_bytes Current weighted upload buffer usage.\n\
             # TYPE nos_upload_in_flight_bytes gauge\n\
             nos_upload_in_flight_bytes {}\n\
             # HELP nos_objects_total Live objects in metadata DB.\n\
             # TYPE nos_objects_total gauge\n\
             nos_objects_total {}\n\
             # HELP nos_storage_bytes_total Live object bytes in metadata DB.\n\
             # TYPE nos_storage_bytes_total gauge\n\
             nos_storage_bytes_total {}\n\
             # HELP nos_logical_bytes_total Logical bytes (same as storage_bytes for v1).\n\
             # TYPE nos_logical_bytes_total gauge\n\
             nos_logical_bytes_total {}\n\
             # HELP nos_replication_pending_events Pending replication_log rows.\n\
             # TYPE nos_replication_pending_events gauge\n\
             nos_replication_pending_events {}\n\
             # HELP nos_replication_errors_total Replication push failures.\n\
             # TYPE nos_replication_errors_total counter\n\
             nos_replication_errors_total {}\n",
            self.http_requests_total.load(Ordering::Relaxed),
            self.http_errors_total.load(Ordering::Relaxed),
            self.bytes_uploaded_total.load(Ordering::Relaxed),
            self.bytes_downloaded_total.load(Ordering::Relaxed),
            self.objects_deleted_total.load(Ordering::Relaxed),
            self.upload_rejected_total.load(Ordering::Relaxed),
            self.orphan_gc_bytes_reclaimed_total.load(Ordering::Relaxed),
            upload_in_flight_bytes,
            total_objects,
            total_bytes,
            total_bytes,
            replication_pending_events,
            self.replication_errors_total.load(Ordering::Relaxed),
        );
        out.push_str(
            "# HELP nos_storage_class_objects Objects per storage_class label.\n\
             # TYPE nos_storage_class_objects gauge\n",
        );
        for (class, count) in storage_class_counts {
            out.push_str(&format!(
                "nos_storage_class_objects{{class=\"{}\"}} {}\n",
                escape_label(class),
                count
            ));
        }
        out
    }
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
