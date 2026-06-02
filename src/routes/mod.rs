pub mod bucket;
pub mod capabilities;
pub mod errors;
pub mod health;
pub mod helpers;
pub mod metrics;
pub mod multipart;
pub mod object;

use std::sync::Arc;

use dashmap::DashMap;

use std::sync::RwLock;

use crate::cluster::config::ClusterConfig;
use crate::config::NosConfig;
use crate::middleware::rate_limit::ClientBucket;
use crate::observability::NosMetrics;
use crate::cluster::StorageBackend;
use crate::storage::engine::StorageEngine;

#[derive(Clone)]
pub struct AppState {
    pub backend: Arc<RwLock<StorageBackend>>,
    pub cluster: Arc<RwLock<ClusterConfig>>,
    pub engine: StorageEngine,
    pub config: Arc<NosConfig>,
    pub bootstrap_token: Option<Arc<String>>,
    pub jwt_secret: Arc<crate::auth::JwtSecret>,
    pub signing_secret: Option<Arc<String>>,
    pub metrics_token: Option<Arc<String>>,
    pub metrics: Arc<NosMetrics>,
    pub rate_limiters: Arc<DashMap<String, ClientBucket>>,
    pub max_body_size: usize,
    pub allow_public_read: bool,
}

impl AppState {
    /// Human: Clone the active storage facade for async handlers (short-lived).
    pub fn backend(&self) -> StorageBackend {
        self.backend
            .read()
            .expect("backend lock poisoned")
            .clone()
    }

    /// Human: Snapshot cluster settings for health and capabilities routes.
    pub fn cluster(&self) -> ClusterConfig {
        self.cluster
            .read()
            .expect("cluster lock poisoned")
            .clone()
    }
}

pub type SharedState = axum::extract::State<Arc<AppState>>;
