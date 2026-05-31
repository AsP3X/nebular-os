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

use crate::config::NosConfig;
use crate::middleware::rate_limit::ClientBucket;
use crate::observability::NosMetrics;
use crate::cluster::StorageBackend;

#[derive(Clone)]
pub struct AppState {
    pub backend: StorageBackend,
    pub config: Arc<NosConfig>,
    pub jwt_secret: Arc<crate::auth::JwtSecret>,
    pub signing_secret: Option<Arc<String>>,
    pub metrics_token: Option<Arc<String>>,
    pub metrics: Arc<NosMetrics>,
    pub rate_limiters: Arc<DashMap<String, ClientBucket>>,
    pub max_body_size: usize,
    pub allow_public_read: bool,
}

pub type SharedState = axum::extract::State<Arc<AppState>>;
