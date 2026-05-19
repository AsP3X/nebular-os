pub mod bucket;
pub mod health;
pub mod metrics;
pub mod object;

use std::sync::Arc;
use crate::storage::engine::StorageEngine;

#[derive(Clone)]
pub struct AppState {
    pub storage: StorageEngine,
    pub jwt_secret: Arc<crate::auth::JwtSecret>,
    pub signing_secret: Option<Arc<String>>,
    pub max_body_size: usize,
    pub allow_public_read: bool,
}

pub type SharedState = axum::extract::State<Arc<AppState>>;
