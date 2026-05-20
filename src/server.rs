use axum::{
    middleware,
    routing::{get, put},
    Router,
};
use tower_http::cors::CorsLayer;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::normalize_path::NormalizePathLayer;
use tower_http::trace::TraceLayer;

use crate::auth::{presigned_or_jwt_middleware, JwtSecret};
use crate::routes::{bucket, health, metrics, object, AppState};
use crate::storage::engine::StorageEngine;

pub async fn create_app(
    storage: StorageEngine,
    jwt_secret: String,
    signing_secret: Option<String>,
    max_body_size: usize,
    allow_public_read: bool,
) -> anyhow::Result<Router> {
    let secret = Arc::new(JwtSecret(jwt_secret));
    let signing_secret = signing_secret.map(Arc::new);
    let state = Arc::new(AppState {
        storage,
        jwt_secret: secret,
        signing_secret,
        max_body_size,
        allow_public_read,
    });

    let auth_layer =
        middleware::from_fn_with_state(state.clone(), presigned_or_jwt_middleware);

    let public_routes = Router::new()
        .route("/health", get(health::health))
        .route("/metrics", get(metrics::metrics));

    let protected_routes = Router::new()
        .route(
            "/{bucket}/{*key}",
            put(object::put_object)
                .delete(object::delete_object)
                .get(object::get_object)
                .head(object::head_object),
        )
        .route("/{bucket}", get(bucket::list_objects))
        .layer(auth_layer);

    let app = public_routes
        .merge(protected_routes)
        .layer(NormalizePathLayer::trim_trailing_slash())
        .layer(CorsLayer::permissive())
        .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http()))
        .with_state(state);

    Ok(app)
}
