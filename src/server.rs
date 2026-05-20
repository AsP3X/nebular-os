use axum::{
    middleware,
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    normalize_path::NormalizePathLayer,
    trace::TraceLayer,
};

use crate::auth::{presigned_or_jwt_middleware, JwtSecret};
use crate::config::NosConfig;
use crate::middleware::{
    metrics_auth::metrics_auth_middleware, rate_limit::rate_limit_middleware,
    rate_limit::new_rate_limit_map,
};
use crate::observability::NosMetrics;
use crate::routes::{bucket, health, metrics, multipart, object, AppState};
use crate::storage::engine::StorageEngine;

pub async fn create_app(storage: StorageEngine, cfg: Arc<NosConfig>) -> anyhow::Result<Router> {
    let metrics = NosMetrics::new();
    let state = Arc::new(AppState {
        storage,
        config: cfg.clone(),
        jwt_secret: Arc::new(JwtSecret(cfg.jwt_secret.clone())),
        signing_secret: cfg.signing_secret.clone().map(Arc::new),
        metrics_token: cfg.metrics_token.clone().map(Arc::new),
        metrics,
        rate_limiters: new_rate_limit_map(),
        max_body_size: cfg.max_body_size,
        allow_public_read: cfg.allow_public_read,
    });

    let auth_layer =
        middleware::from_fn_with_state(state.clone(), presigned_or_jwt_middleware);

    let mut metrics_router = Router::new().route("/metrics", get(metrics::metrics));
    if cfg.metrics_token.is_some() {
        metrics_router = metrics_router.layer(middleware::from_fn_with_state(
            state.clone(),
            metrics_auth_middleware,
        ));
    }

    let multipart_routes = Router::new()
        .route("/{bucket}/_multipart", post(multipart::init_multipart))
        .route(
            "/{bucket}/_multipart/{upload_id}/parts/{part_number}",
            put(multipart::upload_part),
        )
        .route(
            "/{bucket}/_multipart/{upload_id}/complete",
            post(multipart::complete_multipart),
        )
        .route(
            "/{bucket}/_multipart/{upload_id}",
            delete(multipart::abort_multipart),
        );

    let mut protected_routes = Router::new()
        .merge(multipart_routes)
        .route(
            "/{bucket}/{*key}",
            put(object::put_object)
                .delete(object::delete_object)
                .get(object::get_object)
                .head(object::head_object),
        )
        .route("/{bucket}", get(bucket::list_objects))
        .layer(auth_layer);

    if cfg.rate_limit_rps > 0 {
        protected_routes = protected_routes.layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ));
    }

    let public_routes = Router::new().route("/health", get(health::health));

    let cors = build_cors(&cfg);

    let app = public_routes
        .merge(metrics_router)
        .merge(protected_routes)
        .layer(NormalizePathLayer::trim_trailing_slash())
        .layer(cors)
        .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http()))
        .with_state(state);

    Ok(app)
}

fn build_cors(cfg: &NosConfig) -> CorsLayer {
    if cfg.cors_origins.is_empty() {
        return CorsLayer::permissive();
    }
    let origins: Vec<_> = cfg
        .cors_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(Any)
        .allow_headers(Any)
}
