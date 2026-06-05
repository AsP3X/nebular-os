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
use crate::cluster::{auth as cluster_auth, replicate, routes as cluster_routes};
use crate::cluster::StorageBackend;
use crate::config::NosConfig;
use crate::middleware::{
    metrics_auth::metrics_auth_middleware, rate_limit::rate_limit_middleware,
    rate_limit::new_rate_limit_map, upload_budget::upload_budget_middleware,
};
use crate::observability::NosMetrics;
use crate::routes::{batch, bucket, capabilities, health, maintenance, metrics, multipart, object, AppState};

pub async fn create_app(
    backend: StorageBackend,
    engine: crate::storage::engine::StorageEngine,
    cfg: Arc<NosConfig>,
    metrics: Arc<NosMetrics>,
) -> anyhow::Result<Router> {
    let cluster = Arc::new(std::sync::RwLock::new(cfg.cluster.clone()));
    let backend = Arc::new(std::sync::RwLock::new(backend));
    let bootstrap_token = cfg.cluster_bootstrap_token.clone().map(Arc::new);
    let upload_budget = if cfg.upload_max_in_flight_bytes > 0 {
        Some(crate::middleware::UploadBudget::new(
            cfg.upload_max_in_flight_bytes,
            cfg.upload_permit_unit,
        ))
    } else {
        None
    };
    let state = Arc::new(AppState {
        backend,
        cluster,
        engine,
        config: cfg.clone(),
        bootstrap_token,
        jwt_secret: Arc::new(JwtSecret(cfg.jwt_secret.clone())),
        signing_secret: cfg.signing_secret.clone().map(Arc::new),
        metrics_token: cfg.metrics_token.clone().map(Arc::new),
        metrics,
        rate_limiters: new_rate_limit_map(),
        upload_budget,
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
        .route("/_nos/capabilities", get(capabilities::capabilities))
        .route("/_nos/maintenance/orphans", get(maintenance::list_orphans))
        .route(
            "/_nos/maintenance/gc_orphans",
            axum::routing::post(maintenance::gc_orphans),
        )
        .merge(multipart_routes)
        .route("/{bucket}/_batch_delete", axum::routing::post(batch::batch_delete))
        .route(
            "/{bucket}/{*key}",
            put(object::put_object)
                .delete(object::delete_object)
                .get(object::get_object)
                .head(object::head_object),
        )
        .route("/{bucket}", get(bucket::list_objects).delete(bucket::delete_objects_by_prefix));

    if cfg.upload_max_in_flight_bytes > 0 {
        protected_routes = protected_routes.layer(middleware::from_fn_with_state(
            state.clone(),
            upload_budget_middleware,
        ));
    }

    if cfg.rate_limit_rps > 0 {
        protected_routes = protected_routes.layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ));
    }

    protected_routes = protected_routes.layer(auth_layer);

    let mut public_routes = Router::new();

    // Human: Cluster API when clustered or bootstrap token enables runtime config from Ownly.
    // Agent: MERGE /_cluster/* when !standalone OR NOS_CLUSTER_BOOTSTRAP_TOKEN; config routes always in that set.
    let mount_cluster =
        !cfg.cluster.is_standalone() || state.bootstrap_token.is_some();
    if mount_cluster {
        let cluster_layer =
            middleware::from_fn_with_state(state.clone(), cluster_auth::cluster_token_middleware);
        let cluster_router = Router::new()
            .route("/_cluster/health", get(cluster_routes::cluster_health))
            .route(
                "/_cluster/capabilities",
                get(cluster_routes::cluster_capabilities),
            )
            .route(
                "/_cluster/config",
                get(crate::cluster::config_api::get_cluster_config)
                    .put(crate::cluster::config_api::put_cluster_config),
            )
            .route("/_cluster/replicate", post(replicate::replicate))
            .route(
                "/_cluster/assignment/resolve",
                post(cluster_routes::assignment_resolve),
            )
            .route(
                "/_cluster/objects/{bucket}/{*key}",
                axum::routing::get(cluster_routes::cluster_object_get)
                    .head(cluster_routes::cluster_object_head),
            )
            .layer(cluster_layer);
        public_routes = public_routes.merge(cluster_router);
    }

    let cors = build_cors(&cfg);

    // Human: Register liveness/readiness after merge so static paths win over `/{bucket}/{*key}`.
    // Agent: MERGE order alone is not enough — `health`+`ready` matched object routes and returned 401.
    let app = public_routes
        .merge(metrics_router)
        .merge(protected_routes)
        .route("/health", get(health::health))
        .route("/health/ready", get(health::ready))
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
