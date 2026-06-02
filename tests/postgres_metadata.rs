//! Postgres metadata backend integration tests (requires Docker for testcontainers).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use nebular_os::auth::Claims;
use nebular_os::cluster::{build_backend, ClusterConfig};
use nebular_os::config::NosConfig;
use nebular_os::observability::NosMetrics;
use nebular_os::server::create_app;
use nebular_os::storage::engine::{EngineOptions, StorageEngine};
use nebular_os::storage::metadata_backend::MetadataBackendKind;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tower::ServiceExt;

const TEST_SECRET: &str = "test-secret-key-that-is-long-enough-for-hs256-32-bytes!";

fn make_token() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = Claims {
        sub: "user-1".into(),
        email: "test@example.com".into(),
        role: "admin".into(),
        exp: now + 3600,
        iat: now,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap()
}

async fn postgres_app() -> Option<(axum::Router, String, TempDir, testcontainers::ContainerAsync<Postgres>)> {
    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping postgres metadata tests (docker unavailable): {e}");
            return None;
        }
    };
    let host_port = container.get_host_port_ipv4(5432).await.ok()?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/postgres");

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let meta_sqlite = tmp.path().join("system.db");
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let meta_path_str = meta_sqlite.to_string_lossy().replace('\\', "/");

    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
        EngineOptions {
            metadata_backend: MetadataBackendKind::Postgres,
            metadata_database_url: Some(url),
            read_pool_size: 2,
            upload_buffer_size: 64 * 1024,
            ..EngineOptions::default()
        },
    )
    .await
    .ok()?;

    let cfg = Arc::new(NosConfig {
        bind_addr: "127.0.0.1:0".into(),
        data_dir: data_dir_str.to_string(),
        meta_path: meta_path_str.to_string(),
        metadata_backend: MetadataBackendKind::Postgres,
        metadata_database_url: Some(format!(
            "postgres://postgres:postgres@127.0.0.1:{host_port}/postgres"
        )),
        max_logical_bytes: 0,
        jwt_secret: TEST_SECRET.into(),
        signing_secret: None,
        max_body_size: 10_000_000,
        upload_buffer_size: 64 * 1024,
        allow_public_read: false,
        reconcile_on_startup: false,
        reconcile_interval_secs: 0,
        soft_delete_ttl_secs: 0,
        soft_delete_drop_blob: true,
        multipart_upload_ttl_secs: 86_400,
        recompress_on_startup: false,
        recompress_interval_secs: 0,
        recompress_batch_size: 100,
        metrics_token: None,
        rate_limit_rps: 0,
        rate_limit_burst: 50,
        list_scan_cap: 4096,
        multipart_part_size: 8 * 1024 * 1024,
        read_pool_size: 2,
        cors_origins: vec![],
        zstd_level: 3,
        s3_compat: false,
        bucket_policy: nebular_os::config::BucketPolicy::default(),
        s3_access_key: None,
        s3_secret_key: None,
        cluster_bootstrap_token: None,
        cluster: ClusterConfig::standalone(),
    });

    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).ok()?;
    let app = create_app(backend, storage, cfg, metrics).await.ok()?;

    Some((app, make_token(), tmp, container))
}

#[tokio::test]
async fn postgres_metadata_put_get_delete_list() {
    let Some((app, token, _tmp, _container)) = postgres_app().await else {
        return;
    };

    let req = Request::builder()
        .method("PUT")
        .uri("/media/pg-test.bin")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/octet-stream")
        .body(Body::from("postgres-bytes"))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::CREATED
    );

    let req = Request::builder()
        .method("GET")
        .uri("/media/pg-test.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"postgres-bytes");

    let req = Request::builder()
        .method("GET")
        .uri("/media?prefix=pg")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let req = Request::builder()
        .method("DELETE")
        .uri("/media/pg-test.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );

    let req = Request::builder()
        .method("GET")
        .uri("/media/pg-test.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.oneshot(req).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn postgres_metadata_metrics_and_ready() {
    let Some((app, _token, _tmp, _container)) = postgres_app().await else {
        return;
    };

    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["metadata_backend"], "postgres");
    assert_eq!(json["max_logical_bytes"], 0);

    let req = Request::builder()
        .method("GET")
        .uri("/health/ready")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["checks"]["metadata_backend"], "postgres");
    assert_eq!(json["checks"]["postgres_ok"], true);
}
