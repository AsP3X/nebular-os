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
        metadata_mode: nebular_os::storage::metadata_mode::MetadataMode::Full,
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
        bulk_delete_concurrency: 32,
        bulk_delete_batch_limit: 1000,
        upload_max_in_flight_bytes: 32 * 1024 * 1024,
        upload_permit_unit: 5 * 1024 * 1024,
        upload_idle_timeout_secs: 60,
        header_read_timeout_secs: 75,
        send_stall_timeout_secs: 300,
        shutdown_grace_secs: 8,
        max_connections: 0,
        presign_max_ttl_secs: 7 * 24 * 3600,
        orphan_gc_interval_secs: 0,
        rate_limit_bypass_roles: vec!["admin".into()],
        multipart_part_size: 8 * 1024 * 1024,
        read_pool_size: 2,
        cors_origins: vec![],
        zstd_level: 3,
        zstd_level_upload: 3,
        zstd_dict_enabled: false,
        zstd_dict_max_bytes: 112_640,
        zstd_dict_train_batch: 32,
        dedup_enabled: false,
        block_size: 64 * 1024,
        dedup_block_size: 256 * 1024,
        dedup_min_size: 1024 * 1024,
        compress_min_size: 4096,
        compress_block_size: 64 * 1024,
        compress_exclude_extensions: vec![],
        block_cache_entries: 0,
        block_cache_max_bytes: 64 * 1024 * 1024,
        verify_interval_secs: 0,
        verify_batch_size: 100,
        scrub_sample_denom: 1,
        scrub_mode_light: false,
        verify_on_read: false,
        read_buffer_size: 256 * 1024,
        fsync_writes: true,
        webhooks: nebular_os::webhooks::WebhookConfig::default(),
        s3_compat: false,
        bucket_policy: nebular_os::config::BucketPolicy::default(),
        s3_access_key: None,
        s3_secret_key: None,
        s3_access_key_role: "admin".into(),
        legacy_access_key_auth: false,
        jwt_issuer: None,
        jwt_audience: None,
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

async fn call(app: &axum::Router, method: &str, uri: &str, token: &str, body: Body) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(body)
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

#[tokio::test]
async fn postgres_metadata_multipart_parts_and_case_sensitive_prefix() {
    let Some((app, token, _tmp, _container)) = postgres_app().await else {
        return;
    };

    // Human: Part rows (size_bytes/etag) drive gap detection and verification on Postgres too.
    let (status, body) = call(&app, "POST", "/media/_multipart?key=pg-parts.bin", &token, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    let upload_id = serde_json::from_slice::<Value>(&body).unwrap()["upload_id"]
        .as_str()
        .unwrap()
        .to_string();
    for (part, data) in [(1, "alpha-"), (3, "gamma")] {
        let uri = format!("/media/_multipart/{upload_id}/parts/{part}");
        assert_eq!(call(&app, "PUT", &uri, &token, Body::from(data)).await.0, StatusCode::OK);
    }
    let complete = format!("/media/_multipart/{upload_id}/complete");
    assert_eq!(call(&app, "POST", &complete, &token, Body::empty()).await.0, StatusCode::BAD_REQUEST);
    let list = r#"{"parts":[{"part_number":1},{"part_number":3}]}"#;
    assert_eq!(call(&app, "POST", &complete, &token, Body::from(list)).await.0, StatusCode::CREATED);
    let (status, body) = call(&app, "GET", "/media/pg-parts.bin", &token, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"alpha-gamma");

    for key in ["Users/keep.txt", "users/drop.txt"] {
        let uri = format!("/media/{key}");
        assert_eq!(call(&app, "PUT", &uri, &token, Body::from("x")).await.0, StatusCode::CREATED);
    }
    let (status, body) = call(&app, "GET", "/media?prefix=users/&count_only=true", &token, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["count"], 1);
    assert_eq!(call(&app, "DELETE", "/media?prefix=users/", &token, Body::empty()).await.0, StatusCode::OK);
    assert_eq!(call(&app, "GET", "/media/Users/keep.txt", &token, Body::empty()).await.0, StatusCode::OK);
}

#[tokio::test]
async fn postgres_migrations_tolerate_concurrent_starts() {
    use nebular_os::storage::object_meta::{ObjectMetaConnect, ObjectMetaStore};

    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping postgres migration test (docker unavailable): {e}");
            return;
        }
    };
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let connect = || {
        ObjectMetaStore::connect(ObjectMetaConnect {
            backend: MetadataBackendKind::Postgres,
            sqlite_path: String::new(),
            postgres_url: Some(url.clone()),
            read_pool_size: 1,
        })
    };

    // Human: Several nodes booting against a fresh database at once; each applies the schema.
    let results = futures_util::future::join_all((0..6).map(|_| connect())).await;
    for result in &results {
        assert!(result.is_ok(), "{:?}", result.as_ref().err());
    }
    // Human: And again on the migrated database (every start re-applies the idempotent schema).
    assert!(connect().await.is_ok());

    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM nos_schema_migrations")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(versions, [1]);
    assert!(wait_for_valid_key_index(&pool).await, "the key index was never built");
}

/// Whether `idx_nos_objects_active_key` exists and is valid.
async fn key_index_is_valid(pool: &sqlx::PgPool) -> bool {
    let valid: Option<bool> = sqlx::query_scalar(
        "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('idx_nos_objects_active_key')",
    )
    .fetch_optional(pool)
    .await
    .unwrap();
    valid == Some(true)
}

/// `key_index_is_valid`, waiting up to a minute for the background build.
async fn wait_for_valid_key_index(pool: &sqlx::PgPool) -> bool {
    for _ in 0..600 {
        if key_index_is_valid(pool).await {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test]
async fn postgres_rebuilds_an_interrupted_online_index() {
    use nebular_os::storage::object_meta::{ObjectMetaConnect, ObjectMetaStore};

    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping postgres index test (docker unavailable): {e}");
            return;
        }
    };
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let connect = || {
        ObjectMetaStore::connect(ObjectMetaConnect {
            backend: MetadataBackendKind::Postgres,
            sqlite_path: String::new(),
            postgres_url: Some(url.clone()),
            read_pool_size: 1,
        })
    };
    connect().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    assert!(wait_for_valid_key_index(&pool).await);

    // Human: A concurrent build that fails (here: a unique index over duplicates) leaves an invalid index under
    // the name, which `IF NOT EXISTS` alone would keep forever.
    sqlx::query(
        "INSERT INTO nos_objects (bucket, object_key, blob_path, size_bytes) VALUES ('b', 'k1', 'p', 1), ('b', 'k2', 'p', 1)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DROP INDEX idx_nos_objects_active_key").execute(&pool).await.unwrap();
    let failed = sqlx::query(
        "CREATE UNIQUE INDEX CONCURRENTLY idx_nos_objects_active_key ON nos_objects (bucket) WHERE deleted_at IS NULL",
    )
    .execute(&pool)
    .await;
    assert!(failed.is_err());
    assert!(!key_index_is_valid(&pool).await);

    connect().await.unwrap();
    assert!(wait_for_valid_key_index(&pool).await, "the invalid index was not rebuilt");
    let definition: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_nos_objects_active_key'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(definition.contains("(object_key)") && !definition.contains("UNIQUE"), "{definition}");
}

