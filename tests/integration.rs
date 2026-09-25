use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use nebular_os::auth::Claims;
use nebular_os::cluster::{build_backend, ClusterConfig};
use nebular_os::observability::NosMetrics;
use nebular_os::config::NosConfig;
use nebular_os::server::create_app;
use nebular_os::storage::engine::{EngineOptions, StorageEngine};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
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

fn test_config(signing_secret: Option<String>, allow_public_read: bool) -> Arc<NosConfig> {
    test_config_with_cap(signing_secret, allow_public_read, 0)
}

fn test_config_with_cap(
    signing_secret: Option<String>,
    allow_public_read: bool,
    max_logical_bytes: i64,
) -> Arc<NosConfig> {
    Arc::new(NosConfig {
        bind_addr: "127.0.0.1:0".into(),
        data_dir: "./data/blobs".into(),
        meta_path: "./data/meta/metadata.db".into(),
        metadata_backend: nebular_os::storage::metadata_backend::MetadataBackendKind::Sqlite,
        metadata_mode: nebular_os::storage::metadata_mode::MetadataMode::Full,
        metadata_database_url: None,
        max_logical_bytes,
        jwt_secret: TEST_SECRET.into(),
        signing_secret,
        max_body_size: 10_000_000,
        upload_buffer_size: 64 * 1024,
        allow_public_read,
        reconcile_on_startup: false,
        reconcile_interval_secs: 0,
        soft_delete_ttl_secs: 86_400,
        soft_delete_drop_blob: false,
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
    })
}

#[tokio::test]
async fn standalone_ignores_storage_class_header() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/local.bin")
        .header("authorization", format!("Bearer {token}"))
        .header("x-nd-storage-class", "hls-hot")
        .header("content-type", "video/mp4")
        .body(Body::from("local"))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn setup_app_with_cap(
    signing_secret: Option<String>,
    allow_public_read: bool,
    max_logical_bytes: i64,
) -> (axum::Router, String, TempDir) {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");

    std::fs::create_dir_all(&data_dir).unwrap();

    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");

    let cfg = test_config_with_cap(signing_secret, allow_public_read, max_logical_bytes);
    let (app, tmp) = setup_app_with_config_in(cfg, tmp, &meta_path_str, &data_dir_str).await;
    (app, make_token(), tmp)
}

async fn setup_app_with_config_in(
    cfg: Arc<NosConfig>,
    tmp: TempDir,
    meta_path_str: &str,
    data_dir_str: &str,
) -> (axum::Router, TempDir) {
    let storage = StorageEngine::with_full_options(
        meta_path_str,
        data_dir_str,
        EngineOptions {
            upload_buffer_size: cfg.upload_buffer_size,
            read_pool_size: cfg.read_pool_size,
            max_logical_bytes: cfg.max_logical_bytes,
            metadata_backend: cfg.metadata_backend,
            metadata_database_url: cfg.metadata_database_url.clone(),
            compress_block_size: 128 * 1024,
            ..EngineOptions::default()
        },
    )
    .await
    .unwrap();
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).unwrap();
    let app = create_app(backend, storage, cfg, metrics).await.unwrap();
    (app, tmp)
}

async fn setup_app(signing_secret: Option<String>, allow_public_read: bool) -> (axum::Router, String, TempDir) {
    setup_app_with_cap(signing_secret, allow_public_read, 0).await
}

#[tokio::test]
async fn test_put_get_delete() {
    let (app, token, _tmp) = setup_app(None, false).await;

    // PUT
    let req = Request::builder()
        .method("PUT")
        .uri("/music/tracks/song.mp3")
        .header("authorization", format!("Bearer {}", token))
        .header("content-type", "audio/mpeg")
        .body(Body::from("fake audio data"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // GET
    let req = Request::builder()
        .method("GET")
        .uri("/music/tracks/song.mp3")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"fake audio data");

    // DELETE
    let req = Request::builder()
        .method("DELETE")
        .uri("/music/tracks/song.mp3")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // GET after DELETE
    let req = Request::builder()
        .method("GET")
        .uri("/music/tracks/song.mp3")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_unauthorized() {
    let (app, _token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("GET")
        .uri("/music/tracks/song.mp3")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_list_objects() {
    let (app, token, _tmp) = setup_app(None, false).await;

    for key in &["a.mp3", "b.mp3"] {
        let req = Request::builder()
            .method("PUT")
            .uri(format!("/music/{}", key))
            .header("authorization", format!("Bearer {}", token))
            .body(Body::from("data"))
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let req = Request::builder()
        .method("GET")
        .uri("/music")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let keys: Vec<String> = json["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["key"].as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains(&"a.mp3".to_string()));
    assert!(keys.contains(&"b.mp3".to_string()));
}

#[tokio::test]
async fn test_delete_objects_by_prefix() {
    let (app, token, _tmp) = setup_app(None, false).await;

    for key in &["purge/a.bin", "purge/b.bin", "keep/c.bin"] {
        let req = Request::builder()
            .method("PUT")
            .uri(format!("/music/{key}"))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from("data"))
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let req = Request::builder()
        .method("DELETE")
        .uri("/music?prefix=purge/")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["deleted"], 2);
    assert_eq!(json["failed"].as_array().unwrap().len(), 0);
    assert_eq!(json["truncated"], false);

    let req = Request::builder()
        .method("GET")
        .uri("/music")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let keys: Vec<String> = json["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["key"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(keys, vec!["keep/c.bin".to_string()]);
}

#[tokio::test]
async fn test_capabilities_advertises_delete_prefix() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("GET")
        .uri("/_nos/capabilities")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["api"]["delete_prefix"], true);
    assert_eq!(json["api"]["batch_delete"], true);
    assert_eq!(json["api"]["list_count_only"], true);
    assert!(json["api"]["delete_prefix_batch_limit"].as_u64().unwrap() >= 1);
    assert!(json["api"]["bulk_delete_concurrency"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn test_batch_delete() {
    let (app, token, _tmp) = setup_app(None, false).await;

    for key in &["batch/a.bin", "batch/b.bin", "keep/x.bin"] {
        let req = Request::builder()
            .method("PUT")
            .uri(format!("/music/{key}"))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from("data"))
            .unwrap();
        assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::CREATED);
    }

    let req = Request::builder()
        .method("POST")
        .uri("/music/_batch_delete")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"keys":["batch/a.bin","batch/b.bin"]}"#))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["deleted"], 2);
}

#[tokio::test]
async fn test_list_count_only() {
    let (app, token, _tmp) = setup_app(None, false).await;

    for key in &["count/a.bin", "count/b.bin"] {
        let req = Request::builder()
            .method("PUT")
            .uri(format!("/music/{key}"))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from("x"))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();
    }

    let req = Request::builder()
        .method("GET")
        .uri("/music?prefix=count/&count_only=true")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["count"], 2);
}

#[tokio::test]
async fn test_range_request() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let content = b"abcdefghijklmnopqrstuvwxyz";

    let req = Request::builder()
        .method("PUT")
        .uri("/music/alphabet.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::from(&content[..]))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Range: bytes=0-4
    let req = Request::builder()
        .method("GET")
        .uri("/music/alphabet.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("range", "bytes=0-4")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"abcde");
}

#[tokio::test]
async fn test_head_object() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/test.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("content-type", "text/plain")
        .body(Body::from("hello"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("HEAD")
        .uri("/music/test.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cl = response.headers().get("content-length").unwrap();
    assert_eq!(cl, "5");
}

#[tokio::test]
async fn test_not_found() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("GET")
        .uri("/music/nonexistent.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let req = Request::builder()
        .method("HEAD")
        .uri("/music/nonexistent.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_invalid_auth() {
    let (app, _token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("GET")
        .uri("/music/tracks/song.mp3")
        .header("authorization", "Bearer invalid-token")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

fn make_presigned_url(method: &str, base: &str, bucket: &str, key: &str, secret: &str, expires: u64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let payload = format!("{}\n{}\n{}\n{}", method.to_uppercase(), bucket, key, expires);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(payload.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    format!("{}/{}/{}?signature={}&expires={}", base, bucket, key, sig, expires)
}

#[tokio::test]
async fn test_health_endpoint() {
    let (app, _token, _tmp) = setup_app(None, false).await;
    let req = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_health_ready_endpoint() {
    let (app, _token, _tmp) = setup_app(None, false).await;
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
    assert_eq!(json["status"], "ready");
    assert_eq!(json["checks"]["sqlite_write"], true);
    assert_eq!(json["checks"]["sqlite_read"], true);
    assert_eq!(json["checks"]["data_dir_writable"], true);
}

#[tokio::test]
async fn test_put_if_none_match_create_only() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/new-only.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("if-none-match", "*")
        .body(Body::from("first"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("PUT")
        .uri("/music/new-only.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("if-none-match", "*")
        .body(Body::from("second"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], "precondition failed");
}

#[tokio::test]
async fn test_put_if_match_optimistic_update() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/versioned.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::from("v1"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let etag = response
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let req = Request::builder()
        .method("PUT")
        .uri("/music/versioned.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("if-match", "wrong-etag")
        .body(Body::from("v2"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

    let req = Request::builder()
        .method("PUT")
        .uri("/music/versioned.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("if-match", etag)
        .body(Body::from("v2"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri("/music/versioned.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"v2");
}

#[tokio::test]
async fn test_delete_if_match() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/to-delete.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::from("bye"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let etag = response
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let req = Request::builder()
        .method("DELETE")
        .uri("/music/to-delete.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("if-match", "stale")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

    let req = Request::builder()
        .method("DELETE")
        .uri("/music/to-delete.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("if-match", etag)
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_metrics_endpoint() {
    let (app, _token, _tmp) = setup_app(None, false).await;
    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("total_objects").is_some());
    assert!(json.get("total_bytes").is_some());
    assert_eq!(json["metadata_backend"], "sqlite");
    assert_eq!(json["max_logical_bytes"], 0);
    assert_eq!(json["logical_bytes"], json["total_bytes"]);
}

#[tokio::test]
async fn test_max_logical_bytes_rejects_second_put() {
    let cap = 20_i64;
    let (app, token, _tmp) = setup_app_with_cap(None, false, cap).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/small.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("12345678901234567890"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("PUT")
        .uri("/music/another.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("overflow"))
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::INSUFFICIENT_STORAGE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], "insufficient storage");
}

#[tokio::test]
async fn test_presigned_url_access() {
    let (app, token, _tmp) = setup_app(Some("test-signing-secret".into()), false).await;
    let secret = "test-signing-secret";

    // PUT with JWT
    let req = Request::builder()
        .method("PUT")
        .uri("/music/song.mp3")
        .header("authorization", format!("Bearer {}", token))
        .header("content-type", "audio/mpeg")
        .body(Body::from("audio data"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // GET with presigned URL (no JWT)
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() + 3600;
    let url = make_presigned_url("GET", "", "music", "song.mp3", secret, expires);
    let req = Request::builder()
        .method("GET")
        .uri(&url)
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_storage_compression_transparent() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::{is_indexed_blob, NOSI_MAGIC};

    let (app, token, tmp) = setup_app(None, false).await;
    let content = "compressible payload ".repeat(500);

    let req = Request::builder()
        .method("PUT")
        .uri("/music/compressed.bin")
        .header("authorization", format!("Bearer {}", token))
        .header("content-type", "application/octet-stream")
        .body(Body::from(content.clone()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let data_dir = tmp.path().join("blobs");
    let on_disk = std::fs::read(blob_path(
        &data_dir.to_string_lossy(),
        "music",
        "compressed.bin",
    ))
    .unwrap();
    assert!(is_indexed_blob(&on_disk));
    assert!(on_disk.starts_with(NOSI_MAGIC));
    assert!(on_disk.len() < content.len());

    let req = Request::builder()
        .method("GET")
        .uri("/music/compressed.bin")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body, content.as_bytes());

    let req = Request::builder()
        .method("HEAD")
        .uri("/music/compressed.bin")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cl = response.headers().get("content-length").unwrap();
    assert_eq!(cl.to_str().unwrap(), content.len().to_string());
}

#[tokio::test]
async fn test_block_compressed_range_without_full_decode() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::{is_compressed_blob, parse_layout_bytes};

    let (app, token, tmp) = setup_app(None, false).await;
    let content: String = (0..8)
        .map(|i| format!("block-{i}-payload-line\n"))
        .collect::<Vec<_>>()
        .join("")
        .repeat(4000);
    let total = content.len();
    let range_start = total / 3;
    let range_end = range_start + 50_000;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/block-range.bin")
        .header("authorization", format!("Bearer {}", token))
        .header("content-type", "application/octet-stream")
        .body(Body::from(content.clone()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let on_disk = std::fs::read(blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "block-range.bin",
    ))
    .unwrap();
    assert!(is_compressed_blob(&on_disk));
    let layout = parse_layout_bytes(&on_disk).unwrap();
    assert!(layout.block_count() > 1);

    let req = Request::builder()
        .method("GET")
        .uri("/music/block-range.bin")
        .header("authorization", format!("Bearer {}", token))
        .header("range", format!("bytes={range_start}-{range_end}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        body,
        &content.as_bytes()[range_start..=range_end]
    );
}

#[tokio::test]
async fn test_storage_skips_incompressible_media() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::is_compressed_blob;

    let (app, token, tmp) = setup_app(None, false).await;
    let content = "would compress if we tried ".repeat(500);

    let req = Request::builder()
        .method("PUT")
        .uri("/music/track.mp3")
        .header("authorization", format!("Bearer {}", token))
        .header("content-type", "audio/mpeg")
        .body(Body::from(content.clone()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let on_disk = std::fs::read(blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "track.mp3",
    ))
    .unwrap();
    assert!(!is_compressed_blob(&on_disk));
    assert_eq!(on_disk, content.as_bytes());
}

#[tokio::test]
async fn test_expired_presigned_url_rejected() {
    let (app, token, _tmp) = setup_app(Some("test-signing-secret".into()), false).await;
    let secret = "test-signing-secret";

    // PUT with JWT
    let req = Request::builder()
        .method("PUT")
        .uri("/music/song.mp3")
        .header("authorization", format!("Bearer {}", token))
        .header("content-type", "audio/mpeg")
        .body(Body::from("audio data"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // GET with expired presigned URL
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() - 100;
    let url = make_presigned_url("GET", "", "music", "song.mp3", secret, expires);
    let req = Request::builder()
        .method("GET")
        .uri(&url)
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_public_read_object_without_auth() {
    let (app, token, _tmp) = setup_app(None, true).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/public.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::from("public content"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri("/music/public.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"public content");
}

#[tokio::test]
async fn test_public_read_list_still_requires_auth() {
    let (app, _token, _tmp) = setup_app(None, true).await;

    let req = Request::builder()
        .method("GET")
        .uri("/music")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_payload_too_large_returns_413() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
        EngineOptions {
            upload_buffer_size: 4096,
            read_pool_size: 2,
            ..EngineOptions::default()
        },
    )
    .await
    .unwrap();
    let mut cfg = (*test_config(None, false)).clone();
    cfg.max_body_size = 8;
    let cfg = Arc::new(cfg);
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).unwrap();
    let app = create_app(backend, storage, cfg, metrics).await.unwrap();
    let token = make_token();

    let req = Request::builder()
        .method("PUT")
        .uri("/music/big.bin")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::from(vec![0u8; 32]))
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], "payload too large");
}

#[tokio::test]
async fn test_list_delimiter_common_prefixes() {
    let (app, token, _tmp) = setup_app(None, false).await;

    for key in &[
        "tracks/a.mp3",
        "tracks/b.mp3",
        "single.mp3",
    ] {
        let req = Request::builder()
            .method("PUT")
            .uri(format!("/music/{}", key))
            .header("authorization", format!("Bearer {}", token))
            .body(Body::from("data"))
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let req = Request::builder()
        .method("GET")
        .uri("/music?delimiter=/")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let prefixes: Vec<String> = json["common_prefixes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(prefixes.contains(&"tracks/".to_string()));
    let keys: Vec<String> = json["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["key"].as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains(&"single.mp3".to_string()));
    assert!(!keys.iter().any(|k| k.starts_with("tracks/")));
}

#[tokio::test]
async fn test_list_pagination() {
    let (app, token, _tmp) = setup_app(None, false).await;

    for key in &["p1.txt", "p2.txt", "p3.txt"] {
        let req = Request::builder()
            .method("PUT")
            .uri(format!("/music/{}", key))
            .header("authorization", format!("Bearer {}", token))
            .body(Body::from("x"))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();
    }

    let req = Request::builder()
        .method("GET")
        .uri("/music?limit=2")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["items"].as_array().unwrap().len(), 2);
    assert_eq!(json["is_truncated"], true);
    let next = json["next_start_after"].as_str().unwrap();

    let req = Request::builder()
        .method("GET")
        .uri(format!("/music?limit=2&start_after={}", next))
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["items"].as_array().unwrap().len(), 1);
    assert_eq!(json["is_truncated"], false);
}

#[tokio::test]
async fn test_conditional_get_not_modified() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/etag.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::from("hello"))
        .unwrap();
    let put_resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(put_resp.status(), StatusCode::CREATED);
    let etag = put_resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let req = Request::builder()
        .method("GET")
        .uri("/music/etag.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("if-none-match", etag)
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn test_custom_meta_roundtrip() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/meta.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("x-nd-custom-meta-artist", "aurora")
        .body(Body::from("x"))
        .unwrap();
    app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/music/meta.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let artist = response
        .headers()
        .get("x-nd-custom-meta-artist")
        .unwrap();
    assert_eq!(artist, "aurora");
}

#[tokio::test]
async fn test_suffix_range_request() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let content = b"abcdefghijklmnopqrstuvwxyz";

    let req = Request::builder()
        .method("PUT")
        .uri("/music/suffix.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::from(&content[..]))
        .unwrap();
    app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/music/suffix.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("range", "bytes=-4")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"wxyz");
}

#[tokio::test]
async fn test_copy_object() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("PUT")
        .uri("/music/original.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::from("copy-me"))
        .unwrap();
    app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("PUT")
        .uri("/music/copied.txt")
        .header("authorization", format!("Bearer {}", token))
        .header("x-nd-copy-source", "music/original.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri("/music/copied.txt")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"copy-me");
}

#[tokio::test]
async fn test_multipart_upload() {
    let (app, token, _tmp) = setup_app(None, false).await;

    let req = Request::builder()
        .method("POST")
        .uri("/music/_multipart?key=large.bin")
        .header("authorization", format!("Bearer {}", token))
        .header("content-type", "application/octet-stream")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let upload_id = json["upload_id"].as_str().unwrap();

    for (part, data) in [(1, "aaa"), (2, "bbb")] {
        let req = Request::builder()
            .method("PUT")
            .uri(format!(
                "/music/_multipart/{}/parts/{}",
                upload_id, part
            ))
            .header("authorization", format!("Bearer {}", token))
            .body(Body::from(data))
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let req = Request::builder()
        .method("POST")
        .uri(format!("/music/_multipart/{}/complete", upload_id))
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("GET")
        .uri("/music/large.bin")
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"aaabbb");
}

#[tokio::test]
async fn test_metrics_requires_token_when_configured() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
        EngineOptions {
            upload_buffer_size: 64 * 1024,
            read_pool_size: 2,
            compress_block_size: 128 * 1024,
            ..EngineOptions::default()
        },
    )
    .await
    .unwrap();
    let mut cfg = (*test_config(None, false)).clone();
    cfg.metrics_token = Some("metrics-secret".into());
    let cfg = Arc::new(cfg);
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).unwrap();
    let app = create_app(backend, storage, cfg, metrics).await.unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .header("authorization", "Bearer metrics-secret")
        .header("accept", "text/plain")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("nos_objects_total"));
}

async fn setup_engine(opts: EngineOptions) -> (StorageEngine, TempDir) {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(&meta_path_str, &data_dir_str, opts)
        .await
        .unwrap();
    (storage, tmp)
}

#[tokio::test]
async fn test_hard_delete_reclaims_blob_immediately() {
    use nebular_os::storage::blob_path;

    let (storage, tmp) = setup_engine(EngineOptions {
        soft_delete_ttl_secs: 0,
        ..EngineOptions::default()
    })
    .await;

    let data_dir = tmp.path().join("blobs");
    let mut body = std::io::Cursor::new(b"ephemeral");
    storage
        .put_object("music", "tmp.bin", None, None, &mut body)
        .await
        .unwrap();

    let path = blob_path(&data_dir.to_string_lossy(), "music", "tmp.bin");
    assert!(path.exists());

    storage
        .delete_object("music", "tmp.bin", None)
        .await
        .unwrap();
    assert!(!path.exists());
    assert!(!storage.object_exists("music", "tmp.bin").await.unwrap());
}

#[tokio::test]
async fn test_soft_delete_drop_blob_removes_file() {
    use nebular_os::storage::blob_path;

    let (storage, tmp) = setup_engine(EngineOptions {
        soft_delete_drop_blob: true,
        ..EngineOptions::default()
    })
    .await;

    let data_dir = tmp.path().join("blobs");
    let mut body = std::io::Cursor::new(b"drop-me");
    storage
        .put_object("music", "gone.bin", None, None, &mut body)
        .await
        .unwrap();

    let path = blob_path(&data_dir.to_string_lossy(), "music", "gone.bin");
    storage
        .delete_object("music", "gone.bin", None)
        .await
        .unwrap();
    assert!(!path.exists());
    assert!(!storage.object_exists("music", "gone.bin").await.unwrap());
}

#[tokio::test]
async fn test_purge_stale_multipart_uploads() {
    let (storage, tmp) = setup_engine(EngineOptions {
        multipart_upload_ttl_secs: 3_600,
        ..EngineOptions::default()
    })
    .await;

    let init = storage
        .init_multipart("music", "stale.bin", None)
        .await
        .unwrap();
    let upload_id = init.upload_id.clone();
    let part_dir = tmp.path().join("blobs").join(".multipart").join(&upload_id);
    assert!(part_dir.exists());

    let stale = chrono::Utc::now().timestamp() - 7_200;
    sqlx::query("UPDATE multipart_uploads SET created_at = ? WHERE upload_id = ?")
        .bind(stale)
        .bind(&upload_id)
        .execute(storage.write_pool())
        .await
        .unwrap();

    let purged = storage.purge_stale_multipart_uploads().await.unwrap();
    assert_eq!(purged, 1);
    assert!(!part_dir.exists());
}

#[tokio::test]
async fn test_recompress_legacy_raw_blob() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::is_compressed_blob;

    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let logical = b"legacy raw payload ".repeat(300);
    let mut body = std::io::Cursor::new(&logical[..]);
    storage
        .put_object("music", "legacy.bin", None, None, &mut body)
        .await
        .unwrap();

    let path = blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "legacy.bin",
    );
    std::fs::write(&path, &logical[..]).unwrap();
    assert!(!is_compressed_blob(&std::fs::read(&path).unwrap()));

    let report = storage.recompress_legacy_blobs(10).await.unwrap();
    assert_eq!(report.recompressed, 1);
    let on_disk = std::fs::read(&path).unwrap();
    assert!(is_compressed_blob(&on_disk));
    assert!(on_disk.len() < logical.len());

    let outcome = storage
        .get_object("music", "legacy.bin", None, None, None)
        .await
        .unwrap();
    match outcome {
        nebular_os::storage::GetObjectOutcome::Content { stream, .. } => {
            let bytes = axum::body::to_bytes(
                axum::body::Body::from_stream(stream.stream),
                usize::MAX,
            )
            .await
            .unwrap();
            assert_eq!(bytes.as_ref(), &logical[..]);
        }
        _ => panic!("expected content"),
    }
}

#[tokio::test]
#[cfg(unix)]
async fn test_copy_object_shares_storage_via_hard_link() {
    use nebular_os::storage::blob_ops::same_inode;
    use nebular_os::storage::blob_path;

    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let data_dir = tmp.path().join("blobs");
    let mut body = std::io::Cursor::new(b"shared-bytes");
    storage
        .put_object("music", "original.bin", None, None, &mut body)
        .await
        .unwrap();

    storage
        .copy_object("music", "original.bin", "music", "copy.bin", None, None)
        .await
        .unwrap();

    let src = blob_path(&data_dir.to_string_lossy(), "music", "original.bin");
    let dst = blob_path(&data_dir.to_string_lossy(), "music", "copy.bin");
    assert!(same_inode(&src, &dst));

    storage
        .delete_object("music", "copy.bin", None)
        .await
        .unwrap();
    assert!(src.exists());
    assert!(storage.object_exists("music", "original.bin").await.unwrap());
}

fn listener_token() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = Claims {
        sub: "listener-user".into(),
        email: "listener@example.com".into(),
        role: "listener".into(),
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

#[tokio::test]
async fn test_listener_role_cannot_put() {
    let (app, _token, _tmp) = setup_app(Some(TEST_SECRET.into()), false).await;
    let listener = listener_token();
    let req = Request::builder()
        .method("PUT")
        .uri("/music/forbidden.bin")
        .header("authorization", format!("Bearer {listener}"))
        .body(Body::from("data"))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_s3_list_objects_xml_when_compat_enabled() {
    let mut cfg = (*test_config(Some(TEST_SECRET.into()), false)).clone();
    cfg.s3_compat = true;
    let cfg = Arc::new(cfg);
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
        EngineOptions::default(),
    )
    .await
    .unwrap();
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).unwrap();
    let app = create_app(backend, storage, cfg, metrics).await.unwrap();
    let token = make_token();

    let put = Request::builder()
        .method("PUT")
        .uri("/music/s3obj.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("hello s3"))
        .unwrap();
    assert_eq!(app.clone().oneshot(put).await.unwrap().status(), StatusCode::CREATED);

    let list = Request::builder()
        .method("GET")
        .uri("/music?list-type=2")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let list_resp = app.oneshot(list).await.unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("<ListBucketResult"));
    assert!(text.contains("<Key>s3obj.bin</Key>"));
}

#[tokio::test]
async fn test_bucket_policy_denies_other_bucket() {
    let mut cfg = (*test_config(Some(TEST_SECRET.into()), false)).clone();
    cfg.bucket_policy =
        nebular_os::config::BucketPolicy::from_json(r#"{"user-1":["music"]}"#).unwrap();
    let cfg = Arc::new(cfg);
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
        EngineOptions::default(),
    )
    .await
    .unwrap();
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).unwrap();
    let app = create_app(backend, storage, cfg, metrics).await.unwrap();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = Claims {
        sub: "user-1".into(),
        email: "u@example.com".into(),
        role: "admin".into(),
        exp: now + 3600,
        iat: now,
    };
    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/other-bucket")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_recompress_nosz_at_higher_level() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::{
        is_indexed_blob, parse_layout_bytes, BLOB_MAGIC, HEADER_LEN,
    };

    let (storage, tmp) = setup_engine(EngineOptions {
        zstd_level: 22,
        zstd_level_upload: 3,
        ..EngineOptions::default()
    })
    .await;
    let logical = b"recompress me at higher level ".repeat(300);
    let path = blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "low-level.bin",
    );
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();

    let mut low = Vec::new();
    low.extend_from_slice(BLOB_MAGIC);
    low.extend_from_slice(&(logical.len() as u64).to_le_bytes());
    low.extend_from_slice(&zstd::encode_all(&logical[..], 1).unwrap());
    std::fs::write(&path, &low).unwrap();

    storage
        .object_meta()
        .upsert_object(
            &tmp.path().join("blobs").to_string_lossy(),
            "music",
            "low-level.bin",
            logical.len() as i64,
            None,
            "abc",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let report = storage.recompress_blobs(10).await.unwrap();
    assert!(
        report.recompressed >= 1,
        "expected recompression, got {:?}",
        report
    );
    let on_disk = std::fs::read(&path).unwrap();
    assert!(is_indexed_blob(&on_disk));
    assert!(parse_layout_bytes(&on_disk).is_ok());

    let outcome = storage
        .get_object("music", "low-level.bin", None, None, None)
        .await
        .unwrap();
    match outcome {
        nebular_os::storage::GetObjectOutcome::Content { stream, .. } => {
            let bytes = axum::body::to_bytes(
                axum::body::Body::from_stream(stream.stream),
                usize::MAX,
            )
            .await
            .unwrap();
            assert_eq!(bytes.as_ref(), &logical[..]);
        }
        _ => panic!("expected content"),
    }

    // Ensure legacy NOSZ headers still readable
    let mut legacy = Vec::new();
    legacy.extend_from_slice(BLOB_MAGIC);
    legacy.extend_from_slice(&(logical.len() as u64).to_le_bytes());
    legacy.extend_from_slice(&zstd::encode_all(&logical[..], 3).unwrap());
    assert!(legacy.len() > HEADER_LEN);
}

#[tokio::test]
async fn test_dedup_large_object() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::{collect_dedup_refs, is_indexed_blob, NOSI_MAGIC};

    let (storage, tmp) = setup_engine(EngineOptions {
        dedup_enabled: true,
        dedup_min_size: 1024,
        dedup_block_size: 4096,
        ..EngineOptions::default()
    })
    .await;

    let payload = b"dedup-block-payload-".repeat(120); // > 1KB
    let mut body = std::io::Cursor::new(&payload[..]);
    storage
        .put_object("music", "big.bin", None, None, &mut body)
        .await
        .unwrap();

    let path = blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "big.bin",
    );
    let on_disk = std::fs::read(&path).unwrap();
    assert!(is_indexed_blob(&on_disk));
    assert!(on_disk.starts_with(NOSI_MAGIC));
    assert!(!collect_dedup_refs(&on_disk).unwrap().is_empty());

    let outcome = storage
        .get_object("music", "big.bin", None, None, None)
        .await
        .unwrap();
    match outcome {
        nebular_os::storage::GetObjectOutcome::Content { stream, .. } => {
            let bytes = axum::body::to_bytes(
                axum::body::Body::from_stream(stream.stream),
                usize::MAX,
            )
            .await
            .unwrap();
            assert_eq!(bytes.as_ref(), &payload[..]);
        }
        _ => panic!("expected content"),
    }

    // Second object with identical payload should share blocks
    let mut body2 = std::io::Cursor::new(&payload[..]);
    storage
        .put_object("music", "big2.bin", None, None, &mut body2)
        .await
        .unwrap();
    let path2 = blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "big2.bin",
    );
    assert!(path2.exists());
}

#[tokio::test]
async fn test_zstd_dictionary_train_and_use() {
    use nebular_os::storage::compression::{is_indexed_blob, read_indexed_dict_id};

    let (storage, tmp) = setup_engine(EngineOptions {
        zstd_dict_enabled: true,
        zstd_dict_max_bytes: 4096,
        zstd_level: 19,
        zstd_level_upload: 3,
        compress_min_size: 1024,
        ..EngineOptions::default()
    })
    .await;

    for i in 0..8 {
        let text = format!("COMMON log line {i} repeated text for dictionary training\n").repeat(120);
        let mut body = std::io::Cursor::new(text.as_bytes());
        storage
            .put_object("logs", &format!("app-{i}.log"), None, None, &mut body)
            .await
            .unwrap();
    }

    let report = storage.train_zstd_dictionary().await.unwrap();
    assert!(report.samples >= 2);
    assert!(report.trained);
    assert_eq!(report.id, Some(1));
    assert!(storage.dict_store().exists_on_disk(1));

    let sample = "log line 99 repeated text for dictionary training\n".repeat(120);
    let mut body = std::io::Cursor::new(sample.as_bytes());
    storage
        .put_object("logs", "new.log", None, None, &mut body)
        .await
        .unwrap();

    use nebular_os::storage::blob_path;
    let path = blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "logs",
        "new.log",
    );
    let on_disk = std::fs::read(&path).unwrap();
    assert!(is_indexed_blob(&on_disk));
    assert_eq!(read_indexed_dict_id(&on_disk), Some(1));
}

#[tokio::test]
async fn test_nested_keys_use_flat_blob_paths() {
    use nebular_os::storage::{blob_path, blob_path_legacy, encode_blob_filename, hash_prefix};

    let (app, token, tmp) = setup_app(None, false).await;
    let data_dir = tmp.path().join("blobs");
    let main_key = "users/tenant/files/e972685e-a486-4626-a7dc-5256b4be54dc";
    let sidecar_key = "users/tenant/files/e972685e-a486-4626-a7dc-5256b4be54dc/grid-thumbnail.jpg";

    for (key, body) in [
        (main_key, b"original image bytes".as_slice()),
        (sidecar_key, b"thumbnail bytes".as_slice()),
    ] {
        let req = Request::builder()
            .method("PUT")
            .uri(format!("/media/{key}"))
            .header("authorization", format!("Bearer {}", token))
            .header("content-type", "application/octet-stream")
            .body(Body::from(body.to_vec()))
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED, "PUT failed for {key}");
    }

    let shard = hash_prefix(main_key);
    let nested_main = data_dir.join("media").join(&shard).join(main_key);
    assert!(
        !nested_main.exists(),
        "main object must not use nested directories under the shard"
    );

    let encoded_main = data_dir
        .join("media")
        .join(&shard)
        .join(encode_blob_filename(main_key));
    assert!(encoded_main.is_file());

    let sidecar_shard = hash_prefix(sidecar_key);
    let encoded_sidecar = data_dir
        .join("media")
        .join(&sidecar_shard)
        .join(encode_blob_filename(sidecar_key));
    assert!(encoded_sidecar.is_file());

    let legacy_sidecar = blob_path_legacy(
        &data_dir.to_string_lossy(),
        "media",
        sidecar_key,
    );
    assert!(
        !legacy_sidecar.exists(),
        "sidecar must not require a directory where the main blob file lives"
    );

    for key in [main_key, sidecar_key] {
        let req = Request::builder()
            .method("GET")
            .uri(format!("/media/{key}"))
            .header("authorization", format!("Bearer {}", token))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "GET failed for {key}");
    }

    let req = Request::builder()
        .method("GET")
        .uri(format!("/media/{main_key}"))
        .header("authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"original image bytes");

    let path = blob_path(&data_dir.to_string_lossy(), "media", main_key);
    assert_eq!(path, encoded_main);
}

#[tokio::test]
async fn test_recompress_nosi_upgrades_upload_level() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::{
        read_blob_stored_zstd_level, read_indexed_zstd_level, BlobFormat, detect_blob_format,
        is_indexed_blob,
    };

    let (storage, tmp) = setup_engine(EngineOptions {
        zstd_level: 22,
        zstd_level_upload: 3,
        ..EngineOptions::default()
    })
    .await;

    let payload = b"nosi upgrade payload ".repeat(400);
    let mut body = std::io::Cursor::new(&payload[..]);
    storage
        .put_object("music", "upload-level.bin", None, None, &mut body)
        .await
        .unwrap();

    let path = blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "upload-level.bin",
    );
    let before = std::fs::read(&path).unwrap();
    assert!(is_indexed_blob(&before));
    assert_eq!(detect_blob_format(&before), BlobFormat::Nosi);
    assert_eq!(read_indexed_zstd_level(&before), Some(3));

    let report = storage.recompress_blobs(10).await.unwrap();
    assert!(report.recompressed >= 1, "expected NOSI upgrade: {:?}", report);

    let after = std::fs::read(&path).unwrap();
    assert_eq!(read_blob_stored_zstd_level(&after), Some(22));
}

#[tokio::test]
async fn test_verify_blob_integrity_passes_and_detects_corruption() {
    use nebular_os::storage::blob_path;

    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let payload = b"integrity scrub target ".repeat(200);
    let mut body = std::io::Cursor::new(&payload[..]);
    storage
        .put_object("music", "scrub.bin", None, None, &mut body)
        .await
        .unwrap();

    let report = storage.verify_blob_integrity(10).await.unwrap();
    assert!(report.verified >= 1, "expected verified blob: {:?}", report);
    assert_eq!(report.corrupted, 0);

    let path = blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "scrub.bin",
    );
    let mut corrupt = std::fs::read(&path).unwrap();
    if let Some(byte) = corrupt.last_mut() {
        *byte ^= 0xFF;
    }
    std::fs::write(&path, &corrupt).unwrap();

    let bad = storage.verify_blob_integrity(10).await.unwrap();
    assert!(bad.corrupted >= 1, "expected corruption detected: {:?}", bad);
}

// Human: Read full object bytes through the storage engine (decompresses indexed blobs).
// Agent: CALLS get_object; RETURNS logical payload for checksum comparisons in migration tests.
async fn engine_get_bytes(storage: &StorageEngine, bucket: &str, key: &str) -> Vec<u8> {
    let outcome = storage
        .get_object(bucket, key, None, None, None)
        .await
        .unwrap();
    match outcome {
        nebular_os::storage::GetObjectOutcome::Content { stream, .. } => {
            axum::body::to_bytes(axum::body::Body::from_stream(stream.stream), usize::MAX)
                .await
                .unwrap()
                .to_vec()
        }
        _ => panic!("expected content"),
    }
}

// Human: Simulate pre-0.1.4 on-disk layout by moving a fresh PUT blob onto the legacy nested path.
// Agent: WRITES metadata via put_object; RENAMES encoded blob file to blob_path_legacy location.
async fn install_legacy_nested_blob(
    storage: &StorageEngine,
    data_dir: &std::path::Path,
    bucket: &str,
    key: &str,
    bytes: &[u8],
) {
    use nebular_os::storage::{blob_path, blob_path_legacy};

    let mut body = std::io::Cursor::new(bytes);
    storage
        .put_object(bucket, key, Some("application/octet-stream"), None, &mut body)
        .await
        .unwrap();

    let base = data_dir.to_string_lossy();
    let encoded = blob_path(base.as_ref(), bucket, key);
    let legacy = blob_path_legacy(base.as_ref(), bucket, key);
    if let Some(parent) = legacy.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::rename(&encoded, &legacy).unwrap_or_else(|_| {
        std::fs::copy(&encoded, &legacy).unwrap();
        std::fs::remove_file(&encoded).unwrap();
    });
    assert!(legacy.is_file(), "legacy blob must exist at {legacy:?}");
    assert!(!encoded.exists(), "encoded path must be absent before migration");
}

#[tokio::test]
async fn test_migrate_blobs_legacy_nested_layout_preserves_content() {
    use nebular_os::storage::{blob_path, blob_path_legacy, encode_blob_filename};

    let (storage, tmp) = setup_engine(EngineOptions {
        zstd_level: 22,
        zstd_level_upload: 3,
        ..EngineOptions::default()
    })
    .await;
    let data_dir = tmp.path().join("blobs");
    let bucket = "media";
    let main_key = "users/tenant/files/e972685e-a486-4626-a7dc-5256b4be54dc";
    let sidecar_key = "users/tenant/files/e972685e-a486-4626-a7dc-5256b4be54dc/grid-thumbnail.jpg";
    // Human: Payloads above compress_min_size (4096) so migration exercises NOSI re-encode, not tiny raw blobs.
    let main_bytes = b"original image bytes for migration validation ".repeat(120);
    let sidecar_bytes = b"thumbnail bytes for migration validation ".repeat(120);

    install_legacy_nested_blob(&storage, &data_dir, bucket, main_key, &main_bytes).await;
    install_legacy_nested_blob(&storage, &data_dir, bucket, sidecar_key, &sidecar_bytes).await;

    assert_eq!(engine_get_bytes(&storage, bucket, main_key).await, main_bytes);
    assert_eq!(
        engine_get_bytes(&storage, bucket, sidecar_key).await,
        sidecar_bytes
    );

    let report = storage.migrate_blobs(50, None).await.unwrap();
    assert!(
        report.migrated >= 2,
        "expected both legacy objects migrated, got {:?}",
        report
    );
    assert_eq!(report.failed, 0);

    assert_eq!(engine_get_bytes(&storage, bucket, main_key).await, main_bytes);
    assert_eq!(
        engine_get_bytes(&storage, bucket, sidecar_key).await,
        sidecar_bytes
    );

    let base = data_dir.to_string_lossy();
    for key in [main_key, sidecar_key] {
        let encoded = blob_path(base.as_ref(), bucket, key);
        let legacy = blob_path_legacy(base.as_ref(), bucket, key);
        assert!(encoded.is_file(), "encoded blob missing for {key}");
        assert!(!legacy.exists(), "legacy blob must be removed for {key}");
        assert!(
            encoded.ends_with(encode_blob_filename(key).as_str()) || encoded.file_name().is_some(),
            "encoded path must use flat filename for {key}"
        );
    }

    let again = storage.migrate_blobs(50, None).await.unwrap();
    assert_eq!(again.migrated, 0, "second pass must skip migrated blobs: {:?}", again);
    assert!(again.skipped >= 2, "already-migrated rows should be skipped: {:?}", again);

    // Human: Idempotent passes must not change bytes users download via GET.
    assert_eq!(engine_get_bytes(&storage, bucket, main_key).await, main_bytes);
    assert_eq!(
        engine_get_bytes(&storage, bucket, sidecar_key).await,
        sidecar_bytes
    );
}

#[tokio::test]
async fn test_migrate_blobs_legacy_raw_blob_preserves_content() {
    use nebular_os::storage::{blob_path, blob_path_legacy};
    use nebular_os::storage::compression::is_indexed_blob;

    let (storage, tmp) = setup_engine(EngineOptions {
        zstd_level: 22,
        ..EngineOptions::default()
    })
    .await;
    let data_dir = tmp.path().join("blobs");
    let bucket = "music";
    let key = "users/tenant/files/legacy-raw.bin";
    let logical = b"legacy raw payload for migration ".repeat(200);

    install_legacy_nested_blob(&storage, &data_dir, bucket, key, &logical).await;

    let base = data_dir.to_string_lossy();
    let legacy = blob_path_legacy(base.as_ref(), bucket, key);
    std::fs::write(&legacy, &logical).unwrap();
    assert!(!is_indexed_blob(&std::fs::read(&legacy).unwrap()));

    assert_eq!(engine_get_bytes(&storage, bucket, key).await, logical);

    let report = storage.migrate_blobs(10, None).await.unwrap();
    assert_eq!(report.migrated, 1, "{report:?}");
    assert_eq!(report.failed, 0);

    let encoded = blob_path(base.as_ref(), bucket, key);
    assert!(encoded.is_file());
    assert!(!legacy.exists());
    assert!(is_indexed_blob(&std::fs::read(&encoded).unwrap()));

    assert_eq!(engine_get_bytes(&storage, bucket, key).await, logical);
}

#[tokio::test]
async fn test_migrate_blobs_http_endpoint_preserves_content() {
    use nebular_os::storage::{blob_path, blob_path_legacy};

    let (app, token, tmp) = setup_app(None, false).await;
    let data_dir = tmp.path().join("blobs");
    let bucket = "media";
    let key = "users/tenant/files/http-migrate-test.dat";
    let payload = b"http migration payload checksumming".to_vec();

    install_legacy_nested_blob_via_http(&app, &token, &data_dir, bucket, key, &payload).await;

    let before = http_get_bytes(&app, &token, bucket, key).await;
    assert_eq!(before, payload);

    let req = Request::builder()
        .method("POST")
        .uri("/_nos/maintenance/migrate_blobs?limit=10")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(report["migrated"].as_u64().unwrap_or(0) >= 1);

    let after = http_get_bytes(&app, &token, bucket, key).await;
    assert_eq!(after, payload);

    let base = data_dir.to_string_lossy();
    let encoded = blob_path(base.as_ref(), bucket, key);
    let legacy = blob_path_legacy(base.as_ref(), bucket, key);
    assert!(encoded.is_file());
    assert!(!legacy.exists());
}

// Human: PUT then move blob to legacy nested path using the app router's data directory.
// Agent: HTTP PUT + filesystem rename; PREPARES migrate_blobs HTTP integration test fixtures.
async fn install_legacy_nested_blob_via_http(
    app: &axum::Router,
    token: &str,
    data_dir: &std::path::Path,
    bucket: &str,
    key: &str,
    bytes: &[u8],
) {
    use nebular_os::storage::{blob_path, blob_path_legacy};

    let req = Request::builder()
        .method("PUT")
        .uri(format!("/{bucket}/{key}"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/octet-stream")
        .body(Body::from(bytes.to_vec()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let base = data_dir.to_string_lossy();
    let encoded = blob_path(base.as_ref(), bucket, key);
    let legacy = blob_path_legacy(base.as_ref(), bucket, key);
    if let Some(parent) = legacy.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::rename(&encoded, &legacy).unwrap_or_else(|_| {
        std::fs::copy(&encoded, &legacy).unwrap();
        std::fs::remove_file(&encoded).unwrap();
    });
}

#[tokio::test]
async fn test_ownly_style_rewrite_preserves_legacy_nested_content() {
    use nebular_os::storage::{blob_path, blob_path_legacy};

    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let data_dir = tmp.path().join("blobs");
    let bucket = "media";
    let key = "users/tenant/files/rewrite-fallback-test.dat";
    let payload = b"ownly rewrite fallback path payload ".repeat(150);

    install_legacy_nested_blob(&storage, &data_dir, bucket, key, &payload).await;
    assert_eq!(engine_get_bytes(&storage, bucket, key).await, payload);

    // Human: Mirrors Ownly NebulaStorage::rewrite_object_stream — GET logical bytes then PUT same key.
    // Agent: VALIDATES admin client-side migration fallback; WRITES encoded path; REMOVES legacy layout.
    let logical = engine_get_bytes(&storage, bucket, key).await;
    let mut body = std::io::Cursor::new(logical.as_slice());
    storage
        .put_object(bucket, key, Some("application/octet-stream"), None, &mut body)
        .await
        .unwrap();

    assert_eq!(engine_get_bytes(&storage, bucket, key).await, payload);

    let base = data_dir.to_string_lossy();
    let encoded = blob_path(base.as_ref(), bucket, key);
    let legacy = blob_path_legacy(base.as_ref(), bucket, key);
    assert!(encoded.is_file());
    assert!(!legacy.exists());
}

#[tokio::test]
async fn test_scrub_sampling_and_light_mode() {
    use nebular_os::storage::scrub::{scrub_sample_selected, ScrubMode, ScrubOptions};

    let (storage, _tmp) = setup_engine(EngineOptions::default()).await;
    for i in 0..5 {
        let payload = format!("payload-{i}");
        let mut body = std::io::Cursor::new(payload.as_bytes());
        storage
            .put_object("music", &format!("obj-{i}.bin"), None, None, &mut body)
            .await
            .unwrap();
    }

    let report = storage
        .scrub_objects(ScrubOptions {
            limit: 10,
            sample_denom: 1024,
            sample_epoch: 0,
            mode: ScrubMode::Light,
            start_after: None,
        })
        .await
        .unwrap();
    assert!(report.sampled_out > 0 || report.scanned <= 5);
    assert_eq!(report.mode, "light");

    assert!(scrub_sample_selected("music", "obj-0.bin", 1));
}

#[tokio::test]
async fn test_verify_on_read_rejects_corrupt_raw_blob() {
    use nebular_os::storage::blob_path;

    let (storage, tmp) = setup_engine(EngineOptions {
        verify_on_read: true,
        compress_min_size: 10_000_000,
        ..EngineOptions::default()
    })
    .await;

    let mut body = std::io::Cursor::new(b"integrity-check");
    storage
        .put_object("music", "raw.bin", None, None, &mut body)
        .await
        .unwrap();

    let path = blob_path(
        &tmp.path().join("blobs").to_string_lossy(),
        "music",
        "raw.bin",
    );
    let mut bytes = std::fs::read(&path).unwrap();
    if !bytes.is_empty() {
        bytes[0] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
    }

    let result = storage
        .get_object("music", "raw.bin", None, None, None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_webhook_dispatches_on_put() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let hits = Arc::new(AtomicUsize::new(0));
    let hits_bg = hits.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/hook",
            axum::routing::post(move || {
                let hits = hits_bg.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::OK
                }
            }),
        );
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    let mut cfg = (*test_config(None, false)).clone();
    cfg.webhooks = nebular_os::webhooks::WebhookConfig::from_json(&format!(
        r#"{{"music":["http://{addr}/hook"]}}"#
    ))
    .unwrap();
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let cfg = Arc::new(cfg);
    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
        EngineOptions {
            upload_buffer_size: cfg.upload_buffer_size,
            read_pool_size: cfg.read_pool_size,
            ..EngineOptions::default()
        },
    )
    .await
    .unwrap();
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).unwrap();
    let app = create_app(backend, storage, cfg, metrics).await.unwrap();
    let token = make_token();

    let put = Request::builder()
        .method("PUT")
        .uri("/music/hook.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("hooked"))
        .unwrap();
    assert_eq!(
        app.oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );

    for _ in 0..30 {
        if hits.load(Ordering::SeqCst) >= 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("webhook was not delivered");
}

async fn http_get_bytes(app: &axum::Router, token: &str, bucket: &str, key: &str) -> Vec<u8> {
    let req = Request::builder()
        .method("GET")
        .uri(format!("/{bucket}/{key}"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

// Human: Regression tests for the 0.1.5 fixes (stale block cache, Range, upload budget, copy auth,
// reserved presigned subject, query-free request logs, dictionary decode capacity).

async fn setup_app_with_config(cfg: NosConfig) -> (axum::Router, TempDir) {
    let cfg = Arc::new(cfg);
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
        EngineOptions {
            compress_block_size: 128 * 1024,
            ..EngineOptions::default()
        },
    )
    .await
    .unwrap();
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).unwrap();
    let app = create_app(backend, storage, cfg, metrics).await.unwrap();
    (app, tmp)
}

fn token_for(sub: &str, role: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = Claims {
        sub: sub.into(),
        email: format!("{sub}@example.com"),
        role: role.into(),
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

async fn put_status(
    app: &axum::Router,
    token: &str,
    uri: &str,
    body: impl Into<Body>,
) -> StatusCode {
    let req = Request::builder()
        .method("PUT")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(body.into())
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

async fn get_range(app: &axum::Router, token: &str, uri: &str, range: &str) -> axum::response::Response {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("range", range)
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.unwrap()
}

fn header_str<'a>(resp: &'a axum::response::Response, name: &str) -> Option<&'a str> {
    resp.headers().get(name).map(|v| v.to_str().unwrap())
}

#[tokio::test]
async fn test_overwrite_same_size_never_serves_stale_blocks() {
    // Human: Default setup keeps the decoded-block cache on (256 entries); same-size overwrite defeats the length guard.
    let (app, token, _tmp) = setup_app(None, false).await;
    let v1 = "version-one payload line\n".repeat(8_000);
    let v2 = "version-two payload line\n".repeat(8_000);
    assert_eq!(v1.len(), v2.len());

    assert_eq!(put_status(&app, &token, "/music/cached.txt", v1.clone()).await, StatusCode::CREATED);
    assert_eq!(http_get_bytes(&app, &token, "music", "cached.txt").await, v1.as_bytes());
    // Human: Range reads fill the decoded-block cache (full reads only consult it).
    let resp = get_range(&app, &token, "/music/cached.txt", "bytes=150000-150099").await;
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], &v1.as_bytes()[150_000..150_100]);

    assert_eq!(put_status(&app, &token, "/music/cached.txt", v2.clone()).await, StatusCode::CREATED);
    assert_eq!(http_get_bytes(&app, &token, "music", "cached.txt").await, v2.as_bytes());

    let resp = get_range(&app, &token, "/music/cached.txt", "bytes=150000-150099").await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], &v2.as_bytes()[150_000..150_100]);
}

#[tokio::test]
async fn test_range_responses_carry_slice_length_and_status() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let alphabet = b"abcdefghijklmnopqrstuvwxyz";
    assert_eq!(put_status(&app, &token, "/music/range.txt", &alphabet[..]).await, StatusCode::CREATED);

    let resp = get_range(&app, &token, "/music/range.txt", "bytes=0-4").await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(header_str(&resp, "content-length"), Some("5"));
    assert_eq!(header_str(&resp, "content-range"), Some("bytes 0-4/26"));
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"abcde");

    let resp = get_range(&app, &token, "/music/range.txt", "bytes=100-200").await;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(header_str(&resp, "content-range"), Some("bytes */26"));

    for spec in ["bytes=5-1", "bytes=0-1,4-5", "items=0-4"] {
        let resp = get_range(&app, &token, "/music/range.txt", spec).await;
        assert_eq!(resp.status(), StatusCode::OK, "{spec}");
        assert_eq!(header_str(&resp, "content-length"), Some("26"), "{spec}");
        assert!(resp.headers().get("content-range").is_none(), "{spec}");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], &alphabet[..], "{spec}");
    }

    // Human: Block-compressed objects take the indexed range path; headers must match there too.
    let text = "0123456789abcdef".repeat(20_000);
    assert_eq!(put_status(&app, &token, "/music/range-big.txt", text.clone()).await, StatusCode::CREATED);
    let resp = get_range(&app, &token, "/music/range-big.txt", "bytes=200000-200009").await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(header_str(&resp, "content-length"), Some("10"));
    assert_eq!(header_str(&resp, "content-range"), Some("bytes 200000-200009/320000"));
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], &text.as_bytes()[200_000..200_010]);
}

#[tokio::test]
async fn test_upload_bigger_than_budget_runs_when_idle() {
    let mut cfg = (*test_config(None, false)).clone();
    cfg.upload_max_in_flight_bytes = 1024 * 1024;
    cfg.upload_permit_unit = 256 * 1024;
    let (app, _tmp) = setup_app_with_config(cfg).await;
    let token = make_token();
    let payload = vec![7u8; 3 * 1024 * 1024];

    let req = Request::builder()
        .method("PUT")
        .uri("/music/big.bin")
        .header("authorization", format!("Bearer {token}"))
        .header("content-length", payload.len().to_string())
        .body(Body::from(payload.clone()))
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::CREATED);

    // Human: No Content-Length (chunked) is charged NOS_MAX_BODY_SIZE — also above the budget.
    let req = Request::builder()
        .method("PUT")
        .uri("/music/chunked.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(payload))
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_copy_source_needs_read_access_to_source_bucket() {
    let mut cfg = (*test_config(Some(TEST_SECRET.into()), false)).clone();
    cfg.bucket_policy = nebular_os::config::BucketPolicy::from_json(
        r#"{"user-1":["music"],"owner":["private"]}"#,
    )
    .unwrap();
    let (app, _tmp) = setup_app_with_config(cfg).await;
    let owner = token_for("owner", "admin");
    let user = token_for("user-1", "editor");

    assert_eq!(put_status(&app, &owner, "/private/secret.txt", "top secret").await, StatusCode::CREATED);

    for copy_header in ["x-nd-copy-source", "x-amz-copy-source"] {
        let req = Request::builder()
            .method("PUT")
            .uri("/music/stolen.txt")
            .header("authorization", format!("Bearer {user}"))
            .header(copy_header, "private/secret.txt")
            .body(Body::empty())
            .unwrap();
        assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::FORBIDDEN, "{copy_header}");
    }
    let req = Request::builder()
        .method("GET")
        .uri("/music/stolen.txt")
        .header("authorization", format!("Bearer {user}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::NOT_FOUND);

    // Human: Copying within a bucket the caller can read keeps working.
    assert_eq!(put_status(&app, &user, "/music/mine.txt", "mine").await, StatusCode::CREATED);
    let req = Request::builder()
        .method("PUT")
        .uri("/music/mine-copy.txt")
        .header("authorization", format!("Bearer {user}"))
        .header("x-nd-copy-source", "music/mine.txt")
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::CREATED);
    assert_eq!(http_get_bytes(&app, &user, "music", "mine-copy.txt").await, b"mine");
}

#[tokio::test]
async fn test_presigned_put_cannot_copy_objects() {
    let secret = "test-signing-secret";
    let (app, token, _tmp) = setup_app(Some(secret.into()), false).await;
    assert_eq!(put_status(&app, &token, "/music/original.txt", "private bytes").await, StatusCode::CREATED);

    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let url = make_presigned_url("PUT", "", "music", "upload.txt", secret, expires);
    let req = Request::builder()
        .method("PUT")
        .uri(&url)
        .header("x-nd-copy-source", "music/original.txt")
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::FORBIDDEN);

    let req = Request::builder()
        .method("PUT")
        .uri(&url)
        .body(Body::from("fresh upload"))
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_malformed_copy_source_is_rejected() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let req = Request::builder()
        .method("PUT")
        .uri("/music/target.txt")
        .header("authorization", format!("Bearer {token}"))
        .header("x-nd-copy-source", "no-separator")
        .body(Body::from("payload"))
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::BAD_REQUEST);

    let req = Request::builder()
        .method("GET")
        .uri("/music/target.txt")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_jwt_with_presigned_subject_is_rejected() {
    let (app, _token, _tmp) = setup_app(None, false).await;
    let forged = token_for("presigned", "listener");
    assert_eq!(put_status(&app, &forged, "/music/forged.bin", "x").await, StatusCode::UNAUTHORIZED);

    let req = Request::builder()
        .method("DELETE")
        .uri("/music/anything.bin")
        .header("authorization", format!("Bearer {forged}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::UNAUTHORIZED);
}

#[derive(Clone, Default)]
struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn test_request_logs_omit_presigned_signature() {
    let secret = "test-signing-secret";
    let (app, token, _tmp) = setup_app(Some(secret.into()), false).await;
    assert_eq!(put_status(&app, &token, "/music/song.mp3", "audio").await, StatusCode::CREATED);

    let capture = LogCapture::default();
    // Human: Same filter as the server default (main.rs), so the span must be enabled under it too.
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info,tower_http=debug"))
        .with_ansi(false)
        .with_writer(capture.clone())
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);

    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let url = make_presigned_url("GET", "", "music", "song.mp3", secret, expires);
    let signature = url
        .split("signature=")
        .nth(1)
        .and_then(|rest| rest.split('&').next())
        .unwrap()
        .to_string();
    let req = Request::builder()
        .method("GET")
        .uri(&url)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    drop(guard);

    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("path=/music/song.mp3"), "request span missing:\n{logs}");
    assert!(!logs.contains(&signature), "signature leaked into logs:\n{logs}");
}

#[tokio::test]
async fn test_zstd_dictionary_keeps_highly_compressible_objects_readable() {
    let (storage, _tmp) = setup_engine(EngineOptions {
        zstd_dict_enabled: true,
        zstd_dict_max_bytes: 4096,
        zstd_level: 19,
        zstd_level_upload: 3,
        compress_min_size: 1024,
        ..EngineOptions::default()
    })
    .await;

    let mut expected = Vec::new();
    for i in 0..8 {
        let text = format!("COMMON log line {i} repeated text for dictionary training\n").repeat(120);
        let mut body = std::io::Cursor::new(text.clone().into_bytes());
        storage
            .put_object("logs", &format!("app-{i}.log"), None, None, &mut body)
            .await
            .unwrap();
        expected.push((format!("app-{i}.log"), text));
    }
    assert!(storage.train_zstd_dictionary().await.unwrap().trained);

    let fresh = "log line 99 repeated text for dictionary training\n".repeat(120);
    let mut body = std::io::Cursor::new(fresh.clone().into_bytes());
    storage
        .put_object("logs", "new.log", None, None, &mut body)
        .await
        .unwrap();
    expected.push(("new.log".to_string(), fresh));

    // Human: Pre-dictionary blobs and dictionary blobs both compress far beyond 4:1 and must read back.
    for (key, text) in &expected {
        assert_eq!(engine_get_bytes(&storage, "logs", key).await, text.as_bytes(), "{key}");
    }
}

#[tokio::test]
async fn test_zstd_dictionary_is_never_replaced_once_trained() {
    let opts = || EngineOptions {
        zstd_dict_enabled: true,
        zstd_dict_max_bytes: 4096,
        zstd_level: 19,
        zstd_level_upload: 3,
        compress_min_size: 1024,
        ..EngineOptions::default()
    };
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let meta_path = format!("file:{}?mode=memory&cache=shared", uuid::Uuid::new_v4());
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(&meta_path, &data_dir_str, opts())
        .await
        .unwrap();

    for i in 0..8 {
        let text = format!("ALPHA log line {i} first vocabulary for training\n").repeat(120);
        let mut body = std::io::Cursor::new(text.into_bytes());
        storage
            .put_object("logs", &format!("a-{i}.log"), None, None, &mut body)
            .await
            .unwrap();
    }
    assert!(storage.train_zstd_dictionary().await.unwrap().trained);
    let encoded_with_dict = "ALPHA log line 77 first vocabulary for training\n".repeat(120);
    let mut body = std::io::Cursor::new(encoded_with_dict.clone().into_bytes());
    storage
        .put_object("logs", "x.log", None, None, &mut body)
        .await
        .unwrap();

    // Human: New raw samples with other vocabulary would train a different dictionary.
    for i in 0..8 {
        storage
            .delete_object("logs", &format!("a-{i}.log"), None)
            .await
            .unwrap();
    }
    for i in 0..40 {
        let text = format!("OMEGA {i} different words zebra quartz\n").repeat(20);
        let mut body = std::io::Cursor::new(text.into_bytes());
        storage
            .put_object("zz", &format!("b-{i}.txt"), None, None, &mut body)
            .await
            .unwrap();
    }
    let dict_path = data_dir.join(".dict").join("1.zdict");
    let dict_before = std::fs::read(&dict_path).unwrap();
    assert!(!storage.train_zstd_dictionary().await.unwrap().trained);
    assert_eq!(std::fs::read(&dict_path).unwrap(), dict_before);

    // Human: A fresh engine (restart) has no block cache to mask a dictionary mismatch.
    let restarted = StorageEngine::with_full_options(&meta_path, &data_dir_str, opts())
        .await
        .unwrap();
    assert_eq!(
        engine_get_bytes(&restarted, "logs", "x.log").await,
        encoded_with_dict.as_bytes()
    );
}

#[tokio::test]
async fn test_retrained_dictionary_keeps_older_blobs_readable() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::read_indexed_dict_id;

    let opts = || EngineOptions {
        zstd_dict_enabled: true,
        zstd_dict_max_bytes: 4096,
        zstd_level: 19,
        zstd_level_upload: 3,
        compress_min_size: 1024,
        ..EngineOptions::default()
    };
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let meta_path = format!("file:{}?mode=memory&cache=shared", uuid::Uuid::new_v4());
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(&meta_path, &data_dir_str, opts())
        .await
        .unwrap();
    let dict_id_of = |key: &str| read_indexed_dict_id(&std::fs::read(blob_path(&data_dir_str, "logs", key)).unwrap());

    for i in 0..8 {
        let text = format!("ALPHA log line {i} first vocabulary for training\n").repeat(120);
        put_bytes(&storage, "logs", &format!("a-{i}.log"), text.as_bytes()).await;
    }
    assert_eq!(storage.train_zstd_dictionary().await.unwrap().id, Some(1));
    let first = "ALPHA log line 77 first vocabulary for training\n".repeat(120);
    put_bytes(&storage, "logs", "first.log", first.as_bytes()).await;
    assert_eq!(dict_id_of("first.log"), Some(1));

    for i in 0..8 {
        let text = format!("OMEGA entry {i} zebra quartz vocabulary shifted\n").repeat(120);
        put_bytes(&storage, "logs", &format!("b-{i}.log"), text.as_bytes()).await;
    }
    let retrained = storage.retrain_zstd_dictionary().await.unwrap();
    assert!(retrained.trained);
    assert_eq!(retrained.id, Some(2));
    let second = "OMEGA entry 99 zebra quartz vocabulary shifted\n".repeat(120);
    put_bytes(&storage, "logs", "second.log", second.as_bytes()).await;
    assert_eq!(dict_id_of("second.log"), Some(2));

    // Human: After a restart each blob decodes with the dictionary it was written with, and recompression
    // moves the older one to the current dictionary without changing its content.
    let restarted = StorageEngine::with_full_options(&meta_path, &data_dir_str, opts())
        .await
        .unwrap();
    assert_eq!(engine_get_bytes(&restarted, "logs", "first.log").await, first.as_bytes());
    assert_eq!(engine_get_bytes(&restarted, "logs", "second.log").await, second.as_bytes());
    restarted.recompress_blobs(100).await.unwrap();
    assert_eq!(dict_id_of("first.log"), Some(2));
    assert_eq!(engine_get_bytes(&restarted, "logs", "first.log").await, first.as_bytes());
}

#[tokio::test]
async fn test_train_dictionary_endpoint() {
    let train = |token: &str| {
        Request::builder()
            .method("POST")
            .uri("/_nos/maintenance/train_dictionary")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    let (app, token, _tmp) = setup_app(None, false).await;
    assert_eq!(status_and_body(&app, train(&listener_token())).await.0, StatusCode::FORBIDDEN);
    assert_eq!(status_and_body(&app, train(&token)).await.0, StatusCode::BAD_REQUEST);

    let (storage, _tmp) = setup_engine(EngineOptions {
        zstd_dict_enabled: true,
        zstd_dict_max_bytes: 4096,
        compress_min_size: 1024,
        ..EngineOptions::default()
    })
    .await;
    for i in 0..8 {
        let text = format!("COMMON log line {i} repeated text for dictionary training\n").repeat(120);
        put_bytes(&storage, "logs", &format!("app-{i}.log"), text.as_bytes()).await;
    }
    let cfg = test_config(Some(TEST_SECRET.into()), false);
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone()).unwrap();
    let app = create_app(backend, storage, cfg, metrics).await.unwrap();
    for expected_id in [1, 2] {
        let (status, body) = status_and_body(&app, train(&token)).await;
        assert_eq!(status, StatusCode::OK);
        let report: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(report["trained"], true);
        assert_eq!(report["id"], expected_id);
    }
}

#[tokio::test]
async fn test_stalled_upload_times_out_and_frees_budget() {
    let mut cfg = (*test_config(None, false)).clone();
    cfg.upload_max_in_flight_bytes = 1024 * 1024;
    cfg.upload_permit_unit = 256 * 1024;
    cfg.upload_idle_timeout_secs = 1;
    let (app, _tmp) = setup_app_with_config(cfg).await;
    let token = make_token();

    // Human: One chunk, then silence — without an idle timeout this holds the whole budget forever.
    let stalled = futures_util::StreamExt::chain(
        futures_util::stream::iter(vec![Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"first chunk"))]),
        futures_util::stream::pending(),
    );
    let req = Request::builder()
        .method("PUT")
        .uri("/music/stalled.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from_stream(stalled))
        .unwrap();
    let started = std::time::Instant::now();
    let resp = tokio::time::timeout(std::time::Duration::from_secs(10), app.clone().oneshot(req))
        .await
        .expect("stalled upload must not hang")
        .unwrap();
    assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
    assert!(started.elapsed() < std::time::Duration::from_secs(5));

    assert_eq!(put_status(&app, &token, "/music/after.bin", "next upload").await, StatusCode::CREATED);
}

async fn get_json(app: &axum::Router, token: &str, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn test_prefix_operations_are_case_sensitive() {
    let (app, token, _tmp) = setup_app(None, false).await;
    for key in ["Users/keep.txt", "users/a/one.txt", "users/b.txt"] {
        assert_eq!(put_status(&app, &token, &format!("/music/{key}"), "x").await, StatusCode::CREATED);
    }

    let (status, json) = get_json(&app, &token, "/music?prefix=users/&count_only=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["count"], 2, "{json}");

    // Human: Mixed-case keys under a delimiter listing used to slice at the wrong offset.
    let (status, json) = get_json(&app, &token, "/music?prefix=users/&delimiter=/").await;
    assert_eq!(status, StatusCode::OK);
    let text = json.to_string();
    assert!(text.contains("users/a/") && text.contains("users/b.txt"), "{text}");
    assert!(!text.contains("Users/"), "{text}");

    let req = Request::builder()
        .method("DELETE")
        .uri("/music?prefix=users/")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);
    assert_eq!(http_get_bytes(&app, &token, "music", "Users/keep.txt").await, b"x");
}

// Human: Data-integrity regression tests (write path, multipart, magic bytes, preconditions).

fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, body.to_vec())
}

fn authed(method: &str, uri: &str, token: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(body)
        .unwrap()
}

async fn start_multipart(app: &axum::Router, token: &str, key: &str) -> String {
    let (status, body) = send(app, authed("POST", &format!("/music/_multipart?key={key}"), token, Body::empty())).await;
    assert_eq!(status, StatusCode::OK);
    let json: Value = serde_json::from_slice(&body).unwrap();
    json["upload_id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn test_refused_put_keeps_existing_object() {
    let (app, token, _tmp) = setup_app_with_cap(None, false, 100).await;
    assert_eq!(put_status(&app, &token, "/music/capped.bin", vec![b'a'; 60]).await, StatusCode::CREATED);
    assert_eq!(
        put_status(&app, &token, "/music/capped.bin", vec![b'b'; 200]).await,
        StatusCode::INSUFFICIENT_STORAGE
    );
    assert_eq!(http_get_bytes(&app, &token, "music", "capped.bin").await, vec![b'a'; 60]);
}

#[tokio::test]
async fn test_uploads_starting_with_format_magic_read_back() {
    let (app, token, _tmp) = setup_app(None, false).await;
    for (i, magic) in [b"NOSI", b"NOSB", b"NOSZ", b"NOS2", b"NOSD"].into_iter().enumerate() {
        let mut body = magic.to_vec();
        body.extend(noise(96, i as u64 + 1));
        let uri = format!("/music/magic-{i}.bin");
        assert_eq!(put_status(&app, &token, &uri, body.clone()).await, StatusCode::CREATED);
        assert_eq!(http_get_bytes(&app, &token, "music", &format!("magic-{i}.bin")).await, body);
        let resp = get_range(&app, &token, &uri, "bytes=2-9").await;
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        let slice = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&slice[..], &body[2..10]);
    }
}

#[tokio::test]
async fn test_multipart_rejects_gaps_unless_listed() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let upload_id = start_multipart(&app, &token, "gappy.bin").await;
    for (part, data) in [(1, "one-"), (3, "three")] {
        let uri = format!("/music/_multipart/{upload_id}/parts/{part}");
        assert_eq!(send(&app, authed("PUT", &uri, &token, Body::from(data))).await.0, StatusCode::OK);
    }
    let complete = format!("/music/_multipart/{upload_id}/complete");
    let (status, body) = send(&app, authed("POST", &complete, &token, Body::empty())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", String::from_utf8_lossy(&body));
    assert!(String::from_utf8_lossy(&body).contains("part 2 is missing"));

    let (status, _) = send(&app, authed("POST", &complete, &token, Body::from("{}"))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "an empty JSON object means no part list");

    let list = r#"{"parts":[{"part_number":1},{"part_number":3,"etag":"wrong"}]}"#;
    let (status, _) = send(&app, authed("POST", &complete, &token, Body::from(list))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let list = r#"{"parts":[{"part_number":1},{"part_number":3}]}"#;
    let (status, _) = send(&app, authed("POST", &complete, &token, Body::from(list))).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(http_get_bytes(&app, &token, "music", "gappy.bin").await, b"one-three");
}

#[tokio::test]
async fn test_failed_part_retry_keeps_the_good_part() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let upload_id = start_multipart(&app, &token, "retried.bin").await;
    let uri = format!("/music/_multipart/{upload_id}/parts/1");
    let good = noise(64 * 1024, 7);
    assert_eq!(send(&app, authed("PUT", &uri, &token, Body::from(good.clone()))).await.0, StatusCode::OK);

    // Human: A retry whose body dies half-way must not touch the stored part.
    let broken = futures_util::stream::iter(vec![
        Ok(bytes::Bytes::from(vec![b'x'; 1000])),
        Err(std::io::Error::other("client went away")),
    ]);
    let (status, _) = send(&app, authed("PUT", &uri, &token, Body::from_stream(broken))).await;
    assert_ne!(status, StatusCode::OK);

    let complete = format!("/music/_multipart/{upload_id}/complete");
    assert_eq!(send(&app, authed("POST", &complete, &token, Body::empty())).await.0, StatusCode::CREATED);
    assert_eq!(http_get_bytes(&app, &token, "music", "retried.bin").await, good);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_if_none_match_star_creates_once_under_concurrency() {
    let mut cfg = (*test_config(None, false)).clone();
    cfg.upload_max_in_flight_bytes = 0;
    let (app, _tmp) = setup_app_with_config(cfg).await;
    let token = make_token();
    let mut tasks = Vec::new();
    for i in 0..8u64 {
        let app = app.clone();
        let token = token.clone();
        tasks.push(tokio::spawn(async move {
            let req = Request::builder()
                .method("PUT")
                .uri("/music/once.bin")
                .header("authorization", format!("Bearer {token}"))
                .header("if-none-match", "*")
                .body(Body::from(noise(512 * 1024, i + 1)))
                .unwrap();
            app.oneshot(req).await.unwrap().status()
        }));
    }
    let mut created = 0;
    for task in tasks {
        match task.await.unwrap() {
            StatusCode::CREATED => created += 1,
            StatusCode::PRECONDITION_FAILED => {}
            other => panic!("unexpected status {other}"),
        }
    }
    assert_eq!(created, 1, "If-None-Match: * must admit exactly one creator");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_overwrite_never_exposes_a_missing_or_partial_object() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let v1 = "first version of a large compressible document\n".repeat(200_000);
    let v2 = "second version, same shape, different bytes!!!\n".repeat(200_000);
    assert_eq!(v1.len(), v2.len());
    assert_eq!(put_status(&app, &token, "/music/hot.txt", v1.clone()).await, StatusCode::CREATED);

    let writer = {
        let (app, token, v2) = (app.clone(), token.clone(), v2.clone());
        tokio::spawn(async move { put_status(&app, &token, "/music/hot.txt", v2).await })
    };
    // Human: Readers during the overwrite must see one complete version, never 404 or a torn body.
    let mut reads = 0;
    while !writer.is_finished() || reads < 3 {
        let (status, body) = send(&app, authed("GET", "/music/hot.txt", &token, Body::empty())).await;
        assert_eq!(status, StatusCode::OK, "read {reads} during overwrite");
        assert!(body == v1.as_bytes() || body == v2.as_bytes(), "torn read {reads}");
        reads += 1;
    }
    assert_eq!(writer.await.unwrap(), StatusCode::CREATED);
    assert_eq!(http_get_bytes(&app, &token, "music", "hot.txt").await, v2.as_bytes());
}

#[tokio::test]
async fn test_presigned_url_binds_exact_key() {
    let secret = "test-signing-secret";
    let (app, token, _tmp) = setup_app(Some(secret.into()), false).await;
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    // Human: A URL signed for `a` must not write `a/` (a different object).
    let url = make_presigned_url("PUT", "", "music", "a", secret, expires);
    let slashed = url.replacen("/music/a?", "/music/a/?", 1);
    let (status, _) = send(&app, Request::builder().method("PUT").uri(&slashed).body(Body::from("sneaky")).unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let req = authed("GET", "/music/a/", &token, Body::empty());
    assert_eq!(send(&app, req).await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_presigned_urls_only_address_objects() {
    let secret = "test-signing-secret";
    let (app, token, _tmp) = setup_app(Some(secret.into()), false).await;
    assert_eq!(put_status(&app, &token, "/music/listed.txt", "x").await, StatusCode::CREATED);
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    let sign = |method: &str, key: &str| {
        use hmac::{Hmac, Mac};
        let payload = format!("{method}\nmusic\n{key}\n{expires}");
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    };

    // Human: Validly signed, but the target is chosen by an unsigned query string, so presigned must not apply.
    let list = format!("/music?signature={}&expires={expires}", sign("GET", ""));
    let req = Request::builder().method("GET").uri(&list).body(Body::empty()).unwrap();
    assert_eq!(send(&app, req).await.0, StatusCode::UNAUTHORIZED);

    let init = format!("/music/_multipart?key=anything.bin&signature={}&expires={expires}", sign("POST", "_multipart"));
    let req = Request::builder().method("POST").uri(&init).body(Body::empty()).unwrap();
    assert_eq!(send(&app, req).await.0, StatusCode::UNAUTHORIZED);

    let purge = format!("/music?prefix=&signature={}&expires={expires}", sign("DELETE", ""));
    let req = Request::builder().method("DELETE").uri(&purge).body(Body::empty()).unwrap();
    assert_eq!(send(&app, req).await.0, StatusCode::UNAUTHORIZED);

    // Human: The object route keeps working.
    let get = format!("/music/listed.txt?signature={}&expires={expires}", sign("GET", "listed.txt"));
    let req = Request::builder().method("GET").uri(&get).body(Body::empty()).unwrap();
    assert_eq!(send(&app, req).await, (StatusCode::OK, b"x".to_vec()));
}

#[tokio::test]
async fn test_presigned_expiry_is_capped() {
    let secret = "test-signing-secret";
    let (app, token, _tmp) = setup_app(Some(secret.into()), false).await;
    assert_eq!(put_status(&app, &token, "/music/ttl.txt", "x").await, StatusCode::CREATED);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let far = make_presigned_url("GET", "", "music", "ttl.txt", secret, now + 8 * 24 * 3600);
    let req = Request::builder().method("GET").uri(&far).body(Body::empty()).unwrap();
    assert_eq!(send(&app, req).await.0, StatusCode::UNAUTHORIZED);
    let near = make_presigned_url("GET", "", "music", "ttl.txt", secret, now + 3600);
    let req = Request::builder().method("GET").uri(&near).body(Body::empty()).unwrap();
    assert_eq!(send(&app, req).await.0, StatusCode::OK);
}

#[tokio::test]
async fn test_bucket_names_cannot_reach_system_dirs() {
    let (app, token, _tmp) = setup_app(None, false).await;
    for uri in ["/.tmp/x.bin", "/.blocks/x.bin", "/.nos-ready-probe/x.bin", "/a%2Fb/x.bin"] {
        assert_eq!(put_status(&app, &token, uri, "x").await, StatusCode::BAD_REQUEST, "{uri}");
    }
    let (status, body) = send(&app, Request::builder().method("GET").uri("/health/ready").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
}

fn from_peer(mut req: Request<Body>, port: u16) -> Request<Body> {
    let addr = std::net::SocketAddr::from(([203, 0, 113, 7], port));
    req.extensions_mut().insert(axum::extract::ConnectInfo(addr));
    req
}

#[tokio::test]
async fn test_rate_limit_is_per_ip_not_per_connection() {
    let mut cfg = (*test_config(None, false)).clone();
    cfg.rate_limit_rps = 1;
    cfg.rate_limit_burst = 3;
    let (app, _tmp) = setup_app_with_config(cfg).await;
    let listener = listener_token();
    // Human: Each request comes from a new connection (new port) of the same client IP.
    for port in 40_000..40_003 {
        let req = from_peer(authed("GET", "/music?count_only=true", &listener, Body::empty()), port);
        assert_eq!(send(&app, req).await.0, StatusCode::OK, "port {port}");
    }
    let req = from_peer(authed("GET", "/music?count_only=true", &listener, Body::empty()), 40_003);
    assert_eq!(send(&app, req).await.0, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn test_failed_logins_are_throttled_per_ip() {
    let mut cfg = (*test_config(None, false)).clone();
    cfg.rate_limit_rps = 1;
    cfg.rate_limit_burst = 3;
    let (app, _tmp) = setup_app_with_config(cfg).await;
    for attempt in 0..3 {
        let req = from_peer(authed("GET", "/music/x.bin", "not-a-valid-token", Body::empty()), 50_000 + attempt);
        assert_eq!(send(&app, req).await.0, StatusCode::UNAUTHORIZED, "attempt {attempt}");
    }
    let req = from_peer(authed("GET", "/music/x.bin", "not-a-valid-token", Body::empty()), 50_010);
    assert_eq!(send(&app, req).await.0, StatusCode::TOO_MANY_REQUESTS);
    // Human: Valid credentials from the same IP (e.g. behind a shared proxy) are never locked out.
    let req = from_peer(authed("GET", "/music?count_only=true", &make_token(), Body::empty()), 50_011);
    assert_eq!(send(&app, req).await.0, StatusCode::OK);
    // Human: Other clients are unaffected.
    let req = authed("GET", "/music?count_only=true", &make_token(), Body::empty());
    let req = {
        let mut req = req;
        req.extensions_mut().insert(axum::extract::ConnectInfo(std::net::SocketAddr::from(([198, 51, 100, 1], 1))));
        req
    };
    assert_eq!(send(&app, req).await.0, StatusCode::OK);
}

fn with_headers(mut req: Request<Body>, headers: &[(&str, &str)]) -> Request<Body> {
    for (name, value) in headers {
        req.headers_mut().insert(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    req
}

#[tokio::test]
async fn test_conditional_get_follows_rfc9110() {
    let (app, token, _tmp) = setup_app(None, false).await;
    assert_eq!(put_status(&app, &token, "/music/cond.txt", "v1").await, StatusCode::CREATED);
    let resp = app.clone().oneshot(authed("HEAD", "/music/cond.txt", &token, Body::empty())).await.unwrap();
    let etag = resp.headers()["etag"].to_str().unwrap().to_string();
    let last_modified = resp.headers()["last-modified"].to_str().unwrap().to_string();
    assert!(last_modified.ends_with(" GMT"), "Last-Modified must be an IMF-fixdate: {last_modified}");

    // Human: If-None-Match wins over If-Modified-Since — a changed ETag means 200 even if the date says unmodified.
    let far_future = "Tue, 01 Jan 2999 00:00:00 GMT";
    let req = with_headers(
        authed("GET", "/music/cond.txt", &token, Body::empty()),
        &[("if-none-match", "\"something-else\""), ("if-modified-since", &last_modified)],
    );
    assert_eq!(send(&app, req).await.0, StatusCode::OK);

    // Human: Any tag in a list may match.
    let list = format!("\"other\", \"{etag}\"");
    let req = with_headers(authed("GET", "/music/cond.txt", &token, Body::empty()), &[("if-none-match", &list)]);
    assert_eq!(send(&app, req).await.0, StatusCode::NOT_MODIFIED);

    // Human: A date in the future is invalid and must be ignored.
    let req = with_headers(authed("GET", "/music/cond.txt", &token, Body::empty()), &[("if-modified-since", far_future)]);
    assert_eq!(send(&app, req).await.0, StatusCode::OK);

    let req = with_headers(authed("GET", "/music/cond.txt", &token, Body::empty()), &[("if-modified-since", &last_modified)]);
    assert_eq!(send(&app, req).await.0, StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn test_head_agrees_with_get_when_blob_is_missing() {
    use nebular_os::storage::blob_path;
    let (app, token, tmp) = setup_app(None, false).await;
    assert_eq!(put_status(&app, &token, "/music/gone.txt", "bytes").await, StatusCode::CREATED);
    let data_dir = tmp.path().join("blobs");
    std::fs::remove_file(blob_path(&data_dir.to_string_lossy(), "music", "gone.txt")).unwrap();
    let get = send(&app, authed("GET", "/music/gone.txt", &token, Body::empty())).await.0;
    let head = send(&app, authed("HEAD", "/music/gone.txt", &token, Body::empty())).await.0;
    assert_eq!(get, StatusCode::NOT_FOUND);
    assert_eq!(head, get);
}

#[tokio::test]
async fn test_active_content_is_served_sandboxed() {
    let (app, token, _tmp) = setup_app(None, false).await;
    for (key, ctype, sandboxed) in [
        ("page.html", "text/html; charset=utf-8", true),
        ("icon.svg", "image/svg+xml", true),
        ("list.html", "text/plain, text/html", true),
        ("feed.xml", "application/rss+xml", true),
        ("blob.bin", "application/x-unknown", true),
        ("photo.png", "image/png", false),
        ("clip.mp4", "video/mp4", false),
        ("doc.pdf", "application/pdf", false),
        ("notes.txt", "text/plain; charset=utf-8", false),
    ] {
        let req = with_headers(
            authed("PUT", &format!("/music/{key}"), &token, Body::from("<b>hi</b>")),
            &[("content-type", ctype)],
        );
        assert_eq!(send(&app, req).await.0, StatusCode::CREATED);
        let resp = app.clone().oneshot(authed("GET", &format!("/music/{key}"), &token, Body::empty())).await.unwrap();
        assert_eq!(resp.headers()["x-content-type-options"], "nosniff", "{key}");
        assert_eq!(
            resp.headers().get("content-security-policy").map(|v| v.to_str().unwrap()),
            sandboxed.then_some("sandbox"),
            "{key}"
        );
    }
}

#[tokio::test]
async fn test_overlong_keys_are_rejected_cleanly() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let fits = "a".repeat(255);
    assert_eq!(put_status(&app, &token, &format!("/music/{fits}"), "x").await, StatusCode::CREATED);
    let too_long = "a".repeat(256);
    assert_eq!(put_status(&app, &token, &format!("/music/{too_long}"), "x").await, StatusCode::BAD_REQUEST);
    // Human: `/` expands to `%2F` on disk, so 90 separators already exceed the filename limit.
    let nested = "a/".repeat(90);
    assert_eq!(put_status(&app, &token, &format!("/music/{nested}x"), "x").await, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_scrub_accepts_healthy_compressed_objects() {
    use nebular_os::storage::scrub::{ScrubMode, ScrubOptions};

    let (storage, _tmp) = setup_engine(EngineOptions::default()).await;
    // Human: Compresses to a tiny block whose logical length exceeds the bytes left in the file.
    let text = "healthy compressible scrub payload\n".repeat(6_000);
    let mut body = std::io::Cursor::new(text.clone().into_bytes());
    storage.put_object("music", "healthy.txt", None, None, &mut body).await.unwrap();
    let raw = noise(4096, 3);
    let mut body = std::io::Cursor::new(raw);
    storage.put_object("music", "raw.bin", None, None, &mut body).await.unwrap();

    for mode in [ScrubMode::Light, ScrubMode::Deep] {
        let report = storage
            .scrub_objects(ScrubOptions {
                limit: 10,
                sample_denom: 1,
                sample_epoch: 0,
                mode,
                start_after: None,
            })
            .await
            .unwrap();
        assert_eq!(report.corrupted, 0, "{mode:?}: {:?}", report.corrupted_keys);
        assert_eq!(report.verified, 2, "{mode:?}");
    }
}

#[tokio::test]
async fn test_scrub_reports_missing_blob_and_keeps_going() {
    use nebular_os::storage::blob_path;
    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    for key in ["a.bin", "b.bin", "c.bin"] {
        let mut body = std::io::Cursor::new(format!("payload for {key} ").repeat(400).into_bytes());
        storage.put_object("music", key, None, None, &mut body).await.unwrap();
    }
    std::fs::remove_file(blob_path(&tmp.path().join("blobs").to_string_lossy(), "music", "a.bin")).unwrap();
    let report = storage.verify_blob_integrity(10).await.expect("one missing blob must not abort the scrub");
    assert_eq!(report.corrupted, 1);
    assert_eq!(report.corrupted_keys, vec![("music".to_string(), "a.bin".to_string())]);
    assert_eq!(report.verified, 2);
}

#[tokio::test]
async fn test_migrate_skips_a_torn_legacy_blob_and_continues() {
    use nebular_os::storage::{blob_path, blob_path_legacy};
    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let base = tmp.path().join("blobs").to_string_lossy().to_string();
    let bytes = b"compressible legacy raw content line ".repeat(300);
    for key in ["dir/a", "dir/b"] {
        let mut body = std::io::Cursor::new(&bytes[..]);
        storage.put_object("media", key, None, None, &mut body).await.unwrap();
        let legacy = blob_path_legacy(&base, "media", key);
        std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        std::fs::remove_file(blob_path(&base, "media", key)).unwrap();
        let mut raw = bytes.clone();
        if key == "dir/a" {
            raw.truncate(raw.len() - 7); // Human: torn write — length disagrees with metadata
        }
        std::fs::write(&legacy, &raw).unwrap();
    }
    let report = storage.migrate_blobs(50, None).await.expect("one torn blob must not abort the batch");
    assert_eq!((report.migrated, report.failed), (1, 1));
    assert_eq!(engine_get_bytes(&storage, "media", "dir/b").await, bytes);
}

#[tokio::test]
async fn test_deep_scrub_reads_disk_not_the_block_cache() {
    use nebular_os::storage::blob_path;
    let (storage, tmp) = setup_engine(EngineOptions {
        block_cache_entries: 64,
        compress_block_size: 64 * 1024,
        ..EngineOptions::default()
    })
    .await;
    let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 97) as u8).collect();
    let mut body = std::io::Cursor::new(payload.clone());
    storage.put_object("music", "cached.txt", Some("text/plain"), None, &mut body).await.unwrap();
    // Human: Warm the decoded-block cache with a range read covering every block.
    if let nebular_os::storage::GetObjectOutcome::Content { stream, .. } =
        storage.get_object("music", "cached.txt", Some("bytes=0-299998"), None, None).await.unwrap()
    {
        axum::body::to_bytes(axum::body::Body::from_stream(stream.stream), usize::MAX).await.unwrap();
    }
    let path = blob_path(&tmp.path().join("blobs").to_string_lossy(), "music", "cached.txt");
    let mut blob = std::fs::read(&path).unwrap();
    let last = blob.len() - 1;
    blob[last] ^= 0xFF;
    std::fs::write(&path, &blob).unwrap();
    let report = storage.verify_blob_integrity(10).await.unwrap();
    assert_eq!(report.corrupted, 1, "on-disk corruption must be found even with a warm cache");
}

#[tokio::test]
async fn test_long_legacy_keys_stay_readable_and_deletable() {
    use nebular_os::storage::blob_path_legacy;
    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let base = tmp.path().join("blobs").to_string_lossy().to_string();
    // Human: Each nested segment fit the old layout, but the flat filename would be 403 bytes.
    let key = format!("{}/{}", "a".repeat(200), "b".repeat(200));
    storage
        .object_meta()
        .upsert_object(&base, "media", &key, 5, None, "etag", None, None, None)
        .await
        .unwrap();
    let legacy = blob_path_legacy(&base, "media", &key);
    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    std::fs::write(&legacy, b"hello").unwrap();

    assert_eq!(engine_get_bytes(&storage, "media", &key).await, b"hello");
    let mut body = std::io::Cursor::new(b"new".to_vec());
    assert!(matches!(
        storage.put_object("media", &key, None, None, &mut body).await,
        Err(nebular_os::storage::error::StorageError::InvalidRequest(_))
    ));
    storage.delete_object("media", &key, None).await.unwrap();
    assert!(matches!(
        storage.get_object("media", &key, None, None, None).await,
        Err(nebular_os::storage::error::StorageError::NotFound)
    ));
}

async fn put_bytes(storage: &StorageEngine, bucket: &str, key: &str, body: &[u8]) {
    storage
        .put_object(bucket, key, None, None, std::io::Cursor::new(body.to_vec()))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_periodic_scrub_starts_over_after_the_last_key() {
    let (storage, _tmp) = setup_engine(EngineOptions::default()).await;
    for key in ["a.bin", "b.bin", "c.bin"] {
        put_bytes(&storage, "scrub", key, b"scrub me").await;
    }

    let first = storage.scrub_with_defaults(2).await.unwrap();
    assert_eq!((first.scanned, first.is_truncated), (2, true));
    let second = storage.scrub_with_defaults(2).await.unwrap();
    assert_eq!((second.scanned, second.is_truncated), (1, false));
    // Human: The cursor used to stay on the last key, so every later pass was empty and nothing was re-verified.
    let third = storage.scrub_with_defaults(2).await.unwrap();
    assert_eq!(third.scanned, 2, "after the last key the scrub starts over: {third:?}");
    assert_eq!(third.verified, 2);
}

#[tokio::test]
async fn test_recompress_reaches_objects_beyond_the_first_batch() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::compression::{is_indexed_blob, BLOB_MAGIC};

    let (storage, tmp) = setup_engine(EngineOptions {
        zstd_level: 3,
        zstd_level_upload: 3,
        ..EngineOptions::default()
    })
    .await;
    // Human: Two objects that need no work, written first (oldest), then one legacy blob that does.
    put_bytes(&storage, "music", "a-current.bin", &b"already current ".repeat(300)).await;
    put_bytes(&storage, "music", "b-current.bin", &b"also current ".repeat(300)).await;

    let data_dir = tmp.path().join("blobs").to_string_lossy().replace('\\', "/");
    let logical = b"legacy low level blob ".repeat(300);
    let path = blob_path(&data_dir, "music", "z-legacy.bin");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut legacy = Vec::new();
    legacy.extend_from_slice(BLOB_MAGIC);
    legacy.extend_from_slice(&(logical.len() as u64).to_le_bytes());
    legacy.extend_from_slice(&zstd::encode_all(&logical[..], 1).unwrap());
    std::fs::write(&path, &legacy).unwrap();
    storage
        .object_meta()
        .upsert_object(&data_dir, "music", "z-legacy.bin", logical.len() as i64, None, "abc", None, None, None)
        .await
        .unwrap();

    // Human: Batches used to re-read the same oldest rows on every pass, so z-legacy.bin was never reached.
    let first = storage.recompress_blobs(2).await.unwrap();
    assert_eq!((first.scanned, first.recompressed), (2, 0), "{first:?}");
    let second = storage.recompress_blobs(2).await.unwrap();
    assert_eq!((second.scanned, second.recompressed), (1, 1), "{second:?}");
    assert!(is_indexed_blob(&std::fs::read(&path).unwrap()));
    assert_eq!(engine_get_bytes(&storage, "music", "z-legacy.bin").await, logical);

    let third = storage.recompress_blobs(2).await.unwrap();
    assert_eq!(third.scanned, 2, "wraps around to the start: {third:?}");
}

#[tokio::test]
async fn test_maintenance_pages_do_not_skip_a_key_shared_by_buckets() {
    let (storage, _tmp) = setup_engine(EngineOptions::default()).await;
    put_bytes(&storage, "b1", "a.txt", b"a").await;
    for bucket in ["b1", "b2", "b3"] {
        put_bytes(&storage, bucket, "shared.txt", b"shared").await;
    }
    put_bytes(&storage, "b1", "z.txt", b"z").await;

    // Human: The cursor is only the key, so a page boundary inside the run of "shared.txt" rows used to skip
    // the rest of the run.
    for limit in [1, 2, 3] {
        let mut seen = Vec::new();
        let mut start_after: Option<String> = None;
        loop {
            let page = storage
                .object_meta()
                .list_key_page(limit, start_after.as_deref())
                .await
                .unwrap();
            seen.extend(page.rows.iter().map(|(b, k, _)| format!("{b}/{k}")));
            if !page.is_truncated {
                break;
            }
            start_after = page.last_key().map(str::to_string);
        }
        seen.sort();
        assert_eq!(
            seen,
            ["b1/a.txt", "b1/shared.txt", "b1/z.txt", "b2/shared.txt", "b3/shared.txt"],
            "limit {limit}"
        );
    }

    let mut scanned = 0;
    let mut start_after: Option<String> = None;
    loop {
        let report = storage.migrate_blobs(2, start_after.as_deref()).await.unwrap();
        scanned += report.scanned;
        if !report.is_truncated {
            break;
        }
        start_after = report.next_start_after;
    }
    assert_eq!(scanned, 5);
}

#[tokio::test]
async fn test_serve_closes_connections_that_do_not_send_headers() {
    use axum::extract::ConnectInfo;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/ip",
        axum::routing::get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move {
            format!("peer={}", peer.ip())
        }),
    );
    let options = nebular_os::server::ServeOptions {
        header_read_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    };
    tokio::spawn(nebular_os::server::serve(listener, app, options));

    // Human: A complete request is served (and sees the peer address the rate limiter keys on).
    let mut idle = tokio::net::TcpStream::connect(addr).await.unwrap();
    idle.write_all(b"GET /ip HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut buf = [0u8; 1024];
        while !String::from_utf8_lossy(&response).contains("peer=127.0.0.1") {
            let n = idle.read(&mut buf).await.unwrap();
            assert!(n > 0, "closed before responding");
            response.extend_from_slice(&buf[..n]);
        }
    })
    .await
    .expect("no response");
    assert!(response.starts_with(b"HTTP/1.1 200"));

    let mut slow = tokio::net::TcpStream::connect(addr).await.unwrap();
    slow.write_all(b"GET /ip HTTP/1.1\r\nHost: x\r\n").await.unwrap();
    let silent = tokio::net::TcpStream::connect(addr).await.unwrap();

    // Human: Without a header-read timeout all three stayed open indefinitely.
    for (name, mut conn) in [
        ("idle keep-alive", idle),
        ("half-sent headers", slow),
        ("silent", silent),
    ] {
        let closed = tokio::time::timeout(Duration::from_secs(5), async {
            let mut buf = [0u8; 1024];
            loop {
                match conn.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => continue,
                }
            }
        })
        .await;
        assert!(closed.is_ok(), "{name} connection was not closed");
    }
}

#[tokio::test]
async fn test_sampled_periodic_scrub_reaches_every_object() {
    use nebular_os::storage::blob_path;

    let (storage, tmp) = setup_engine(EngineOptions {
        scrub_sample_denom: 4,
        ..EngineOptions::default()
    })
    .await;
    let data_dir = tmp.path().join("blobs").to_string_lossy().replace('\\', "/");
    let keys: Vec<String> = (0..40).map(|i| format!("obj-{i:02}.bin")).collect();
    for key in &keys {
        put_bytes(&storage, "scrub", key, b"small raw object").await;
        // Human: Corrupt every blob, so each scrub reports exactly the keys it checked.
        std::fs::write(blob_path(&data_dir, "scrub", key), b"damaged").unwrap();
    }

    let mut found = std::collections::BTreeSet::new();
    for pass in 0..4 {
        let report = storage.scrub_with_defaults(1_000).await.unwrap();
        assert!(!report.is_truncated);
        assert!(report.scanned < 40, "pass {pass} sampled everything: {report:?}");
        found.extend(report.corrupted_keys.into_iter().map(|(_, key)| key));
    }
    // Human: The sample was fixed, so the same quarter was re-checked every pass and the rest never were.
    assert_eq!(found.len(), 40, "4 passes at 1/4 sampling must reach every object: {found:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_scrub_does_not_report_objects_overwritten_mid_check() {
    use nebular_os::storage::scrub::{ScrubMode, ScrubOptions};

    let (storage, _tmp) = setup_engine(EngineOptions {
        fsync_writes: false,
        ..EngineOptions::default()
    })
    .await;
    let storage = std::sync::Arc::new(storage);
    // Human: Incompressible (xorshift) so the object stays a raw blob, which deep scrub checks against its ETag.
    let random = |seed: u8| -> Vec<u8> {
        let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ u64::from(seed);
        (0..512 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    };
    put_bytes(&storage, "race", "video.bin", &random(0)).await;

    // Human: A reader that saw the old row and then hashed the new blob (or the reverse) used to report
    // corruption; with writers excluded for the re-check it can't.
    let writer = {
        let storage = storage.clone();
        tokio::spawn(async move {
            for seed in 1..=60u8 {
                put_bytes(&storage, "race", "video.bin", &random(seed)).await;
            }
        })
    };
    let mut corrupted = Vec::new();
    while !writer.is_finished() {
        let report = storage
            .scrub_objects(ScrubOptions {
                limit: 10,
                mode: ScrubMode::Deep,
                ..ScrubOptions::default()
            })
            .await
            .unwrap();
        corrupted.extend(report.corrupted_keys);
    }
    writer.await.unwrap();
    assert!(corrupted.is_empty(), "false corruption reports: {corrupted:?}");
}

const S3_ACCESS: &str = "AKIDNEBULARTEST";
const S3_SECRET: &str = "nebular-test-secret-key-0123456789abcdef";

fn sigv4_config() -> NosConfig {
    let mut cfg = (*test_config(None, false)).clone();
    cfg.s3_access_key = Some(S3_ACCESS.into());
    cfg.s3_secret_key = Some(S3_SECRET.into());
    cfg
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::Mac;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn amz_now() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}

/// Human: A minimal client-side SigV4 signer for canonical paths and empty queries (the verifier itself is
/// pinned to AWS's published examples in `sigv4.rs`).
fn sigv4_authorization(method: &str, path: &str, headers: &[(&str, String)], payload_hash: &str) -> String {
    use sha2::Digest;
    let mut headers: Vec<(String, String)> =
        headers.iter().map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string())).collect();
    headers.sort();
    let amz_date = headers.iter().find(|(k, _)| k == "x-amz-date").unwrap().1.clone();
    let signed: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let canonical = format!("{method}\n{path}\n\n{canonical_headers}\n{}\n{payload_hash}", signed.join(";"));
    let scope = format!("{}/us-east-1/s3/aws4_request", &amz_date[..8]);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(sha2::Sha256::digest(canonical.as_bytes()))
    );
    let mut key = hmac_sha256(format!("AWS4{S3_SECRET}").as_bytes(), &amz_date.as_bytes()[..8]);
    for part in ["us-east-1", "s3", "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));
    format!("AWS4-HMAC-SHA256 Credential={S3_ACCESS}/{scope}, SignedHeaders={}, Signature={signature}", signed.join(";"))
}

fn signed_request(method: &str, path: &str, body: &[u8], claimed_sha256: Option<&str>) -> Request<Body> {
    use sha2::Digest;
    let payload_hash = claimed_sha256
        .map(str::to_string)
        .unwrap_or_else(|| hex::encode(sha2::Sha256::digest(body)));
    let headers = [
        ("host", "localhost".to_string()),
        ("x-amz-date", amz_now()),
        ("x-amz-content-sha256", payload_hash.clone()),
    ];
    let mut builder = Request::builder().method(method).uri(path);
    for (name, value) in &headers {
        builder = builder.header(*name, value);
    }
    builder
        .header("authorization", sigv4_authorization(method, path, &headers, &payload_hash))
        .body(Body::from(body.to_vec()))
        .unwrap()
}

async fn status_and_body(app: &axum::Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap().to_vec();
    (status, body)
}

#[tokio::test]
async fn test_sigv4_signed_requests_are_bound_to_their_payload() {
    let (app, _tmp) = setup_app_with_config(sigv4_config()).await;

    let put = signed_request("PUT", "/music/signed.txt", b"signed body", None);
    assert_eq!(status_and_body(&app, put).await.0, StatusCode::CREATED);
    let (status, body) = status_and_body(&app, signed_request("GET", "/music/signed.txt", b"", None)).await;
    assert_eq!((status, body.as_slice()), (StatusCode::OK, &b"signed body"[..]));

    // Human: A body that doesn't hash to the signed x-amz-content-sha256 is refused and nothing is stored.
    let claimed = {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(b"what was signed"))
    };
    let tampered = signed_request("PUT", "/music/tampered.txt", b"something else", Some(&claimed));
    assert_eq!(status_and_body(&app, tampered).await.0, StatusCode::BAD_REQUEST);
    let (status, _) = status_and_body(&app, signed_request("GET", "/music/tampered.txt", b"", None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Human: UNSIGNED-PAYLOAD leaves the body unchecked, as in S3.
    let unsigned = signed_request("PUT", "/music/unsigned.txt", b"anything", Some("UNSIGNED-PAYLOAD"));
    assert_eq!(status_and_body(&app, unsigned).await.0, StatusCode::CREATED);

    // Human: A signature for one path doesn't authorize another, and the legacy scheme is off by default.
    let mut moved = signed_request("GET", "/music/signed.txt", b"", None);
    *moved.uri_mut() = "/music/unsigned.txt".parse().unwrap();
    assert_eq!(status_and_body(&app, moved).await.0, StatusCode::UNAUTHORIZED);
    let legacy = Request::builder()
        .method("GET")
        .uri("/music/signed.txt")
        .header("authorization", format!("NOS {S3_ACCESS}:deadbeef"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(status_and_body(&app, legacy).await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_sigv4_presigned_urls_expire_and_cover_behaviour_headers() {
    use sha2::Digest;
    let (app, _tmp) = setup_app_with_config(sigv4_config()).await;
    let token = make_token();
    let put = Request::builder()
        .method("PUT")
        .uri("/music/shared.txt")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("shared"))
        .unwrap();
    assert_eq!(status_and_body(&app, put).await.0, StatusCode::CREATED);

    let presign = |method: &str, path: &str, issued: chrono::DateTime<chrono::Utc>, expires: u64| -> String {
        let amz_date = issued.format("%Y%m%dT%H%M%SZ").to_string();
        let scope = format!("{}/us-east-1/s3/aws4_request", &amz_date[..8]);
        let query = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={S3_ACCESS}%2F{}&X-Amz-Date={amz_date}&X-Amz-Expires={expires}&X-Amz-SignedHeaders=host",
            scope.replace('/', "%2F")
        );
        let canonical = format!("{method}\n{path}\n{query}\nhost:localhost\n\nhost\nUNSIGNED-PAYLOAD");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(sha2::Sha256::digest(canonical.as_bytes()))
        );
        let mut key = hmac_sha256(format!("AWS4{S3_SECRET}").as_bytes(), &amz_date.as_bytes()[..8]);
        for part in ["us-east-1", "s3", "aws4_request"] {
            key = hmac_sha256(&key, part.as_bytes());
        }
        format!("{path}?{query}&X-Amz-Signature={}", hex::encode(hmac_sha256(&key, string_to_sign.as_bytes())))
    };
    let get = |uri: String| Request::builder().method("GET").uri(uri).header("host", "localhost").body(Body::empty()).unwrap();

    let now = chrono::Utc::now();
    let (status, body) = status_and_body(&app, get(presign("GET", "/music/shared.txt", now, 300))).await;
    assert_eq!((status, body.as_slice()), (StatusCode::OK, &b"shared"[..]));
    let stale = now - chrono::Duration::seconds(600);
    assert_eq!(status_and_body(&app, get(presign("GET", "/music/shared.txt", stale, 300))).await.0, StatusCode::UNAUTHORIZED);

    // Human: A presigned PUT can't be turned into a copy by adding a header the signature doesn't cover.
    let mut copy = Request::builder()
        .method("PUT")
        .uri(presign("PUT", "/music/copy.txt", now, 300))
        .header("host", "localhost")
        .header("x-nd-copy-source", "music/shared.txt")
        .body(Body::empty())
        .unwrap();
    assert_eq!(status_and_body(&app, copy).await.0, StatusCode::UNAUTHORIZED);
    copy = Request::builder()
        .method("PUT")
        .uri(presign("PUT", "/music/copy.txt", now, 300))
        .header("host", "localhost")
        .body(Body::from("via presigned PUT"))
        .unwrap();
    assert_eq!(status_and_body(&app, copy).await.0, StatusCode::CREATED);

    // Human: Query signing covers one object's route only: a URL presigned for a batch delete (or a listing) let
    // whoever held it send any body — here, deleting every key in the bucket.
    let batch = Request::builder()
        .method("POST")
        .uri(presign("POST", "/music/_batch_delete", now, 300))
        .header("host", "localhost")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"keys":["shared.txt"]}"#))
        .unwrap();
    assert_eq!(status_and_body(&app, batch).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(status_and_body(&app, get(presign("GET", "/music", now, 300))).await.0, StatusCode::UNAUTHORIZED);
    let (status, body) = status_and_body(&app, get(presign("GET", "/music/shared.txt", now, 300))).await;
    assert_eq!((status, body.as_slice()), (StatusCode::OK, &b"shared"[..]));
}

#[tokio::test]
async fn test_content_md5_is_verified_on_upload() {
    use base64::Engine as _;
    use md5::Digest;
    let (app, token, _tmp) = setup_app(None, false).await;
    let put = |key: &str, body: &'static [u8], md5_of: &[u8]| {
        Request::builder()
            .method("PUT")
            .uri(format!("/music/{key}"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-md5", base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(md5_of)))
            .body(Body::from(body))
            .unwrap()
    };
    assert_eq!(status_and_body(&app, put("ok.txt", b"payload", b"payload")).await.0, StatusCode::CREATED);
    let (status, body) = status_and_body(&app, put("bad.txt", b"payload", b"other")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", String::from_utf8_lossy(&body));
    let get = Request::builder()
        .uri("/music/bad.txt")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(status_and_body(&app, get).await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_jwt_issuer_and_audience_are_enforced_when_configured() {
    let mut cfg = (*test_config(None, false)).clone();
    cfg.jwt_issuer = Some("ownly".into());
    cfg.jwt_audience = Some("nebular".into());
    let (app, _tmp) = setup_app_with_config(cfg).await;
    let token = |extra: serde_json::Value| {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut claims = serde_json::json!({"sub": "svc", "email": "s@x.y", "role": "admin", "iat": now, "exp": now + 600});
        claims.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(TEST_SECRET.as_bytes())).unwrap()
    };
    let list = |t: String| {
        Request::builder().uri("/music").header("authorization", format!("Bearer {t}")).body(Body::empty()).unwrap()
    };
    let ok = token(serde_json::json!({"iss": "ownly", "aud": "nebular"}));
    assert_eq!(status_and_body(&app, list(ok)).await.0, StatusCode::OK);
    for bad in [
        serde_json::json!({}),
        serde_json::json!({"iss": "someone-else", "aud": "nebular"}),
        serde_json::json!({"iss": "ownly", "aud": "other-service"}),
        serde_json::json!({"iss": "ownly"}),
    ] {
        assert_eq!(status_and_body(&app, list(token(bad.clone()))).await.0, StatusCode::UNAUTHORIZED, "{bad}");
    }
}

#[tokio::test]
async fn test_multipart_part_numbers_are_bounded() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let init = Request::builder()
        .method("POST")
        .uri("/music/_multipart?key=big.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = status_and_body(&app, init).await;
    assert_eq!(status, StatusCode::OK);
    let upload_id = serde_json::from_slice::<Value>(&body).unwrap()["upload_id"].as_str().unwrap().to_string();
    for (part, expected) in [(0, StatusCode::BAD_REQUEST), (10_001, StatusCode::BAD_REQUEST), (10_000, StatusCode::OK)] {
        let put = Request::builder()
            .method("PUT")
            .uri(format!("/music/_multipart/{upload_id}/parts/{part}"))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from("part"))
            .unwrap();
        assert_eq!(status_and_body(&app, put).await.0, expected, "part {part}");
    }
}

#[tokio::test]
async fn test_if_range_and_conditional_head_follow_rfc_9110() {
    let (app, token, _tmp) = setup_app(None, false).await;
    let auth = format!("Bearer {token}");
    let put = Request::builder()
        .method("PUT")
        .uri("/music/ranged.txt")
        .header("authorization", &auth)
        .body(Body::from("0123456789abcdefghij"))
        .unwrap();
    assert_eq!(status_and_body(&app, put).await.0, StatusCode::CREATED);
    let head = app
        .clone()
        .oneshot(Request::builder().method("HEAD").uri("/music/ranged.txt").header("authorization", &auth).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let etag = head.headers()["etag"].to_str().unwrap().to_string();
    let last_modified = head.headers()["last-modified"].to_str().unwrap().to_string();

    // Human: A 304 to HEAD carries the validators, like the 200 would.
    let not_modified = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/music/ranged.txt")
                .header("authorization", &auth)
                .header("if-none-match", format!("\"{etag}\""))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(not_modified.headers()["etag"].to_str().unwrap(), etag);
    assert!(not_modified.headers().contains_key("last-modified"));

    let ranged = |range: &str, if_range: String| {
        Request::builder()
            .uri("/music/ranged.txt")
            .header("authorization", &auth)
            .header("range", range)
            .header("if-range", if_range)
            .body(Body::empty())
            .unwrap()
    };
    let cases = [
        ("bytes=0-3", etag.clone(), StatusCode::PARTIAL_CONTENT, &b"0123"[..]),
        ("bytes=0-3", format!("\"{etag}\""), StatusCode::PARTIAL_CONTENT, &b"0123"[..]),
        ("bytes=0-3", "\"some-other-version\"".to_string(), StatusCode::OK, &b"0123456789abcdefghij"[..]),
        ("bytes=0-3", format!("W/\"{etag}\""), StatusCode::OK, &b"0123456789abcdefghij"[..]),
        ("bytes=0-3", last_modified.clone(), StatusCode::PARTIAL_CONTENT, &b"0123"[..]),
        ("bytes=0-3", "Mon, 01 Jan 2001 00:00:00 GMT".to_string(), StatusCode::OK, &b"0123456789abcdefghij"[..]),
        ("bytes=100-200", "\"some-other-version\"".to_string(), StatusCode::OK, &b"0123456789abcdefghij"[..]),
    ];
    for (range, if_range, status, body) in cases {
        let (got_status, got_body) = status_and_body(&app, ranged(range, if_range.clone())).await;
        assert_eq!((got_status, got_body.as_slice()), (status, body), "{range} If-Range: {if_range}");
    }
    let (status, _) = status_and_body(&app, ranged("bytes=100-200", etag.clone())).await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test]
async fn test_serve_cuts_off_clients_that_stop_reading() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const BODY: usize = 64 * 1024 * 1024;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/big",
        axum::routing::get(|| async {
            let chunks = (0..BODY / (1024 * 1024))
                .map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![7u8; 1024 * 1024])));
            Body::from_stream(futures_util::stream::iter(chunks))
        }),
    );
    let options = nebular_os::server::ServeOptions {
        send_stall_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    };
    tokio::spawn(nebular_os::server::serve(listener, app, options));

    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(b"GET /big HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    // Human: Stop reading long enough for the server's writes to stay blocked past the timeout.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut received = 0usize;
    let finished = tokio::time::timeout(Duration::from_secs(10), async {
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => received += n,
            }
        }
    })
    .await;
    assert!(finished.is_ok(), "the server kept the stalled connection open");
    assert!(received < BODY, "the whole body arrived ({received} bytes)");
}

#[tokio::test]
async fn test_serve_holds_new_connections_at_the_limit() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route("/ok", axum::routing::get(|| async { "ok" }));
    let options = nebular_os::server::ServeOptions { max_connections: 1, ..Default::default() };
    tokio::spawn(nebular_os::server::serve(listener, app, options));

    let first = tokio::net::TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
    second.write_all(b"GET /ok HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    let mut buf = [0u8; 256];
    assert!(
        tokio::time::timeout(Duration::from_millis(500), second.read(&mut buf)).await.is_err(),
        "a second connection was served while the first held the only slot"
    );
    drop(first);
    let n = tokio::time::timeout(Duration::from_secs(5), second.read(&mut buf))
        .await
        .expect("the queued connection was never served")
        .unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_reads_never_mix_versions_during_overwrites() {
    use futures_util::StreamExt;
    use nebular_os::storage::GetObjectOutcome;

    let (storage, _tmp) = setup_engine(EngineOptions {
        fsync_writes: false,
        ..EngineOptions::default()
    })
    .await;
    let storage = Arc::new(storage);
    // Human: Versions of different sizes and formats (raw vs compressed), so a mixed read can't go unnoticed.
    let versions: Vec<Vec<u8>> = vec![b"short raw version".to_vec(), b"a longer, compressible version ".repeat(4_000)];
    put_bytes(&storage, "race", "doc.txt", &versions[0]).await;

    let writer = {
        let (storage, versions) = (storage.clone(), versions.clone());
        tokio::spawn(async move {
            for i in 0..80 {
                put_bytes(&storage, "race", "doc.txt", &versions[i % 2]).await;
            }
        })
    };
    let mut reads = 0;
    let mut mixed = Vec::new();
    while !writer.is_finished() {
        let Ok(GetObjectOutcome::Content { mut stream, content_length, meta, .. }) =
            storage.get_object("race", "doc.txt", None, None, None).await
        else {
            continue;
        };
        let mut body = Vec::new();
        let mut failed = false;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => body.extend_from_slice(&chunk),
                Err(_) => failed = true,
            }
        }
        reads += 1;
        let etag_matches = meta.etag.as_deref() == Some(format!("{:016x}", xxhash_rust::xxh3::xxh3_64(&body)).as_str());
        if failed || body.len() as u64 != content_length || !etag_matches {
            mixed.push((reads, body.len(), content_length));
        }
    }
    writer.await.unwrap();
    assert!(reads > 10, "too few reads to mean anything ({reads})");
    assert!(mixed.is_empty(), "reads that mixed versions: {mixed:?}");
}

#[tokio::test]
async fn test_interrupted_overwrites_are_resolved_on_restart() {
    use nebular_os::storage::blob_path;

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs").to_string_lossy().replace('\\', "/");
    std::fs::create_dir_all(&data_dir).unwrap();
    let meta_path = tmp.path().join("metadata.db").to_string_lossy().replace('\\', "/");
    let open = || StorageEngine::with_full_options(&meta_path, &data_dir, EngineOptions::default());
    let etag = |bytes: &[u8]| format!("{:016x}", xxhash_rust::xxh3::xxh3_64(bytes));
    let scratch = std::path::Path::new(&data_dir).join(".tmp");
    {
        let engine = open().await.unwrap();
        put_bytes(&engine, "music", "song.txt", b"version one").await;
        put_bytes(&engine, "music", "notes.txt", b"committed v1").await;
        put_bytes(&engine, "music", "notes.txt", b"committed v2").await;
        // Human: A record left by an overwrite of v1 that never committed, after which another write replaced
        // the object: undoing that overwrite now would put v1's bytes under v3's metadata.
        put_bytes(&engine, "music", "later.txt", b"later v1").await;
        std::fs::copy(blob_path(&data_dir, "music", "later.txt"), scratch.join("stale.prev")).unwrap();
        put_bytes(&engine, "music", "later.txt", b"later v3").await;
    }
    std::fs::write(
        scratch.join("stale.swap"),
        serde_json::json!({
            "bucket": "music", "key": "later.txt",
            "new_etag": etag(b"later v2"), "old_etag": etag(b"later v1"),
        })
        .to_string(),
    )
    .unwrap();

    // Human: An overwrite that died after renaming its blob in but before committing metadata: new bytes in
    // place, the previous ones in the `.prev` backup, the journal entry beside it.
    let song = blob_path(&data_dir, "music", "song.txt");
    std::fs::hard_link(&song, scratch.join("crashed.prev")).unwrap();
    std::fs::write(scratch.join("incoming"), b"version two").unwrap();
    std::fs::rename(scratch.join("incoming"), &song).unwrap();
    std::fs::write(
        scratch.join("crashed.swap"),
        serde_json::json!({
            "bucket": "music", "key": "song.txt",
            "new_etag": etag(b"version two"), "old_etag": etag(b"version one"),
        })
        .to_string(),
    )
    .unwrap();
    // Human: And one that died after committing metadata but before cleaning up.
    std::fs::write(scratch.join("committed.prev"), b"committed v1").unwrap();
    std::fs::write(
        scratch.join("committed.swap"),
        serde_json::json!({
            "bucket": "music", "key": "notes.txt",
            "new_etag": etag(b"committed v2"), "old_etag": etag(b"committed v1"),
        })
        .to_string(),
    )
    .unwrap();

    let engine = open().await.unwrap();
    assert_eq!(engine_get_bytes(&engine, "music", "song.txt").await, b"version one");
    assert_eq!(engine_get_bytes(&engine, "music", "notes.txt").await, b"committed v2");
    assert_eq!(engine_get_bytes(&engine, "music", "later.txt").await, b"later v3");
    let leftovers: Vec<_> = std::fs::read_dir(&scratch).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[tokio::test]
async fn test_overwrites_leave_no_journal_behind() {
    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    put_bytes(&storage, "music", "a.txt", b"first").await;
    put_bytes(&storage, "music", "a.txt", b"second").await;
    put_bytes(&storage, "music", "a.txt", &b"third, compressible ".repeat(500)).await;
    let scratch = tmp.path().join("blobs").join(".tmp");
    let leftovers: Vec<_> = std::fs::read_dir(&scratch).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    assert_eq!(engine_get_bytes(&storage, "music", "a.txt").await, b"third, compressible ".repeat(500));
}

#[tokio::test]
async fn test_legacy_raw_blobs_starting_with_a_format_magic_stay_readable() {
    use futures_util::StreamExt;
    use nebular_os::storage::blob_path;
    use nebular_os::storage::scrub::{ScrubMode, ScrubOptions};

    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let data_dir = tmp.path().join("blobs").to_string_lossy().replace('\\', "/");
    let mut objects = Vec::new();
    // Human: Uploads stored unwrapped by builds before 0.2.0, whose first bytes look like a Nebular header.
    for (i, magic) in [b"NOSI", b"NOSB", b"NOSZ", b"NOS2", b"NOSD"].into_iter().enumerate() {
        let mut body = magic.to_vec();
        body.extend((0..5_000u32).map(|n| (n * 7 + i as u32) as u8));
        let key = format!("legacy-{i}.bin");
        let path = blob_path(&data_dir, "music", &key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &body).unwrap();
        let etag = format!("{:016x}", xxhash_rust::xxh3::xxh3_64(&body));
        storage
            .object_meta()
            .upsert_object(&data_dir, "music", &key, body.len() as i64, None, &etag, None, None, None)
            .await
            .unwrap();
        objects.push((key, body));
    }
    let range_of = |key: String| {
        let storage = &storage;
        async move {
            match storage.get_object("music", &key, Some("bytes=10-19"), None, None).await.unwrap() {
                nebular_os::storage::GetObjectOutcome::Content { mut stream, .. } => {
                    let mut out = Vec::new();
                    while let Some(chunk) = stream.next().await {
                        out.extend_from_slice(&chunk.unwrap());
                    }
                    out
                }
                _ => panic!("not modified"),
            }
        }
    };
    for (key, body) in &objects {
        assert_eq!(&engine_get_bytes(&storage, "music", key).await, body, "{key}");
        assert_eq!(range_of(key.clone()).await, body[10..20], "{key} range");
    }
    let scrub = storage
        .scrub_objects(ScrubOptions { limit: 100, mode: ScrubMode::Deep, ..ScrubOptions::default() })
        .await
        .unwrap();
    assert_eq!((scrub.verified, scrub.corrupted), (5, 0), "{:?}", scrub.corrupted_keys);

    let migrated = storage.migrate_blobs(100, None).await.unwrap();
    assert_eq!(migrated.failed, 0, "{migrated:?}");
    for (key, body) in &objects {
        assert_eq!(&engine_get_bytes(&storage, "music", key).await, body, "{key} after migration");
    }
    for (key, _) in &objects {
        storage.delete_object("music", key, None).await.unwrap();
        assert!(matches!(
            storage.get_object("music", key, None, None, None).await,
            Err(nebular_os::storage::error::StorageError::NotFound)
        ));
    }
}

#[tokio::test]
async fn test_deep_scrub_catches_a_valid_blob_of_the_wrong_version() {
    use nebular_os::storage::blob_path;
    use nebular_os::storage::scrub::{ScrubMode, ScrubOptions};

    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let data_dir = tmp.path().join("blobs").to_string_lossy().replace('\\', "/");
    put_bytes(&storage, "music", "a.txt", &b"first document, compressible ".repeat(400)).await;
    put_bytes(&storage, "music", "b.txt", &b"other document, compressible ".repeat(400)).await;
    // Human: a.txt's metadata now pairs with b.txt's (internally valid) compressed blob — same size, other bytes.
    std::fs::copy(blob_path(&data_dir, "music", "b.txt"), blob_path(&data_dir, "music", "a.txt")).unwrap();

    let report = storage
        .scrub_objects(ScrubOptions { limit: 10, mode: ScrubMode::Deep, ..ScrubOptions::default() })
        .await
        .unwrap();
    assert_eq!(report.corrupted_keys, vec![("music".to_string(), "a.txt".to_string())], "{report:?}");
}

fn dedup_engine_options() -> EngineOptions {
    EngineOptions {
        dedup_enabled: true,
        dedup_min_size: 0,
        dedup_block_size: 4096,
        compress_block_size: 4096,
        compress_min_size: 1024,
        soft_delete_ttl_secs: 0,
        ..EngineOptions::default()
    }
}

fn three_blocks() -> Vec<u8> {
    [b"alpha block ".repeat(342), b"bravo block ".repeat(342), b"charlie blk ".repeat(342)]
        .iter()
        .map(|b| b[..4096].to_vec())
        .collect::<Vec<_>>()
        .concat()
}

#[tokio::test]
async fn test_dedup_never_shares_a_block_holding_other_bytes() {
    use nebular_os::storage::blocks::BlockStore;

    let (storage, tmp) = setup_engine(dedup_engine_options()).await;
    let data_dir = tmp.path().join("blobs").to_string_lossy().replace('\\', "/");
    let body = three_blocks();
    // Human: Plant different bytes under the second chunk's hash, as a (crafted) collision would.
    let store = BlockStore::new(&data_dir);
    let planted = store.block_path(BlockStore::hash_block(&body[4096..8192]));
    std::fs::create_dir_all(planted.parent().unwrap()).unwrap();
    std::fs::write(&planted, vec![b'X'; 4096]).unwrap();

    put_bytes(&storage, "music", "doc.bin", &body).await;
    assert_eq!(engine_get_bytes(&storage, "music", "doc.bin").await, body);
    assert_eq!(std::fs::read(&planted).unwrap(), vec![b'X'; 4096], "the planted block was not touched");
}

#[tokio::test]
async fn test_released_dedup_blocks_are_collected_only_after_the_grace_period() {
    use nebular_os::storage::blocks::BlockStore;
    use std::time::{Duration, SystemTime};

    let (storage, tmp) = setup_engine(dedup_engine_options()).await;
    let data_dir = tmp.path().join("blobs").to_string_lossy().replace('\\', "/");
    let pool = storage.system_write_pool().clone();
    let body = three_blocks();
    let store = BlockStore::new(&data_dir);
    let block_files: Vec<_> = body.chunks(4096).map(|c| store.block_path(BlockStore::hash_block(c))).collect();
    let age_everything = |age: Duration| {
        let pool = pool.clone();
        let files = block_files.clone();
        async move {
            let past = SystemTime::now() - age;
            for file in &files {
                std::fs::File::options().write(true).open(file).unwrap().set_modified(past).unwrap();
            }
            let released = chrono::Utc::now().timestamp() - age.as_secs() as i64;
            sqlx::query("UPDATE dedup_blocks SET released_at = ? WHERE released_at IS NOT NULL")
                .bind(released)
                .execute(&pool)
                .await
                .unwrap();
        }
    };
    let grace = Duration::from_secs(3600);
    let gc = || BlockStore::gc_released_blocks(&pool, &data_dir, grace, 100);

    put_bytes(&storage, "music", "x.bin", &body).await;
    storage.delete_object("music", "x.bin", None).await.unwrap();
    assert_eq!(gc().await.unwrap(), 0, "released blocks stay for the grace period");
    assert!(block_files.iter().all(|f| f.exists()));

    // Human: An upload shares the released blocks again before they are collected.
    put_bytes(&storage, "music", "y.bin", &body).await;
    age_everything(Duration::from_secs(7200)).await;
    assert_eq!(gc().await.unwrap(), 0, "referenced blocks are never collected");
    assert_eq!(engine_get_bytes(&storage, "music", "y.bin").await, body);

    // Human: Released long ago, but shared a moment ago (mtime refreshed) — still kept.
    storage.delete_object("music", "y.bin", None).await.unwrap();
    age_everything(Duration::from_secs(7200)).await;
    std::fs::File::options().write(true).open(&block_files[0]).unwrap().set_modified(SystemTime::now()).unwrap();
    assert_eq!(gc().await.unwrap(), 2);
    assert!(block_files[0].exists() && !block_files[1].exists() && !block_files[2].exists());
}

#[tokio::test]
async fn test_keys_differing_only_in_case_or_with_windows_characters_keep_their_own_bytes() {
    use nebular_os::storage::hash_prefix;

    let (storage, _tmp) = setup_engine(EngineOptions::default()).await;
    // Human: Blobs share a directory only within a hash shard, so pick key pairs that land in the same one —
    // exactly where a case- or normalization-folding filesystem would merge them.
    let same_shard = |make: &dyn Fn(u32) -> (String, String)| {
        (0..100_000)
            .map(make)
            .find(|(a, b)| hash_prefix(a) == hash_prefix(b))
            .unwrap()
    };
    let (upper, lower) = same_shard(&|i| (format!("Report{i}.PDF"), format!("report{i}.pdf")));
    let (composed, decomposed) = same_shard(&|i| (format!("{i}-caf\u{e9}"), format!("{i}-cafe\u{301}")));
    let objects = [
        (upper.as_str(), &b"upper case"[..]),
        (lower.as_str(), &b"lower case"[..]),
        (composed.as_str(), &b"composed"[..]),
        (decomposed.as_str(), &b"decomposed"[..]),
        ("logs/2024-01-01T10:00:00Z.log", &b"colons"[..]),
        ("con.txt", &b"device name"[..]),
    ];
    for (key, body) in objects {
        put_bytes(&storage, "music", key, body).await;
    }
    for (key, body) in objects {
        assert_eq!(engine_get_bytes(&storage, "music", key).await, body, "{key}");
    }
}

#[tokio::test]
async fn test_writing_or_deleting_a_key_leaves_its_case_alike_neighbour_alone() {
    use nebular_os::storage::hash_prefix;

    let (storage, _tmp) = setup_engine(EngineOptions::default()).await;
    let (upper, lower) = (0..100_000)
        .map(|i| (format!("Report{i}.PDF"), format!("report{i}.pdf")))
        .find(|(a, b)| hash_prefix(a) == hash_prefix(b))
        .unwrap();
    // Human: On a filesystem that ignores case, the old plain filename of `upper` opens `lower`'s blob; writing
    // or deleting `upper` cleaned up that "previous copy" — and deleted `lower`'s bytes.
    put_bytes(&storage, "music", &lower, b"keep me").await;
    put_bytes(&storage, "music", &upper, b"first").await;
    put_bytes(&storage, "music", &upper, b"second").await;
    assert_eq!(engine_get_bytes(&storage, "music", &lower).await, b"keep me");
    storage.delete_object("music", &upper, None).await.unwrap();
    assert_eq!(engine_get_bytes(&storage, "music", &lower).await, b"keep me");
    // Human: A missing key must not be served from its neighbour's file either.
    assert!(matches!(
        storage.get_object("music", &upper, None, None, None).await,
        Err(nebular_os::storage::error::StorageError::NotFound)
    ));
}

#[tokio::test]
async fn test_blobs_under_the_other_filename_scheme_are_found_and_migrated() {
    use nebular_os::storage::{blob_path, encode_blob_filename, hash_prefix};

    let (storage, tmp) = setup_engine(EngineOptions::default()).await;
    let data_dir = tmp.path().join("blobs").to_string_lossy().replace('\\', "/");
    // Human: The name this filesystem doesn't use: plain ("Report.PDF", every earlier release) where names are
    // portable (case-insensitive filesystems), portable where they are plain — a data directory moved from macOS
    // or Windows to Linux used to lose every object whose name needed escaping.
    let other = if encode_blob_filename("Report.PDF") == "Report.PDF" {
        "%52eport.%50%44%46"
    } else {
        "Report.PDF"
    };
    let plain = std::path::Path::new(&data_dir).join("music").join(hash_prefix("Report.PDF")).join(other);
    std::fs::create_dir_all(plain.parent().unwrap()).unwrap();
    std::fs::write(&plain, b"stored by an older build").unwrap();
    let etag = format!("{:016x}", xxhash_rust::xxh3::xxh3_64(b"stored by an older build"));
    storage
        .object_meta()
        .upsert_object(&data_dir, "music", "Report.PDF", 24, None, &etag, None, None, None)
        .await
        .unwrap();

    assert_eq!(engine_get_bytes(&storage, "music", "Report.PDF").await, b"stored by an older build");
    let report = storage.migrate_blobs(10, None).await.unwrap();
    assert_eq!(report.failed, 0, "{report:?}");
    assert!(blob_path(&data_dir, "music", "Report.PDF").exists());
    assert_eq!(engine_get_bytes(&storage, "music", "Report.PDF").await, b"stored by an older build");
}

#[tokio::test]
async fn test_serve_until_finishes_requests_in_progress_then_stops() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route("/ok", axum::routing::get(|| async { "ok" }))
        .route(
            "/slow",
            axum::routing::get(|| async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                "done"
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let options = nebular_os::server::ServeOptions {
        shutdown_grace: Duration::from_secs(10),
        ..Default::default()
    };
    let server = tokio::spawn(nebular_os::server::serve_until(listener, app, options, async {
        let _ = stopped.await;
    }));

    // Human: Everything the server sends until it closes the connection; None if it keeps it open.
    let read_all = |mut conn: tokio::net::TcpStream| async move {
        let mut out = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), conn.read_to_end(&mut out))
            .await
            .ok()
            .map(|_| String::from_utf8_lossy(&out).into_owned())
    };
    let mut idle = tokio::net::TcpStream::connect(addr).await.unwrap();
    idle.write_all(b"GET /ok HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    let mut first = [0u8; 256];
    let n = idle.read(&mut first).await.unwrap();
    assert!(String::from_utf8_lossy(&first[..n]).starts_with("HTTP/1.1 200"));
    let mut busy = tokio::net::TcpStream::connect(addr).await.unwrap();
    busy.write_all(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    stop.send(()).unwrap();
    // Human: The request in progress completes; the idle keep-alive connection is closed rather than kept.
    let busy_reply = read_all(busy).await.expect("the connection stayed open after its last response");
    assert!(busy_reply.starts_with("HTTP/1.1 200") && busy_reply.ends_with("done"), "{busy_reply}");
    assert_eq!(read_all(idle).await.as_deref(), Some(""), "the idle connection was not closed");
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("serve_until didn't return after its connections ended")
        .unwrap()
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(tokio::net::TcpStream::connect(addr).await.is_err(), "still accepting connections");
}

#[tokio::test]
async fn test_serve_until_stops_waiting_after_the_grace_period() {
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/hang",
        axum::routing::get(|| async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            "never"
        }),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let options = nebular_os::server::ServeOptions {
        shutdown_grace: Duration::from_millis(300),
        ..Default::default()
    };
    let server = tokio::spawn(nebular_os::server::serve_until(listener, app, options, async {
        let _ = stopped.await;
    }));
    let mut hanging = tokio::net::TcpStream::connect(addr).await.unwrap();
    hanging.write_all(b"GET /hang HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("a request that never finishes held up shutdown")
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_writes_never_overshoot_the_capacity_cap() {
    let (storage, _tmp) = setup_engine(EngineOptions {
        max_logical_bytes: 1_000,
        ..EngineOptions::default()
    })
    .await;
    let storage = Arc::new(storage);
    // Human: Twenty writers race for room for ten 100-byte objects; the cap check and the commits overlap.
    let writers: Vec<_> = (0..20)
        .map(|i| {
            let storage = storage.clone();
            tokio::spawn(async move {
                let key = format!("obj-{i}");
                let written = storage
                    .put_object("quota", &key, None, None, std::io::Cursor::new(vec![b'x'; 100]))
                    .await;
                (key, written)
            })
        })
        .collect();
    let mut stored = Vec::new();
    for writer in writers {
        match writer.await.unwrap() {
            (key, Ok(_)) => stored.push(key),
            (_, Err(nebular_os::storage::error::StorageError::InsufficientStorage)) => {}
            (key, Err(e)) => panic!("{key}: unexpected error: {e:?}"),
        }
    }
    assert_eq!(stored.len(), 10);
    assert_eq!(storage.total_bytes().await.unwrap(), 1_000);
    // Human: Room freed by a delete is available again (reservations were released, not leaked). Which writers
    // won the race varies, so free the room of one that did.
    storage.delete_object("quota", &stored[0], None).await.unwrap();
    put_bytes(&storage, "quota", "after-delete", &[b'y'; 100]).await;
}

#[tokio::test]
async fn test_only_objects_system_routes_shadow_are_refused() {
    use nebular_os::storage::error::StorageError;

    let (storage, _tmp) = setup_engine(EngineOptions::default()).await;
    for (bucket, key) in [
        ("_nos", "x"),
        ("_cluster", "x"),
        ("health", "ready"),
        ("media", "_batch_delete"),
        ("media", "_multipart"),
        ("media", "_multipart/upload-id"),
        ("media", "_multipart/upload-id/complete"),
        ("media", "_multipart/upload-id/parts/3"),
    ] {
        let refused = storage
            .put_object(bucket, key, None, None, std::io::Cursor::new(b"x".to_vec()))
            .await;
        assert!(matches!(refused, Err(StorageError::InvalidRequest(_))), "{bucket}/{key}: {refused:?}");
    }
    // Human: Buckets named like an endpoint stayed usable before; only exact collisions are unreachable.
    for (bucket, key) in [("health", "check.json"), ("metrics", "daily.csv"), ("media", "_multipart/a/b/c/d")] {
        put_bytes(&storage, bucket, key, b"fine").await;
        assert_eq!(engine_get_bytes(&storage, bucket, key).await, b"fine");
    }
}

#[tokio::test]
async fn test_multipart_complete_ignores_bodies_that_are_not_a_part_list() {
    let (app, token, _tmp) = setup_app(None, false).await;
    // Human: Earlier releases ignored the body; clients send nulls, S3-style XML or nothing at all.
    for (i, body) in ["null", "<CompleteMultipartUpload/>", "{}", r#"{"parts":null}"#, ""].into_iter().enumerate() {
        let key = format!("lenient-{i}.bin");
        let upload_id = start_multipart(&app, &token, &key).await;
        let part = format!("/music/_multipart/{upload_id}/parts/1");
        assert_eq!(send(&app, authed("PUT", &part, &token, Body::from("data"))).await.0, StatusCode::OK);
        let complete = format!("/music/_multipart/{upload_id}/complete");
        let (status, reply) = send(&app, authed("POST", &complete, &token, Body::from(body))).await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}: {}", String::from_utf8_lossy(&reply));
        assert_eq!(http_get_bytes(&app, &token, "music", &key).await, b"data");
    }
    // Human: A `parts` field that isn't a part list is still an error, not silently ignored.
    let upload_id = start_multipart(&app, &token, "strict.bin").await;
    let part = format!("/music/_multipart/{upload_id}/parts/1");
    assert_eq!(send(&app, authed("PUT", &part, &token, Body::from("data"))).await.0, StatusCode::OK);
    let complete = format!("/music/_multipart/{upload_id}/complete");
    let (status, _) = send(&app, authed("POST", &complete, &token, Body::from(r#"{"parts":"all"}"#))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
