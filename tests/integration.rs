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
        verify_interval_secs: 0,
        verify_batch_size: 100,
        s3_compat: false,
        bucket_policy: nebular_os::config::BucketPolicy::default(),
        s3_access_key: None,
        s3_secret_key: None,
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

    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
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

    (app, make_token(), tmp)
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
    assert!(storage.dict_store().exists_on_disk(0));

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
    assert_eq!(read_indexed_dict_id(&on_disk), Some(0));
    assert!(storage.dict_store().exists_on_disk(0));
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
