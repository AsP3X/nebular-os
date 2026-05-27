use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use nebular_os::auth::Claims;
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
    Arc::new(NosConfig {
        bind_addr: "127.0.0.1:0".into(),
        data_dir: "./data/blobs".into(),
        meta_path: "./data/meta/metadata.db".into(),
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
        multipart_part_size: 8 * 1024 * 1024,
        read_pool_size: 2,
        cors_origins: vec![],
        zstd_level: 3,
        s3_compat: false,
        bucket_policy: nebular_os::config::BucketPolicy::default(),
        s3_access_key: None,
        s3_secret_key: None,
    })
}

async fn setup_app(signing_secret: Option<String>, allow_public_read: bool) -> (axum::Router, String, TempDir) {
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
            ..EngineOptions::default()
        },
    )
    .await
    .unwrap();

    let cfg = test_config(signing_secret, allow_public_read);
    let app = create_app(storage, cfg).await.unwrap();

    (app, make_token(), tmp)
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
    use nebular_os::storage::compression::{is_compressed_blob, BLOB_MAGIC};

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
    assert!(is_compressed_blob(&on_disk));
    assert!(on_disk.starts_with(BLOB_MAGIC));
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
    let app = create_app(storage, Arc::new(cfg)).await.unwrap();
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
            ..EngineOptions::default()
        },
    )
    .await
    .unwrap();
    let mut cfg = (*test_config(None, false)).clone();
    cfg.metrics_token = Some("metrics-secret".into());
    let app = create_app(storage, Arc::new(cfg)).await.unwrap();

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

    storage.delete_object("music", "tmp.bin").await.unwrap();
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
    storage.delete_object("music", "gone.bin").await.unwrap();
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
    let logical = b"legacy raw payload ".repeat(200);
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
        .copy_object("music", "original.bin", "music", "copy.bin")
        .await
        .unwrap();

    let src = blob_path(&data_dir.to_string_lossy(), "music", "original.bin");
    let dst = blob_path(&data_dir.to_string_lossy(), "music", "copy.bin");
    assert!(same_inode(&src, &dst));

    storage.delete_object("music", "copy.bin").await.unwrap();
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
    let app = create_app(storage, cfg).await.unwrap();
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
    let app = create_app(storage, cfg).await.unwrap();

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
