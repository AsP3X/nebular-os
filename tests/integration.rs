use axum::body::Body;
use axum::http::{Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use nebular_os::auth::Claims;
use nebular_os::server::create_app;
use nebular_os::storage::engine::StorageEngine;
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

async fn setup_app(signing_secret: Option<String>) -> (axum::Router, String, TempDir) {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("blobs");

    std::fs::create_dir_all(&data_dir).unwrap();

    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");

    let storage = StorageEngine::new(
        &meta_path_str,
        &data_dir_str,
    )
    .await
    .unwrap();

    let app = create_app(
        storage,
        TEST_SECRET.into(),
        signing_secret,
        10_000_000,
        false,
    )
    .await
    .unwrap();

    (app, make_token(), tmp)
}

#[tokio::test]
async fn test_put_get_delete() {
    let (app, token, _tmp) = setup_app(None).await;

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
    let (app, _token, _tmp) = setup_app(None).await;

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
    let (app, token, _tmp) = setup_app(None).await;

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
    let (app, token, _tmp) = setup_app(None).await;

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
    let (app, token, _tmp) = setup_app(None).await;

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
    let (app, token, _tmp) = setup_app(None).await;

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
    let (app, _token, _tmp) = setup_app(None).await;

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
    let (app, _token, _tmp) = setup_app(None).await;
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
    let (app, _token, _tmp) = setup_app(None).await;
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
    let (app, token, _tmp) = setup_app(Some("test-signing-secret".into())).await;
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
async fn test_expired_presigned_url_rejected() {
    let (app, token, _tmp) = setup_app(Some("test-signing-secret".into())).await;
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
