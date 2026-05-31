//! Cluster-mode integration tests (replicated). Standalone tests remain in integration.rs.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use nebular_os::auth::Claims;
use nebular_os::cluster::{
    apply_replication_event_bytes, build_backend, drain_once, ClusterConfig, ClusterMode,
    ReplicationEvent, ReplicationOp,
};
use nebular_os::config::NosConfig;
use nebular_os::server::create_app;
use nebular_os::storage::engine::{EngineOptions, StorageEngine};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower::ServiceExt;

const TEST_SECRET: &str = "test-secret-key-that-is-long-enough-for-hs256-32-bytes!";
const CLUSTER_TOKEN: &str = "cluster-test-token-at-least-thirty-two-characters-long";

fn make_token() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
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

fn cluster_test_config(
    node_id: &str,
    peers: &str,
    role: &str,
    replication_factor: u32,
) -> Arc<NosConfig> {
    Arc::new(NosConfig {
        bind_addr: "127.0.0.1:0".into(),
        data_dir: "./data/blobs".into(),
        meta_path: "./data/meta/metadata.db".into(),
        jwt_secret: TEST_SECRET.into(),
        signing_secret: None,
        max_body_size: 10_000_000,
        upload_buffer_size: 64 * 1024,
        allow_public_read: false,
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
        cluster: ClusterConfig {
            mode: ClusterMode::Replicated,
            node_id: node_id.into(),
            instance_id: node_id.into(),
            region_label: None,
            cluster_token: Some(CLUSTER_TOKEN.into()),
            peers_raw: Some(peers.into()),
            storage_classes: vec!["default".into()],
            replication_group: "default".into(),
            replication_role: role.into(),
            replication_factor,
            replication_pending_events: 0,
            default_storage_class: "default".into(),
            assignment_rules_raw: None,
            assignment_forward: false,
        },
    })
}

fn assigned_rules_json() -> String {
    r#"{"rules":[
        {"storage_class":"hls-hot","prefix":"users/","mime_prefix":"video/","assigned_node":"node-hot"},
        {"storage_class":"cold","assigned_node":"node-cold"}
    ]}"#
    .into()
}

fn assigned_test_config(
    node_id: &str,
    storage_classes: &[&str],
    peers: &str,
) -> Arc<NosConfig> {
    let base = cluster_test_config(node_id, peers, "member", 1);
    Arc::new(NosConfig {
        cluster: ClusterConfig {
            mode: ClusterMode::Assigned,
            storage_classes: storage_classes.iter().map(|s| (*s).to_string()).collect(),
            assignment_rules_raw: Some(assigned_rules_json()),
            ..base.cluster.clone()
        },
        ..(*base).clone()
    })
}

async fn engine_and_backend(
    cfg: &Arc<NosConfig>,
    tmp: &TempDir,
) -> (nebular_os::cluster::StorageBackend, String) {
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
    let backend = build_backend(storage, cfg).unwrap();
    (backend, data_dir_str)
}

#[tokio::test]
async fn cluster_idempotent_replay() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 2);
    let (backend, _) = engine_and_backend(&cfg, &tmp).await;
    let engine = backend.engine().clone();
    let log = match &backend {
        nebular_os::cluster::StorageBackend::Replicated(r) => r.replication_log(),
        _ => panic!("expected replicated backend"),
    };

    engine
        .put_object(
            "music",
            "idempotent.bin",
            None,
            None,
            std::io::Cursor::new(b"same-bytes"),
        )
        .await
        .unwrap();

    let event = ReplicationEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        origin_node: "node-a".into(),
        op: ReplicationOp::Put,
        bucket: "music".into(),
        key: "idempotent.bin".into(),
        etag: Some("dummy".into()),
        size: Some(10),
        payload_path: None,
        created_at: 1,
    };

    apply_replication_event_bytes(&engine, log, &event, Some(b"same-bytes".to_vec()))
        .await
        .unwrap();
    apply_replication_event_bytes(&engine, log, &event, Some(b"same-bytes".to_vec()))
        .await
        .unwrap();

    let count = engine.object_count().await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn readonly_replica_rejects_put() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-ro", "node-b=http://127.0.0.1:1", "readonly", 2);
    let (backend, _) = engine_and_backend(&cfg, &tmp).await;
    let app = create_app(backend, cfg).await.unwrap();
    let token = make_token();

    let req = Request::builder()
        .method("PUT")
        .uri("/music/readonly.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("nope"))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], "node is read-only replica");
}

#[tokio::test]
async fn cluster_replicate_eventually() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let cfg_b = cluster_test_config(
        "node-b",
        &format!("node-a=http://127.0.0.1:1"),
        "member",
        2,
    );
    let (backend_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = create_app(backend_b, cfg_b.clone()).await.unwrap();
    let app_b_client = app_b.clone();
    tokio::spawn(async move {
        axum::serve(listener_b, app_b.into_make_service())
            .await
            .unwrap();
    });

    let peers = format!("node-b=http://{}", addr_b);
    let cfg_a = cluster_test_config("node-a", &peers, "member", 2);
    let (backend_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = create_app(backend_a.clone(), cfg_a.clone())
        .await
        .unwrap();
    let token = make_token();

    let put = Request::builder()
        .method("PUT")
        .uri("/music/cluster.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("replicated-payload"))
        .unwrap();
    assert_eq!(
        app_a.clone().oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );

    let replicated = match &backend_a {
        nebular_os::cluster::StorageBackend::Replicated(r) => r.clone(),
        _ => panic!("expected replicated"),
    };
    let peers = nebular_os::cluster::peer::PeerRegistry::from_peers_raw(&peers).unwrap();
    let client = reqwest::Client::new();
    for _ in 0..20 {
        let _: () = drain_once(
            &client,
            replicated.replication_log(),
            &peers,
            &cfg_a.cluster,
            CLUSTER_TOKEN,
        )
        .await
        .unwrap();
        let get = Request::builder()
            .method("GET")
            .uri("/music/cluster.bin")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app_b_client.clone().oneshot(get).await.unwrap();
        if resp.status() == StatusCode::OK {
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), b"replicated-payload");
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("object not replicated to peer within timeout");
}

#[tokio::test]
async fn assigned_routes_video_to_hot() {
    let tmp_hot = TempDir::new().unwrap();
    let tmp_cold = TempDir::new().unwrap();
    let peers = "node-hot=http://127.0.0.1:1,node-cold=http://127.0.0.1:2";

    let cfg_hot = assigned_test_config("node-hot", &["hls-hot", "default"], peers);
    let (backend_hot, _) = engine_and_backend(&cfg_hot, &tmp_hot).await;
    let app_hot = create_app(backend_hot, cfg_hot).await.unwrap();
    let token = make_token();

    let put_hot = Request::builder()
        .method("PUT")
        .uri("/music/users/clip.mp4")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "video/mp4")
        .body(Body::from("video-bytes"))
        .unwrap();
    assert_eq!(
        app_hot.oneshot(put_hot).await.unwrap().status(),
        StatusCode::CREATED
    );

    let cfg_cold = assigned_test_config("node-cold", &["cold"], peers);
    let (backend_cold, _) = engine_and_backend(&cfg_cold, &tmp_cold).await;
    let app_cold = create_app(backend_cold, cfg_cold).await.unwrap();

    let put_cold = Request::builder()
        .method("PUT")
        .uri("/music/users/clip.mp4")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "video/mp4")
        .body(Body::from("video-bytes"))
        .unwrap();
    let response = app_cold.oneshot(put_cold).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], "object not assigned to this node");
    assert_eq!(json["storage_class"], "hls-hot");
    assert_eq!(json["assigned_node"], "node-hot");
}
