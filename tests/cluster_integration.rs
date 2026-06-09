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
use nebular_os::observability::NosMetrics;
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
        metadata_backend: nebular_os::storage::metadata_backend::MetadataBackendKind::Sqlite,
        metadata_mode: nebular_os::storage::metadata_mode::MetadataMode::Full,
        metadata_database_url: None,
        max_logical_bytes: 0,
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
            replication_read_repair: false,
            replication_heal_on_read: false,
            replication_async: true,
            replication_prefixes: Vec::new(),
            replication_exclude_prefixes: Vec::new(),
            replication_max_attempts: 20,
            replication_peer_concurrency: 4,
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
) -> (nebular_os::cluster::StorageBackend, StorageEngine, String) {
    let data_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&data_dir).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let meta_path_str = format!("file:{}?mode=memory&cache=shared", id);
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    let storage = StorageEngine::with_full_options(
        &meta_path_str,
        &data_dir_str,
        EngineOptions {
            upload_buffer_size: cfg.upload_buffer_size,
            read_pool_size: cfg.read_pool_size,
            max_logical_bytes: cfg.max_logical_bytes,
            metadata_backend: cfg.metadata_backend,
            metadata_database_url: cfg.metadata_database_url.clone(),
            ..EngineOptions::default()
        },
    )
    .await
    .unwrap();
    let metrics = NosMetrics::new();
    let backend = build_backend(storage.clone(), &cfg.cluster, metrics).unwrap();
    (backend, storage, data_dir_str)
}

async fn app_with_metrics(
    backend: nebular_os::cluster::StorageBackend,
    engine: StorageEngine,
    cfg: Arc<NosConfig>,
) -> axum::Router {
    let metrics = NosMetrics::new();
    create_app(backend, engine, cfg, metrics).await.unwrap()
}

#[tokio::test]
async fn cluster_idempotent_replay() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 2);
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
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
        storage_class: "default".into(),
        replication_group: "default".into(),
        content_type: Some("text/plain".into()),
        custom_meta: Some(r#"{"tag":"v1"}"#.into()),
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

    let meta = engine
        .head_object("music", "idempotent.bin", None, None)
        .await
        .unwrap()
        .expect("object metadata");
    assert_eq!(meta.mime_type.as_deref(), Some("text/plain"));
    assert_eq!(meta.custom_meta.as_deref(), Some(r#"{"tag":"v1"}"#));
}

#[tokio::test]
async fn readonly_replica_rejects_put() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-ro", "node-b=http://127.0.0.1:1", "readonly", 2);
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let app = app_with_metrics(backend, engine, cfg).await;
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
        "node-a=http://127.0.0.1:1",
        "member",
        2,
    );
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b, cfg_b.clone()).await;
    let app_b_client = app_b.clone();
    tokio::spawn(async move {
        axum::serve(listener_b, app_b.into_make_service())
            .await
            .unwrap();
    });

    let peers = format!("node-b=http://{}", addr_b);
    let cfg_a = cluster_test_config("node-a", &peers, "member", 2);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a.clone(), engine_a.clone(), cfg_a.clone()).await;
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
        let metrics = NosMetrics::new();
        let _: () = drain_once(
            &client,
            replicated.replication_log(),
            &peers,
            &cfg_a.cluster,
            CLUSTER_TOKEN,
            &metrics,
            None,
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
    let (backend_hot, engine_hot, _) = engine_and_backend(&cfg_hot, &tmp_hot).await;
    let app_hot = app_with_metrics(backend_hot, engine_hot, cfg_hot).await;
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
    let (backend_cold, engine_cold, _) = engine_and_backend(&cfg_cold, &tmp_cold).await;
    let app_cold = app_with_metrics(backend_cold, engine_cold, cfg_cold).await;

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

fn combined_rules_json() -> String {
    r#"{"rules":[{"storage_class":"hls-hot","prefix":"users/","mime_prefix":"video/","assigned_node":"node-hot"}]}"#
        .into()
}

fn combined_hot_config(peers: &str) -> Arc<NosConfig> {
    let base = cluster_test_config("node-hot", peers, "member", 2);
    Arc::new(NosConfig {
        cluster: ClusterConfig {
            mode: ClusterMode::ReplicatedAssigned,
            storage_classes: vec!["hls-hot".into(), "default".into()],
            assignment_rules_raw: Some(combined_rules_json()),
            ..base.cluster.clone()
        },
        ..(*base).clone()
    })
}

fn combined_rep_config(peers: &str) -> Arc<NosConfig> {
    let base = cluster_test_config("node-rep", peers, "member", 1);
    Arc::new(NosConfig {
        cluster: ClusterConfig {
            mode: ClusterMode::Replicated,
            storage_classes: vec!["hls-hot".into(), "default".into()],
            replication_factor: 2,
            ..base.cluster.clone()
        },
        ..(*base).clone()
    })
}

#[tokio::test]
async fn replicated_assigned_replicates_class_to_peer() {
    let tmp_hot = TempDir::new().unwrap();
    let tmp_rep = TempDir::new().unwrap();

    let listener_rep = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_rep = listener_rep.local_addr().unwrap();

    let peers_hot = format!("node-rep=http://{};hls-hot;group=default", addr_rep);
    let cfg_rep = combined_rep_config("node-hot=http://127.0.0.1:1;group=default");
    let (backend_rep, engine_rep, _) = engine_and_backend(&cfg_rep, &tmp_rep).await;
    let app_rep = app_with_metrics(backend_rep, engine_rep, cfg_rep.clone()).await;
    let app_rep_client = app_rep.clone();
    tokio::spawn(async move {
        axum::serve(listener_rep, app_rep.into_make_service())
            .await
            .unwrap();
    });

    let cfg_hot = combined_hot_config(&peers_hot);
    let (backend_hot, engine_hot, _) = engine_and_backend(&cfg_hot, &tmp_hot).await;
    let log = match &backend_hot {
        nebular_os::cluster::StorageBackend::Assigned(b) => {
            b.replication_log().expect("replicated inner").clone()
        }
        _ => panic!("expected assigned backend"),
    };
    let app_hot = app_with_metrics(backend_hot, engine_hot, cfg_hot.clone()).await;
    let token = make_token();

    let put = Request::builder()
        .method("PUT")
        .uri("/music/users/clip.mp4")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "video/mp4")
        .body(Body::from("combined-mode-payload"))
        .unwrap();
    assert_eq!(
        app_hot.clone().oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );

    let peers = nebular_os::cluster::peer::PeerRegistry::from_peers_raw(&peers_hot).unwrap();
    let client = reqwest::Client::new();
    let metrics = NosMetrics::new();
    for _ in 0..30 {
        drain_once(
            &client,
            &log,
            &peers,
            &cfg_hot.cluster,
            CLUSTER_TOKEN,
            &metrics,
            None,
        )
        .await
        .unwrap();
        let get = Request::builder()
            .method("GET")
            .uri("/music/users/clip.mp4")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app_rep_client.clone().oneshot(get).await.unwrap();
        if resp.status() == StatusCode::OK {
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), b"combined-mode-payload");
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("combined mode replication did not reach peer");
}

#[tokio::test]
async fn read_repair_fetches_from_peer() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();

    let cfg_a = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 1);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a, engine_a, cfg_a).await;
    tokio::spawn(async move {
        axum::serve(listener_a, app_a.into_make_service())
            .await
            .unwrap();
    });

    let peers_b = format!("node-a=http://{}", addr_a);
    let base_b = cluster_test_config("node-b", &peers_b, "member", 1);
    let cfg_b = Arc::new(NosConfig {
        cluster: ClusterConfig {
            replication_read_repair: true,
            ..base_b.cluster.clone()
        },
        ..(*base_b).clone()
    });
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b, cfg_b.clone()).await;
    let token = make_token();

    let client = reqwest::Client::new();
    let resp = client
        .put(format!("http://{}/music/repair.bin", addr_a))
        .header("authorization", format!("Bearer {token}"))
        .body("repair-bytes")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let get = Request::builder()
        .method("GET")
        .uri("/music/repair.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app_b.oneshot(get).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"repair-bytes");
}

fn assigned_forward_config(node_id: &str, peers: &str) -> Arc<NosConfig> {
    let base = assigned_test_config(node_id, &["cold"], peers);
    Arc::new(NosConfig {
        cluster: ClusterConfig {
            assignment_forward: true,
            ..base.cluster.clone()
        },
        ..(*base).clone()
    })
}

#[tokio::test]
async fn assignment_forward_proxies_put_to_hot() {
    let tmp_hot = TempDir::new().unwrap();
    let tmp_cold = TempDir::new().unwrap();

    let listener_hot = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_hot = listener_hot.local_addr().unwrap();

    let peers = format!("node-hot=http://{addr_hot}");
    let cfg_hot = assigned_test_config("node-hot", &["hls-hot", "default"], &peers);
    let (backend_hot, engine_hot, _) = engine_and_backend(&cfg_hot, &tmp_hot).await;
    let app_hot = app_with_metrics(backend_hot, engine_hot, cfg_hot).await;
    let app_hot_client = app_hot.clone();
    tokio::spawn(async move {
        axum::serve(listener_hot, app_hot.into_make_service())
            .await
            .unwrap();
    });

    let cfg_cold = assigned_forward_config("node-cold", &peers);
    let (backend_cold, engine_cold, _) = engine_and_backend(&cfg_cold, &tmp_cold).await;
    let app_cold = app_with_metrics(backend_cold, engine_cold, cfg_cold).await;
    let token = make_token();

    let put = Request::builder()
        .method("PUT")
        .uri("/music/users/forwarded.mp4")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "video/mp4")
        .body(Body::from("forwarded-bytes"))
        .unwrap();
    assert_eq!(
        app_cold.oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );

    let get = Request::builder()
        .method("GET")
        .uri("/music/users/forwarded.mp4")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app_hot_client.oneshot(get).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn replication_retry_after_failed_push() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let cfg_b = cluster_test_config("node-b", "node-a=http://127.0.0.1:1", "member", 2);
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b, cfg_b.clone()).await;
    let app_b_client = app_b.clone();
    tokio::spawn(async move {
        axum::serve(listener_b, app_b.into_make_service())
            .await
            .unwrap();
    });

    let peers = format!("node-b=http://{addr_b};group=default");
    let cfg_a = cluster_test_config("node-a", &peers, "member", 2);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let replicated = match &backend_a {
        nebular_os::cluster::StorageBackend::Replicated(r) => r.clone(),
        _ => panic!("expected replicated"),
    };
    let token = make_token();
    let app_a = app_with_metrics(backend_a, engine_a, cfg_a.clone()).await;
    let put = Request::builder()
        .method("PUT")
        .uri("/music/retry.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("retry-payload"))
        .unwrap();
    assert_eq!(
        app_a.oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );

    let log = replicated.replication_log();
    let peers_bad = nebular_os::cluster::peer::PeerRegistry::from_peers_raw(
        "node-b=http://127.0.0.1:1;group=default",
    )
    .unwrap();
    let client = reqwest::Client::new();
    let metrics = NosMetrics::new();
    drain_once(
        &client,
        log,
        &peers_bad,
        &cfg_a.cluster,
        CLUSTER_TOKEN,
        &metrics,
        None,
    )
    .await
    .unwrap();

    let pending = log.list_pending(8).await.unwrap();
    assert!(pending.is_empty() || !pending[0].event_id.is_empty());
    let event_id = sqlx::query_as::<_, (String,)>(
        "SELECT event_id FROM replication_log WHERE status = 'failed' LIMIT 1",
    )
    .fetch_one(log.pool())
    .await
    .unwrap()
    .0;

    sqlx::query(
        "UPDATE replication_log SET next_retry_at = 0, status = 'failed' WHERE event_id = ?",
    )
    .bind(&event_id)
    .execute(log.pool())
    .await
    .unwrap();

    let peers_ok = nebular_os::cluster::peer::PeerRegistry::from_peers_raw(&peers).unwrap();
    for _ in 0..20 {
        drain_once(
            &client,
            log,
            &peers_ok,
            &cfg_a.cluster,
            CLUSTER_TOKEN,
            &metrics,
            None,
        )
        .await
        .unwrap();
        let get = Request::builder()
            .method("GET")
            .uri("/music/retry.bin")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        if app_b_client.clone().oneshot(get).await.unwrap().status() == StatusCode::OK {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("replication did not succeed after retry");
}

#[tokio::test]
async fn replication_group_mismatch_skips_peer() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let cfg_b = cluster_test_config("node-b", "node-a=http://127.0.0.1:1", "member", 2);
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b, cfg_b).await;
    tokio::spawn(async move {
        axum::serve(listener_b, app_b.into_make_service())
            .await
            .unwrap();
    });

    let peers = format!("node-b=http://{addr_b};group=other");
    let cfg_a = cluster_test_config("node-a", &peers, "member", 2);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let replicated = match &backend_a {
        nebular_os::cluster::StorageBackend::Replicated(r) => r.clone(),
        _ => panic!("expected replicated"),
    };
    let token = make_token();
    let app_a = app_with_metrics(backend_a, engine_a, cfg_a.clone()).await;
    let put = Request::builder()
        .method("PUT")
        .uri("/music/group.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("group-test"))
        .unwrap();
    assert_eq!(
        app_a.oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );

    let log = replicated.replication_log();
    let peers_reg = nebular_os::cluster::peer::PeerRegistry::from_peers_raw(&peers).unwrap();
    let metrics = NosMetrics::new();
    drain_once(
        &reqwest::Client::new(),
        log,
        &peers_reg,
        &cfg_a.cluster,
        CLUSTER_TOKEN,
        &metrics,
        None,
    )
    .await
    .unwrap();

    let failed: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM replication_log WHERE status = 'failed'",
    )
    .fetch_one(log.pool())
    .await
    .unwrap();
    assert!(failed.0 >= 1);

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr_b}/music/group.bin"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

const BOOTSTRAP_TOKEN: &str = "bootstrap-test-token-at-least-thirty-two-chars";

fn bootstrap_standalone_config() -> Arc<NosConfig> {
    let base = cluster_test_config("node-a", "node-a=http://127.0.0.1:1", "member", 1);
    Arc::new(NosConfig {
        cluster: ClusterConfig::standalone(),
        cluster_bootstrap_token: Some(BOOTSTRAP_TOKEN.into()),
        ..(*base).clone()
    })
}

#[tokio::test]
async fn runtime_cluster_config_apply_via_bootstrap() {
    let tmp = TempDir::new().unwrap();
    let cfg = bootstrap_standalone_config();
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let app = app_with_metrics(backend, engine, cfg).await;

    let body = serde_json::json!({
        "mode": "replicated",
        "node_id": "node-a",
        "cluster_token": CLUSTER_TOKEN,
        "peers": [
            { "id": "node-a", "url": "http://127.0.0.1:9000" },
            { "id": "node-b", "url": "http://127.0.0.1:9001" }
        ],
        "storage_classes": ["default"],
        "replication_factor": 2
    });
    let put = Request::builder()
        .method("PUT")
        .uri("/_cluster/config")
        .header("authorization", format!("Bearer {BOOTSTRAP_TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(put).await.unwrap().status(),
        StatusCode::OK
    );

    let health = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(health).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["cluster_mode"], "replicated");
    assert_eq!(json["node_id"], "node-a");
}

#[tokio::test]
async fn replication_metadata_e2e() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let cfg_b = cluster_test_config("node-b", "node-a=http://127.0.0.1:1", "member", 2);
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b, cfg_b.clone()).await;
    let app_b_client = app_b.clone();
    tokio::spawn(async move {
        axum::serve(listener_b, app_b.into_make_service())
            .await
            .unwrap();
    });

    let peers = format!("node-b=http://{}", addr_b);
    let cfg_a = cluster_test_config("node-a", &peers, "member", 2);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a.clone(), engine_a.clone(), cfg_a.clone()).await;
    let token = make_token();

    let put = Request::builder()
        .method("PUT")
        .uri("/music/meta-e2e.bin")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("x-nd-custom-meta-tag", "replicated")
        .body(Body::from(r#"{"ok":true}"#))
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
        let metrics = NosMetrics::new();
        drain_once(
            &client,
            replicated.replication_log(),
            &peers,
            &cfg_a.cluster,
            CLUSTER_TOKEN,
            &metrics,
            None,
        )
        .await
        .unwrap();
        let head = Request::builder()
            .method("HEAD")
            .uri("/music/meta-e2e.bin")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app_b_client.clone().oneshot(head).await.unwrap();
        if resp.status() == StatusCode::OK {
            assert_eq!(
                resp.headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok()),
                Some("application/json")
            );
            assert_eq!(
                resp.headers()
                    .get("x-nd-custom-meta-tag")
                    .and_then(|v| v.to_str().ok()),
                Some("replicated")
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("metadata did not replicate to peer");
}

#[tokio::test]
async fn replication_prefix_skips_excluded_key() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let cfg_b = cluster_test_config("node-b", "node-a=http://127.0.0.1:1", "member", 2);
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b, cfg_b.clone()).await;
    let app_b_client = app_b.clone();
    tokio::spawn(async move {
        axum::serve(listener_b, app_b.into_make_service())
            .await
            .unwrap();
    });

    let peers = format!("node-b=http://{}", addr_b);
    let base_a = cluster_test_config("node-a", &peers, "member", 2);
    let cfg_a = Arc::new(NosConfig {
        cluster: ClusterConfig {
            replication_exclude_prefixes: vec!["skip/".into()],
            ..base_a.cluster.clone()
        },
        ..(*base_a).clone()
    });
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a.clone(), engine_a.clone(), cfg_a.clone()).await;
    let token = make_token();

    let put_skip = Request::builder()
        .method("PUT")
        .uri("/music/skip/excluded.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("skip-me"))
        .unwrap();
    assert_eq!(
        app_a.clone().oneshot(put_skip).await.unwrap().status(),
        StatusCode::CREATED
    );

    let replicated = match &backend_a {
        nebular_os::cluster::StorageBackend::Replicated(r) => r.clone(),
        _ => panic!("expected replicated"),
    };
    let peer_reg = nebular_os::cluster::peer::PeerRegistry::from_peers_raw(&peers).unwrap();
    let client = reqwest::Client::new();
    let metrics = NosMetrics::new();
    for _ in 0..10 {
        drain_once(
            &client,
            replicated.replication_log(),
            &peer_reg,
            &cfg_a.cluster,
            CLUSTER_TOKEN,
            &metrics,
            None,
        )
        .await
        .unwrap();
    }

    let get_skip = Request::builder()
        .method("GET")
        .uri("/music/skip/excluded.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app_b_client.clone().oneshot(get_skip).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    let put_ok = Request::builder()
        .method("PUT")
        .uri("/music/ok/included.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("include-me"))
        .unwrap();
    assert_eq!(
        app_a.oneshot(put_ok).await.unwrap().status(),
        StatusCode::CREATED
    );

    for _ in 0..20 {
        drain_once(
            &client,
            replicated.replication_log(),
            &peer_reg,
            &cfg_a.cluster,
            CLUSTER_TOKEN,
            &metrics,
            None,
        )
        .await
        .unwrap();
        let get_ok = Request::builder()
            .method("GET")
            .uri("/music/ok/included.bin")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app_b_client.clone().oneshot(get_ok).await.unwrap();
        if resp.status() == StatusCode::OK {
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), b"include-me");
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("non-excluded key did not replicate");
}

#[tokio::test]
async fn heal_on_read_persists_object() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();

    let cfg_a = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 1);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a, engine_a, cfg_a).await;
    tokio::spawn(async move {
        axum::serve(listener_a, app_a.into_make_service())
            .await
            .unwrap();
    });

    let peers_b = format!("node-a=http://{}", addr_a);
    let base_b = cluster_test_config("node-b", &peers_b, "member", 1);
    let cfg_b = Arc::new(NosConfig {
        cluster: ClusterConfig {
            replication_read_repair: true,
            replication_heal_on_read: true,
            ..base_b.cluster.clone()
        },
        ..(*base_b).clone()
    });
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b.clone(), cfg_b.clone()).await;
    let token = make_token();

    let client = reqwest::Client::new();
    let resp = client
        .put(format!("http://{}/music/heal.bin", addr_a))
        .header("authorization", format!("Bearer {token}"))
        .body("heal-bytes")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let get = Request::builder()
        .method("GET")
        .uri("/music/heal.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app_b.oneshot(get).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    engine_b
        .head_object("music", "heal.bin", None, None)
        .await
        .expect("heal-on-read should persist object locally")
        .expect("heal-on-read should persist object locally");
}

#[tokio::test]
async fn replication_status_reports_pending() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 2);
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let app = app_with_metrics(backend, engine, cfg).await;
    let token = make_token();

    let put = Request::builder()
        .method("PUT")
        .uri("/music/status-pending.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from("pending"))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );

    let status_req = Request::builder()
        .method("GET")
        .uri("/_nos/maintenance/replication_status")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(status_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["pending"].as_u64().unwrap_or(0) >= 1);
}
