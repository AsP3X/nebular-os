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
        wire_checksum: None,
        created_at: 1,
        version: 0,
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
    let log_a = backend_a.replication_log().unwrap().clone();
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
    let log_b = backend_b.replication_log().unwrap().clone();
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
    // Human: The healed copy carries the version node-a has, so an older replicated change can't replace it.
    let version_a = log_a.key_version("music", "heal.bin").await.unwrap().expect("node-a versioned its write");
    assert_eq!(log_b.key_version("music", "heal.bin").await.unwrap(), Some(version_a));
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

#[tokio::test]
async fn replication_wire_checksum_mismatch_rejected() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 2);
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let log = match &backend {
        nebular_os::cluster::StorageBackend::Replicated(r) => r.replication_log(),
        _ => panic!("expected replicated"),
    };

    engine
        .put_object(
            "music",
            "wire.bin",
            None,
            None,
            std::io::Cursor::new(b"bytes"),
        )
        .await
        .unwrap();

    let event = ReplicationEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        origin_node: "node-b".into(),
        op: ReplicationOp::Put,
        bucket: "music".into(),
        key: "wire-remote.bin".into(),
        etag: Some("dummy".into()),
        size: Some(5),
        payload_path: None,
        storage_class: "default".into(),
        replication_group: "default".into(),
        content_type: None,
        custom_meta: None,
        wire_checksum: Some("0000000000000000".into()),
        created_at: 1,
        version: 0,
    };

    let err = apply_replication_event_bytes(&engine, log, &event, Some(b"bytes".to_vec()))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        nebular_os::storage::error::StorageError::Internal(_)
    ));
}

#[tokio::test]
async fn dead_letter_replay_resets_event() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 2);
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let log = match &backend {
        nebular_os::cluster::StorageBackend::Replicated(r) => r.replication_log(),
        _ => panic!("expected replicated"),
    };

    engine
        .put_object(
            "music",
            "dl-replay.bin",
            None,
            None,
            std::io::Cursor::new(b"dl"),
        )
        .await
        .unwrap();

    let event = log
        .enqueue_put(
            &engine
                .head_object("music", "dl-replay.bin", None, None)
                .await
                .unwrap()
                .expect("meta"),
            "default",
            "default",
        )
        .await
        .unwrap();

    for _ in 0..20 {
        log.mark_failed(&event.event_id, 20).await.unwrap();
    }

    let app = app_with_metrics(backend, engine, cfg).await;
    let token = make_token();
    let replay = Request::builder()
        .method("POST")
        .uri(format!(
            "/_nos/maintenance/replication_replay?event_id={}",
            event.event_id
        ))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(replay).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["replayed"], true);
}

#[tokio::test]
async fn read_repair_and_peer_route_serve_ranges() {
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
        .put(format!("http://{}/music/ranged.bin", addr_a))
        .header("authorization", format!("Bearer {token}"))
        .body("repair-bytes")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Human: Peer route over real HTTP — a wrong Content-Length would surface as a body error here.
    let peer = client
        .get(format!("http://{}/_cluster/objects/music/ranged.bin", addr_a))
        .header("authorization", format!("Bearer {CLUSTER_TOKEN}"))
        .header("range", "bytes=7-")
        .send()
        .await
        .unwrap();
    assert_eq!(peer.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(peer.headers()["content-range"], "bytes 7-11/12");
    assert_eq!(peer.bytes().await.unwrap().as_ref(), b"bytes");

    let get = |range: &'static str| {
        Request::builder()
            .method("GET")
            .uri("/music/ranged.bin")
            .header("authorization", format!("Bearer {token}"))
            .header("range", range)
            .body(Body::empty())
            .unwrap()
    };
    let response = app_b.clone().oneshot(get("bytes=0-5")).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["content-length"], "6");
    assert_eq!(response.headers()["content-range"], "bytes 0-5/12");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"repair");

    let response = app_b.clone().oneshot(get("bytes=100-")).await.unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(response.headers()["content-range"], "bytes */12");

    // Human: HEAD answers like GET when read repair can serve the object from a peer.
    let head = Request::builder()
        .method("HEAD")
        .uri("/music/ranged.bin")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app_b.oneshot(head).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-length"], "12");
}

#[tokio::test]
async fn bootstrap_token_only_configures_an_unconfigured_node() {
    let tmp = TempDir::new().unwrap();
    let cfg = bootstrap_standalone_config();
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let app = app_with_metrics(backend, engine, cfg).await;
    let call = |method: &str, uri: &str, token: &str, body: Body| {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(body)
            .unwrap()
    };

    // Human: Never object data, even before the node is configured.
    let resp = app.clone().oneshot(call("GET", "/_cluster/objects/music/x.bin", BOOTSTRAP_TOKEN, Body::empty())).await.unwrap();
    assert_ne!(resp.status(), StatusCode::OK);

    let body = serde_json::json!({
        "mode": "replicated",
        "node_id": "node-a",
        "cluster_token": CLUSTER_TOKEN,
        "peers": [{ "id": "node-a", "url": "http://127.0.0.1:9000" }],
        "storage_classes": ["default"],
        "replication_factor": 1
    });
    let resp = app.clone().oneshot(call("PUT", "/_cluster/config", BOOTSTRAP_TOKEN, Body::from(body.to_string()))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Human: Once a cluster token exists the bootstrap token is retired.
    let resp = app.clone().oneshot(call("GET", "/_cluster/config", BOOTSTRAP_TOKEN, Body::empty())).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let resp = app.oneshot(call("GET", "/_cluster/config", CLUSTER_TOKEN, Body::empty())).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn cluster_replicates_compressible_objects_as_content() {
    // Human: Compressible objects are stored as NOSI containers; peers must receive the content, not the container.
    let payload = "replicated compressible line of text\n".repeat(6_000);
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
        .uri("/music/doc.txt")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(payload.clone()))
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
            .uri("/music/doc.txt")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app_b_client.clone().oneshot(get).await.unwrap();
        if resp.status() == StatusCode::OK {
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), payload.as_bytes());
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("object not replicated to peer within timeout");
}

#[tokio::test]
async fn cluster_replicates_objects_larger_than_two_mib() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let cfg_b = cluster_test_config("node-b", "node-a=http://127.0.0.1:1", "member", 2);
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b, cfg_b.clone()).await;
    let app_b_client = app_b.clone();
    tokio::spawn(async move {
        axum::serve(listener_b, app_b.into_make_service()).await.unwrap();
    });
    let peers = format!("node-b=http://{}", addr_b);
    let cfg_a = cluster_test_config("node-a", &peers, "member", 2);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a.clone(), engine_a.clone(), cfg_a.clone()).await;
    let token = make_token();

    // Human: Above axum's 2 MiB multipart default — one compressible (shipped decoded), one raw.
    let text = "replicated compressible line of text\n".repeat(90_000).into_bytes();
    let mut raw = vec![0u8; 3 * 1024 * 1024];
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for b in raw.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    for (key, payload) in [("big.txt", &text), ("big.bin", &raw)] {
        let put = Request::builder()
            .method("PUT")
            .uri(format!("/music/{key}"))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from(payload.clone()))
            .unwrap();
        assert_eq!(app_a.clone().oneshot(put).await.unwrap().status(), StatusCode::CREATED);
    }

    let replicated = match &backend_a {
        nebular_os::cluster::StorageBackend::Replicated(r) => r.clone(),
        _ => panic!("expected replicated"),
    };
    let peers = nebular_os::cluster::peer::PeerRegistry::from_peers_raw(&peers).unwrap();
    let client = reqwest::Client::new();
    let metrics = NosMetrics::new();
    drain_once(&client, replicated.replication_log(), &peers, &cfg_a.cluster, CLUSTER_TOKEN, &metrics, None)
        .await
        .unwrap();
    for (key, payload) in [("big.txt", &text), ("big.bin", &raw)] {
        let get = Request::builder()
            .method("GET")
            .uri(format!("/music/{key}"))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app_b_client.clone().oneshot(get).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{key} not replicated");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert!(body.as_ref() == payload.as_slice(), "{key} content differs");
    }
}

#[tokio::test]
async fn replication_backfill_pages_through_every_object() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 2);
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    // Human: Written straight to the engine, so no replication events exist for them yet.
    for key in ["a.bin", "b.bin", "c.bin"] {
        engine
            .put_object("music", key, None, None, std::io::Cursor::new(b"backfill".to_vec()))
            .await
            .unwrap();
    }

    // Human: Every call used to take the same oldest `limit` rows, so c.bin was never enqueued.
    let mut enqueued = 0;
    let mut calls = 0;
    let mut start_after: Option<String> = None;
    loop {
        let report = backend
            .backfill_replication(2, start_after.as_deref())
            .await
            .unwrap();
        enqueued += report.enqueued;
        calls += 1;
        if !report.is_truncated {
            break;
        }
        start_after = report.next_start_after;
    }
    assert_eq!((enqueued, calls), (3, 2));
    let status = backend.replication_status().await.unwrap();
    assert!(status.pending >= 3, "{status:?}");
}

#[tokio::test]
async fn scrub_heal_takes_only_the_version_local_metadata_names() {
    use nebular_os::storage::scrub::{ScrubMode, ScrubOptions};

    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let cfg_a = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 1);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a, engine_a.clone(), cfg_a).await;
    tokio::spawn(async move {
        axum::serve(listener_a, app_a.into_make_service()).await.unwrap();
    });

    let cfg_b = cluster_test_config("node-b", &format!("node-a=http://{addr_a}"), "member", 2);
    let (backend_b, engine_b, data_dir_b) = engine_and_backend(&cfg_b, &tmp_b).await;
    let put = |engine: StorageEngine, key: &'static str, body: &'static [u8]| async move {
        engine
            .put_object("music", key, None, None, std::io::Cursor::new(body.to_vec()))
            .await
            .unwrap()
    };
    // Human: Written straight to each engine, so nothing replicates between the nodes.
    put(engine_a.clone(), "same.bin", b"identical on both nodes").await;
    put(engine_b.clone(), "same.bin", b"identical on both nodes").await;
    put(engine_a.clone(), "differs.bin", b"the peer's other version").await;
    let local = put(engine_b.clone(), "differs.bin", b"this node's version!!!!").await;
    for key in ["same.bin", "differs.bin"] {
        let path = nebular_os::storage::blob_path(&data_dir_b, "music", key);
        let len = std::fs::metadata(&path).unwrap().len() as usize;
        std::fs::write(&path, vec![b'#'; len]).unwrap();
    }

    let report = backend_b
        .scrub_objects(ScrubOptions {
            limit: 10,
            mode: ScrubMode::Deep,
            ..ScrubOptions::default()
        })
        .await
        .unwrap();
    assert_eq!((report.recovered, report.corrupted), (1, 1), "{report:?}");
    let healed = engine_b.head_object("music", "same.bin", None, None).await.unwrap().unwrap();
    assert!(matches!(
        engine_b.get_object("music", "same.bin", None, None, None).await.unwrap(),
        nebular_os::storage::GetObjectOutcome::Content { .. }
    ));
    assert_eq!(healed.size, b"identical on both nodes".len() as i64);
    // Human: Heal used to take any peer copy, replacing this node's version with the peer's other one.
    let kept = engine_b.head_object("music", "differs.bin", None, None).await.unwrap().unwrap();
    assert_eq!(kept.etag, local.etag, "the damaged object was replaced by another version");
}

#[tokio::test]
async fn backfill_replicates_objects_in_the_legacy_nested_layout() {
    use nebular_os::storage::{blob_path, blob_path_legacy};

    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let cfg_a = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 1);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a, engine_a.clone(), cfg_a).await;
    tokio::spawn(async move {
        axum::serve(listener_a, app_a.into_make_service()).await.unwrap();
    });

    let cfg_b = cluster_test_config("node-b", &format!("node-a=http://{addr_a}"), "member", 2);
    let (backend_b, engine_b, data_dir_b) = engine_and_backend(&cfg_b, &tmp_b).await;
    let body = b"written before the flat layout existed".repeat(50);
    engine_b
        .put_object("music", "old/nested.bin", None, None, std::io::Cursor::new(body.clone()))
        .await
        .unwrap();
    let legacy = blob_path_legacy(&data_dir_b, "music", "old/nested.bin");
    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    std::fs::rename(blob_path(&data_dir_b, "music", "old/nested.bin"), &legacy).unwrap();

    let report = backend_b.backfill_replication(10, None).await.unwrap();
    assert_eq!(report.enqueued, 1, "{report:?}");
    // Human: The event recorded the flat path, where this blob isn't, so it used to dead-letter.
    for _ in 0..50 {
        if let Ok(nebular_os::storage::GetObjectOutcome::Content { mut stream, .. }) =
            engine_a.get_object("music", "old/nested.bin", None, None, None).await
        {
            let mut got = Vec::new();
            while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
                got.extend_from_slice(&chunk.unwrap());
            }
            assert_eq!(got, body);
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("backfilled legacy-layout object never reached the peer");
}

fn versioned_event(op: ReplicationOp, key: &str, version: i64, origin: &str) -> ReplicationEvent {
    ReplicationEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        origin_node: origin.into(),
        op,
        bucket: "music".into(),
        key: key.into(),
        etag: None,
        size: None,
        payload_path: None,
        storage_class: "default".into(),
        replication_group: "default".into(),
        content_type: None,
        custom_meta: None,
        wire_checksum: None,
        created_at: version / 1_000_000,
        version,
    }
}

/// The object's bytes, or None when it doesn't exist.
async fn stored(engine: &StorageEngine, key: &str) -> Option<Vec<u8>> {
    match engine.get_object("music", key, None, None, None).await {
        Ok(nebular_os::storage::GetObjectOutcome::Content { mut stream, .. }) => {
            let mut body = Vec::new();
            while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
                body.extend_from_slice(&chunk.unwrap());
            }
            Some(body)
        }
        Err(nebular_os::storage::error::StorageError::NotFound) => None,
        other => panic!("unexpected read result for {key}: {:?}", other.err()),
    }
}

#[tokio::test]
async fn replicated_changes_apply_only_when_newer() {
    let tmp = TempDir::new().unwrap();
    let cfg = cluster_test_config("node-b", "node-a=http://127.0.0.1:1", "member", 2);
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let log = backend.replication_log().unwrap();
    const T: i64 = 1_700_000_000_000_000;
    let put = |key: &str, version: i64, origin: &str| versioned_event(ReplicationOp::Put, key, version, origin);
    let apply = |event: ReplicationEvent, body: &'static [u8]| {
        let engine = engine.clone();
        async move {
            apply_replication_event_bytes(&engine, log, &event, Some(body.to_vec()))
                .await
                .unwrap()
        }
    };

    apply(put("k", T + 2, "node-a"), b"new").await;
    // Human: A late retry of an older write used to overwrite the newer object.
    apply(put("k", T + 1, "node-a"), b"old").await;
    assert_eq!(stored(&engine, "k").await.as_deref(), Some(&b"new"[..]));

    let delete = versioned_event(ReplicationOp::Delete, "k", T + 3, "node-a");
    apply_replication_event_bytes(&engine, log, &delete, None).await.unwrap();
    assert_eq!(stored(&engine, "k").await, None);
    // Human: ...and an older put arriving after a delete resurrected the object.
    apply(put("k", T + 2, "node-c"), b"resurrected").await;
    assert_eq!(stored(&engine, "k").await, None);

    apply(put("k", T + 4, "node-a"), b"newest").await;
    assert_eq!(stored(&engine, "k").await.as_deref(), Some(&b"newest"[..]));
    // Human: Same time on two nodes: the node id breaks the tie, the same way on every node.
    apply(put("k", T + 4, "node-z"), b"tie-z").await;
    apply(put("k", T + 4, "node-0"), b"tie-0").await;
    assert_eq!(stored(&engine, "k").await.as_deref(), Some(&b"tie-z"[..]));

    // Human: A write made here now wins over a change made elsewhere earlier, not over one made later.
    backend
        .put_object(
            "music",
            "k",
            None,
            None,
            std::io::Cursor::new(b"local".to_vec()),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    apply(put("k", T + 5, "node-a"), b"stale").await;
    assert_eq!(stored(&engine, "k").await.as_deref(), Some(&b"local"[..]));
    let later = chrono::Utc::now().timestamp_micros() + 3_600_000_000;
    apply(put("k", later, "node-a"), b"later").await;
    assert_eq!(stored(&engine, "k").await.as_deref(), Some(&b"later"[..]));
}

#[tokio::test]
async fn read_repair_never_serves_or_restores_a_key_deleted_here() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let cfg_a = cluster_test_config("node-a", "node-b=http://127.0.0.1:1", "member", 1);
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    for key in ["deleted.bin", "dir/kept a#b?.bin"] {
        engine_a
            .put_object("music", key, None, None, std::io::Cursor::new(b"on node a".to_vec()))
            .await
            .unwrap();
    }
    let app_a = app_with_metrics(backend_a, engine_a, cfg_a).await;
    tokio::spawn(async move {
        axum::serve(listener_a, app_a.into_make_service()).await.unwrap();
    });

    let base_b = cluster_test_config("node-b", &format!("node-a=http://{addr_a}"), "member", 1);
    let cfg_b = Arc::new(NosConfig {
        cluster: ClusterConfig {
            replication_read_repair: true,
            replication_heal_on_read: true,
            ..base_b.cluster.clone()
        },
        ..(*base_b).clone()
    });
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    // Human: node-b applied a delete node-a hasn't applied yet.
    let now = chrono::Utc::now().timestamp_micros();
    let delete = versioned_event(ReplicationOp::Delete, "deleted.bin", now, "node-x");
    apply_replication_event_bytes(&engine_b, backend_b.replication_log().unwrap(), &delete, None)
        .await
        .unwrap();
    let app_b = app_with_metrics(backend_b, engine_b.clone(), cfg_b).await;
    let token = make_token();
    let get = |uri: &str| {
        Request::builder()
            .uri(uri)
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };

    let resp = app_b.clone().oneshot(get("/music/deleted.bin")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "served a key deleted here");
    assert_eq!(stored(&engine_b, "deleted.bin").await, None, "healed a key deleted here");

    // Human: Keys that need escaping reach the peer intact (`#` and `?` used to cut the URL short).
    let resp = app_b.oneshot(get("/music/dir/kept%20a%23b%3F.bin")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(stored(&engine_b, "dir/kept a#b?.bin").await.as_deref(), Some(&b"on node a"[..]));
}

/// Two assigned-mode nodes listing each other, with forwarding on.
async fn symmetric_assigned_pair() -> (axum::Router, StorageEngine, StorageEngine, TempDir, TempDir) {
    let tmp_hot = TempDir::new().unwrap();
    let tmp_cold = TempDir::new().unwrap();
    let listener_hot = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_cold = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peers = format!(
        "node-hot=http://{},node-cold=http://{}",
        listener_hot.local_addr().unwrap(),
        listener_cold.local_addr().unwrap()
    );
    let base_hot = assigned_forward_config("node-hot", &peers);
    let cfg_hot = Arc::new(NosConfig {
        cluster: ClusterConfig {
            storage_classes: vec!["hls-hot".into(), "default".into()],
            ..base_hot.cluster.clone()
        },
        ..(*base_hot).clone()
    });
    let cfg_cold = assigned_forward_config("node-cold", &peers);
    let (backend_hot, engine_hot, _) = engine_and_backend(&cfg_hot, &tmp_hot).await;
    let (backend_cold, engine_cold, _) = engine_and_backend(&cfg_cold, &tmp_cold).await;
    let app_hot = app_with_metrics(backend_hot, engine_hot.clone(), cfg_hot).await;
    let app_cold = app_with_metrics(backend_cold, engine_cold.clone(), cfg_cold).await;
    let served_cold = app_cold.clone();
    tokio::spawn(async move { axum::serve(listener_hot, app_hot.into_make_service()).await.unwrap() });
    tokio::spawn(async move { axum::serve(listener_cold, served_cold.into_make_service()).await.unwrap() });
    (app_cold, engine_hot, engine_cold, tmp_hot, tmp_cold)
}

#[tokio::test]
async fn assigned_deletes_reach_every_node_exactly_once() {
    let (app_cold, engine_hot, engine_cold, _tmp_hot, _tmp_cold) = symmetric_assigned_pair().await;
    let token = make_token();
    let request = |method: &str, uri: &str, if_match: Option<&str>| {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"));
        if let Some(etag) = if_match {
            req = req.header("if-match", etag);
        }
        let app = app_cold.clone();
        let req = req.body(Body::empty()).unwrap();
        async move {
            let resp = tokio::time::timeout(Duration::from_secs(20), app.oneshot(req))
                .await
                .expect("the request bounced between the nodes")
                .unwrap();
            let status = resp.status();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            (status, body)
        }
    };
    let send = |method: &str, uri: &str, if_match: Option<&str>| {
        let sent = request(method, uri, if_match);
        async move { sent.await.0 }
    };

    // Human: Symmetric peers used to forward a prefix delete back and forth until a hop failed.
    for (engine, key) in [(&engine_hot, "users/a.bin"), (&engine_cold, "users/b.bin")] {
        engine
            .put_object("music", key, None, None, std::io::Cursor::new(b"x".to_vec()))
            .await
            .unwrap();
    }
    let (status, body) = request("DELETE", "/music?prefix=users/", None).await;
    assert_eq!(status, StatusCode::OK);
    let report: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(report["failed"], serde_json::json!([]), "{report}");
    assert_eq!(report["deleted"], 2, "{report}");
    assert_eq!(stored(&engine_hot, "users/a.bin").await, None);
    assert_eq!(stored(&engine_cold, "users/b.bin").await, None);

    // Human: A video placed on node-hot by its Content-Type; its DELETE carries no Content-Type, so rule
    // resolution points at node-cold — deleting only there used to leave the object on node-hot.
    let meta = engine_hot
        .put_object("music", "users/clip.mp4", Some("video/mp4"), None, std::io::Cursor::new(b"v".to_vec()))
        .await
        .unwrap();
    assert_eq!(
        send("DELETE", "/music/users/clip.mp4", Some("\"0000000000000000\"")).await,
        StatusCode::PRECONDITION_FAILED
    );
    assert!(stored(&engine_hot, "users/clip.mp4").await.is_some());
    let etag = format!("\"{}\"", meta.etag.unwrap());
    assert_eq!(send("DELETE", "/music/users/clip.mp4", Some(&etag)).await, StatusCode::NO_CONTENT);
    assert_eq!(stored(&engine_hot, "users/clip.mp4").await, None);
}

#[tokio::test]
async fn a_config_reload_keeps_every_node_replicating() {
    let tmp_b = TempDir::new().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let cfg_b = cluster_test_config("node-b", "node-a=http://127.0.0.1:1", "member", 1);
    let (backend_b, engine_b, _) = engine_and_backend(&cfg_b, &tmp_b).await;
    let app_b = app_with_metrics(backend_b, engine_b.clone(), cfg_b).await;
    tokio::spawn(async move { axum::serve(listener_b, app_b.into_make_service()).await.unwrap() });

    // Human: node-c replicates to node-b from the start; node-a is configured at runtime.
    let tmp_c = TempDir::new().unwrap();
    let cfg_c = cluster_test_config("node-c", &format!("node-b=http://{addr_b}"), "member", 2);
    let (backend_c, engine_c, _) = engine_and_backend(&cfg_c, &tmp_c).await;
    let app_c = app_with_metrics(backend_c, engine_c, cfg_c).await;
    let tmp_a = TempDir::new().unwrap();
    let cfg_a = bootstrap_standalone_config();
    let (backend_a, engine_a, _) = engine_and_backend(&cfg_a, &tmp_a).await;
    let app_a = app_with_metrics(backend_a, engine_a, cfg_a).await;
    let config = serde_json::json!({
        "mode": "replicated",
        "node_id": "node-a",
        "cluster_token": CLUSTER_TOKEN,
        "peers": [{ "id": "node-b", "url": format!("http://{addr_b}") }],
    });
    let reload = Request::builder()
        .method("PUT")
        .uri("/_cluster/config")
        .header("authorization", format!("Bearer {BOOTSTRAP_TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(config.to_string()))
        .unwrap();
    assert_eq!(app_a.clone().oneshot(reload).await.unwrap().status(), StatusCode::OK);

    let token = make_token();
    for (app, key) in [(&app_a, "from-a.bin"), (&app_c, "from-c.bin")] {
        let put = Request::builder()
            .method("PUT")
            .uri(format!("/music/{key}"))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from(key.to_string()))
            .unwrap();
        assert_eq!(app.clone().oneshot(put).await.unwrap().status(), StatusCode::CREATED);
    }
    // Human: A reload used to stop every replication worker in the process (node-c's too), and could stop the
    // reloaded node's new worker as well.
    for _ in 0..100 {
        if stored(&engine_b, "from-a.bin").await.is_some() && stored(&engine_b, "from-c.bin").await.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "replicated: from-a {}, from-c {}",
        stored(&engine_b, "from-a.bin").await.is_some(),
        stored(&engine_b, "from-c.bin").await.is_some()
    );
}

#[tokio::test]
async fn assigned_mode_forwards_only_requests_it_can_authenticate_upstream() {
    use hmac::{Hmac, Mac};

    const SIGNING_SECRET: &str = "presign-secret-for-assigned-forwarding-tests-1234567890";
    let tmp = TempDir::new().unwrap();
    let base = assigned_forward_config("node-hot", "node-hot=http://127.0.0.1:1,node-cold=http://127.0.0.1:1");
    let cfg = Arc::new(NosConfig {
        signing_secret: Some(SIGNING_SECRET.into()),
        cluster: ClusterConfig {
            storage_classes: vec!["hls-hot".into(), "default".into()],
            ..base.cluster.clone()
        },
        ..(*base).clone()
    });
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let app = app_with_metrics(backend, engine, cfg).await;

    // Human: A DELETE carries no Content-Type, so the rules place this key on node-cold. A presigned URL has no
    // Authorization header to pass on, so it can't be forwarded: the answer is "not assigned" (it was a 500).
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(SIGNING_SECRET.as_bytes()).unwrap();
    mac.update(format!("DELETE\nmusic\nusers/clip.mp4\n{expires}").as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    let delete = Request::builder()
        .method("DELETE")
        .uri(format!("/music/users/clip.mp4?signature={signature}&expires={expires}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.oneshot(delete).await.unwrap().status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_rejected_cluster_config_is_not_saved() {
    let tmp = TempDir::new().unwrap();
    let cfg = bootstrap_standalone_config();
    let (backend, engine, _) = engine_and_backend(&cfg, &tmp).await;
    let app = app_with_metrics(backend, engine.clone(), cfg).await;
    let config = serde_json::json!({
        "mode": "replicated+assigned",
        "node_id": "node-a",
        "cluster_token": CLUSTER_TOKEN,
        "peers": [{ "id": "node-b", "url": "http://127.0.0.1:1" }],
        "assignment_rules": { "rules": [] },
    });
    let put = Request::builder()
        .method("PUT")
        .uri("/_cluster/config")
        .header("authorization", format!("Bearer {BOOTSTRAP_TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(config.to_string()))
        .unwrap();
    assert_eq!(app.oneshot(put).await.unwrap().status(), StatusCode::BAD_REQUEST);
    // Human: It used to be saved before it was found invalid, and then every start failed on it.
    assert!(engine.load_cluster_config_snapshot().await.unwrap().is_none());
}

#[tokio::test]
async fn assigned_forwarding_keeps_keys_and_answers_intact() {
    let (app_cold, engine_hot, _engine_cold, _tmp_hot, _tmp_cold) = symmetric_assigned_pair().await;
    let token = make_token();
    let send = |method: &str, uri: &str, headers: &[(&str, &str)], body: &'static str| {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"));
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        let app = app_cold.clone();
        let req = req.body(Body::from(body)).unwrap();
        async move { app.oneshot(req).await.unwrap().status() }
    };

    // Human: A conditional PUT forwarded to the owning node answers with that node's 412 (it was a 500).
    let video = [("content-type", "video/mp4")];
    assert_eq!(send("PUT", "/music/users/clip.mp4", &video, "v1").await, StatusCode::CREATED);
    let create_only = [("content-type", "video/mp4"), ("if-none-match", "*")];
    assert_eq!(
        send("PUT", "/music/users/clip.mp4", &create_only, "v2").await,
        StatusCode::PRECONDITION_FAILED
    );

    // Human: URL parsing turns `users/./b.bin` into `users/b.bin`, so fanning this delete out deleted a
    // different object on the other nodes. It is refused before anything is deleted.
    engine_hot
        .put_object("music", "users/b.bin", None, None, std::io::Cursor::new(b"keep".to_vec()))
        .await
        .unwrap();
    assert_eq!(send("DELETE", "/music/users/./b.bin", &[], "").await, StatusCode::BAD_REQUEST);
    assert_eq!(send("DELETE", "/music/x/../users/b.bin", &[], "").await, StatusCode::BAD_REQUEST);
    assert_eq!(stored(&engine_hot, "users/b.bin").await.as_deref(), Some(&b"keep"[..]));
}
