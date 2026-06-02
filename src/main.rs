use std::sync::Arc;
use std::time::Duration;

use nebular_os::{cluster, config, observability::NosMetrics, secrets, server, storage};

use anyhow::Result;
use axum::serve;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,tower_http=debug")),
        )
        .init();

    let mut cfg = config::NosConfig::from_env()?;
    secrets::validate_jwt_secret(&cfg.jwt_secret)?;
    if let Some(ref signing) = cfg.signing_secret {
        secrets::validate_signing_secret(signing)?;
    }

    tracing::info!(?cfg, "Configuration loaded (env)");

    let storage = storage::engine::StorageEngine::with_full_options(
        &cfg.meta_path,
        &cfg.data_dir,
        storage::engine::EngineOptions {
            upload_buffer_size: cfg.upload_buffer_size,
            list_scan_cap: cfg.list_scan_cap,
            multipart_part_size: cfg.multipart_part_size,
            soft_delete_ttl_secs: cfg.soft_delete_ttl_secs,
            soft_delete_drop_blob: cfg.soft_delete_drop_blob,
            multipart_upload_ttl_secs: cfg.multipart_upload_ttl_secs,
            recompress_batch_size: cfg.recompress_batch_size,
            read_pool_size: cfg.read_pool_size,
            zstd_level: cfg.zstd_level,
        },
    )
    .await?;
    tracing::info!("Storage engine initialized");

    if let Some(snap) = storage.load_cluster_config_snapshot().await? {
        match snap.into_cluster_config() {
            Ok(loaded) => {
                tracing::info!(
                    mode = loaded.mode.as_str(),
                    node_id = %loaded.node_id,
                    "Loaded persisted cluster configuration"
                );
                cfg.cluster = loaded;
            }
            Err(e) => {
                tracing::error!(error = %e, "Ignoring invalid persisted cluster config");
            }
        }
    }

    let cfg = Arc::new(cfg);

    if cfg.reconcile_on_startup {
        let report = storage.reconcile().await?;
        tracing::info!(?report, "Startup reconciliation finished");
    }

    if cfg.recompress_on_startup {
        let report = storage
            .recompress_legacy_blobs(cfg.recompress_batch_size)
            .await?;
        tracing::info!(?report, "Startup legacy blob recompression finished");
    }

    if cfg.reconcile_interval_secs > 0 {
        let engine = storage.clone();
        let interval = cfg.reconcile_interval_secs;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            loop {
                ticker.tick().await;
                match engine.reconcile().await {
                    Ok(report) => tracing::info!(?report, "Periodic reconciliation finished"),
                    Err(e) => tracing::error!(error = %e, "Periodic reconciliation failed"),
                }
            }
        });
    }

    spawn_storage_maintenance(storage.clone(), cfg.clone());

    let metrics = NosMetrics::new();
    let backend = cluster::build_backend(storage.clone(), &cfg.cluster, metrics.clone())?;
    let app = server::create_app(backend, storage, cfg.clone(), metrics).await?;

    let listener = TcpListener::bind(&cfg.bind_addr).await?;
    tracing::info!("Listening on {}", cfg.bind_addr);

    // Human: Expose peer IP to rate-limit middleware via ConnectInfo<SocketAddr>.
    // Agent: into_make_service_with_connect_info; REQUIRED for per-IP NOS_RATE_LIMIT_RPS.
    serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

fn spawn_storage_maintenance(storage: storage::StorageEngine, cfg: Arc<config::NosConfig>) {
    let purge_soft = cfg.soft_delete_ttl_secs > 0;
    let purge_multipart = cfg.multipart_upload_ttl_secs > 0;
    let recompress = cfg.recompress_interval_secs > 0;
    if !purge_soft && !purge_multipart && !recompress {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(300));
        loop {
            ticker.tick().await;
            if purge_soft {
                match storage.purge_soft_deleted().await {
                    Ok(n) if n > 0 => tracing::info!(purged = n, "Soft-delete purge completed"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "Soft-delete purge failed"),
                }
            }
            if purge_multipart {
                match storage.purge_stale_multipart_uploads().await {
                    Ok(n) if n > 0 => {
                        tracing::info!(purged = n, "Stale multipart upload purge completed")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "Stale multipart upload purge failed"),
                }
            }
            if recompress {
                match storage
                    .recompress_legacy_blobs(cfg.recompress_batch_size)
                    .await
                {
                    Ok(report) if report.recompressed > 0 => {
                        tracing::info!(?report, "Periodic legacy blob recompression finished")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "Legacy blob recompression failed"),
                }
            }
        }
    });
}
