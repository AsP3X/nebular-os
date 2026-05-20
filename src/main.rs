use std::sync::Arc;
use std::time::Duration;

use nebular_os::{config, secrets, server, storage};

use anyhow::Result;
use axum::serve;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,tower_http=debug")),
        )
        .init();

    let cfg = Arc::new(config::NosConfig::from_env()?);
    secrets::validate_jwt_secret(&cfg.jwt_secret)?;
    if let Some(ref signing) = cfg.signing_secret {
        secrets::validate_signing_secret(signing)?;
    }
    tracing::info!(?cfg, "Configuration loaded");

    let storage = storage::engine::StorageEngine::with_full_options(
        &cfg.meta_path,
        &cfg.data_dir,
        storage::engine::EngineOptions {
            upload_buffer_size: cfg.upload_buffer_size,
            list_scan_cap: cfg.list_scan_cap,
            multipart_part_size: cfg.multipart_part_size,
            soft_delete_ttl_secs: cfg.soft_delete_ttl_secs,
            read_pool_size: cfg.read_pool_size,
        },
    )
    .await?;
    tracing::info!("Storage engine initialized");

    if cfg.reconcile_on_startup {
        let report = storage.reconcile().await?;
        tracing::info!(?report, "Startup reconciliation finished");
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

    if cfg.soft_delete_ttl_secs > 0 {
        let engine = storage.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(300));
            loop {
                ticker.tick().await;
                match engine.purge_soft_deleted().await {
                    Ok(n) if n > 0 => tracing::info!(purged = n, "Soft-delete purge completed"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "Soft-delete purge failed"),
                }
            }
        });
    }

    let app = server::create_app(storage, cfg.clone()).await?;

    let listener = TcpListener::bind(&cfg.bind_addr).await?;
    tracing::info!("Listening on {}", cfg.bind_addr);

    serve(listener, app).await?;
    Ok(())
}
