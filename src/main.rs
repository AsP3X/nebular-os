use std::sync::Arc;

use nebular_os::{background_jobs, cluster, config, observability::NosMetrics, secrets, server, storage};

use anyhow::Result;
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
    warn_about_access_key_settings(&cfg);

    let engine_opts = storage::engine::EngineOptions {
        upload_buffer_size: cfg.upload_buffer_size,
        list_scan_cap: cfg.list_scan_cap,
        multipart_part_size: cfg.multipart_part_size,
        soft_delete_ttl_secs: cfg.soft_delete_ttl_secs,
        soft_delete_drop_blob: cfg.soft_delete_drop_blob,
        multipart_upload_ttl_secs: cfg.multipart_upload_ttl_secs,
        recompress_batch_size: cfg.recompress_batch_size,
        read_pool_size: cfg.read_pool_size,
        zstd_level: cfg.zstd_level,
        zstd_level_upload: cfg.zstd_level_upload,
        zstd_dict_enabled: cfg.zstd_dict_enabled,
        zstd_dict_max_bytes: cfg.zstd_dict_max_bytes,
        zstd_dict_train_batch: cfg.zstd_dict_train_batch,
        dedup_enabled: cfg.dedup_enabled,
        dedup_block_size: cfg.dedup_block_size,
        dedup_min_size: cfg.dedup_min_size,
        metadata_backend: cfg.metadata_backend,
        metadata_mode: cfg.metadata_mode,
        metadata_database_url: cfg.metadata_database_url.clone(),
        max_logical_bytes: cfg.max_logical_bytes,
        bulk_delete_concurrency: cfg.bulk_delete_concurrency,
        bulk_delete_batch_limit: cfg.bulk_delete_batch_limit,
        compress_min_size: cfg.compress_min_size,
        compress_block_size: cfg.compress_block_size,
        compress_exclude_extensions: cfg.compress_exclude_extensions.clone(),
        block_cache_entries: cfg.block_cache_entries,
        block_cache_max_bytes: cfg.block_cache_max_bytes,
        verify_batch_size: cfg.verify_batch_size,
        scrub_sample_denom: cfg.scrub_sample_denom,
        scrub_mode_light: cfg.scrub_mode_light,
        verify_on_read: cfg.verify_on_read,
        read_buffer_size: cfg.read_buffer_size,
        fsync_writes: cfg.fsync_writes,
    };

    let storage = storage::engine::StorageEngine::with_full_options(
        &cfg.meta_path,
        &cfg.data_dir,
        engine_opts,
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
                if !cfg.cluster.is_standalone() {
                    tracing::warn!(
                        env_mode = cfg.cluster.mode.as_str(),
                        "the cluster configuration saved through PUT /_cluster/config replaces the NOS_CLUSTER_* \
                         environment settings; change it through that API"
                    );
                }
                cfg.cluster = loaded;
            }
            Err(e) => {
                tracing::error!(error = %e, "Ignoring invalid persisted cluster config");
            }
        }
    }

    if !cfg.cluster.is_standalone() {
        tracing::warn!(
            mode = cfg.cluster.mode.as_str(),
            "cluster modes are experimental: replication is asynchronous, and nodes writing the same key \
             converge on the last write by wall-clock time (keep node clocks synchronized)"
        );
    }
    if cfg.cluster.mode_includes_replication() && cfg.cluster.replication_factor <= 1 {
        tracing::warn!(
            "replication factor 1 keeps no copies on peers: writes here are not replicated; set \
             NOS_REPLICATION_FACTOR (or replication_factor) to 2 or more"
        );
    }

    let cfg = Arc::new(cfg);

    if cfg.reconcile_on_startup {
        let report = storage.reconcile().await?;
        tracing::info!(?report, "Startup reconciliation finished");
    }

    let metrics = NosMetrics::new();
    let backend = cluster::build_backend(storage.clone(), &cfg.cluster, metrics.clone())?;
    background_jobs::spawn_background_jobs(storage.clone(), backend.clone(), cfg.clone());

    let app = server::create_app(backend, storage, cfg.clone(), metrics).await?;

    let listener = TcpListener::bind(&cfg.bind_addr).await?;
    tracing::info!("Listening on {}", cfg.bind_addr);

    // Human: Serves with connection timeouts and limits, exposing the peer IP (ConnectInfo) to the rate limiter,
    // until SIGTERM or Ctrl-C; then requests in progress get NOS_SHUTDOWN_GRACE_SECS to finish.
    server::serve_until(listener, app, server::ServeOptions::from_config(&cfg), shutdown_signal()).await?;
    tracing::info!("Server stopped");
    Ok(())
}

/// Human: Resolves on Ctrl-C (SIGINT) or SIGTERM — what `docker stop` and orchestrators send. As PID 1 in a
/// container the server ignored SIGTERM, so every stop waited out the kill timeout and cut requests off.
/// Agent: SIGTERM (unix only) | Ctrl-C; a listener that can't be installed waits forever, leaving the other.
async fn shutdown_signal() {
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(e) = result {
                tracing::warn!(error = %e, "cannot listen for Ctrl-C");
                std::future::pending::<()>().await;
            }
        }
        () = terminate => {}
    }
    tracing::info!("Shutdown requested");
}

/// Human: Access-key settings that start fine but leave a gap: the replayable legacy scheme, half a key pair,
/// or a secret short enough to guess.
fn warn_about_access_key_settings(cfg: &config::NosConfig) {
    match (&cfg.s3_access_key, &cfg.s3_secret_key) {
        (Some(_), None) | (None, Some(_)) => tracing::warn!(
            "only one of NOS_S3_ACCESS_KEY / NOS_S3_SECRET_KEY is set; access-key authentication is disabled"
        ),
        (Some(_), Some(secret)) if secret.len() < 16 => {
            tracing::warn!("NOS_S3_SECRET_KEY is shorter than 16 characters; use a long random secret")
        }
        _ => {}
    }
    if cfg.legacy_access_key_auth {
        tracing::warn!(
            "NOS_LEGACY_ACCESS_KEY_AUTH is on: `NOS` signatures cover only method and bucket and never expire; \
             move clients to SigV4 and turn it off"
        );
    }
}
