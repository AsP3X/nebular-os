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

    let cfg = config::NosConfig::from_env()?;
    secrets::validate_jwt_secret(&cfg.jwt_secret)?;
    if let Some(ref signing) = cfg.signing_secret {
        secrets::validate_signing_secret(signing)?;
    }
    tracing::info!(?cfg, "Configuration loaded");

    let storage = storage::engine::StorageEngine::new(&cfg.meta_path, &cfg.data_dir).await?;
    tracing::info!("Storage engine initialized");

    let app = server::create_app(
        storage,
        cfg.jwt_secret,
        cfg.signing_secret,
        cfg.max_body_size,
        cfg.allow_public_read,
    )
    .await?;

    let listener = TcpListener::bind(&cfg.bind_addr).await?;
    tracing::info!("Listening on {}", cfg.bind_addr);

    serve(listener, app).await?;
    Ok(())
}
