//! Human: Hot-reload cluster topology after PUT /_cluster/config without process restart.
//! Agent: Persists snapshot; rebuilds StorageBackend; bumps replication worker generation.

use std::sync::{Arc, RwLock};

use crate::observability::NosMetrics;
use crate::storage::engine::StorageEngine;

use super::backend::{build_backend, StorageBackend};
use super::config::ClusterConfig;
use super::store::ClusterConfigSnapshot;
use super::replicated::worker::bump_worker_epoch;

/// Human: Apply admin-provided cluster config and swap the live storage facade.
/// Agent: WRITES cluster_runtime_config; UPDATES cluster + backend RwLocks; STOPS old replication workers.
pub async fn apply_cluster_snapshot(
    engine: &StorageEngine,
    metrics: &Arc<NosMetrics>,
    cluster: &Arc<RwLock<ClusterConfig>>,
    backend: &Arc<RwLock<StorageBackend>>,
    snap: ClusterConfigSnapshot,
) -> anyhow::Result<()> {
    let new_cluster = snap.clone().into_cluster_config()?;
    engine.save_cluster_config_snapshot(&snap).await?;
    let new_backend = build_backend(engine.clone(), &new_cluster, metrics.clone())?;
    bump_worker_epoch();
    *cluster.write().map_err(|e| anyhow::anyhow!("cluster lock poisoned: {e}"))? = new_cluster;
    *backend.write().map_err(|e| anyhow::anyhow!("backend lock poisoned: {e}"))? = new_backend;
    tracing::info!("Cluster configuration applied (hot reload)");
    Ok(())
}
