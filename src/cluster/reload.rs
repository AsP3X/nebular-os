//! Human: Hot-reload cluster topology after PUT /_cluster/config without process restart.
//! Agent: Persists snapshot; rebuilds StorageBackend; stops the replaced backend's background tasks.

use std::sync::{Arc, RwLock};

use crate::observability::NosMetrics;
use crate::storage::engine::StorageEngine;

use super::backend::{build_backend, StorageBackend};
use super::config::ClusterConfig;
use super::store::ClusterConfigSnapshot;

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
    // Human: Build (which validates) before saving: a configuration saved but not buildable made every later
    // start fail.
    let new_backend = build_backend(engine.clone(), &new_cluster, metrics.clone())?;
    if let Err(e) = engine.save_cluster_config_snapshot(&snap).await {
        new_backend.shutdown();
        return Err(e.into());
    }
    *cluster.write().map_err(|e| anyhow::anyhow!("cluster lock poisoned: {e}"))? = new_cluster;
    let replaced = std::mem::replace(
        &mut *backend.write().map_err(|e| anyhow::anyhow!("backend lock poisoned: {e}"))?,
        new_backend,
    );
    // Human: Stop the replaced backend's worker. This used to bump a process-wide generation after the new
    // worker had started, which could stop the new worker too and leave the node replicating nothing.
    replaced.shutdown();
    tracing::info!("Cluster configuration applied (hot reload)");
    Ok(())
}
