//! Human: Persisted cluster topology for runtime apply (Ownly setup / admin pushes config).
//! Agent: SQLite table cluster_runtime_config; load at startup; save on PUT /_cluster/config.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::storage::engine::StorageEngine;
use crate::storage::error::{internal, StorageError};

use super::config::{ClusterConfig, ClusterMode};
use super::peer::PeerRegistry;

/// Human: JSON blob stored in SQLite — mirrors cluster env without bootstrap secrets.
/// Agent: Serialized on PUT; merged into ClusterConfig via `into_cluster_config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfigSnapshot {
    pub mode: String,
    pub node_id: String,
    #[serde(default)]
    pub instance_id: Option<String>,
    #[serde(default)]
    pub region_label: Option<String>,
    pub cluster_token: String,
    pub peers: Vec<ClusterPeerSnapshot>,
    #[serde(default)]
    pub storage_classes: Vec<String>,
    #[serde(default = "default_replication_group")]
    pub replication_group: String,
    #[serde(default = "default_replication_role")]
    pub replication_role: String,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    #[serde(default)]
    pub replication_read_repair: bool,
    #[serde(default = "default_true")]
    pub replication_async: bool,
    #[serde(default = "default_storage_class")]
    pub default_storage_class: String,
    #[serde(default)]
    pub assignment_rules: Option<serde_json::Value>,
    #[serde(default)]
    pub assignment_forward: bool,
    #[serde(default)]
    pub replication_heal_on_read: bool,
    #[serde(default)]
    pub replication_prefixes: Vec<String>,
    #[serde(default)]
    pub replication_exclude_prefixes: Vec<String>,
    #[serde(default = "default_replication_max_attempts")]
    pub replication_max_attempts: u32,
    #[serde(default = "default_replication_peer_concurrency")]
    pub replication_peer_concurrency: u32,
}

fn default_replication_max_attempts() -> u32 {
    20
}
fn default_replication_peer_concurrency() -> u32 {
    4
}

fn default_replication_group() -> String {
    "default".into()
}
fn default_replication_role() -> String {
    "member".into()
}
fn default_replication_factor() -> u32 {
    1
}
fn default_storage_class() -> String {
    "default".into()
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterPeerSnapshot {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub storage_classes: Vec<String>,
    #[serde(default)]
    pub replication_group: Option<String>,
}

impl ClusterConfigSnapshot {
    pub fn into_cluster_config(self) -> Result<ClusterConfig> {
        let mode = ClusterMode::parse(&self.mode)?;
        if mode == ClusterMode::Standalone {
            anyhow::bail!("persisted cluster mode cannot be standalone");
        }
        if self.cluster_token.trim().is_empty() {
            anyhow::bail!("cluster_token is required");
        }
        if self.peers.is_empty() {
            anyhow::bail!("peers must list at least one node");
        }
        if !self.replication_async {
            anyhow::bail!("replication_async=false is not supported in v1");
        }

        let assignment_rules_raw = match (&mode, &self.assignment_rules) {
            (ClusterMode::Assigned | ClusterMode::ReplicatedAssigned, None) => {
                anyhow::bail!("assignment_rules is required for assigned cluster modes");
            }
            (_, None) => None,
            (_, Some(v)) => Some(if v.is_string() {
                v.as_str().unwrap().to_string()
            } else {
                serde_json::to_string(v).context("assignment_rules must be JSON")?
            }),
        };

        let storage_classes = if self.storage_classes.is_empty() {
            vec!["default".into()]
        } else {
            self.storage_classes
        };

        let node_id = self.node_id.trim().to_string();
        let instance_id = self
            .instance_id
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| node_id.clone());

        Ok(ClusterConfig {
            mode,
            node_id,
            instance_id,
            region_label: self.region_label.filter(|s| !s.trim().is_empty()),
            cluster_token: Some(self.cluster_token),
            peers_raw: Some(PeerRegistry::peers_to_raw(&self.peers)?),
            storage_classes,
            replication_group: self.replication_group,
            replication_role: self.replication_role,
            replication_factor: self.replication_factor,
            replication_pending_events: 0,
            replication_read_repair: self.replication_read_repair,
            replication_heal_on_read: self.replication_heal_on_read,
            replication_async: self.replication_async,
            replication_prefixes: self.replication_prefixes.clone(),
            replication_exclude_prefixes: self.replication_exclude_prefixes.clone(),
            replication_max_attempts: self.replication_max_attempts,
            replication_peer_concurrency: self.replication_peer_concurrency,
            default_storage_class: self.default_storage_class,
            assignment_rules_raw,
            assignment_forward: self.assignment_forward,
        })
    }

    pub fn from_config(cfg: &ClusterConfig) -> Result<Self> {
        if cfg.is_standalone() {
            anyhow::bail!("cannot snapshot standalone cluster config");
        }
        let token = cfg
            .cluster_token
            .clone()
            .filter(|t| !t.is_empty())
            .context("cluster_token is required")?;
        let peers_raw = cfg
            .peers_raw
            .as_deref()
            .context("peers_raw is required")?;
        let peers = PeerRegistry::from_peers_raw(peers_raw)?
            .peers
            .into_iter()
            .map(|(id, entry)| ClusterPeerSnapshot {
                id,
                url: entry.url,
                storage_classes: entry.storage_classes,
                replication_group: entry.replication_group,
            })
            .collect();

        let assignment_rules = cfg.assignment_rules_raw.as_ref().and_then(|raw| {
            if raw.trim().starts_with('{') {
                serde_json::from_str(raw).ok()
            } else {
                None
            }
        });

        Ok(Self {
            mode: cfg.mode.as_str().to_string(),
            node_id: cfg.node_id.clone(),
            instance_id: Some(cfg.instance_id.clone()),
            region_label: cfg.region_label.clone(),
            cluster_token: token,
            peers,
            storage_classes: cfg.storage_classes.clone(),
            replication_group: cfg.replication_group.clone(),
            replication_role: cfg.replication_role.clone(),
            replication_factor: cfg.replication_factor,
            replication_read_repair: cfg.replication_read_repair,
            replication_heal_on_read: cfg.replication_heal_on_read,
            replication_async: cfg.replication_async,
            replication_prefixes: cfg.replication_prefixes.clone(),
            replication_exclude_prefixes: cfg.replication_exclude_prefixes.clone(),
            replication_max_attempts: cfg.replication_max_attempts,
            replication_peer_concurrency: cfg.replication_peer_concurrency,
            default_storage_class: cfg.default_storage_class.clone(),
            assignment_rules,
            assignment_forward: cfg.assignment_forward,
        })
    }
}

impl StorageEngine {
    pub async fn load_cluster_config_snapshot(&self) -> Result<Option<ClusterConfigSnapshot>, StorageError> {
        let Some(json) = self.object_meta().load_cluster_config_json().await? else {
            return Ok(None);
        };
        let snap: ClusterConfigSnapshot =
            serde_json::from_str(&json).context("parse persisted cluster config").map_err(internal)?;
        Ok(Some(snap))
    }

    pub async fn save_cluster_config_snapshot(
        &self,
        snap: &ClusterConfigSnapshot,
    ) -> Result<(), StorageError> {
        let json = serde_json::to_string(snap)
            .context("serialize cluster config")
            .map_err(internal)?;
        self.object_meta().save_cluster_config_json(&json).await
    }
}
