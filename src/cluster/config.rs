use std::env;

use anyhow::{bail, Context, Result};

use super::assignment::AssignmentRules;
use super::peer::PeerRegistry;

/// Human: Deployment topology for Nebular — standalone is the default and ignores cluster env.
/// Agent: ClusterMode parsed from NOS_CLUSTER_MODE; Standalone => no /_cluster routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMode {
    Standalone,
    Replicated,
    Assigned,
    ReplicatedAssigned,
}

impl ClusterMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Replicated => "replicated",
            Self::Assigned => "assigned",
            Self::ReplicatedAssigned => "replicated+assigned",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "standalone" => Ok(Self::Standalone),
            "replicated" => Ok(Self::Replicated),
            "assigned" => Ok(Self::Assigned),
            "replicated+assigned" | "replicated_assigned" => Ok(Self::ReplicatedAssigned),
            other => bail!("unsupported NOS_CLUSTER_MODE: {other}"),
        }
    }
}

/// Human: Cluster-related settings; when mode is standalone, peer/token env vars are ignored.
/// Agent: ClusterConfig::from_env; is_standalone gates /_cluster mount in server.rs.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub mode: ClusterMode,
    pub node_id: String,
    pub instance_id: String,
    pub region_label: Option<String>,
    pub cluster_token: Option<String>,
    pub peers_raw: Option<String>,
    pub storage_classes: Vec<String>,
    pub replication_group: String,
    pub replication_role: String,
    pub replication_factor: u32,
    pub replication_pending_events: u64,
    pub replication_read_repair: bool,
    pub replication_heal_on_read: bool,
    pub replication_async: bool,
    pub replication_prefixes: Vec<String>,
    pub replication_exclude_prefixes: Vec<String>,
    pub replication_max_attempts: u32,
    pub replication_peer_concurrency: u32,
    pub default_storage_class: String,
    pub assignment_rules_raw: Option<String>,
    pub assignment_forward: bool,
}

impl ClusterConfig {
    /// Human: Default for tests and unset NOS_CLUSTER_MODE — no cluster behavior.
    /// Agent: mode=Standalone; storage_classes=["default"]; replication_lag=0.
    pub fn standalone() -> Self {
        let node_id = default_node_id();
        Self {
            mode: ClusterMode::Standalone,
            node_id: node_id.clone(),
            instance_id: node_id,
            region_label: None,
            cluster_token: None,
            peers_raw: None,
            storage_classes: vec!["default".into()],
            replication_group: "default".into(),
            replication_role: "member".into(),
            replication_factor: 1,
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
        }
    }

    pub fn assignment_rules(&self) -> Result<AssignmentRules> {
        let raw = self
            .assignment_rules_raw
            .as_deref()
            .context("NOS_ASSIGNMENT_RULES is required for assigned cluster modes")?;
        AssignmentRules::load(raw, &self.default_storage_class)
    }

    pub fn peer_registry(&self) -> Result<PeerRegistry> {
        let raw = self
            .peers_raw
            .as_deref()
            .context("NOS_CLUSTER_PEERS is required when NOS_CLUSTER_MODE is not standalone")?;
        PeerRegistry::from_peers_raw(raw)
    }

    pub fn is_standalone(&self) -> bool {
        self.mode == ClusterMode::Standalone
    }

    /// Human: True when this node should replicate writes to peers.
    /// Agent: Replicated or ReplicatedAssigned; Assigned-only is false until Phase 3 gates.
    pub fn mode_includes_replication(&self) -> bool {
        matches!(
            self.mode,
            ClusterMode::Replicated | ClusterMode::ReplicatedAssigned
        )
    }

    pub fn is_readonly_replica(&self) -> bool {
        self.replication_role.eq_ignore_ascii_case("readonly")
    }

    /// Human: True when writes are gated by storage class and assignment rules.
    /// Agent: Assigned or ReplicatedAssigned modes.
    pub fn mode_includes_assignment(&self) -> bool {
        matches!(
            self.mode,
            ClusterMode::Assigned | ClusterMode::ReplicatedAssigned
        )
    }

    pub fn from_env() -> Result<Self> {
        let mode = match env::var("NOS_CLUSTER_MODE") {
            Ok(v) => ClusterMode::parse(&v)?,
            Err(_) => ClusterMode::Standalone,
        };

        if mode == ClusterMode::Standalone {
            return Ok(Self::standalone());
        }

        let node_id = env::var("NOS_NODE_ID").unwrap_or_else(|_| default_node_id());
        let instance_id = env::var("NOS_INSTANCE_ID").unwrap_or_else(|_| node_id.clone());
        let cluster_token = env::var("NOS_CLUSTER_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())
            .context("NOS_CLUSTER_TOKEN is required when NOS_CLUSTER_MODE is not standalone")?;
        let peers_raw = env::var("NOS_CLUSTER_PEERS")
            .ok()
            .filter(|s| !s.is_empty())
            .context("NOS_CLUSTER_PEERS is required when NOS_CLUSTER_MODE is not standalone")?;

        let storage_classes: Vec<String> = env::var("NOS_STORAGE_CLASSES")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.split(',')
                    .map(|c| c.trim().to_string())
                    .filter(|c| !c.is_empty())
                    .collect()
            })
            .unwrap_or_else(|| vec!["default".into()]);

        let replication_group =
            env::var("NOS_REPLICATION_GROUP").unwrap_or_else(|_| "default".into());
        let replication_role = env::var("NOS_REPLICATION_ROLE").unwrap_or_else(|_| "member".into());
        let replication_factor = env::var("NOS_REPLICATION_FACTOR")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<u32>()
                    .context("NOS_REPLICATION_FACTOR must be a valid u32")
            })
            .transpose()?
            .unwrap_or(1);

        let default_storage_class =
            env::var("NOS_DEFAULT_STORAGE_CLASS").unwrap_or_else(|_| "default".into());
        let assignment_rules_raw = env::var("NOS_ASSIGNMENT_RULES")
            .ok()
            .filter(|s| !s.is_empty());
        let assignment_forward = env::var("NOS_ASSIGNMENT_FORWARD")
            .ok()
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(false);
        let replication_read_repair = env::var("NOS_REPLICATION_READ_REPAIR")
            .ok()
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(false);
        let replication_heal_on_read = env::var("NOS_REPLICATION_HEAL_ON_READ")
            .ok()
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(false);
        let replication_prefixes = env::var("NOS_REPLICATION_PREFIXES")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let replication_exclude_prefixes = env::var("NOS_REPLICATION_EXCLUDE_PREFIXES")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let replication_max_attempts = env::var("NOS_REPLICATION_MAX_ATTEMPTS")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<u32>()
                    .context("NOS_REPLICATION_MAX_ATTEMPTS must be a valid u32")
            })
            .transpose()?
            .unwrap_or(20);
        let replication_peer_concurrency = env::var("NOS_REPLICATION_PEER_CONCURRENCY")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<u32>()
                    .context("NOS_REPLICATION_PEER_CONCURRENCY must be a valid u32")
            })
            .transpose()?
            .unwrap_or(4);
        let replication_async = env::var("NOS_REPLICATION_ASYNC")
            .ok()
            .map(|s| !(s == "0" || s.eq_ignore_ascii_case("false")))
            .unwrap_or(true);
        if !replication_async {
            bail!("NOS_REPLICATION_ASYNC=false (synchronous quorum replication) is not supported in v1");
        }

        if matches!(mode, ClusterMode::Assigned | ClusterMode::ReplicatedAssigned)
            && assignment_rules_raw.is_none()
        {
            bail!("NOS_ASSIGNMENT_RULES is required when NOS_CLUSTER_MODE is assigned or replicated+assigned");
        }

        Ok(Self {
            mode,
            node_id,
            instance_id,
            region_label: env::var("NOS_REGION_LABEL").ok().filter(|s| !s.is_empty()),
            cluster_token: Some(cluster_token),
            peers_raw: Some(peers_raw),
            storage_classes,
            replication_group,
            replication_role,
            replication_factor,
            replication_pending_events: 0,
            replication_read_repair,
            replication_heal_on_read,
            replication_async,
            replication_prefixes,
            replication_exclude_prefixes,
            replication_max_attempts,
            replication_peer_concurrency,
            default_storage_class,
            assignment_rules_raw,
            assignment_forward,
        })
    }
}

fn default_node_id() -> String {
    env::var("COMPUTERNAME")
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_else(|_| "node".into())
}
