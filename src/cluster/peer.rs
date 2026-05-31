//! Human: HTTP client and peer registry for inter-node replication (Phase 2+).
//! Agent: PeerRegistry parses NOS_CLUSTER_PEERS; POST /_cluster/replicate uses reqwest in Phase 2.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};

/// Human: Parsed peer list from NOS_CLUSTER_PEERS for outbound replication calls.
/// Agent: Map node_id -> base URL; duplicate ids rejected at parse time.
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    pub peers: HashMap<String, String>,
}

impl PeerRegistry {
    pub fn from_peers_raw(raw: &str) -> Result<Self> {
        let mut peers = HashMap::new();
        for entry in raw.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let (id, url) = entry
                .split_once('=')
                .with_context(|| format!("invalid peer entry (expected id=url): {entry}"))?;
            if id.is_empty() || url.is_empty() {
                bail!("invalid peer entry: {entry}");
            }
            if peers.insert(id.to_string(), url.to_string()).is_some() {
                bail!("duplicate peer id: {id}");
            }
        }
        if peers.is_empty() {
            bail!("NOS_CLUSTER_PEERS must list at least one peer");
        }
        Ok(Self { peers })
    }
}
