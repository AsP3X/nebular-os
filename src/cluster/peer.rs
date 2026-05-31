//! Human: HTTP client and peer registry for inter-node replication (Phase 2+).
//! Agent: PeerRegistry parses NOS_CLUSTER_PEERS; optional ;classes= on each peer entry.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};

/// Human: One cluster peer with base URL and storage classes it accepts.
/// Agent: Parsed from id=url or id=url;class-a,class-b in NOS_CLUSTER_PEERS.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub url: String,
    pub storage_classes: Vec<String>,
}

/// Human: Parsed peer list from NOS_CLUSTER_PEERS for outbound replication calls.
/// Agent: Map node_id -> PeerEntry; worker filters by event.storage_class.
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    pub peers: HashMap<String, PeerEntry>,
}

impl PeerRegistry {
    pub fn from_peers_raw(raw: &str) -> Result<Self> {
        let mut peers = HashMap::new();
        for entry in raw.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let (id_part, rest) = entry
                .split_once('=')
                .with_context(|| format!("invalid peer entry (expected id=url): {entry}"))?;
            let (url, classes) = match rest.split_once(';') {
                Some((url, classes)) => (
                    url.to_string(),
                    classes
                        .split(',')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect(),
                ),
                None => (rest.to_string(), Vec::new()),
            };
            if id_part.is_empty() || url.is_empty() {
                bail!("invalid peer entry: {entry}");
            }
            if peers
                .insert(
                    id_part.to_string(),
                    PeerEntry {
                        url,
                        storage_classes: classes,
                    },
                )
                .is_some()
            {
                bail!("duplicate peer id: {id_part}");
            }
        }
        if peers.is_empty() {
            bail!("NOS_CLUSTER_PEERS must list at least one peer");
        }
        Ok(Self { peers })
    }

    /// Human: Peers that accept this storage class (empty peer class list = accepts all).
    /// Agent: Phase 4 filter for replicated+assigned worker peer selection.
    pub fn peers_for_class<'a>(
        &'a self,
        storage_class: &str,
    ) -> impl Iterator<Item = (&'a String, &'a PeerEntry)> {
        self.peers.iter().filter(move |(_, entry)| {
            entry.storage_classes.is_empty()
                || entry
                    .storage_classes
                    .iter()
                    .any(|c| c == storage_class)
        })
    }
}
