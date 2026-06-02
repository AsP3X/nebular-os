use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use super::config::ClusterConfig;
use super::peer::PeerRegistry;

/// Human: Optional client hints for assignment resolution on mutating object routes.
/// Agent: Built from x-nd-storage-class, Content-Type, Content-Length, custom meta headers.
#[derive(Debug, Clone, Default)]
pub struct WriteContext {
    pub storage_class_header: Option<String>,
    pub content_type: Option<String>,
    pub custom_meta_storage_class: Option<String>,
    pub content_length: Option<u64>,
    /// Caller `Authorization` header value (used for NOS_ASSIGNMENT_FORWARD).
    pub authorization: Option<String>,
    /// Optional override for replication peer group selection (`x-nd-replication-group`).
    pub replication_group_header: Option<String>,
}

/// Human: Replication group for enqueue/worker — header wins over NOS_REPLICATION_GROUP.
/// Agent: READS WriteContext.replication_group_header else ClusterConfig.replication_group.
pub fn replication_group_for_write(ctx: Option<&WriteContext>, cluster: &ClusterConfig) -> String {
    ctx.and_then(|c| c.replication_group_header.clone())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| cluster.replication_group.clone())
}

/// Human: JSON rules that map object shape to a storage class (and optional target node hint).
/// Agent: Parsed from NOS_ASSIGNMENT_RULES file or inline JSON; first matching rule wins.
#[derive(Debug, Clone, Deserialize)]
pub struct AssignmentRulesFile {
    pub rules: Vec<AssignmentRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AssignmentRule {
    pub storage_class: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub suffix: Option<String>,
    #[serde(default)]
    pub mime_prefix: Option<String>,
    #[serde(default)]
    pub min_bytes: Option<u64>,
    #[serde(default)]
    pub assigned_node: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AssignmentRules {
    pub rules: Vec<AssignmentRule>,
    pub default_class: String,
}

impl AssignmentRules {
    pub fn load(raw: &str, default_class: &str) -> Result<Self> {
        let trimmed = raw.trim();
        let json = if trimmed.starts_with('{') {
            trimmed.to_string()
        } else {
            std::fs::read_to_string(Path::new(trimmed))
                .with_context(|| format!("read NOS_ASSIGNMENT_RULES file: {trimmed}"))?
        };
        let file: AssignmentRulesFile =
            serde_json::from_str(&json).context("NOS_ASSIGNMENT_RULES must be valid JSON")?;
        if file.rules.is_empty() {
            anyhow::bail!("NOS_ASSIGNMENT_RULES must contain at least one rule");
        }
        Ok(Self {
            rules: file.rules,
            default_class: default_class.to_string(),
        })
    }

    /// Human: Pick storage class for a write using header hints, then rule patterns, then default.
    /// Agent: ORDER header > custom_meta class > first matching rule > NOS_DEFAULT_STORAGE_CLASS.
    pub fn resolve_class(
        &self,
        _bucket: &str,
        key: &str,
        ctx: Option<&WriteContext>,
    ) -> String {
        if let Some(ctx) = ctx {
            if let Some(h) = &ctx.storage_class_header
                && !h.is_empty()
            {
                return h.clone();
            }
            if let Some(h) = &ctx.custom_meta_storage_class
                && !h.is_empty()
            {
                return h.clone();
            }
        }

        let content_type = ctx.and_then(|c| c.content_type.as_deref());
        let size = ctx.and_then(|c| c.content_length).unwrap_or(0);

        for rule in &self.rules {
            if let Some(prefix) = &rule.prefix
                && !key.starts_with(prefix)
            {
                continue;
            }
            if let Some(suffix) = &rule.suffix
                && !key.ends_with(suffix)
            {
                continue;
            }
            if let Some(mime_prefix) = &rule.mime_prefix {
                match content_type {
                    Some(ct) if ct.starts_with(mime_prefix) => {}
                    _ => continue,
                }
            }
            if let Some(min) = rule.min_bytes
                && size < min
            {
                continue;
            }
            return rule.storage_class.clone();
        }

        self.default_class.clone()
    }

    pub fn assigned_node_hint(&self, storage_class: &str, peers: &PeerRegistry, self_id: &str) -> Option<String> {
        if let Some(rule) = self
            .rules
            .iter()
            .find(|r| r.storage_class == storage_class)
            && let Some(node) = &rule.assigned_node
        {
            return Some(node.clone());
        }
        peers
            .peers
            .keys()
            .find(|id| id.as_str() != self_id)
            .cloned()
    }
}

/// Human: Resolved placement used by the write gate and debug resolve endpoint.
/// Agent: accept_local IF storage_class in NOS_STORAGE_CLASSES.
#[derive(Debug, Clone)]
pub struct AssignmentResolution {
    pub storage_class: String,
    pub assigned_node: Option<String>,
    pub accept_local: bool,
}

impl AssignmentResolution {
    pub fn resolve(
        rules: &AssignmentRules,
        cluster: &ClusterConfig,
        peers: &PeerRegistry,
        bucket: &str,
        key: &str,
        ctx: Option<&WriteContext>,
    ) -> Self {
        let storage_class = rules.resolve_class(bucket, key, ctx);
        let accept_local = cluster.storage_classes.iter().any(|c| c == &storage_class);
        let assigned_node = if accept_local {
            None
        } else {
            rules.assigned_node_hint(&storage_class, peers, &cluster.node_id)
        };
        Self {
            storage_class,
            assigned_node,
            accept_local,
        }
    }
}
