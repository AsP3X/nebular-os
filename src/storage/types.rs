use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub bucket: String,
    pub key: String,
    pub size: i64,
    pub mime_type: Option<String>,
    pub etag: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub custom_meta: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_node: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListItem {
    pub key: String,
    pub size: i64,
    pub mime_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_node: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListCountResult {
    pub count: u64,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListResult {
    pub items: Vec<ListItem>,
    /// Folder-like prefixes when `delimiter` is set (S3-style `CommonPrefixes`).
    pub common_prefixes: Vec<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    /// True when more keys exist beyond this page.
    pub is_truncated: bool,
    /// Pass as `start_after` on the next list request when `is_truncated` is true.
    pub next_start_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletePrefixFailure {
    pub key: String,
    pub error: String,
}

/// Metadata for keys removed in a prefix delete (used by replicated backends).
#[derive(Debug, Clone)]
pub struct DeletedObjectRef {
    pub key: String,
    pub storage_class: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeletePrefixOutcome {
    pub deleted: u64,
    pub failed: Vec<DeletePrefixFailure>,
    pub truncated: bool,
    pub next_start_after: Option<String>,
    pub deleted_objects: Vec<DeletedObjectRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletePrefixResponse {
    pub deleted: u64,
    pub failed: Vec<DeletePrefixFailure>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_start_after: Option<String>,
}

impl From<DeletePrefixOutcome> for DeletePrefixResponse {
    fn from(outcome: DeletePrefixOutcome) -> Self {
        Self {
            deleted: outcome.deleted,
            failed: outcome.failed,
            truncated: outcome.truncated,
            next_start_after: outcome.next_start_after,
        }
    }
}
