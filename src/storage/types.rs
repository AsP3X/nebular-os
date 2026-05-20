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
    pub custom_meta: Option<String>, // JSON blob
}

#[derive(Debug, Clone, Serialize)]
pub struct ListItem {
    pub key: String,
    pub size: i64,
    pub mime_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: DateTime<Utc>,
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
