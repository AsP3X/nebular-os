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
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
}
