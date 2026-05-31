use std::collections::HashMap;
use std::env;
use std::fmt;
use anyhow::{Context, Result};

use crate::cluster::ClusterConfig;

/// Human: Optional per-subject bucket allow-lists loaded from NOS_BUCKET_POLICY JSON.
/// Agent: EMPTY map => allow all buckets; non-empty => sub must list bucket explicitly.
#[derive(Clone, Default)]
pub struct BucketPolicy(pub HashMap<String, Vec<String>>);

impl BucketPolicy {
    pub fn from_json(raw: &str) -> Result<Self> {
        if raw.trim().is_empty() {
            return Ok(Self::default());
        }
        let map: HashMap<String, Vec<String>> =
            serde_json::from_str(raw).context("NOS_BUCKET_POLICY must be valid JSON object")?;
        Ok(Self(map))
    }

    pub fn allows(&self, sub: &str, bucket: &str) -> bool {
        if self.0.is_empty() {
            return true;
        }
        self.0
            .get(sub)
            .is_some_and(|buckets| buckets.iter().any(|b| b == bucket))
    }
}

#[derive(Clone)]
pub struct NosConfig {
    pub bind_addr: String,
    pub data_dir: String,
    pub meta_path: String,
    pub jwt_secret: String,
    pub signing_secret: Option<String>,
    pub max_body_size: usize,
    pub upload_buffer_size: usize,
    pub allow_public_read: bool,
    pub reconcile_on_startup: bool,
    pub reconcile_interval_secs: u64,
    pub soft_delete_ttl_secs: i64,
    pub soft_delete_drop_blob: bool,
    pub multipart_upload_ttl_secs: i64,
    pub recompress_on_startup: bool,
    pub recompress_interval_secs: u64,
    pub recompress_batch_size: usize,
    pub metrics_token: Option<String>,
    pub rate_limit_rps: u32,
    pub rate_limit_burst: u32,
    pub list_scan_cap: i64,
    pub multipart_part_size: usize,
    pub read_pool_size: u32,
    pub cors_origins: Vec<String>,
    pub zstd_level: i32,
    pub s3_compat: bool,
    pub bucket_policy: BucketPolicy,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub cluster: ClusterConfig,
}

impl fmt::Debug for NosConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NosConfig")
            .field("bind_addr", &self.bind_addr)
            .field("data_dir", &self.data_dir)
            .field("meta_path", &self.meta_path)
            .field("jwt_secret", &"[REDACTED]")
            .field("signing_secret", &"[REDACTED]")
            .field("max_body_size", &self.max_body_size)
            .field("upload_buffer_size", &self.upload_buffer_size)
            .field("allow_public_read", &self.allow_public_read)
            .field("reconcile_on_startup", &self.reconcile_on_startup)
            .field("reconcile_interval_secs", &self.reconcile_interval_secs)
            .field("soft_delete_ttl_secs", &self.soft_delete_ttl_secs)
            .field("soft_delete_drop_blob", &self.soft_delete_drop_blob)
            .field("multipart_upload_ttl_secs", &self.multipart_upload_ttl_secs)
            .field("recompress_on_startup", &self.recompress_on_startup)
            .field("recompress_interval_secs", &self.recompress_interval_secs)
            .field("recompress_batch_size", &self.recompress_batch_size)
            .field("metrics_token", &self.metrics_token.as_ref().map(|_| "[REDACTED]"))
            .field("rate_limit_rps", &self.rate_limit_rps)
            .field("rate_limit_burst", &self.rate_limit_burst)
            .field("list_scan_cap", &self.list_scan_cap)
            .field("multipart_part_size", &self.multipart_part_size)
            .field("read_pool_size", &self.read_pool_size)
            .field("cors_origins", &self.cors_origins)
            .field("zstd_level", &self.zstd_level)
            .field("s3_compat", &self.s3_compat)
            .field(
                "bucket_policy",
                &self.bucket_policy.0.keys().collect::<Vec<_>>(),
            )
            .field("s3_access_key", &self.s3_access_key.as_ref().map(|_| "[REDACTED]"))
            .field("cluster_mode", &self.cluster.mode.as_str())
            .field("node_id", &self.cluster.node_id)
            .finish()
    }
}

fn parse_bool(s: &str) -> bool {
    s.eq_ignore_ascii_case("true") || s == "1"
}

impl NosConfig {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();
        Ok(Self {
            bind_addr: env::var("NOS_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:9000".into()),
            data_dir: env::var("NOS_DATA_DIR").unwrap_or_else(|_| "./data/blobs".into()),
            meta_path: env::var("NOS_META_PATH").unwrap_or_else(|_| "./data/meta/metadata.db".into()),
            jwt_secret: env::var("NOS_JWT_SECRET").context("NOS_JWT_SECRET must be set")?,
            signing_secret: env::var("NOS_SIGNING_SECRET").ok(),
            max_body_size: env::var("NOS_MAX_BODY_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_MAX_BODY_SIZE must be a valid usize"))
                .transpose()?
                .unwrap_or(104_857_600),
            upload_buffer_size: env::var("NOS_UPLOAD_BUFFER_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_UPLOAD_BUFFER_SIZE must be a valid usize"))
                .transpose()?
                .unwrap_or(256 * 1024),
            allow_public_read: env::var("NOS_ALLOW_PUBLIC_READ")
                .ok()
                .map(|s| parse_bool(&s))
                .unwrap_or(false),
            reconcile_on_startup: env::var("NOS_RECONCILE_ON_STARTUP")
                .ok()
                .map(|s| parse_bool(&s))
                .unwrap_or(false),
            reconcile_interval_secs: env::var("NOS_RECONCILE_INTERVAL_SECS")
                .ok()
                .map(|s| s.parse().context("NOS_RECONCILE_INTERVAL_SECS must be a valid u64"))
                .transpose()?
                .unwrap_or(0),
            soft_delete_ttl_secs: env::var("NOS_SOFT_DELETE_TTL_SECS")
                .ok()
                .map(|s| s.parse().context("NOS_SOFT_DELETE_TTL_SECS must be a valid i64"))
                .transpose()?
                .unwrap_or(86_400),
            soft_delete_drop_blob: env::var("NOS_SOFT_DELETE_DROP_BLOB")
                .ok()
                .map(|s| parse_bool(&s))
                .unwrap_or(false),
            multipart_upload_ttl_secs: env::var("NOS_MULTIPART_UPLOAD_TTL_SECS")
                .ok()
                .map(|s| {
                    s.parse()
                        .context("NOS_MULTIPART_UPLOAD_TTL_SECS must be a valid i64")
                })
                .transpose()?
                .unwrap_or(86_400),
            recompress_on_startup: env::var("NOS_RECOMPRESS_ON_STARTUP")
                .ok()
                .map(|s| parse_bool(&s))
                .unwrap_or(false),
            recompress_interval_secs: env::var("NOS_RECOMPRESS_INTERVAL_SECS")
                .ok()
                .map(|s| s.parse().context("NOS_RECOMPRESS_INTERVAL_SECS must be a valid u64"))
                .transpose()?
                .unwrap_or(0),
            recompress_batch_size: env::var("NOS_RECOMPRESS_BATCH_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_RECOMPRESS_BATCH_SIZE must be a valid usize"))
                .transpose()?
                .unwrap_or(100),
            metrics_token: env::var("NOS_METRICS_TOKEN").ok().filter(|s| !s.is_empty()),
            rate_limit_rps: env::var("NOS_RATE_LIMIT_RPS")
                .ok()
                .map(|s| s.parse().context("NOS_RATE_LIMIT_RPS must be a valid u32"))
                .transpose()?
                .unwrap_or(0),
            rate_limit_burst: env::var("NOS_RATE_LIMIT_BURST")
                .ok()
                .map(|s| s.parse().context("NOS_RATE_LIMIT_BURST must be a valid u32"))
                .transpose()?
                .unwrap_or(50),
            list_scan_cap: env::var("NOS_LIST_SCAN_CAP")
                .ok()
                .map(|s| s.parse().context("NOS_LIST_SCAN_CAP must be a valid i64"))
                .transpose()?
                .unwrap_or(4096),
            multipart_part_size: env::var("NOS_MULTIPART_PART_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_MULTIPART_PART_SIZE must be a valid usize"))
                .transpose()?
                .unwrap_or(8 * 1024 * 1024),
            read_pool_size: env::var("NOS_READ_POOL_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_READ_POOL_SIZE must be a valid u32"))
                .transpose()?
                .unwrap_or(4),
            cors_origins: env::var("NOS_CORS_ORIGINS")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|o| o.trim().to_string())
                        .filter(|o| !o.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            zstd_level: env::var("NOS_ZSTD_LEVEL")
                .ok()
                .map(|s| s.parse().context("NOS_ZSTD_LEVEL must be a valid i32"))
                .transpose()?
                .map(crate::storage::compression::clamp_zstd_level)
                .unwrap_or(crate::storage::compression::DEFAULT_ZSTD_LEVEL),
            s3_compat: env::var("NOS_S3_COMPAT")
                .ok()
                .map(|s| parse_bool(&s))
                .unwrap_or(false),
            bucket_policy: env::var("NOS_BUCKET_POLICY")
                .ok()
                .map(|s| BucketPolicy::from_json(&s))
                .transpose()?
                .unwrap_or_default(),
            s3_access_key: env::var("NOS_S3_ACCESS_KEY").ok().filter(|s| !s.is_empty()),
            s3_secret_key: env::var("NOS_S3_SECRET_KEY").ok().filter(|s| !s.is_empty()),
            cluster: ClusterConfig::from_env()?,
        })
    }
}
