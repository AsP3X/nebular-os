use std::collections::HashMap;
use std::env;
use std::fmt;
use anyhow::{Context, Result};

use crate::cluster::ClusterConfig;
use crate::storage::metadata_backend::MetadataBackendKind;
use crate::storage::metadata_mode::MetadataMode;

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
    pub metadata_backend: MetadataBackendKind,
    pub metadata_mode: MetadataMode,
    pub metadata_database_url: Option<String>,
    pub max_logical_bytes: i64,
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
    pub bulk_delete_concurrency: usize,
    pub bulk_delete_batch_limit: u64,
    pub upload_max_in_flight_bytes: u64,
    pub upload_permit_unit: u64,
    pub orphan_gc_interval_secs: u64,
    pub rate_limit_bypass_roles: Vec<String>,
    pub multipart_part_size: usize,
    pub read_pool_size: u32,
    pub cors_origins: Vec<String>,
    /// Background / maintenance zstd level (NOS_ZSTD_LEVEL, default 22).
    pub zstd_level: i32,
    /// Fast upload zstd level (NOS_ZSTD_LEVEL_UPLOAD, default 3).
    pub zstd_level_upload: i32,
    pub zstd_dict_enabled: bool,
    pub zstd_dict_max_bytes: usize,
    pub zstd_dict_train_batch: usize,
    pub dedup_enabled: bool,
    /// Unified block size default (NOS_BLOCK_SIZE); compress/dedup-specific env vars override.
    pub block_size: usize,
    pub dedup_block_size: usize,
    pub dedup_min_size: u64,
    pub compress_min_size: usize,
    pub compress_block_size: usize,
    pub compress_exclude_extensions: Vec<String>,
    pub block_cache_entries: usize,
    pub verify_interval_secs: u64,
    pub verify_batch_size: usize,
    pub s3_compat: bool,
    pub bucket_policy: BucketPolicy,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub cluster: ClusterConfig,
    /// Human: One-time operator token for PUT /_cluster/config before cluster_token is set.
    pub cluster_bootstrap_token: Option<String>,
}

impl fmt::Debug for NosConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NosConfig")
            .field("bind_addr", &self.bind_addr)
            .field("data_dir", &self.data_dir)
            .field("meta_path", &self.meta_path)
            .field("metadata_backend", &self.metadata_backend)
            .field(
                "metadata_database_url",
                &self.metadata_database_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field("max_logical_bytes", &self.max_logical_bytes)
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
            .field("bulk_delete_concurrency", &self.bulk_delete_concurrency)
            .field("bulk_delete_batch_limit", &self.bulk_delete_batch_limit)
            .field("multipart_part_size", &self.multipart_part_size)
            .field("read_pool_size", &self.read_pool_size)
            .field("cors_origins", &self.cors_origins)
            .field("zstd_level", &self.zstd_level)
            .field("zstd_level_upload", &self.zstd_level_upload)
            .field("zstd_dict_enabled", &self.zstd_dict_enabled)
            .field("dedup_enabled", &self.dedup_enabled)
            .field("block_size", &self.block_size)
            .field("compress_min_size", &self.compress_min_size)
            .field("compress_block_size", &self.compress_block_size)
            .field("block_cache_entries", &self.block_cache_entries)
            .field("verify_interval_secs", &self.verify_interval_secs)
            .field("compress_exclude_extensions", &self.compress_exclude_extensions)
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
            metadata_backend: {
                let raw = env::var("NOS_METADATA_BACKEND").unwrap_or_else(|_| "sqlite".into());
                MetadataBackendKind::parse_env(&raw).map_err(anyhow::Error::msg)?
            },
            metadata_mode: {
                let raw = env::var("NOS_METADATA_MODE").unwrap_or_else(|_| "full".into());
                MetadataMode::parse_env(&raw).map_err(anyhow::Error::msg)?
            },
            metadata_database_url: env::var("NOS_METADATA_DATABASE_URL")
                .ok()
                .filter(|s| !s.is_empty()),
            max_logical_bytes: env::var("NOS_MAX_LOGICAL_BYTES")
                .ok()
                .map(|s| s.parse().context("NOS_MAX_LOGICAL_BYTES must be a valid i64"))
                .transpose()?
                .unwrap_or(0),
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
            bulk_delete_concurrency: env::var("NOS_BULK_DELETE_CONCURRENCY")
                .ok()
                .map(|s| {
                    s.parse()
                        .context("NOS_BULK_DELETE_CONCURRENCY must be a valid usize")
                })
                .transpose()?
                .unwrap_or(32),
            bulk_delete_batch_limit: env::var("NOS_BULK_DELETE_BATCH_LIMIT")
                .ok()
                .map(|s| {
                    s.parse()
                        .context("NOS_BULK_DELETE_BATCH_LIMIT must be a valid u64")
                })
                .transpose()?
                .unwrap_or(1000),
            upload_max_in_flight_bytes: env::var("NOS_UPLOAD_MAX_IN_FLIGHT_BYTES")
                .ok()
                .map(|s| {
                    s.parse()
                        .context("NOS_UPLOAD_MAX_IN_FLIGHT_BYTES must be a valid u64")
                })
                .transpose()?
                .unwrap_or(32 * 1024 * 1024),
            upload_permit_unit: env::var("NOS_UPLOAD_PERMIT_UNIT")
                .ok()
                .map(|s| s.parse().context("NOS_UPLOAD_PERMIT_UNIT must be a valid u64"))
                .transpose()?
                .unwrap_or(5 * 1024 * 1024),
            orphan_gc_interval_secs: env::var("NOS_ORPHAN_GC_INTERVAL_SECS")
                .ok()
                .map(|s| {
                    s.parse()
                        .context("NOS_ORPHAN_GC_INTERVAL_SECS must be a valid u64")
                })
                .transpose()?
                .unwrap_or(0),
            rate_limit_bypass_roles: env::var("NOS_RATE_LIMIT_BYPASS_ROLES")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|r| r.trim().to_ascii_lowercase())
                        .filter(|r| !r.is_empty())
                        .collect()
                })
                .unwrap_or_else(|| vec!["admin".into()]),
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
            zstd_level_upload: env::var("NOS_ZSTD_LEVEL_UPLOAD")
                .ok()
                .map(|s| {
                    s.parse()
                        .context("NOS_ZSTD_LEVEL_UPLOAD must be a valid i32")
                })
                .transpose()?
                .map(crate::storage::compression::clamp_zstd_level)
                .unwrap_or(crate::storage::compression::DEFAULT_ZSTD_LEVEL_UPLOAD),
            zstd_dict_enabled: env::var("NOS_ZSTD_DICT_ENABLED")
                .ok()
                .map(|s| parse_bool(&s))
                .unwrap_or(false),
            zstd_dict_max_bytes: env::var("NOS_ZSTD_DICT_MAX_BYTES")
                .ok()
                .map(|s| s.parse().context("NOS_ZSTD_DICT_MAX_BYTES must be a valid usize"))
                .transpose()?
                .unwrap_or(112_640),
            zstd_dict_train_batch: env::var("NOS_ZSTD_DICT_TRAIN_BATCH")
                .ok()
                .map(|s| s.parse().context("NOS_ZSTD_DICT_TRAIN_BATCH must be a valid usize"))
                .transpose()?
                .unwrap_or(32),
            dedup_enabled: env::var("NOS_DEDUP_ENABLED")
                .ok()
                .map(|s| parse_bool(&s))
                .unwrap_or(false),
            block_size: {
                let default = crate::storage::compression::DEFAULT_BLOCK_SIZE;
                env::var("NOS_BLOCK_SIZE")
                    .ok()
                    .map(|s| s.parse().context("NOS_BLOCK_SIZE must be a valid usize"))
                    .transpose()?
                    .unwrap_or(default)
            },
            dedup_block_size: env::var("NOS_DEDUP_BLOCK_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_DEDUP_BLOCK_SIZE must be a valid usize"))
                .transpose()?
                .unwrap_or({
                    let default = crate::storage::compression::DEFAULT_BLOCK_SIZE;
                    env::var("NOS_BLOCK_SIZE")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(default)
                }),
            dedup_min_size: env::var("NOS_DEDUP_MIN_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_DEDUP_MIN_SIZE must be a valid u64"))
                .transpose()?
                .unwrap_or(1024 * 1024),
            compress_min_size: env::var("NOS_COMPRESS_MIN_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_COMPRESS_MIN_SIZE must be a valid usize"))
                .transpose()?
                .unwrap_or(crate::storage::compressibility::DEFAULT_MIN_COMPRESSIBLE_SIZE),
            compress_block_size: env::var("NOS_COMPRESS_BLOCK_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_COMPRESS_BLOCK_SIZE must be a valid usize"))
                .transpose()?
                .unwrap_or({
                    let default = crate::storage::compression::DEFAULT_BLOCK_SIZE;
                    env::var("NOS_BLOCK_SIZE")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(default)
                }),
            block_cache_entries: env::var("NOS_BLOCK_CACHE_ENTRIES")
                .ok()
                .map(|s| s.parse().context("NOS_BLOCK_CACHE_ENTRIES must be a valid usize"))
                .transpose()?
                .unwrap_or(256),
            verify_interval_secs: env::var("NOS_VERIFY_INTERVAL_SECS")
                .ok()
                .map(|s| s.parse().context("NOS_VERIFY_INTERVAL_SECS must be a valid u64"))
                .transpose()?
                .unwrap_or(0),
            verify_batch_size: env::var("NOS_VERIFY_BATCH_SIZE")
                .ok()
                .map(|s| s.parse().context("NOS_VERIFY_BATCH_SIZE must be a valid usize"))
                .transpose()?
                .unwrap_or(100),
            compress_exclude_extensions: env::var("NOS_COMPRESS_EXCLUDE_EXTENSIONS")
                .ok()
                .map(|s| crate::storage::compressibility::parse_exclude_extensions(&s))
                .unwrap_or_default(),
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
            cluster_bootstrap_token: env::var("NOS_CLUSTER_BOOTSTRAP_TOKEN")
                .ok()
                .filter(|s| !s.is_empty()),
        })
        .and_then(|cfg| cfg.validate_metadata().map(|_| cfg))
    }

    fn validate_metadata(&self) -> Result<()> {
        if self.metadata_backend == MetadataBackendKind::Postgres
            && self
                .metadata_database_url
                .as_ref()
                .is_none_or(|s| s.is_empty())
        {
            anyhow::bail!(
                "NOS_METADATA_DATABASE_URL is required when NOS_METADATA_BACKEND=postgres"
            );
        }
        if self.metadata_backend == MetadataBackendKind::Postgres
            && self.metadata_mode != MetadataMode::BlobOnly
            && self.cluster.mode != crate::cluster::config::ClusterMode::Standalone
        {
            anyhow::bail!(
                "postgres metadata backend requires NOS_CLUSTER_MODE=standalone (cluster replication uses SQLite replication_log)"
            );
        }
        Ok(())
    }
}
