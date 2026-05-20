use std::env;
use std::fmt;
use anyhow::{Context, Result};

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
            .finish()
    }
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
                .map(|s| {
                    s.parse()
                        .context("NOS_UPLOAD_BUFFER_SIZE must be a valid usize")
                })
                .transpose()?
                .unwrap_or(256 * 1024),
            allow_public_read: env::var("NOS_ALLOW_PUBLIC_READ")
                .ok()
                .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
                .unwrap_or(false),
        })
    }
}
