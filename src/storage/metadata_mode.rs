use std::fmt;

/// How Nebular maintains object metadata alongside on-disk blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataMode {
    /// Full object index in SQLite or Postgres (default).
    Full,
    /// Bytes-only store — Ownly (or another index) owns lifecycle; no metadata rows.
    BlobOnly,
}

impl MetadataMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::BlobOnly => "blob_only",
        }
    }

    pub fn parse_env(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "full" => Ok(Self::Full),
            "blob_only" | "blob-only" | "blobonly" => Ok(Self::BlobOnly),
            other => Err(format!(
                "unsupported NOS_METADATA_MODE={other:?} (expected full or blob_only)"
            )),
        }
    }

    pub fn is_blob_only(self) -> bool {
        matches!(self, Self::BlobOnly)
    }
}

impl fmt::Display for MetadataMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
