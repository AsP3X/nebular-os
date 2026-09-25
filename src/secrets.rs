// Human: Nebula must not start with compose dev placeholders or repo-known JWT/signing defaults.
// Agent: VALIDATES NOS_JWT_SECRET and optional NOS_SIGNING_SECRET; MIN 32 chars; SAME weak list as Aurora backend.

pub const MIN_SECRET_LEN: usize = 32;

/// Fewest distinct characters a secret may use — rejects placeholders like 32 × "a" or "1234" repeated.
pub const MIN_DISTINCT_CHARS: usize = 8;

const KNOWN_WEAK_SECRETS: &[&str] = &[
    "change-me-in-production",
    "change-me-in-production-jwt-secret",
    "dev-jwt-secret-change-me",
    "dev-master-secret-change-me",
    "dev-nos-jwt-secret-change-me",
    "dev-nos-signing-secret-change-me",
];

pub fn is_weak_secret(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty()
        || trimmed == "GENERATE_ME"
        || KNOWN_WEAK_SECRETS.contains(&trimmed)
}

fn validate_field(env_name: &str, value: &str) -> anyhow::Result<()> {
    if is_weak_secret(value) {
        anyhow::bail!(
            "{env_name} is unset, still GENERATE_ME, or a known weak default. \
             Set a random secret (at least {MIN_SECRET_LEN} characters)."
        );
    }
    if value.len() < MIN_SECRET_LEN {
        anyhow::bail!(
            "{env_name} must be at least {MIN_SECRET_LEN} characters (got {}).",
            value.len()
        );
    }
    let distinct = value.chars().collect::<std::collections::HashSet<_>>().len();
    if distinct < MIN_DISTINCT_CHARS {
        anyhow::bail!(
            "{env_name} uses only {distinct} distinct characters; set a random secret (e.g. `openssl rand -hex 32`)."
        );
    }
    Ok(())
}

pub fn validate_jwt_secret(jwt_secret: &str) -> anyhow::Result<()> {
    validate_field("NOS_JWT_SECRET", jwt_secret)
}

pub fn validate_signing_secret(signing_secret: &str) -> anyhow::Result<()> {
    validate_field("NOS_SIGNING_SECRET", signing_secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weak_secrets_are_rejected() {
        assert!(validate_jwt_secret("change-me-in-production").is_err());
        assert!(validate_jwt_secret("short").is_err());
        assert!(validate_jwt_secret(&"a".repeat(64)).is_err());
        assert!(validate_jwt_secret(&"1234".repeat(16)).is_err());
        assert!(validate_jwt_secret("3f9c1e7a5b2d8046f1e3c5a7b9d0e2f4").is_ok());
        assert!(validate_signing_secret("smoke-signing-secret-0123456789abcdef-012345").is_ok());
    }
}
