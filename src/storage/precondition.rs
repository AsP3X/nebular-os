use super::error::StorageError;
use super::types::ObjectMetadata;

/// Human: Compares a stored etag with a client If-Match / If-None-Match token.
/// Agent: NORMALIZES quotes; MATCHES stored==candidate OR stored==trimmed client value.
pub fn etag_matches(stored: &str, candidate: &str) -> bool {
    let candidate = candidate.trim().trim_matches('"');
    stored == candidate || stored == candidate.trim()
}

/// Human: Enforces If-Match / If-None-Match on mutating object requests before upload or delete proceeds.
/// Agent: If-None-Match:*+existing=>PreconditionFailed; If-Match+missing/mismatch=>PreconditionFailed; If-Match:* requires existing.
pub fn check_write_preconditions(
    existing: Option<&ObjectMetadata>,
    if_match: Option<&str>,
    if_none_match: Option<&str>,
) -> Result<(), StorageError> {
    if let Some(none_match) = if_none_match
        && none_match == "*"
        && existing.is_some()
    {
        return Err(StorageError::PreconditionFailed);
    }

    let Some(match_val) = if_match else {
        return Ok(());
    };

    let Some(meta) = existing else {
        return Err(StorageError::PreconditionFailed);
    };

    if match_val == "*" {
        return Ok(());
    }

    let Some(stored) = meta.etag.as_deref() else {
        return Err(StorageError::PreconditionFailed);
    };

    if etag_matches(stored, match_val) {
        Ok(())
    } else {
        Err(StorageError::PreconditionFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn sample_meta(etag: &str) -> ObjectMetadata {
        let now = Utc::now();
        ObjectMetadata {
            bucket: "b".into(),
            key: "k".into(),
            size: 1,
            mime_type: None,
            etag: Some(etag.into()),
            created_at: now,
            updated_at: now,
            custom_meta: None,
            deleted_at: None,
        }
    }

    #[test]
    fn create_if_absent_succeeds_when_missing() {
        assert!(check_write_preconditions(None, None, Some("*")).is_ok());
    }

    #[test]
    fn create_if_absent_fails_when_present() {
        let meta = sample_meta("abc");
        assert!(matches!(
            check_write_preconditions(Some(&meta), None, Some("*")),
            Err(StorageError::PreconditionFailed)
        ));
    }

    #[test]
    fn if_match_requires_existing_object() {
        assert!(matches!(
            check_write_preconditions(None, Some("abc"), None),
            Err(StorageError::PreconditionFailed)
        ));
    }

    #[test]
    fn if_match_succeeds_on_matching_etag() {
        let meta = sample_meta("deadbeef");
        assert!(check_write_preconditions(Some(&meta), Some("deadbeef"), None).is_ok());
    }
}
