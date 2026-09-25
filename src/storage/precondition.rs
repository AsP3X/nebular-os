use super::error::StorageError;
use super::types::ObjectMetadata;

/// Human: True when any entity-tag in a client If-Match / If-None-Match list equals the stored etag.
/// Agent: SPLITS on commas; STRIPS weak prefix `W/` and quotes (our etags are never weak, so weak and
/// strong comparison agree); `*` is handled by callers.
pub fn etag_matches(stored: &str, candidate: &str) -> bool {
    candidate.split(',').any(|tag| {
        let tag = tag.trim();
        let tag = tag.strip_prefix("W/").unwrap_or(tag);
        tag.trim_matches('"') == stored
    })
}

/// Human: RFC 9110 §13.1.3 — whether a conditional GET or HEAD is answered `304 Not Modified`.
/// Agent: If-None-Match takes precedence (If-Modified-Since is then ignored); a future date is ignored.
pub fn is_not_modified(
    meta: &ObjectMetadata,
    if_none_match: Option<&str>,
    if_modified_since: Option<i64>,
) -> bool {
    if let Some(etag) = if_none_match {
        return etag == "*"
            || meta
                .etag
                .as_deref()
                .is_some_and(|stored| etag_matches(stored, etag));
    }
    if let Some(since) = if_modified_since
        && since <= chrono::Utc::now().timestamp()
    {
        return meta.updated_at.timestamp() <= since;
    }
    false
}

/// Human: RFC 9110 §13.1.5 — whether a Range may be served under `If-Range`. The validator must be a strong
/// entity-tag equal to the current one, or exactly the current Last-Modified date; otherwise the client gets
/// the whole representation, so it never splices bytes from two versions.
/// Agent: Weak tags never match; unquoted values that aren't dates are compared as our (unquoted) ETags.
pub fn if_range_matches(meta: &ObjectMetadata, if_range: &str) -> bool {
    let value = if_range.trim();
    if value.starts_with("W/") {
        return false;
    }
    if value.starts_with('"') {
        return meta
            .etag
            .as_deref()
            .is_some_and(|stored| value.trim_matches('"') == stored);
    }
    if let Ok(date) = chrono::DateTime::parse_from_rfc2822(value) {
        return meta.updated_at.timestamp() == date.timestamp();
    }
    meta.etag.as_deref() == Some(value)
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
            storage_class: None,
            origin_node: None,
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
