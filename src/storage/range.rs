/// Parses RFC 7233 `bytes=` ranges including suffix form `bytes=-N`.
pub fn parse_content_range(value: &str, total_size: u64) -> Option<(u64, u64)> {
    let value = value.trim();
    if !value.starts_with("bytes=") {
        return None;
    }
    let range = &value[6..];
    let parts: Vec<&str> = range.splitn(2, '-').collect();
    if parts.len() != 2 {
        return None;
    }
    if parts[0].is_empty() {
        let suffix: u64 = parts[1].parse().ok()?;
        if total_size == 0 {
            return None;
        }
        let start = total_size.saturating_sub(suffix);
        return Some((start, total_size.saturating_sub(1)));
    }
    let start: u64 = parts[0].parse().ok()?;
    let end = if parts[1].is_empty() {
        total_size.saturating_sub(1)
    } else {
        parts[1].parse().ok()?
    };
    if start > end || (total_size > 0 && start >= total_size) {
        return None;
    }
    Some((start, end))
}
