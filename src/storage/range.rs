/// How a GET should treat its `Range` header for an object of a given size (RFC 9110 §14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeRequest {
    /// Serve the full body with 200: malformed spec, unknown unit, or multiple ranges.
    Ignore,
    /// Serve inclusive bytes `start..=end` with 206.
    Satisfiable { start: u64, end: u64 },
    /// No requested byte exists: answer 416.
    Unsatisfiable,
}

/// Human: Evaluates a single `bytes=` range, including suffix form `bytes=-N`, against `total_size`.
/// Agent: RETURNS Ignore for anything we do not serve as 206 (servers MAY ignore Range); never errors.
pub fn evaluate_range(value: &str, total_size: u64) -> RangeRequest {
    let value = value.trim();
    let Some(spec) = value
        .get(..6)
        .filter(|unit| unit.eq_ignore_ascii_case("bytes="))
        .map(|_| &value[6..])
    else {
        return RangeRequest::Ignore;
    };
    if spec.contains(',') {
        return RangeRequest::Ignore;
    }
    let Some((first, last)) = spec.split_once('-') else {
        return RangeRequest::Ignore;
    };
    let (first, last) = (first.trim(), last.trim());

    if first.is_empty() {
        let Ok(suffix) = last.parse::<u64>() else {
            return RangeRequest::Ignore;
        };
        if suffix == 0 || total_size == 0 {
            return RangeRequest::Unsatisfiable;
        }
        return RangeRequest::Satisfiable {
            start: total_size - suffix.min(total_size),
            end: total_size - 1,
        };
    }

    let Ok(start) = first.parse::<u64>() else {
        return RangeRequest::Ignore;
    };
    let last = if last.is_empty() {
        None
    } else {
        match last.parse::<u64>() {
            Ok(v) if v >= start => Some(v),
            _ => return RangeRequest::Ignore,
        }
    };
    if start >= total_size {
        return RangeRequest::Unsatisfiable;
    }
    let end = last.map_or(total_size - 1, |v| v.min(total_size - 1));
    RangeRequest::Satisfiable { start, end }
}

/// Satisfiable span of a `Range` value; `None` for both ignored and unsatisfiable ranges.
#[deprecated(note = "use evaluate_range, which separates ignored from unsatisfiable ranges")]
pub fn parse_content_range(value: &str, total_size: u64) -> Option<(u64, u64)> {
    match evaluate_range(value, total_size) {
        RangeRequest::Satisfiable { start, end } => Some((start, end)),
        RangeRequest::Ignore | RangeRequest::Unsatisfiable => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use RangeRequest::*;

    #[test]
    fn satisfiable_forms() {
        assert_eq!(evaluate_range("bytes=0-4", 26), Satisfiable { start: 0, end: 4 });
        assert_eq!(evaluate_range("bytes=20-", 26), Satisfiable { start: 20, end: 25 });
        assert_eq!(evaluate_range("bytes=20-999", 26), Satisfiable { start: 20, end: 25 });
        assert_eq!(evaluate_range("bytes=-4", 26), Satisfiable { start: 22, end: 25 });
        assert_eq!(evaluate_range("bytes=-999", 26), Satisfiable { start: 0, end: 25 });
        assert_eq!(evaluate_range("Bytes=1-1", 26), Satisfiable { start: 1, end: 1 });
    }

    #[test]
    fn unsatisfiable_forms() {
        assert_eq!(evaluate_range("bytes=26-", 26), Unsatisfiable);
        assert_eq!(evaluate_range("bytes=100-200", 26), Unsatisfiable);
        assert_eq!(evaluate_range("bytes=-0", 26), Unsatisfiable);
        assert_eq!(evaluate_range("bytes=0-", 0), Unsatisfiable);
        assert_eq!(evaluate_range("bytes=-5", 0), Unsatisfiable);
    }

    #[test]
    fn ignored_forms() {
        assert_eq!(evaluate_range("bytes=5-1", 26), Ignore);
        assert_eq!(evaluate_range("bytes=0-1,4-5", 26), Ignore);
        assert_eq!(evaluate_range("bytes=abc", 26), Ignore);
        assert_eq!(evaluate_range("bytes=a-b", 26), Ignore);
        assert_eq!(evaluate_range("items=0-4", 26), Ignore);
        assert_eq!(evaluate_range("", 26), Ignore);
    }
}
