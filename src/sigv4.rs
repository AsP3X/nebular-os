//! Human: AWS Signature Version 4 for access-key requests (NOS_S3_ACCESS_KEY / NOS_S3_SECRET_KEY): the
//! `Authorization: AWS4-HMAC-SHA256 …` header and presigned `X-Amz-*` query URLs. A signature binds the
//! method, path, query, signed headers, payload hash and a timestamp, so it can't be replayed for other
//! requests or indefinitely — unlike the legacy `NOS` scheme, which signed only method and bucket.
//! Agent: PURE verification (no I/O); callers pass `now` (unix seconds). aws-chunked streaming is refused.

use axum::http::HeaderMap;
use chrono::NaiveDateTime;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const SERVICE: &str = "s3";
const TERMINATOR: &str = "aws4_request";
const DATE_FORMAT: &str = "%Y%m%dT%H%M%SZ";
/// How far a header-signed request's timestamp may be from our clock (AWS uses 15 minutes).
const MAX_CLOCK_SKEW_SECS: i64 = 15 * 60;
/// Longest lifetime AWS allows a presigned URL.
const MAX_PRESIGN_EXPIRES_SECS: u64 = 7 * 24 * 3600;

type HmacSha256 = Hmac<Sha256>;

/// The access key the server accepts.
#[derive(Clone, Copy)]
pub struct Credentials<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
}

/// The request as it arrived: raw (still percent-encoded) path and query.
#[derive(Clone, Copy)]
pub struct RequestParts<'a> {
    pub method: &'a str,
    pub raw_path: &'a str,
    pub raw_query: &'a str,
    pub headers: &'a HeaderMap,
}

/// What a valid signature says about the request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadHash {
    /// `UNSIGNED-PAYLOAD` (always the case for presigned URLs): the body isn't covered.
    Unsigned,
    /// The body must hash to this SHA-256.
    Sha256([u8; 32]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SigV4Error {
    Malformed(&'static str),
    UnknownAccessKey,
    Expired,
    ClockSkew,
    BadSignature,
    /// aws-chunked (`STREAMING-…`) payloads, which sign each chunk separately.
    UnsupportedPayload,
}

impl std::fmt::Display for SigV4Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(what) => write!(f, "malformed SigV4 request: {what}"),
            Self::UnknownAccessKey => f.write_str("unknown access key"),
            Self::Expired => f.write_str("presigned URL expired"),
            Self::ClockSkew => f.write_str("request time too far from server time"),
            Self::BadSignature => f.write_str("signature does not match"),
            Self::UnsupportedPayload => f.write_str(
                "aws-chunked payload signing is not supported; sign with UNSIGNED-PAYLOAD or the body's SHA-256",
            ),
        }
    }
}

/// True when the query carries a presigned SigV4 signature.
pub fn is_presigned(raw_query: &str) -> bool {
    raw_query
        .split('&')
        .any(|pair| pair.split_once('=').map_or(pair, |(k, _)| k) == "X-Amz-Algorithm")
}

/// Human: Verify an `Authorization: AWS4-HMAC-SHA256 Credential=…, SignedHeaders=…, Signature=…` request.
/// Agent: REQUIRES x-amz-date within ±15 min and host + every x-amz-*/x-nd-* header signed; RETURNS the
/// payload hash the caller must enforce on the body.
pub fn verify_header(
    parts: RequestParts<'_>,
    authorization: &str,
    creds: Credentials<'_>,
    now: i64,
) -> Result<PayloadHash, SigV4Error> {
    let fields = authorization
        .strip_prefix(ALGORITHM)
        .filter(|rest| rest.starts_with(' '))
        .ok_or(SigV4Error::Malformed("authorization algorithm"))?;
    let (mut credential, mut signed_headers, mut signature) = (None, None, None);
    for field in fields.split(',') {
        let (name, value) = field
            .trim()
            .split_once('=')
            .ok_or(SigV4Error::Malformed("authorization field"))?;
        match name {
            "Credential" => credential = Some(value),
            "SignedHeaders" => signed_headers = Some(value),
            "Signature" => signature = Some(value),
            _ => return Err(SigV4Error::Malformed("authorization field")),
        }
    }
    let credential = credential.ok_or(SigV4Error::Malformed("missing Credential"))?;
    let signed_headers = signed_headers.ok_or(SigV4Error::Malformed("missing SignedHeaders"))?;
    let signature = signature.ok_or(SigV4Error::Malformed("missing Signature"))?;

    let amz_date = header_str(parts.headers, "x-amz-date").ok_or(SigV4Error::Malformed("missing x-amz-date"))?;
    let request_time = parse_amz_date(amz_date)?;
    if (now - request_time).abs() > MAX_CLOCK_SKEW_SECS {
        return Err(SigV4Error::ClockSkew);
    }
    let signed = parse_signed_headers(signed_headers, parts.headers, &["host", "x-amz-date"])?;
    let (payload_hash, canonical_payload) = match header_str(parts.headers, "x-amz-content-sha256") {
        None => (PayloadHash::Sha256(decode_sha256(EMPTY_SHA256)?), EMPTY_SHA256),
        Some(UNSIGNED_PAYLOAD) => (PayloadHash::Unsigned, UNSIGNED_PAYLOAD),
        Some(value) if value.starts_with("STREAMING-") => return Err(SigV4Error::UnsupportedPayload),
        Some(value) => (PayloadHash::Sha256(decode_sha256(value)?), value),
    };
    let canonical = canonical_request(parts, &canonical_query(parts.raw_query, false), &signed, canonical_payload)?;
    check_signature(creds, credential, amz_date, &canonical, signature)?;
    Ok(payload_hash)
}

/// Human: Verify a presigned URL (`X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
/// `X-Amz-SignedHeaders`, `X-Amz-Signature`). The body is never covered.
/// Agent: `max_ttl_secs` 0 = AWS's 7-day limit only; issue time may run ahead of ours by the skew allowance.
pub fn verify_presigned(parts: RequestParts<'_>, creds: Credentials<'_>, now: i64, max_ttl_secs: u64) -> Result<(), SigV4Error> {
    let param = |name: &str| -> Result<String, SigV4Error> {
        parts
            .raw_query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find(|(k, _)| *k == name)
            .map(|(_, v)| percent_decode(v))
            .ok_or(SigV4Error::Malformed("missing presign parameter"))
    };
    if param("X-Amz-Algorithm")? != ALGORITHM {
        return Err(SigV4Error::Malformed("presign algorithm"));
    }
    let credential = param("X-Amz-Credential")?;
    let amz_date = param("X-Amz-Date")?;
    let signature = param("X-Amz-Signature")?;
    let expires: u64 = param("X-Amz-Expires")?
        .parse()
        .map_err(|_| SigV4Error::Malformed("X-Amz-Expires"))?;
    let limit = match max_ttl_secs {
        0 => MAX_PRESIGN_EXPIRES_SECS,
        ttl => ttl.min(MAX_PRESIGN_EXPIRES_SECS),
    };
    if expires == 0 || expires > limit {
        return Err(SigV4Error::Malformed("X-Amz-Expires out of range"));
    }
    let issued = parse_amz_date(&amz_date)?;
    if issued - now > MAX_CLOCK_SKEW_SECS {
        return Err(SigV4Error::ClockSkew);
    }
    if now > issued.saturating_add(expires as i64) {
        return Err(SigV4Error::Expired);
    }
    let signed = parse_signed_headers(&param("X-Amz-SignedHeaders")?, parts.headers, &["host"])?;
    let canonical = canonical_request(parts, &canonical_query(parts.raw_query, true), &signed, UNSIGNED_PAYLOAD)?;
    check_signature(creds, &credential, &amz_date, &canonical, &signature)
}

fn check_signature(
    creds: Credentials<'_>,
    credential: &str,
    amz_date: &str,
    canonical_request: &str,
    signature: &str,
) -> Result<(), SigV4Error> {
    // Human: Credential = AKID/YYYYMMDD/region/s3/aws4_request (the access key id itself has no '/').
    let mut scope = credential.splitn(2, '/');
    let access_key = scope.next().unwrap_or_default();
    let scope = scope.next().ok_or(SigV4Error::Malformed("credential scope"))?;
    let scope_parts: Vec<&str> = scope.split('/').collect();
    let [date, region, service, terminator] = scope_parts[..] else {
        return Err(SigV4Error::Malformed("credential scope"));
    };
    if service != SERVICE || terminator != TERMINATOR || region.is_empty() || !amz_date.starts_with(date) {
        return Err(SigV4Error::Malformed("credential scope"));
    }
    if !crate::auth::constant_time_eq(access_key, creds.access_key) {
        return Err(SigV4Error::UnknownAccessKey);
    }
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let mut key = hmac(format!("AWS4{}", creds.secret_key).as_bytes(), date.as_bytes());
    for part in [region, service, terminator] {
        key = hmac(&key, part.as_bytes());
    }
    let expected = hex::encode(hmac(&key, string_to_sign.as_bytes()));
    if crate::auth::constant_time_eq(&signature.to_ascii_lowercase(), &expected) {
        Ok(())
    } else {
        Err(SigV4Error::BadSignature)
    }
}

fn canonical_request(
    parts: RequestParts<'_>,
    canonical_query: &str,
    signed: &[String],
    payload: &str,
) -> Result<String, SigV4Error> {
    let mut canonical_headers = String::new();
    for name in signed {
        let values: Vec<String> = parts
            .headers
            .get_all(name.as_str())
            .iter()
            .map(|v| v.to_str().map(collapse_whitespace))
            .collect::<Result<_, _>>()
            .map_err(|_| SigV4Error::Malformed("header value"))?;
        canonical_headers.push_str(&format!("{name}:{}\n", values.join(",")));
    }
    Ok(format!(
        "{}\n{}\n{canonical_query}\n{canonical_headers}\n{}\n{payload}",
        parts.method,
        canonical_uri(parts.raw_path),
        signed.join(";"),
    ))
}

/// Human: The signed header list, which must include `required` and every header that changes what a request
/// does (x-amz-*, x-nd-*): an unsigned `x-nd-copy-source` added to a presigned PUT would otherwise copy with
/// the key's authority.
fn parse_signed_headers(list: &str, headers: &HeaderMap, required: &[&str]) -> Result<Vec<String>, SigV4Error> {
    let signed: Vec<String> = list.split(';').map(str::to_ascii_lowercase).collect();
    if signed.iter().any(String::is_empty) || !signed.windows(2).all(|w| w[0] < w[1]) {
        return Err(SigV4Error::Malformed("SignedHeaders must be sorted and unique"));
    }
    for name in required {
        if !signed.iter().any(|s| s == name) {
            return Err(SigV4Error::Malformed("required header not signed"));
        }
    }
    for name in headers.keys() {
        let name = name.as_str();
        if (name.starts_with("x-amz-") || name.starts_with("x-nd-")) && !signed.iter().any(|s| s == name) {
            return Err(SigV4Error::Malformed("x-amz-* / x-nd-* header not signed"));
        }
    }
    for name in &signed {
        if headers.get(name.as_str()).is_none() {
            return Err(SigV4Error::Malformed("signed header missing"));
        }
    }
    Ok(signed)
}

/// S3's canonical URI: the decoded path re-encoded once (RFC 3986 unreserved characters and `/` kept).
fn canonical_uri(raw_path: &str) -> String {
    let decoded = urlencoding::decode_binary(raw_path.as_bytes());
    let uri = uri_encode(&decoded, false);
    if uri.is_empty() { "/".to_string() } else { uri }
}

/// Sorted `name=value` pairs, each re-encoded; presigned URLs leave out their own signature.
fn canonical_query(raw_query: &str, drop_signature: bool) -> String {
    let mut pairs: Vec<(String, String)> = raw_query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                uri_encode(&urlencoding::decode_binary(name.as_bytes()), true),
                uri_encode(&urlencoding::decode_binary(value.as_bytes()), true),
            )
        })
        .filter(|(name, _)| !(drop_signature && name == "X-Amz-Signature"))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn uri_encode(bytes: &[u8], encode_slash: bool) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') || (b == b'/' && !encode_slash) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn percent_decode(value: &str) -> String {
    String::from_utf8_lossy(&urlencoding::decode_binary(value.as_bytes())).into_owned()
}

fn collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn parse_amz_date(value: &str) -> Result<i64, SigV4Error> {
    NaiveDateTime::parse_from_str(value, DATE_FORMAT)
        .map(|t| t.and_utc().timestamp())
        .map_err(|_| SigV4Error::Malformed("x-amz-date"))
}

fn decode_sha256(value: &str) -> Result<[u8; 32], SigV4Error> {
    let mut out = [0u8; 32];
    hex::decode_to_slice(value, &mut out).map_err(|_| SigV4Error::Malformed("x-amz-content-sha256"))?;
    Ok(out)
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    // Human: The worked examples from AWS's "Signature Calculations for the Authorization Header" and
    // "Authenticating Requests: Using Query Parameters" pages for S3.
    const AWS_KEY: Credentials<'static> = Credentials {
        access_key: "AKIAIOSFODNN7EXAMPLE",
        secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    };
    const EXAMPLE_TIME: i64 = 1_369_353_600; // 2013-05-24T00:00:00Z

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, HeaderValue::from_str(value).unwrap());
        }
        map
    }

    fn get_object_headers() -> HeaderMap {
        headers(&[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ])
    }

    const GET_OBJECT_AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";

    #[test]
    fn aws_get_object_example_verifies() {
        let headers = get_object_headers();
        let parts = RequestParts { method: "GET", raw_path: "/test.txt", raw_query: "", headers: &headers };
        assert_eq!(
            verify_header(parts, GET_OBJECT_AUTH, AWS_KEY, EXAMPLE_TIME),
            Ok(PayloadHash::Sha256(decode_sha256(EMPTY_SHA256).unwrap()))
        );
    }

    #[test]
    fn tampering_or_replaying_the_example_fails() {
        let headers = get_object_headers();
        let parts = RequestParts { method: "GET", raw_path: "/test.txt", raw_query: "", headers: &headers };
        let other_path = RequestParts { raw_path: "/other.txt", ..parts };
        assert_eq!(verify_header(other_path, GET_OBJECT_AUTH, AWS_KEY, EXAMPLE_TIME), Err(SigV4Error::BadSignature));
        let other_method = RequestParts { method: "DELETE", ..parts };
        assert_eq!(verify_header(other_method, GET_OBJECT_AUTH, AWS_KEY, EXAMPLE_TIME), Err(SigV4Error::BadSignature));
        let other_query = RequestParts { raw_query: "versionId=1", ..parts };
        assert_eq!(verify_header(other_query, GET_OBJECT_AUTH, AWS_KEY, EXAMPLE_TIME), Err(SigV4Error::BadSignature));
        // Human: The same request an hour later is refused: the signed timestamp is outside the skew window.
        assert_eq!(verify_header(parts, GET_OBJECT_AUTH, AWS_KEY, EXAMPLE_TIME + 3600), Err(SigV4Error::ClockSkew));
        let wrong_key = Credentials { secret_key: "not-the-secret", ..AWS_KEY };
        assert_eq!(verify_header(parts, GET_OBJECT_AUTH, wrong_key, EXAMPLE_TIME), Err(SigV4Error::BadSignature));
        let other_id = Credentials { access_key: "AKIDOTHER", ..AWS_KEY };
        assert_eq!(verify_header(parts, GET_OBJECT_AUTH, other_id, EXAMPLE_TIME), Err(SigV4Error::UnknownAccessKey));
    }

    #[test]
    fn behaviour_changing_headers_must_be_signed() {
        let mut headers = get_object_headers();
        headers.insert("x-nd-copy-source", HeaderValue::from_static("private/secret.txt"));
        let parts = RequestParts { method: "GET", raw_path: "/test.txt", raw_query: "", headers: &headers };
        assert!(matches!(
            verify_header(parts, GET_OBJECT_AUTH, AWS_KEY, EXAMPLE_TIME),
            Err(SigV4Error::Malformed(_))
        ));
    }

    #[test]
    fn aws_chunked_payloads_are_refused() {
        let mut headers = get_object_headers();
        headers.insert("x-amz-content-sha256", HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"));
        let parts = RequestParts { method: "PUT", raw_path: "/test.txt", raw_query: "", headers: &headers };
        assert_eq!(verify_header(parts, GET_OBJECT_AUTH, AWS_KEY, EXAMPLE_TIME), Err(SigV4Error::UnsupportedPayload));
    }

    const PRESIGNED_QUERY: &str = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";

    #[test]
    fn aws_presigned_example_verifies_until_it_expires() {
        let headers = headers(&[("host", "examplebucket.s3.amazonaws.com")]);
        let parts = RequestParts { method: "GET", raw_path: "/test.txt", raw_query: PRESIGNED_QUERY, headers: &headers };
        assert!(is_presigned(PRESIGNED_QUERY));
        assert_eq!(verify_presigned(parts, AWS_KEY, EXAMPLE_TIME + 60, 0), Ok(()));
        assert_eq!(verify_presigned(parts, AWS_KEY, EXAMPLE_TIME + 86_401, 0), Err(SigV4Error::Expired));
        // Human: A server-side TTL cap below the URL's own lifetime refuses it outright.
        assert!(matches!(verify_presigned(parts, AWS_KEY, EXAMPLE_TIME + 60, 3600), Err(SigV4Error::Malformed(_))));
        let put = RequestParts { method: "PUT", ..parts };
        assert_eq!(verify_presigned(put, AWS_KEY, EXAMPLE_TIME + 60, 0), Err(SigV4Error::BadSignature));
    }

    #[test]
    fn canonical_uri_and_query_encoding() {
        assert_eq!(canonical_uri("/bucket/a%20b%2Bc%25.txt"), "/bucket/a%20b%2Bc%25.txt");
        assert_eq!(canonical_uri("/bucket/a b+c.txt"), "/bucket/a%20b%2Bc.txt");
        assert_eq!(canonical_uri("/bucket/%E2%82%AC/x"), "/bucket/%E2%82%AC/x");
        assert_eq!(canonical_query("prefix=a/b&max=10&empty", false), "empty=&max=10&prefix=a%2Fb");
        assert_eq!(canonical_query("b=2&a=1&X-Amz-Signature=abc", true), "a=1&b=2");
    }
}
