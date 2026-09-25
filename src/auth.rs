use axum::{
    body::Body,
    extract::{MatchedPath, Request, State},
    http::{header, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use hmac::{Hmac, Mac};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::config::{BucketPolicy, NosConfig};
use crate::middleware::rate_limit::{client_ip, take_token, too_many_requests};
use crate::routes::body_digest::PayloadSha256;
use crate::routes::AppState;
use crate::sigv4;

/// Human: Maps JWT role strings to allowed HTTP verbs on protected object routes.
/// Agent: admin=all; editor|uploader=mutations+read; listener|readonly=GET|HEAD only; unknown=deny.
pub fn role_allows_method(role: &str, method: &Method) -> bool {
    match role.to_ascii_lowercase().as_str() {
        "admin" => true,
        "editor" | "uploader" => true,
        "listener" | "readonly" | "read_only" => {
            matches!(method, &Method::GET | &Method::HEAD)
        }
        _ => false,
    }
}

/// True for the role names `role_allows_method` grants anything to.
pub fn is_known_role(role: &str) -> bool {
    matches!(
        role.to_ascii_lowercase().as_str(),
        "admin" | "editor" | "uploader" | "listener" | "readonly" | "read_only"
    )
}

/// Human: How the caller authenticated; inserted into request extensions next to `Claims`.
/// Agent: Presigned => the signature binds one method on one bucket/key and grants nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Jwt,
    AccessKey,
    Presigned,
}

/// Subject on the synthetic claims of presigned requests; reserved, so JWTs carrying it are rejected.
const PRESIGNED_SUB: &str = "presigned";

/// Human: The only route a presigned URL may address — its signature binds method, bucket and key, and
/// nothing else about the request, so routes steered by query strings or bodies (list, prefix/batch delete,
/// multipart) must use a JWT.
const PRESIGNED_ROUTE: &str = "/{bucket}/{*key}";

/// Tolerance for signer clocks ahead of ours when enforcing NOS_PRESIGN_MAX_TTL_SECS.
const PRESIGN_CLOCK_SKEW_SECS: u64 = 300;

/// Routes whose body is an upload, streamed to storage; a signed payload hash is checked while it streams.
const STREAMED_UPLOAD_ROUTES: [&str; 2] = [PRESIGNED_ROUTE, "/{bucket}/_multipart/{upload_id}/parts/{part_number}"];

/// Largest non-upload body (batch delete, multipart complete) buffered to check a signed payload hash.
const MAX_BUFFERED_SIGNED_BODY: usize = 1024 * 1024;

/// Human: After JWT or access-key authentication, decide if this principal may call this method on this bucket.
/// Agent: role_allows_method AND bucket_policy allows sub; presigned requests never reach this check.
pub fn authorize_request(
    claims: &Claims,
    method: &Method,
    bucket: &str,
    bucket_policy: &BucketPolicy,
) -> bool {
    if !role_allows_method(&claims.role, method) {
        return false;
    }
    bucket_policy.allows(&claims.sub, bucket)
}

/// Human: Server-side copy reads a second object, so its source bucket needs its own read check.
/// Agent: Presigned or unauthenticated => deny; JWT/access key => authorize_request(GET, src_bucket).
pub fn authorize_copy_source(
    auth: Option<AuthMethod>,
    claims: Option<&Claims>,
    src_bucket: &str,
    bucket_policy: &BucketPolicy,
) -> bool {
    match (auth, claims) {
        (Some(AuthMethod::Jwt | AuthMethod::AccessKey), Some(claims)) => {
            authorize_request(claims, &Method::GET, src_bucket, bucket_policy)
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub email: String,
    pub role: String,
    pub exp: i64,
    pub iat: i64,
}

pub struct JwtSecret(pub String);

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error": "forbidden"})),
    )
        .into_response()
}

fn unauthorized() -> Response {
    let body = Json(json!({"error": "unauthorized"}));
    let mut resp = (StatusCode::UNAUTHORIZED, body).into_response();
    resp.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        "Bearer".parse().unwrap(),
    );
    resp
}

type HmacSha256 = Hmac<Sha256>;

/// Generates a presigned URL signature.
/// The signed payload format is: "{METHOD}\n{bucket}\n{key}\n{expires}"
/// Keys must not contain newlines (enforced by sanitize_key).
pub fn generate_signature(
    method: &str,
    secret: &str,
    bucket: &str,
    key: &str,
    expires: u64,
) -> anyhow::Result<String> {
    let payload = format!("{}\n{}\n{}\n{}", method.to_uppercase(), bucket, key, expires);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
    mac.update(payload.as_bytes());
    let result = mac.finalize();
    Ok(hex::encode(result.into_bytes()))
}

pub(crate) fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut result = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        result |= x ^ y;
    }
    result == 0
}

pub fn verify_signature(
    method: &str,
    secret: &str,
    bucket: &str,
    key: &str,
    expires: u64,
    signature: &str,
) -> bool {
    let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => return false,
    };
    if expires <= now {
        return false;
    }
    let expected = match generate_signature(method, secret, bucket, key, expires) {
        Ok(sig) => sig,
        Err(_) => return false,
    };
    constant_time_eq(signature, &expected)
}

/// True for object GET/HEAD paths (`/{bucket}/{key}`), not bucket list or system routes.
fn is_public_object_read(req: &Request) -> bool {
    let method = req.method();
    if method != Method::GET && method != Method::HEAD {
        return false;
    }
    let path = req.uri().path().trim_start_matches('/');
    if path == "health" || path.starts_with("health/") || path == "metrics" {
        return false;
    }
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    segments.len() >= 2
}

fn request_bucket(req: &Request) -> String {
    let path_segments = req.uri().path();
    let segments: Vec<&str> = path_segments.trim_start_matches('/').splitn(2, '/').collect();
    let bucket = segments.first().copied().unwrap_or("");
    urlencoding::decode(bucket)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| bucket.to_string())
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Claims for a request signed with the configured access key.
fn access_key_claims(access_key: &str, role: &str, now: i64) -> Claims {
    Claims {
        sub: access_key.to_string(),
        email: format!("{access_key}@nos-access-key"),
        role: role.to_string(),
        exp: now + 3600,
        iat: now,
    }
}

/// Human: The legacy `NOS <key>:<hex hmac(METHOD\nbucket\nkey\n)>` scheme — it doesn't cover the object key,
/// query or time, so any captured signature works forever. Only honoured with NOS_LEGACY_ACCESS_KEY_AUTH.
fn verify_legacy_access_key(
    auth_header: &str,
    method: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
) -> bool {
    let Some((key, sig)) = auth_header
        .strip_prefix("NOS ")
        .and_then(|rest| rest.split_once(':'))
    else {
        return false;
    };
    if !constant_time_eq(key, access_key) {
        return false;
    }
    let payload = format!("{}\n{}\n{}\n", method.to_uppercase(), bucket, key);
    let Ok(mut mac) = HmacSha256::new_from_slice(secret_key.as_bytes()) else {
        return false;
    };
    mac.update(payload.as_bytes());
    constant_time_eq(sig, &hex::encode(mac.finalize().into_bytes()))
}

fn jwt_validation(cfg: &NosConfig) -> Validation {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    validation.validate_nbf = false;
    let mut required = vec!["exp"];
    if let Some(issuer) = &cfg.jwt_issuer {
        validation.set_issuer(&[issuer]);
        required.push("iss");
    }
    if let Some(audience) = &cfg.jwt_audience {
        validation.set_audience(&[audience]);
        required.push("aud");
    }
    validation.set_required_spec_claims(&required);
    validation
}

fn bad_digest() -> Response {
    let message = crate::storage::error::BadDigest.to_string();
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
}

/// Human: Authenticate a SigV4 request (Authorization header or presigned query) against the configured access
/// key. The signature binds the whole request, so the key's role and bucket policy apply as for a JWT; a signed
/// payload hash is enforced on the body — while an upload streams, or here for small JSON bodies.
async fn authenticate_sigv4(
    state: &AppState,
    mut req: Request,
    next: Next,
    authorization: Option<&str>,
    bucket: &str,
) -> Response {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let (Some(access_key), Some(secret_key)) = (
        state.config.s3_access_key.as_deref(),
        state.config.s3_secret_key.as_deref(),
    ) else {
        tracing::warn!(%path, %method, "SigV4 auth rejected: NOS_S3_ACCESS_KEY / NOS_S3_SECRET_KEY are not set");
        return unauthorized();
    };
    let creds = sigv4::Credentials { access_key, secret_key };
    // Human: Like Nebular's own presigned URLs, SigV4 query signing (UNSIGNED-PAYLOAD) is for one object's route:
    // list, prefix/batch delete and multipart are steered by a body or query the URL doesn't pin down, so a URL
    // presigned for `POST /{bucket}/_batch_delete` let whoever held it delete any key in the bucket.
    let header_signed = authorization.is_some_and(|h| h.starts_with(sigv4::ALGORITHM));
    let on_object_route = req
        .extensions()
        .get::<MatchedPath>()
        .is_some_and(|matched| matched.as_str() == PRESIGNED_ROUTE);
    if !header_signed && !on_object_route {
        tracing::warn!(%path, %method, "SigV4 presigned auth rejected: not an object route");
        return unauthorized();
    }
    let now = unix_now();
    let verified = {
        let parts = sigv4::RequestParts {
            method: method.as_str(),
            raw_path: req.uri().path(),
            raw_query: req.uri().query().unwrap_or(""),
            headers: req.headers(),
        };
        match authorization.filter(|h| h.starts_with(sigv4::ALGORITHM)) {
            Some(header) => sigv4::verify_header(parts, header, creds, now),
            None => sigv4::verify_presigned(parts, creds, now, state.config.presign_max_ttl_secs)
                .map(|()| sigv4::PayloadHash::Unsigned),
        }
    };
    let payload = match verified {
        Ok(payload) => payload,
        Err(e @ sigv4::SigV4Error::UnsupportedPayload) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": e.to_string() }))).into_response();
        }
        Err(e) => {
            tracing::warn!(%path, %method, reason = %e, "SigV4 auth rejected");
            return unauthorized();
        }
    };
    let claims = access_key_claims(access_key, &state.config.s3_access_key_role, now);
    if !authorize_request(&claims, &method, bucket, &state.config.bucket_policy) {
        return forbidden();
    }
    tracing::info!(%path, %method, "SigV4 auth accepted");
    req.extensions_mut().insert(claims);
    req.extensions_mut().insert(AuthMethod::AccessKey);
    if let sigv4::PayloadHash::Sha256(expected) = payload {
        let streamed = method == Method::PUT
            && req
                .extensions()
                .get::<MatchedPath>()
                .is_some_and(|matched| STREAMED_UPLOAD_ROUTES.contains(&matched.as_str()));
        if streamed {
            req.extensions_mut().insert(PayloadSha256(expected));
        } else {
            let (parts, body) = req.into_parts();
            let Ok(bytes) = axum::body::to_bytes(body, MAX_BUFFERED_SIGNED_BODY).await else {
                return crate::routes::errors::payload_too_large_response();
            };
            if Sha256::digest(&bytes).as_slice() != expected {
                return bad_digest();
            }
            req = Request::from_parts(parts, Body::from(bytes));
        }
    }
    next.run(req).await
}

/// Human: Authenticate protected routes. With NOS_RATE_LIMIT_RPS set, failed attempts spend a per-IP budget
/// and are answered 429 once it is empty. Valid credentials always pass, so clients sharing an IP (proxy, NAT)
/// can't lock each other out.
/// Agent: Only UNAUTHORIZED outcomes touch the failure budget; the route rate limiter still runs after auth.
pub async fn presigned_or_jwt_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let rps = state.config.rate_limit_rps;
    if rps == 0 {
        return authenticate(state, req, next).await;
    }
    let client = client_ip(&req);
    let response = authenticate(state.clone(), req, next).await;
    if response.status() == StatusCode::UNAUTHORIZED
        && !take_token(
            &state.auth_failures,
            client,
            rps as f64,
            state.config.rate_limit_burst as f64,
        )
    {
        state.metrics.inc_errors();
        return too_many_requests();
    }
    response
}

async fn authenticate(state: Arc<AppState>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let http_method = req.method().clone();
    let method = http_method.to_string();
    let bucket = request_bucket(&req);

    // Human: Public-read mode allows unauthenticated GET/HEAD on objects only.
    // Agent: READS allow_public_read; BYPASS auth for GET|HEAD with >=2 path segments; LIST /{bucket} still requires JWT/presigned.
    if state.allow_public_read && is_public_object_read(&req) {
        tracing::info!(%path, %method, "public read accepted");
        return next.run(req).await;
    }

    let jwt_secret = state.jwt_secret.clone();
    let signing_secret = state.signing_secret.clone();

    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);

    if auth_header
        .as_deref()
        .is_some_and(|h| h.starts_with(sigv4::ALGORITHM))
        || req.uri().query().is_some_and(sigv4::is_presigned)
    {
        return authenticate_sigv4(&state, req, next, auth_header.as_deref(), &bucket).await;
    }

    if let Some(header) = auth_header.as_deref().filter(|h| h.starts_with("NOS ")) {
        if !state.config.legacy_access_key_auth {
            tracing::warn!(%path, %method, "legacy NOS access-key auth rejected: disabled (use SigV4 or set NOS_LEGACY_ACCESS_KEY_AUTH)");
            return unauthorized();
        }
        let (Some(access_key), Some(secret_key)) = (
            state.config.s3_access_key.as_deref(),
            state.config.s3_secret_key.as_deref(),
        ) else {
            return unauthorized();
        };
        if !verify_legacy_access_key(header, http_method.as_str(), &bucket, access_key, secret_key) {
            tracing::warn!(%path, %method, "legacy NOS access-key auth rejected: invalid signature");
            return unauthorized();
        }
        let claims = access_key_claims(access_key, &state.config.s3_access_key_role, unix_now());
        if !authorize_request(&claims, &http_method, &bucket, &state.config.bucket_policy) {
            return forbidden();
        }
        req.extensions_mut().insert(claims);
        req.extensions_mut().insert(AuthMethod::AccessKey);
        return next.run(req).await;
    }

    if let Some(token) = auth_header.as_deref().and_then(|h| h.strip_prefix("Bearer ")) {
            let validation = jwt_validation(&state.config);

            if let Ok(token_data) = decode::<Claims>(
                token,
                &DecodingKey::from_secret(jwt_secret.0.as_bytes()),
                &validation,
            ) {
                if token_data.claims.sub == PRESIGNED_SUB {
                    tracing::warn!(%path, %method, "jwt auth rejected: reserved subject");
                    return unauthorized();
                }
                if !authorize_request(
                    &token_data.claims,
                    &http_method,
                    &bucket,
                    &state.config.bucket_policy,
                ) {
                    return forbidden();
                }
                tracing::info!(sub = %token_data.claims.sub, role = %token_data.claims.role, %path, %method, "jwt auth accepted");
                req.extensions_mut().insert(token_data.claims);
                req.extensions_mut().insert(AuthMethod::Jwt);
                return next.run(req).await;
            } else {
                tracing::warn!(%path, %method, "jwt auth rejected: invalid token");
            }
        }

    let Some(secret) = signing_secret else {
        tracing::warn!(%path, %method, "auth failed: no signing_secret configured and no valid JWT");
        return unauthorized();
    };

    let query = req.uri().query().unwrap_or("");
    let mut signature = None;
    let mut expires = None;

    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        match k.as_ref() {
            "signature" => signature = Some(v.into_owned()),
            "expires" => expires = v.parse::<u64>().ok(),
            _ => {}
        }
    }

    let (Some(signature), Some(expires)) = (signature, expires) else {
        tracing::warn!(%path, %method, "presigned auth rejected: missing signature or expires");
        return unauthorized();
    };

    let path_segments = req.uri().path();
    let segments: Vec<&str> = path_segments.trim_start_matches('/').splitn(2, '/').collect();
    let bucket = segments.first().copied().unwrap_or("");
    let key = segments.get(1).copied().unwrap_or("");

    let bucket = urlencoding::decode(bucket).unwrap_or_else(|_| bucket.into());
    let key = urlencoding::decode(key).unwrap_or_else(|_| key.into());

    if bucket.is_empty() {
        tracing::warn!(%path, %method, "presigned auth rejected: empty bucket");
        return unauthorized();
    }

    let on_object_route = req
        .extensions()
        .get::<MatchedPath>()
        .is_some_and(|matched| matched.as_str() == PRESIGNED_ROUTE);
    if !on_object_route || key.is_empty() {
        tracing::warn!(%path, %method, "presigned auth rejected: not an object route");
        return unauthorized();
    }
    let max_ttl = state.config.presign_max_ttl_secs;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Human: Allow for signer clocks running a few minutes ahead of ours.
    if max_ttl > 0 && expires > now_secs.saturating_add(max_ttl).saturating_add(PRESIGN_CLOCK_SKEW_SECS) {
        tracing::warn!(%bucket, %key, %method, expires, "presigned auth rejected: expiry beyond NOS_PRESIGN_MAX_TTL_SECS");
        return unauthorized();
    }

    let method_str = req.method().as_str();
    if verify_signature(method_str, &secret, &bucket, &key, expires, &signature) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let claims = Claims {
            sub: PRESIGNED_SUB.to_string(),
            email: PRESIGNED_SUB.to_string(),
            role: "listener".to_string(),
            exp: i64::try_from(expires).unwrap_or(i64::MAX),
            iat: now,
        };
        // Human: No role/bucket-policy check — the verified signature already binds method, bucket and key.
        tracing::info!(%bucket, %key, %method, expires, "presigned auth accepted");
        req.extensions_mut().insert(claims);
        req.extensions_mut().insert(AuthMethod::Presigned);
        next.run(req).await
    } else {
        tracing::warn!(%bucket, %key, %method, expires, "presigned auth rejected: invalid signature");
        unauthorized()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BucketPolicy;

    #[test]
    fn listener_cannot_put() {
        assert!(!role_allows_method("listener", &Method::PUT));
        assert!(role_allows_method("listener", &Method::GET));
    }

    #[test]
    fn bucket_policy_restricts_sub() {
        let policy = BucketPolicy::from_json(r#"{"user-1":["music"]}"#).unwrap();
        let claims = Claims {
            sub: "user-1".into(),
            email: "a@b.c".into(),
            role: "admin".into(),
            exp: 0,
            iat: 0,
        };
        assert!(authorize_request(&claims, &Method::GET, "music", &policy));
        assert!(!authorize_request(&claims, &Method::GET, "other", &policy));
    }

    #[test]
    fn presigned_subject_gets_no_bypass() {
        let claims = Claims {
            sub: PRESIGNED_SUB.into(),
            email: "x@y.z".into(),
            role: "listener".into(),
            exp: 0,
            iat: 0,
        };
        let policy = BucketPolicy::default();
        assert!(!authorize_request(&claims, &Method::PUT, "music", &policy));
        assert!(!authorize_request(&claims, &Method::DELETE, "music", &policy));
    }

    #[test]
    fn copy_source_needs_read_access_to_source_bucket() {
        let policy = BucketPolicy::from_json(r#"{"user-1":["music"]}"#).unwrap();
        let claims = Claims {
            sub: "user-1".into(),
            email: "a@b.c".into(),
            role: "editor".into(),
            exp: 0,
            iat: 0,
        };
        let jwt = Some(AuthMethod::Jwt);
        assert!(authorize_copy_source(jwt, Some(&claims), "music", &policy));
        assert!(!authorize_copy_source(jwt, Some(&claims), "private", &policy));
        assert!(!authorize_copy_source(
            Some(AuthMethod::Presigned),
            Some(&claims),
            "music",
            &policy
        ));
        assert!(!authorize_copy_source(None, Some(&claims), "music", &policy));
        assert!(!authorize_copy_source(jwt, None, "music", &policy));
    }
}
