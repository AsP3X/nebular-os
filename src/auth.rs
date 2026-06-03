use axum::{
    extract::{Request, State},
    http::{header, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use hmac::{Hmac, Mac};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use std::sync::Arc;

use crate::config::BucketPolicy;
use crate::routes::AppState;

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

/// Human: After authentication succeeds, decide if this principal may call this method on this bucket.
/// Agent: presigned=>allow (method bound by signature); else role_allows_method AND bucket_policy allows sub.
pub fn authorize_request(
    claims: &Claims,
    method: &Method,
    bucket: &str,
    bucket_policy: &BucketPolicy,
) -> bool {
    if claims.sub == "presigned" {
        return true;
    }
    if !role_allows_method(&claims.role, method) {
        return false;
    }
    bucket_policy.allows(&claims.sub, bucket)
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

fn constant_time_eq(a: &str, b: &str) -> bool {
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

fn try_nos_access_key_auth(
    auth_header: &str,
    method: &str,
    bucket: &str,
    _key: &str,
    access_key: &str,
    secret_key: &str,
) -> Option<Claims> {
    let rest = auth_header.strip_prefix("NOS ")?;
    let (key, sig) = rest.split_once(':')?;
    if key != access_key {
        return None;
    }
    let payload = format!("{}
{}
{}
", method.to_uppercase(), bucket, key);
    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes()).ok()?;
    mac.update(payload.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());
    if !constant_time_eq(sig, &expected) {
        return None;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    Some(Claims {
        sub: key.to_string(),
        email: format!("{key}@nos-access-key"),
        role: "admin".to_string(),
        exp: now + 3600,
        iat: now,
    })
}

pub async fn presigned_or_jwt_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();
    let http_method = req.method().clone();
    let method = http_method.to_string();
    let bucket = request_bucket(&req);
    let path_segments = req.uri().path();
    let segments: Vec<&str> = path_segments.trim_start_matches('/').splitn(2, '/').collect();
    let key = segments.get(1).copied().unwrap_or("");
    let key = urlencoding::decode(key)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| key.to_string());

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
        .and_then(|h| h.to_str().ok());

    if let (Some(header), Some(access_key), Some(secret_key)) = (
        auth_header,
        state.config.s3_access_key.as_deref(),
        state.config.s3_secret_key.as_deref(),
    ) && let Some(claims) = try_nos_access_key_auth(
        header,
        http_method.as_str(),
        &bucket,
        &key,
        access_key,
        secret_key,
    ) {
        if !authorize_request(&claims, &http_method, &bucket, &state.config.bucket_policy) {
            return forbidden();
        }
        req.extensions_mut().insert(claims);
        return next.run(req).await;
    }

    if let Some(header) = auth_header
        && let Some(token) = header.strip_prefix("Bearer ") {
            let mut validation = Validation::new(Algorithm::HS256);
            validation.validate_exp = true;
            validation.validate_nbf = false;

            if let Ok(token_data) = decode::<Claims>(
                token,
                &DecodingKey::from_secret(jwt_secret.0.as_bytes()),
                &validation,
            ) {
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

    let method_str = req.method().as_str();
    if verify_signature(method_str, &secret, &bucket, &key, expires, &signature) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let claims = Claims {
            sub: "presigned".to_string(),
            email: "presigned".to_string(),
            role: "listener".to_string(),
            exp: i64::try_from(expires).unwrap_or(i64::MAX),
            iat: now,
        };
        if !authorize_request(&claims, &http_method, &bucket, &state.config.bucket_policy) {
            return forbidden();
        }
        tracing::info!(%bucket, %key, %method, expires, "presigned auth accepted");
        req.extensions_mut().insert(claims);
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
}
