pub mod xml;

use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

/// Human: Detect when a client expects AWS S3 XML (ListObjectsV2 or AWS SDK headers).
/// Agent: TRUE when NOS_S3_COMPAT and (list-type=2 query OR x-amz-* request header present).
pub fn wants_s3_response(headers: &HeaderMap, query: Option<&str>, s3_compat: bool) -> bool {
    if !s3_compat {
        return false;
    }
    if let Some(q) = query {
        for (k, v) in url::form_urlencoded::parse(q.as_bytes()) {
            if k == "list-type" && v == "2" {
                return true;
            }
        }
    }
    headers.keys().any(|k| {
        k.as_str()
            .to_ascii_lowercase()
            .starts_with("x-amz-")
    })
}

/// Human: Build S3-style XML error body while keeping status codes aligned with native API.
/// Agent: EMITS <Error><Code/><Message/></Error>; Content-Type application/xml.
pub fn s3_error_response(status: StatusCode, code: &str, message: &str) -> Response {
    let body = xml::error_xml(code, message);
    (
        status,
        [(header::CONTENT_TYPE, "application/xml")],
        body,
    )
        .into_response()
}

/// Human: When S3 compat is on, map native JSON errors to XML for clients that requested S3 mode.
/// Agent: WRAPS error string; checks use_xml flag from list handler.
pub fn maybe_s3_json_error(
    status: StatusCode,
    message: &str,
    s3_compat: bool,
    use_xml: bool,
) -> Response {
    if s3_compat && use_xml {
        let code = match status {
            StatusCode::NOT_FOUND => "NoSuchKey",
            StatusCode::UNAUTHORIZED => "AccessDenied",
            StatusCode::FORBIDDEN => "AccessDenied",
            StatusCode::PAYLOAD_TOO_LARGE => "EntityTooLarge",
            StatusCode::RANGE_NOT_SATISFIABLE => "InvalidRange",
            StatusCode::TOO_MANY_REQUESTS => "SlowDown",
            _ => "InternalError",
        };
        return s3_error_response(status, code, message);
    }
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}
