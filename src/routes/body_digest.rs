//! Human: End-to-end checks on upload bodies. `Content-MD5` and a SigV4-signed payload SHA-256 are verified
//! while the body streams; a mismatch fails the upload with `400` before anything is committed.
//! Agent: DigestCheck yields io::Error(BadDigest) in place of end-of-stream; map_io_error turns it into 400.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::http::{Extensions, HeaderMap};
use base64::Engine as _;
use bytes::Bytes;
use futures_util::Stream;
use md5::Md5;
use sha2::{Digest, Sha256};

use crate::storage::error::BadDigest;

/// Request extension: the SHA-256 a SigV4 signature bound the body to (set by the auth middleware).
#[derive(Debug, Clone, Copy)]
pub struct PayloadSha256(pub [u8; 32]);

/// Digests an upload body must match.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExpectedDigests {
    pub md5: Option<[u8; 16]>,
    pub sha256: Option<[u8; 32]>,
}

impl ExpectedDigests {
    /// From `Content-MD5` (base64 of the MD5, RFC 1864) and the signed payload hash; `Err` = unusable header.
    pub fn from_request(headers: &HeaderMap, extensions: &Extensions) -> Result<Self, &'static str> {
        let md5 = match headers.get("content-md5") {
            None => None,
            Some(value) => {
                let decoded = value
                    .to_str()
                    .ok()
                    .and_then(|v| base64::engine::general_purpose::STANDARD.decode(v.trim()).ok())
                    .ok_or("Content-MD5 is not valid base64")?;
                Some(<[u8; 16]>::try_from(decoded.as_slice()).map_err(|_| "Content-MD5 is not an MD5 digest")?)
            }
        };
        Ok(Self {
            md5,
            sha256: extensions.get::<PayloadSha256>().map(|p| p.0),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.md5.is_none() && self.sha256.is_none()
    }
}

/// Human: Passes body chunks through while hashing them; at the end, a mismatch replaces end-of-stream with
/// an error so the upload is abandoned instead of committed.
pub struct DigestCheck<S> {
    inner: S,
    md5: Option<(Md5, [u8; 16])>,
    sha256: Option<(Sha256, [u8; 32])>,
    finished: bool,
}

impl<S> DigestCheck<S> {
    pub fn new(inner: S, expected: ExpectedDigests) -> Self {
        Self {
            inner,
            md5: expected.md5.map(|d| (Md5::new(), d)),
            sha256: expected.sha256.map(|d| (Sha256::new(), d)),
            finished: false,
        }
    }

    fn matches(&mut self) -> bool {
        let md5_ok = self
            .md5
            .take()
            .is_none_or(|(hasher, expected)| hasher.finalize().as_slice() == expected);
        let sha_ok = self
            .sha256
            .take()
            .is_none_or(|(hasher, expected)| hasher.finalize().as_slice() == expected);
        md5_ok && sha_ok
    }
}

impl<S> Stream for DigestCheck<S>
where
    S: Stream<Item = io::Result<Bytes>> + Unpin,
{
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                if let Some((hasher, _)) = self.md5.as_mut() {
                    hasher.update(&chunk);
                }
                if let Some((hasher, _)) = self.sha256.as_mut() {
                    hasher.update(&chunk);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(None) => {
                self.finished = true;
                if self.matches() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(io::Error::other(BadDigest))))
                }
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    fn body(chunks: &[&'static [u8]]) -> impl Stream<Item = io::Result<Bytes>> + Unpin {
        futures_util::stream::iter(chunks.iter().map(|c| Ok(Bytes::from_static(c))).collect::<Vec<_>>())
    }

    async fn drain(mut stream: impl Stream<Item = io::Result<Bytes>> + Unpin) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn matching_digests_pass_the_body_through() {
        let expected = ExpectedDigests {
            md5: Some(Md5::digest(b"hello world").into()),
            sha256: Some(Sha256::digest(b"hello world").into()),
        };
        let got = drain(DigestCheck::new(body(&[b"hello ", b"world"]), expected)).await.unwrap();
        assert_eq!(got, b"hello world");
    }

    #[tokio::test]
    async fn a_mismatch_ends_the_body_with_bad_digest() {
        let expected = ExpectedDigests { md5: Some(Md5::digest(b"something else").into()), sha256: None };
        let err = drain(DigestCheck::new(body(&[b"hello world"]), expected)).await.unwrap_err();
        assert!(err.get_ref().is_some_and(|inner| inner.is::<BadDigest>()));
    }

    #[test]
    fn content_md5_header_is_parsed_and_validated() {
        let mut headers = HeaderMap::new();
        headers.insert("content-md5", "XrY7u+Ae7tCTyyK7j1rNww==".parse().unwrap());
        let parsed = ExpectedDigests::from_request(&headers, &Extensions::new()).unwrap();
        assert_eq!(parsed.md5, Some(Md5::digest(b"hello world").into()));
        headers.insert("content-md5", "not base64!".parse().unwrap());
        assert!(ExpectedDigests::from_request(&headers, &Extensions::new()).is_err());
        headers.insert("content-md5", "aGVsbG8=".parse().unwrap());
        assert!(ExpectedDigests::from_request(&headers, &Extensions::new()).is_err());
    }
}
