//! Human: The HTTP client for node-to-node requests — replication, repair, forwarding and health checks. It
//! bounds connecting and every wait for a peer's response, so a peer that accepts connections but never answers
//! can't hang those tasks; one process-wide client also reuses connections across requests.
//! Agent: cheap to clone (shared pool); per-request `.timeout()` still applies where a total budget is needed.

use std::sync::LazyLock;
use std::time::Duration;

/// Longest wait for a TCP (and TLS) connection to a peer.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest silence while waiting for or reading a peer's response.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| build(Some(READ_TIMEOUT)));

static UPLOAD_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| build(None));

fn build(read_timeout: Option<Duration>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60));
    if let Some(timeout) = read_timeout {
        builder = builder.read_timeout(timeout);
    }
    builder.build().unwrap_or_else(|e| {
        tracing::error!(error = %e, "cannot build the cluster HTTP client; using one without timeouts");
        reqwest::Client::new()
    })
}

/// The shared client for requests to cluster peers.
pub fn client() -> reqwest::Client {
    CLIENT.clone()
}

/// Human: The client for requests that upload a body. reqwest's read timeout starts when a request is sent and
/// isn't reset while the body uploads, so with it every transfer that took longer than a minute failed; these
/// requests carry a total budget instead (`transfer_timeout`).
/// Agent: connect timeout only; CALLERS MUST set `.timeout(transfer_timeout(len))` or wrap the call in one.
pub fn upload_client() -> reqwest::Client {
    UPLOAD_CLIENT.clone()
}

/// Human: A total time budget for sending `bytes` to a peer: a minute plus a second per MiB, so a peer that
/// stops reading a large body is given up on eventually without cutting off slow but moving transfers.
pub fn transfer_timeout(bytes: u64) -> Duration {
    Duration::from_secs(60).saturating_add(Duration::from_secs(bytes / (1024 * 1024)))
}
