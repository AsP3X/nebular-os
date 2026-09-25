use axum::{
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, Request},
    middleware,
    routing::{delete, get, post, put},
    Router,
};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::Sleep;
use tower::{Layer, ServiceBuilder, ServiceExt};
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    trace::TraceLayer,
};

use crate::auth::{presigned_or_jwt_middleware, JwtSecret};
use crate::cluster::{auth as cluster_auth, replicate, routes as cluster_routes};
use crate::cluster::StorageBackend;
use crate::config::NosConfig;
use crate::middleware::{
    metrics_auth::metrics_auth_middleware, rate_limit::rate_limit_middleware,
    rate_limit::new_rate_limit_map, upload_budget::upload_budget_middleware,
};
use crate::observability::NosMetrics;
use crate::webhooks::WebhookDispatcher;
use crate::routes::{batch, bucket, capabilities, health, maintenance, metrics, multipart, object, AppState};

/// Connection handling for `serve`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ServeOptions {
    /// How long a connection may take to send a request's headers, idle keep-alive time included.
    pub header_read_timeout: Option<Duration>,
    /// How long a response write may stay blocked because the client stopped reading.
    pub send_stall_timeout: Option<Duration>,
    /// Connections served at once; at the limit new ones wait in the listen queue (0 = no limit).
    pub max_connections: usize,
    /// After shutdown is requested, how long requests in progress may take to finish (`serve_until`).
    pub shutdown_grace: Duration,
}

impl ServeOptions {
    pub fn from_config(cfg: &NosConfig) -> Self {
        let secs = |s: u64| (s > 0).then(|| Duration::from_secs(s));
        Self {
            header_read_timeout: secs(cfg.header_read_timeout_secs),
            send_stall_timeout: secs(cfg.send_stall_timeout_secs),
            max_connections: cfg.max_connections,
            shutdown_grace: Duration::from_secs(cfg.shutdown_grace_secs),
        }
    }
}

/// Human: Accept connections and serve `app` — what `axum::serve(listener, app.into_make_service_with_connect_info())`
/// does, plus limits it has no settings for: hyper's header-read timeout (axum 0.8 never gives hyper the timer it
/// needs, so clients could hold connections open forever by sending headers slowly or not at all), a timeout for
/// clients that stop reading a response, and an optional cap on concurrent connections.
/// Agent: HTTP/1 only, as before (axum's http2 feature is off). hyper's http1 builder, not hyper-util's auto
/// builder: auto first waits for the protocol preface without a timer, so a silent client was never timed out.
/// Inserts ConnectInfo<SocketAddr> for the rate limiter.
pub async fn serve(listener: TcpListener, app: Router, options: ServeOptions) -> std::io::Result<()> {
    serve_until(listener, app, options, std::future::pending()).await
}

/// Human: `serve` until `shutdown` completes, then stop gracefully: stop accepting, let requests in progress finish
/// (idle keep-alive connections close at once) and return once they have — or once `options.shutdown_grace` is
/// up, with whatever is still open cut off. Without this, `docker stop` or a rolling update killed the server
/// mid-request after its timeout.
/// Agent: RETURNS Ok after draining; connections still open at the deadline are dropped with the runtime.
pub async fn serve_until(
    listener: TcpListener,
    app: Router,
    options: ServeOptions,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    let slots = (options.max_connections > 0).then(|| Arc::new(Semaphore::new(options.max_connections)));
    // Human: Every connection task holds a sender; the receiver sees the channel close once the last one ends.
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let (open_tx, mut open_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::pin!(shutdown);
    loop {
        // Human: At the connection limit, stop accepting until a connection ends; new ones queue in the kernel.
        let slot = match &slots {
            Some(slots) => tokio::select! {
                permit = slots.clone().acquire_owned() => {
                    Some(permit.expect("the connection semaphore is never closed"))
                }
                () = &mut shutdown => break,
            },
            None => None,
        };
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            () = &mut shutdown => break,
        };
        let (stream, remote) = match accepted {
            Ok(accepted) => accepted,
            Err(e) if is_connection_error(&e) => continue,
            Err(e) => {
                // Human: E.g. out of file descriptors — back off instead of spinning (as axum::serve does).
                tracing::error!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let service = axum::Extension(ConnectInfo(remote))
            .layer(app.clone())
            .map_request(|req: Request<Incoming>| req.map(Body::new));
        let mut stop = stop_rx.clone();
        let open = open_tx.clone();
        tokio::spawn(async move {
            let _slot = slot;
            let _open = open;
            let mut builder = http1::Builder::new();
            builder
                .timer(TokioTimer::new())
                .header_read_timeout(options.header_read_timeout);
            let io = TokioIo::new(SendStallTimeout::new(stream, options.send_stall_timeout));
            let connection = builder
                .serve_connection(io, TowerToHyperService::new(service))
                .with_upgrades();
            tokio::pin!(connection);
            let result = tokio::select! {
                result = connection.as_mut() => result,
                // Human: The flag only ever changes to "stopping", so any change means stop.
                _ = stop.changed() => {
                    // Human: Finish the request in progress, then close (an idle connection closes now).
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            };
            if let Err(e) = result {
                tracing::trace!(error = %e, remote = %remote, "connection closed with an error");
            }
        });
    }

    drop(listener);
    let _ = stop_tx.send(true);
    drop(open_tx);
    tracing::info!(grace_secs = options.shutdown_grace.as_secs(), "stopped accepting connections; finishing requests in progress");
    if tokio::time::timeout(options.shutdown_grace, open_rx.recv()).await.is_err() {
        tracing::warn!("shutdown grace period over; closing the connections still open");
    }
    Ok(())
}

/// Human: Fails a connection whose peer stops accepting bytes: a write that stays blocked for `timeout` errors
/// instead of holding the response's buffers (read-ahead blocks of a compressed download) indefinitely.
/// Agent: Reads pass through (header and upload idle timeouts cover them); the timer resets on any progress.
struct SendStallTimeout<IO> {
    io: IO,
    timeout: Option<Duration>,
    blocked: Option<Pin<Box<Sleep>>>,
}

impl<IO> SendStallTimeout<IO> {
    fn new(io: IO, timeout: Option<Duration>) -> Self {
        Self { io, timeout, blocked: None }
    }

    fn check<T>(&mut self, cx: &mut Context<'_>, poll: Poll<std::io::Result<T>>) -> Poll<std::io::Result<T>> {
        let Poll::Pending = poll else {
            self.blocked = None;
            return poll;
        };
        let Some(timeout) = self.timeout else {
            return Poll::Pending;
        };
        let timer = self.blocked.get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        if timer.as_mut().poll(cx).is_ready() {
            self.blocked = None;
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "client stopped reading the response",
            )));
        }
        Poll::Pending
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for SendStallTimeout<IO> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for SendStallTimeout<IO> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.io).poll_write(cx, buf);
        this.check(cx, poll)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.io).poll_write_vectored(cx, bufs);
        this.check(cx, poll)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.io).poll_flush(cx);
        this.check(cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.io).poll_shutdown(cx);
        this.check(cx, poll)
    }
}

fn is_connection_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

pub async fn create_app(
    backend: StorageBackend,
    engine: crate::storage::engine::StorageEngine,
    cfg: Arc<NosConfig>,
    metrics: Arc<NosMetrics>,
) -> anyhow::Result<Router> {
    let cluster = Arc::new(std::sync::RwLock::new(cfg.cluster.clone()));
    let backend = Arc::new(std::sync::RwLock::new(backend));
    let bootstrap_token = cfg.cluster_bootstrap_token.clone().map(Arc::new);
    let upload_budget = if cfg.upload_max_in_flight_bytes > 0 {
        Some(crate::middleware::UploadBudget::new(
            cfg.upload_max_in_flight_bytes,
            cfg.upload_permit_unit,
        ))
    } else {
        None
    };
    let metrics_for_webhooks = metrics.clone();
    let webhooks = WebhookDispatcher::new(cfg.webhooks.clone(), metrics_for_webhooks);
    let state = Arc::new(AppState {
        backend,
        cluster,
        engine,
        config: cfg.clone(),
        bootstrap_token,
        jwt_secret: Arc::new(JwtSecret(cfg.jwt_secret.clone())),
        signing_secret: cfg.signing_secret.clone().map(Arc::new),
        metrics_token: cfg.metrics_token.clone().map(Arc::new),
        metrics,
        webhooks,
        rate_limiters: new_rate_limit_map(),
        auth_failures: new_rate_limit_map(),
        upload_budget,
        max_body_size: cfg.max_body_size,
        allow_public_read: cfg.allow_public_read,
        storage_stats: Arc::default(),
    });

    let auth_layer =
        middleware::from_fn_with_state(state.clone(), presigned_or_jwt_middleware);

    let mut metrics_router = Router::new().route("/metrics", get(metrics::metrics));
    if cfg.metrics_token.is_some() {
        metrics_router = metrics_router.layer(middleware::from_fn_with_state(
            state.clone(),
            metrics_auth_middleware,
        ));
    }

    let multipart_routes = Router::new()
        .route("/{bucket}/_multipart", post(multipart::init_multipart))
        .route(
            "/{bucket}/_multipart/{upload_id}/parts/{part_number}",
            put(multipart::upload_part),
        )
        .route(
            "/{bucket}/_multipart/{upload_id}/complete",
            post(multipart::complete_multipart),
        )
        .route(
            "/{bucket}/_multipart/{upload_id}",
            delete(multipart::abort_multipart),
        );

    let mut protected_routes = Router::new()
        .route("/_nos/capabilities", get(capabilities::capabilities))
        .route("/_nos/maintenance/orphans", get(maintenance::list_orphans))
        .route(
            "/_nos/maintenance/gc_orphans",
            axum::routing::post(maintenance::gc_orphans),
        )
        .route(
            "/_nos/maintenance/verify_blobs",
            axum::routing::post(maintenance::verify_blobs),
        )
        .route(
            "/_nos/maintenance/migrate_blobs",
            axum::routing::post(maintenance::migrate_blobs),
        )
        .route(
            "/_nos/maintenance/train_dictionary",
            axum::routing::post(maintenance::train_dictionary),
        )
        .route(
            "/_nos/maintenance/replication_status",
            get(maintenance::replication_status),
        )
        .route(
            "/_nos/maintenance/replication_replay",
            axum::routing::post(maintenance::replication_replay),
        )
        .merge(multipart_routes)
        .route("/{bucket}/_batch_delete", axum::routing::post(batch::batch_delete))
        .route(
            "/{bucket}/{*key}",
            put(object::put_object)
                .delete(object::delete_object)
                .get(object::get_object)
                .head(object::head_object),
        )
        .route("/{bucket}", get(bucket::list_objects).delete(bucket::delete_objects_by_prefix));

    if cfg.upload_max_in_flight_bytes > 0 {
        protected_routes = protected_routes.layer(middleware::from_fn_with_state(
            state.clone(),
            upload_budget_middleware,
        ));
    }

    if cfg.rate_limit_rps > 0 {
        protected_routes = protected_routes.layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ));
    }

    protected_routes = protected_routes.layer(auth_layer);

    let mut public_routes = Router::new();

    // Human: Cluster API when clustered or bootstrap token enables runtime config from Ownly.
    // Agent: MERGE /_cluster/* when !standalone OR NOS_CLUSTER_BOOTSTRAP_TOKEN; config routes always in that set.
    let mount_cluster =
        !cfg.cluster.is_standalone() || state.bootstrap_token.is_some();
    if mount_cluster {
        let cluster_layer =
            middleware::from_fn_with_state(state.clone(), cluster_auth::cluster_token_middleware);
        let cluster_router = Router::new()
            .route("/_cluster/health", get(cluster_routes::cluster_health))
            .route(
                "/_cluster/capabilities",
                get(cluster_routes::cluster_capabilities),
            )
            .route(
                "/_cluster/config",
                get(crate::cluster::config_api::get_cluster_config)
                    .put(crate::cluster::config_api::put_cluster_config),
            )
            // Human: Payloads are spooled to disk, so axum's 2 MiB default body limit must not apply.
            .route(
                "/_cluster/replicate",
                post(replicate::replicate).layer(DefaultBodyLimit::disable()),
            )
            .route(
                "/_cluster/replication/backfill",
                axum::routing::post(cluster_routes::replication_backfill),
            )
            .route(
                "/_cluster/assignment/resolve",
                post(cluster_routes::assignment_resolve),
            )
            .route(
                "/_cluster/objects/{bucket}/{*key}",
                axum::routing::get(cluster_routes::cluster_object_get)
                    .head(cluster_routes::cluster_object_head),
            )
            .layer(cluster_layer);
        public_routes = public_routes.merge(cluster_router);
    }

    let cors = build_cors(&cfg);

    // Human: Register liveness/readiness after merge so static paths win over `/{bucket}/{*key}`.
    // Agent: MERGE order alone is not enough — `health`+`ready` matched object routes and returned 401.
    let app = public_routes
        .merge(metrics_router)
        .merge(protected_routes)
        .route("/health", get(health::health))
        .route("/health/ready", get(health::ready))
        .layer(cors)
        .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http().make_span_with(request_span)))
        .layer(middleware::from_fn(run_to_completion))
        .with_state(state);

    Ok(app)
}

/// Human: Finish every request even when its client disconnects. hyper drops a request's future when the
/// connection closes, which could stop a write after its blob was renamed into place but before its metadata
/// was committed (or before a replicated change was recorded) — leaving bytes and metadata disagreeing.
/// Agent: tokio::spawn(next.run(req)) and await it; a panicking handler answers 500 `{"error": ...}`.
async fn run_to_completion(req: Request, next: middleware::Next) -> axum::response::Response {
    use axum::response::IntoResponse;

    match tokio::spawn(next.run(req)).await {
        Ok(response) => response,
        Err(e) => {
            tracing::error!(error = %e, "request handler failed");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({ "error": "internal server error" })),
            )
                .into_response()
        }
    }
}

/// Human: Request span with the path only — presigned URLs carry their signature in the query string.
/// Agent: MIRRORS tower_http DefaultMakeSpan (same target + debug level, so `tower_http=debug` filters still
/// enable it) but NEVER records uri/query.
fn request_span(req: &axum::http::Request<axum::body::Body>) -> tracing::Span {
    tracing::debug_span!(
        target: "tower_http::trace::make_span",
        "request",
        method = %req.method(),
        path = %req.uri().path(),
        version = ?req.version(),
    )
}

fn build_cors(cfg: &NosConfig) -> CorsLayer {
    if cfg.cors_origins.is_empty() {
        return CorsLayer::permissive();
    }
    let origins: Vec<_> = cfg
        .cors_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(Any)
        .allow_headers(Any)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn requests_finish_after_the_client_disconnects() {
        let finished = Arc::new(AtomicBool::new(false));
        let flag = finished.clone();
        let app = Router::new()
            .route(
                "/slow",
                post(move || {
                    let flag = flag.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        flag.store(true, Ordering::SeqCst);
                        "done"
                    }
                }),
            )
            .layer(middleware::from_fn(run_to_completion));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, app, ServeOptions::default()));

        let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
        conn.write_all(b"POST /slow HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(conn);
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(finished.load(Ordering::SeqCst), "the handler was cancelled with its connection");
    }
}
