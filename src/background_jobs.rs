//! Human: Periodic storage maintenance. Every job runs on its own timer, so a slow or long-interval job never
//! delays another (they used to share one 300s loop that ignored the configured intervals and, with orphan GC
//! enabled, waited for the orphan GC timer on every pass).
//! Agent: SPAWNS one tokio task per enabled job; intervals of 0 disable a job; RETURNS the task handles.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::cluster::StorageBackend;
use crate::config::NosConfig;
use crate::storage::StorageEngine;

/// How often the cheap TTL-based purges (soft deletes, abandoned multipart uploads, `.tmp` scratch) run.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(300);

/// Age after which an upload scratch file in `.tmp` counts as abandoned.
const STALE_TMP_AGE: Duration = Duration::from_secs(3600);

/// Orphan blobs removed per orphan GC run.
const ORPHAN_GC_BATCH: usize = 500;

/// How long an unreferenced dedup block is kept before it is deleted (an upload may be about to share it).
const RELEASED_BLOCK_GRACE: Duration = Duration::from_secs(3600);

/// Unreferenced dedup blocks deleted per housekeeping run.
const RELEASED_BLOCK_BATCH: i64 = 1000;

/// How often finished replication history is pruned.
const REPLICATION_PRUNE_INTERVAL: Duration = Duration::from_secs(3600);

/// Human: How long delivered/applied replication events and delete tombstones are kept. Tombstones must outlive
/// any event still in flight (events dead-letter within hours), or an older put could undo a delete.
const REPLICATION_HISTORY_RETENTION: Duration = Duration::from_secs(7 * 86_400);

/// Human: Start all enabled maintenance jobs. Startup reconciliation (if configured) has already run; the
/// startup recompression runs here, as the first pass of the periodic job when that is enabled.
pub fn spawn_background_jobs(
    storage: StorageEngine,
    backend: StorageBackend,
    cfg: Arc<NosConfig>,
) -> Vec<JoinHandle<()>> {
    let mut jobs = Vec::new();

    {
        let storage = storage.clone();
        let purge_soft = cfg.soft_delete_ttl_secs > 0;
        let purge_multipart = cfg.multipart_upload_ttl_secs > 0;
        jobs.push(spawn_periodic(HOUSEKEEPING_INTERVAL, true, move || {
            housekeeping(storage.clone(), purge_soft, purge_multipart)
        }));
    }

    {
        // Human: Always scheduled — a node switched to a replicating mode at runtime (PUT /_cluster/config) keeps
        // its history in the same tables, and on a standalone node the tables are empty.
        let storage = storage.clone();
        jobs.push(spawn_periodic(REPLICATION_PRUNE_INTERVAL, false, move || {
            prune_replication_history(storage.clone())
        }));
    }

    if cfg.recompress_interval_secs > 0 {
        let storage = storage.clone();
        let batch = cfg.recompress_batch_size;
        let dict = cfg.zstd_dict_enabled;
        jobs.push(spawn_periodic(
            Duration::from_secs(cfg.recompress_interval_secs),
            true,
            move || recompress(storage.clone(), batch, dict),
        ));
    } else if cfg.recompress_on_startup {
        jobs.push(tokio::spawn(recompress(
            storage.clone(),
            cfg.recompress_batch_size,
            cfg.zstd_dict_enabled,
        )));
    }

    if cfg.verify_interval_secs > 0 {
        let backend = backend.clone();
        let batch = cfg.verify_batch_size;
        jobs.push(spawn_periodic(
            Duration::from_secs(cfg.verify_interval_secs),
            true,
            move || verify(backend.clone(), batch),
        ));
    }

    if cfg.orphan_gc_interval_secs > 0 {
        let storage = storage.clone();
        jobs.push(spawn_periodic(
            Duration::from_secs(cfg.orphan_gc_interval_secs),
            true,
            move || orphan_gc(storage.clone()),
        ));
    }

    if cfg.reconcile_interval_secs > 0 {
        let storage = storage.clone();
        // Human: NOS_RECONCILE_ON_STARTUP already reconciled during boot — don't repeat it right away.
        let run_now = !cfg.reconcile_on_startup;
        jobs.push(spawn_periodic(
            Duration::from_secs(cfg.reconcile_interval_secs),
            run_now,
            move || reconcile(storage.clone()),
        ));
    }

    jobs
}

/// Human: Run `job` every `every` (first run now, or after one period). A run that takes longer than the
/// period pushes the next one back instead of triggering a burst of catch-up runs.
/// Agent: REQUIRES every > 0 (tokio intervals panic on zero).
pub fn spawn_periodic<F, Fut>(every: Duration, run_now: bool, mut job: F) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let start = if run_now {
            Instant::now()
        } else {
            Instant::now() + every
        };
        let mut ticker = tokio::time::interval_at(start, every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            job().await;
        }
    })
}

async fn housekeeping(storage: StorageEngine, purge_soft: bool, purge_multipart: bool) {
    if purge_soft {
        match storage.purge_soft_deleted().await {
            Ok(n) if n > 0 => tracing::info!(purged = n, "Soft-delete purge completed"),
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "Soft-delete purge failed"),
        }
    }
    if purge_multipart {
        match storage.purge_stale_multipart_uploads().await {
            Ok(n) if n > 0 => tracing::info!(purged = n, "Stale multipart upload purge completed"),
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "Stale multipart upload purge failed"),
        }
    }
    // Human: Always run — interrupted uploads leave `{data_dir}/.tmp` scratch files even when other GC is off.
    match storage.purge_stale_tmp_files(STALE_TMP_AGE).await {
        Ok(n) if n > 0 => tracing::info!(purged = n, "Stale .tmp upload scratch files removed"),
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "Stale .tmp purge failed"),
    }
    match crate::storage::blocks::BlockStore::gc_released_blocks(
        storage.system_write_pool(),
        storage.data_dir(),
        RELEASED_BLOCK_GRACE,
        RELEASED_BLOCK_BATCH,
    )
    .await
    {
        Ok(n) if n > 0 => tracing::info!(removed = n, "Unreferenced dedup blocks removed"),
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "Dedup block GC failed"),
    }
}

async fn prune_replication_history(storage: StorageEngine) {
    let retention = REPLICATION_HISTORY_RETENTION.as_secs() as i64;
    match crate::cluster::replicated::prune_history(storage.system_write_pool(), retention).await {
        Ok(report) if report.events + report.tombstones > 0 => {
            tracing::info!(?report, "Replication history pruned")
        }
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "Replication history pruning failed"),
    }
}

async fn recompress(storage: StorageEngine, batch: usize, train_dict: bool) {
    match storage.recompress_blobs(batch).await {
        Ok(report) if report.recompressed > 0 => {
            tracing::info!(?report, "Periodic blob recompression finished")
        }
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "Blob recompression failed"),
    }
    if train_dict {
        match storage.train_zstd_dictionary().await {
            Ok(report) if report.trained => {
                tracing::info!(?report, "Periodic dictionary training finished")
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "Dictionary training failed"),
        }
    }
}

async fn verify(backend: StorageBackend, batch: usize) {
    match backend.scrub_with_defaults(batch).await {
        Ok(report) if report.corrupted > 0 => {
            tracing::warn!(?report, "Periodic blob integrity verification found issues")
        }
        Ok(report) if report.verified > 0 => {
            tracing::debug!(?report, "Periodic blob integrity verification finished")
        }
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "Blob integrity verification failed"),
    }
}

async fn orphan_gc(storage: StorageEngine) {
    match storage.gc_orphan_blobs(None, None, ORPHAN_GC_BATCH).await {
        Ok(report) if report.removed > 0 => tracing::info!(?report, "Periodic orphan GC completed"),
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "Periodic orphan GC failed"),
    }
}

async fn reconcile(storage: StorageEngine) {
    match storage.reconcile().await {
        Ok(report) => tracing::info!(?report, "Periodic reconciliation finished"),
        Err(e) => tracing::error!(error = %e, "Periodic reconciliation failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type Run = std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;

    fn counter_job(count: &Arc<AtomicUsize>, takes: Duration) -> impl FnMut() -> Run + Send + 'static {
        let count = count.clone();
        move || {
            let count = count.clone();
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(takes).await;
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_long_interval_job_does_not_hold_back_a_short_one() {
        let housekeeping = Arc::new(AtomicUsize::new(0));
        let orphan_gc = Arc::new(AtomicUsize::new(0));
        spawn_periodic(Duration::from_secs(300), true, counter_job(&housekeeping, Duration::ZERO));
        spawn_periodic(Duration::from_secs(86_400), true, counter_job(&orphan_gc, Duration::ZERO));

        tokio::time::sleep(Duration::from_secs(3_050)).await;
        assert_eq!(housekeeping.load(Ordering::SeqCst), 11, "runs at t=0,300,…,3000");
        assert_eq!(orphan_gc.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn run_now_false_waits_one_period() {
        let runs = Arc::new(AtomicUsize::new(0));
        spawn_periodic(Duration::from_secs(600), false, counter_job(&runs, Duration::ZERO));
        tokio::time::sleep(Duration::from_secs(599)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_run_delays_the_next_instead_of_bursting() {
        let runs = Arc::new(AtomicUsize::new(0));
        // Human: Each run takes 1000s against a 100s period: back-to-back runs, never a catch-up burst.
        spawn_periodic(Duration::from_secs(100), true, counter_job(&runs, Duration::from_secs(1_000)));
        tokio::time::sleep(Duration::from_secs(2_500)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 3, "runs start at t=0, 1000, 2000");
    }
}
