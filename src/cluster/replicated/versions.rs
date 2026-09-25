//! Human: Per-key versions for replication. Every change to a replicated key gets a version — when it was made
//! on its origin node (microseconds, that node's clock) with the node id as tie-break — recorded here next to
//! the key's current state, deletes included (tombstones). A replicated change is applied only when it is newer
//! than what this node has, so a late retry can't overwrite newer data, an older put can't undo a delete, and
//! nodes that accept writes for the same key converge on the last writer.
//! Agent: table replication_versions in the system SQLite; rows change only under the key's write lock (the
//! replication CommitHooks), except `store_if_absent` (backfill) and tombstone pruning.

use std::cmp::Ordering;

use sqlx::{Executor, Sqlite};

use crate::storage::error::{internal, StorageError};

/// When and where a change was made. Ordered by time, then origin node id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub micros: i64,
    pub origin: String,
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.micros
            .cmp(&other.micros)
            .then_with(|| self.origin.cmp(&other.origin))
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Version {
    /// As sent in `x-nd-replication-version`: `{micros};{origin}`.
    pub fn to_header(&self) -> String {
        format!("{};{}", self.micros, self.origin)
    }

    pub fn from_header(value: &str) -> Option<Self> {
        let (micros, origin) = value.split_once(';')?;
        Some(Self {
            micros: micros.trim().parse().ok()?,
            origin: origin.to_string(),
        })
    }
}

/// A key's recorded state: the version of its last change and whether that change deleted it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyVersion {
    pub version: Version,
    pub deleted: bool,
}

pub(crate) const HEADER: &str = "x-nd-replication-version";

pub(crate) fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

pub(crate) async fn load<'e, E>(executor: E, bucket: &str, key: &str) -> Result<Option<KeyVersion>, StorageError>
where
    E: Executor<'e, Database = Sqlite>,
{
    let row: Option<(i64, String, i64)> = sqlx::query_as(
        "SELECT version, origin, deleted FROM replication_versions WHERE bucket = ? AND key = ?",
    )
    .bind(bucket)
    .bind(key)
    .fetch_optional(executor)
    .await
    .map_err(internal)?;
    Ok(row.map(|(micros, origin, deleted)| KeyVersion {
        version: Version { micros, origin },
        deleted: deleted != 0,
    }))
}

/// Human: Record a change made on this node, with a version later than the key's current one (the clock may
/// have stepped back); RETURNS that version's time.
/// Agent: UPSERTS replication_versions (version = MAX(now_us, current + 1), origin, deleted); CALLER HOLDS the key lock.
pub(crate) async fn store_local<'e, E>(
    executor: E,
    bucket: &str,
    key: &str,
    origin: &str,
    deleted: bool,
) -> Result<i64, StorageError>
where
    E: Executor<'e, Database = Sqlite>,
{
    let now = now_micros();
    sqlx::query_scalar(
        "INSERT INTO replication_versions (bucket, key, version, origin, deleted, recorded_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(bucket, key) DO UPDATE SET
             version = MAX(excluded.version, replication_versions.version + 1),
             origin = excluded.origin,
             deleted = excluded.deleted,
             recorded_at = excluded.recorded_at
         RETURNING version",
    )
    .bind(bucket)
    .bind(key)
    .bind(now)
    .bind(origin)
    .bind(deleted)
    .bind(now / 1_000_000)
    .fetch_one(executor)
    .await
    .map_err(internal)
}

/// Record `state` as the key's current state.
/// Agent: UPSERTS replication_versions unconditionally; CALLER HOLDS the key lock and has compared versions.
pub(crate) async fn store<'e, E>(executor: E, bucket: &str, key: &str, state: &KeyVersion) -> Result<(), StorageError>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "INSERT INTO replication_versions (bucket, key, version, origin, deleted, recorded_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(bucket, key) DO UPDATE SET
             version = excluded.version,
             origin = excluded.origin,
             deleted = excluded.deleted,
             recorded_at = excluded.recorded_at",
    )
    .bind(bucket)
    .bind(key)
    .bind(state.version.micros)
    .bind(&state.version.origin)
    .bind(state.deleted)
    .bind(now_micros() / 1_000_000)
    .execute(executor)
    .await
    .map_err(internal)?;
    Ok(())
}

/// Record `state` unless the key already has a version; RETURNS the key's version afterwards.
/// Agent: INSERT … ON CONFLICT no-op UPDATE so RETURNING yields the existing row; used by backfill without a lock.
pub(crate) async fn store_if_absent<'e, E>(
    executor: E,
    bucket: &str,
    key: &str,
    state: &KeyVersion,
) -> Result<Version, StorageError>
where
    E: Executor<'e, Database = Sqlite>,
{
    let (micros, origin): (i64, String) = sqlx::query_as(
        "INSERT INTO replication_versions (bucket, key, version, origin, deleted, recorded_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(bucket, key) DO UPDATE SET version = replication_versions.version
         RETURNING version, origin",
    )
    .bind(bucket)
    .bind(key)
    .bind(state.version.micros)
    .bind(&state.version.origin)
    .bind(state.deleted)
    .bind(now_micros() / 1_000_000)
    .fetch_one(executor)
    .await
    .map_err(internal)?;
    Ok(Version { micros, origin })
}

/// Human: Forget tombstones recorded before `before` (unix seconds), at most `limit`; RETURNS how many. After
/// that a put older than the delete could apply again, so keep them longer than any event can stay in flight.
/// Agent: DELETES deleted = 1 rows with recorded_at < before, LIMIT limit (idx_repl_versions_tombstones).
pub(crate) async fn prune_tombstones<'e, E>(executor: E, before: i64, limit: i64) -> Result<u64, StorageError>
where
    E: Executor<'e, Database = Sqlite>,
{
    let result = sqlx::query(
        "DELETE FROM replication_versions WHERE rowid IN (
             SELECT rowid FROM replication_versions WHERE deleted = 1 AND recorded_at < ? LIMIT ?
         )",
    )
    .bind(before)
    .bind(limit)
    .execute(executor)
    .await
    .map_err(internal)?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(micros: i64, origin: &str) -> Version {
        Version {
            micros,
            origin: origin.into(),
        }
    }

    #[test]
    fn versions_order_by_time_then_origin() {
        assert!(v(2, "a") > v(1, "z"));
        assert!(v(1, "b") > v(1, "a"));
        assert_eq!(v(1, "a").cmp(&v(1, "a")), Ordering::Equal);
    }

    #[test]
    fn header_round_trips_origins_with_separators() {
        let version = v(1_700_000_000_123_456, "node;b");
        assert_eq!(Version::from_header(&version.to_header()), Some(version));
        assert_eq!(Version::from_header("12"), None);
        assert_eq!(Version::from_header("x;node"), None);
    }

    #[tokio::test]
    async fn local_versions_only_move_forward() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        crate::storage::object_meta::init_system_sqlite_schema(&pool)
            .await
            .unwrap();
        let far_future = KeyVersion {
            version: v(i64::MAX / 2, "other"),
            deleted: false,
        };
        store(&pool, "b", "k", &far_future).await.unwrap();
        let next = store_local(&pool, "b", "k", "me", true).await.unwrap();
        assert_eq!(next, i64::MAX / 2 + 1);
        let stored = load(&pool, "b", "k").await.unwrap().unwrap();
        assert_eq!(stored.version, v(next, "me"));
        assert!(stored.deleted);

        let kept = store_if_absent(&pool, "b", "k", &far_future).await.unwrap();
        assert_eq!(kept, v(next, "me"));
        let fresh = store_if_absent(&pool, "b", "new", &far_future).await.unwrap();
        assert_eq!(fresh, far_future.version);
    }
}
