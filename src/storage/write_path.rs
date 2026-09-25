//! Human: The one place an object's blob is replaced — staged uploads/copies become the current version here.
//! Agent: Callers hold the key lock (KeyLocks); metadata commit follows an fsynced rename; the previous
//! blob is restored when the metadata write fails and its dedup refs are released only after success.
//! Overwrites are journaled (`.tmp/{id}.swap` beside the `.prev` backup) so a crash between the rename and
//! the metadata commit is rolled back at the next start (`recover_interrupted_swaps`).

use std::path::{Path, PathBuf};

use futures_util::future::BoxFuture;
use tokio::fs;

use super::blob_ops::{rename_into_place, sync_dir, sync_file};
use super::blocks::BlockStore;
use super::engine::{StorageEngine, TempFileGuard};
use super::error::{internal, map_io_error, StorageError};
use super::precondition::check_write_preconditions;
use super::streaming::{stage_temp_blob, StagedBlob};
use super::types::ObjectMetadata;
use super::{blob_path, blob_path_variants};

/// What a committed change left behind, as reported to a `CommitHook`.
#[derive(Debug, Clone, Copy)]
pub enum Committed<'a> {
    /// The object's new version.
    Written(&'a ObjectMetadata),
    /// The object is gone (or never existed); its storage class when it had one.
    Deleted { storage_class: Option<&'a str> },
}

/// Human: Acts on a change inside the object's write lock — `before` sees it first and can veto it, `after`
/// runs once it is committed — so whatever the hook records about the change can't interleave with another
/// writer of the key. Replication uses it to version every change.
/// Agent: CALLED with the key lock held, so it must never take that key's lock itself. A `before` error vetoes
/// the change (nothing is written) and is returned; an `after` error is returned although the change is
/// committed. Bulk deletes call only `after`, once per deleted key.
pub trait CommitHook: Send + Sync {
    fn before<'a>(&'a self, bucket: &'a str, key: &'a str) -> BoxFuture<'a, Result<(), StorageError>>;

    fn after<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        committed: Committed<'a>,
    ) -> BoxFuture<'a, Result<(), StorageError>>;
}

/// If-Match / If-None-Match evaluated while the object's write lock is held, and an optional hook run there.
#[derive(Clone, Copy, Default)]
pub struct WriteConditions<'a> {
    pub if_match: Option<&'a str>,
    pub if_none_match: Option<&'a str>,
    pub hook: Option<&'a dyn CommitHook>,
}

impl WriteConditions<'_> {
    fn is_empty(&self) -> bool {
        self.if_match.is_none() && self.if_none_match.is_none()
    }
}

impl std::fmt::Debug for WriteConditions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteConditions")
            .field("if_match", &self.if_match)
            .field("if_none_match", &self.if_none_match)
            .field("hook", &self.hook.is_some())
            .finish()
    }
}

/// Human: An overwrite in progress, made durable before the new blob replaces the old one. If the process dies
/// while the metadata still names `old_etag`, the next start restores the `.prev` backup with the same id.
#[derive(serde::Serialize, serde::Deserialize)]
struct SwapIntent {
    bucket: String,
    key: String,
    new_etag: String,
    /// The version the backup holds; absent in records that can't be restored safely.
    #[serde(default)]
    old_etag: Option<String>,
}

const SWAP_INTENT: &str = "swap";
const SWAP_BACKUP: &str = "prev";

/// Metadata written when a staged blob becomes an object's current version.
pub(crate) enum MetaCommit<'a> {
    Upsert {
        size: u64,
        content_type: Option<&'a str>,
        etag: &'a str,
        custom_meta: Option<&'a str>,
    },
    CopyOf(&'a ObjectMetadata),
}

impl MetaCommit<'_> {
    fn size(&self) -> u64 {
        match self {
            MetaCommit::Upsert { size, .. } => *size,
            MetaCommit::CopyOf(src) => src.size.max(0) as u64,
        }
    }

    fn etag(&self) -> Option<&str> {
        match self {
            MetaCommit::Upsert { etag, .. } => Some(etag),
            MetaCommit::CopyOf(src) => src.etag.as_deref(),
        }
    }
}

impl StorageEngine {
    fn scratch_path(&self, suffix: &str) -> PathBuf {
        self.tmp_path(&uuid::Uuid::new_v4().to_string(), suffix)
    }

    fn tmp_path(&self, id: &str, suffix: &str) -> PathBuf {
        PathBuf::from(format!("{}/.tmp/{id}.{suffix}", self.data_dir()))
    }

    /// Write `intent` durably (file and `.tmp` directory fsynced) before the swap it describes.
    async fn write_swap_intent(&self, path: &Path, intent: &SwapIntent) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(intent).map_err(internal)?;
        fs::write(path, bytes).await.map_err(internal)?;
        if self.fsync_writes() {
            sync_file(path).await?;
            if let Some(dir) = path.parent() {
                sync_dir(dir).await.map_err(internal)?;
            }
        }
        Ok(())
    }

    /// Human: Finish or undo overwrites that a crash interrupted: an object whose metadata still names the
    /// previous version gets its previous blob back, so bytes and metadata agree again. A record whose object has
    /// changed since (a later write or delete) is only cleared — restoring it would undo that change.
    /// Agent: RUNS at engine start, before any request, each record under its key's write lock; RETURNS how many
    /// objects were rolled back. Dedup refs of a discarded blob are left (a leak, never a double release).
    pub(crate) async fn recover_interrupted_swaps(&self) -> Result<usize, StorageError> {
        let tmp = PathBuf::from(format!("{}/.tmp", self.data_dir()));
        let mut entries = match fs::read_dir(&tmp).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(internal(e)),
        };
        let mut restored = 0;
        while let Some(entry) = entries.next_entry().await.map_err(internal)? {
            let intent_path = entry.path();
            if intent_path.extension().and_then(|e| e.to_str()) != Some(SWAP_INTENT) {
                continue;
            }
            let backup = intent_path.with_extension(SWAP_BACKUP);
            // Human: An intent that can't be read was torn while being written, i.e. before the rename:
            // the previous blob is still in place.
            let intent: Option<SwapIntent> = fs::read(&intent_path)
                .await
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok());
            if let Some(intent) = intent {
                let _key_guard = self.key_locks().lock(&intent.bucket, &intent.key).await;
                let current = self
                    .object_meta()
                    .try_fetch_active_metadata(&intent.bucket, &intent.key)
                    .await?
                    .and_then(|meta| meta.etag);
                let committed = current.as_deref() == Some(intent.new_etag.as_str());
                let still_previous = intent.old_etag.is_some() && current == intent.old_etag;
                if !committed && still_previous && backup.exists() {
                    let final_path = blob_path(self.data_dir(), &intent.bucket, &intent.key);
                    rename_into_place(&backup, &final_path).await.map_err(map_io_error)?;
                    if self.fsync_writes()
                        && let Some(dir) = final_path.parent()
                    {
                        sync_dir(dir).await.map_err(internal)?;
                    }
                    tracing::warn!(
                        bucket = %intent.bucket, key = %intent.key,
                        "restored the previous version of an object whose overwrite was interrupted"
                    );
                    restored += 1;
                }
            }
            let _ = fs::remove_file(&backup).await;
            fs::remove_file(&intent_path).await.map_err(internal)?;
        }
        Ok(restored)
    }

    /// Human: Encode an uploaded temp file (PUT body or assembled multipart parts) and commit it as the object.
    /// Agent: Encoding runs before the key lock; only the precondition/capacity checks and the swap run under it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn commit_upload_file(
        &self,
        bucket: &str,
        key: &str,
        tmp_path: &Path,
        size: u64,
        etag: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        conditions: WriteConditions<'_>,
    ) -> Result<ObjectMetadata, StorageError> {
        let staging = self.scratch_path("stage");
        let _staging_guard = TempFileGuard {
            path: staging.clone(),
        };
        let opts = self.blob_finalize_options(None, key, content_type);
        let staged = stage_temp_blob(tmp_path, &staging, size, &opts).await?;
        let (blob_file, new_refs): (&Path, &[(u64, u32)]) = match &staged {
            StagedBlob::Raw => (tmp_path, &[]),
            StagedBlob::Encoded { refs } => (staging.as_path(), refs),
        };
        // Human: Make the bytes durable before taking any lock — fsync can be slow and must not
        // serialize other writers.
        if self.fsync_writes()
            && let Err(e) = sync_file(blob_file).await
        {
            if !new_refs.is_empty() {
                let _ = BlockStore::dec_refs(self.system_write_pool(), self.data_dir(), new_refs).await;
            }
            return Err(e);
        }

        let _key_guard = self.key_locks().lock(bucket, key).await;
        self.commit_staged_locked(
            bucket,
            key,
            blob_file,
            new_refs,
            MetaCommit::Upsert {
                size,
                content_type,
                etag,
                custom_meta,
            },
            conditions,
        )
        .await
    }

    /// Human: Make `staged` the object's blob: re-check preconditions and capacity, rename over the blob path
    /// (directory fsynced), commit metadata, then drop the previous version. Failure leaves the previous object intact.
    /// Agent: CALLER HOLDS the key lock for (bucket, key) and has fsynced `staged`; `new_refs` are released when
    /// nothing was committed. Runs `conditions.hook` around the swap.
    pub(crate) async fn commit_staged_locked(
        &self,
        bucket: &str,
        key: &str,
        staged: &Path,
        new_refs: &[(u64, u32)],
        meta: MetaCommit<'_>,
        conditions: WriteConditions<'_>,
    ) -> Result<ObjectMetadata, StorageError> {
        let vetoed = match conditions.hook {
            Some(hook) => hook.before(bucket, key).await.err(),
            None => None,
        };
        let result = match vetoed {
            Some(e) => Err(e),
            None => self.swap_in_staged(bucket, key, staged, meta, conditions).await,
        };
        if result.is_err() && !new_refs.is_empty() {
            let _ = BlockStore::dec_refs(self.system_write_pool(), self.data_dir(), new_refs).await;
        }
        let committed = result?;
        if let Some(hook) = conditions.hook {
            hook.after(bucket, key, Committed::Written(&committed)).await?;
        }
        Ok(committed)
    }

    async fn swap_in_staged(
        &self,
        bucket: &str,
        key: &str,
        staged: &Path,
        meta: MetaCommit<'_>,
        conditions: WriteConditions<'_>,
    ) -> Result<ObjectMetadata, StorageError> {
        let blob_only = self.metadata_mode().is_blob_only();
        let existing = if blob_only {
            None
        } else {
            self.object_meta().try_fetch_active_metadata(bucket, key).await?
        };
        if !conditions.is_empty() && !blob_only {
            check_write_preconditions(existing.as_ref(), conditions.if_match, conditions.if_none_match)?;
        }

        // Human: Admission under NOS_MAX_LOGICAL_BYTES, held until the metadata below names the new size.
        // Agent: the capacity lock is taken inside reserve_capacity, after the key lock (lock order: key -> capacity).
        let _capacity = self
            .reserve_capacity(existing.as_ref().map_or(0, |m| m.size), meta.size())
            .await?;

        let final_path = blob_path(self.data_dir(), bucket, key);
        let previous = super::existing_blob_paths(&blob_path_variants(self.data_dir(), bucket, key));
        let mut old_refs = Vec::new();
        for path in &previous {
            match BlockStore::manifest_entries(path) {
                Ok(refs) => old_refs.extend(refs),
                Err(e) => tracing::warn!(path = %path.display(), error = %e, "cannot read previous blob refs"),
            }
        }

        let parent = final_path
            .parent()
            .ok_or_else(|| internal(anyhow::anyhow!("blob path has no parent")))?
            .to_path_buf();
        fs::create_dir_all(&parent).await.map_err(internal)?;

        // Human: Keep the current version reachable until metadata points at the new one — never swap
        // without a way back. Hard link first (no copy); fall back to a byte copy (no hard links, EMLINK).
        let swap_id = uuid::Uuid::new_v4().to_string();
        let backup = if final_path.exists() {
            let backup = self.tmp_path(&swap_id, SWAP_BACKUP);
            if std::fs::hard_link(&final_path, &backup).is_err()
                && let Err(e) = fs::copy(&final_path, &backup).await
            {
                let _ = fs::remove_file(&backup).await;
                return Err(internal(anyhow::anyhow!("cannot back up current blob: {e}")));
            }
            Some(backup)
        } else {
            None
        };
        // Human: Journal the overwrite so a crash between the rename and the metadata commit can be undone.
        // A new key needs none: a crash leaves only an unreferenced file, which readers never see.
        let intent = match (&backup, blob_only, meta.etag()) {
            (Some(_), false, Some(new_etag)) => {
                let path = self.tmp_path(&swap_id, SWAP_INTENT);
                let intent = SwapIntent {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    new_etag: new_etag.to_string(),
                    old_etag: existing.as_ref().and_then(|m| m.etag.clone()),
                };
                if let Err(e) = self.write_swap_intent(&path, &intent).await {
                    let _ = fs::remove_file(&path).await;
                    if let Some(backup) = &backup {
                        let _ = fs::remove_file(backup).await;
                    }
                    return Err(e);
                }
                Some(path)
            }
            _ => None,
        };

        if let Err(e) = rename_into_place(staged, &final_path).await {
            if let Some(backup) = &backup {
                let _ = fs::remove_file(backup).await;
            }
            if let Some(intent) = &intent {
                let _ = fs::remove_file(intent).await;
            }
            return Err(map_io_error(e));
        }
        if self.fsync_writes()
            && let Err(e) = sync_dir(&parent).await
        {
            tracing::warn!(dir = %parent.display(), error = %e, "fsync of blob directory failed");
        }

        let committed = if blob_only {
            Ok(blob_only_metadata(bucket, key, &meta))
        } else {
            match meta {
                MetaCommit::Upsert {
                    size,
                    content_type,
                    etag,
                    custom_meta,
                } => {
                    self.object_meta()
                        .upsert_object(
                            self.data_dir(),
                            bucket,
                            key,
                            size as i64,
                            content_type,
                            etag,
                            custom_meta,
                            None,
                            None,
                        )
                        .await
                }
                MetaCommit::CopyOf(src) => {
                    self.object_meta()
                        .copy_object_metadata(src, bucket, key)
                        .await
                }
            }
        };

        match committed {
            Ok(meta) => {
                for path in previous.iter().filter(|p| **p != final_path) {
                    let _ = fs::remove_file(path).await;
                }
                if let Some(backup) = &backup {
                    let _ = fs::remove_file(backup).await;
                }
                if let Some(intent) = &intent {
                    let _ = fs::remove_file(intent).await;
                }
                // Human: The new version is committed; a refcount hiccup only leaks blocks, so don't fail the write.
                if !old_refs.is_empty()
                    && let Err(e) =
                        BlockStore::dec_refs(self.system_write_pool(), self.data_dir(), &old_refs).await
                {
                    tracing::warn!(%bucket, %key, error = %e, "releasing previous blob refs failed");
                }
                Ok(meta)
            }
            Err(e) => {
                let restored = match &backup {
                    Some(backup) => rename_into_place(backup, &final_path).await,
                    None => fs::remove_file(&final_path).await,
                };
                match restored {
                    // Human: Rolled back; the journal entry is done.
                    Ok(()) => {
                        if let Some(intent) = &intent {
                            let _ = fs::remove_file(intent).await;
                        }
                    }
                    // Human: Keep the journal entry: the next start retries the restore.
                    Err(restore_err) => tracing::error!(
                        %bucket, %key, backup = ?backup, error = %restore_err,
                        "could not restore previous blob after failed metadata commit"
                    ),
                }
                Err(e)
            }
        }
    }
}

fn blob_only_metadata(bucket: &str, key: &str, meta: &MetaCommit<'_>) -> ObjectMetadata {
    let now = chrono::Utc::now();
    let (size, content_type, etag, custom_meta) = match meta {
        MetaCommit::Upsert {
            size,
            content_type,
            etag,
            custom_meta,
        } => (
            *size as i64,
            content_type.map(str::to_string),
            Some(etag.to_string()),
            custom_meta.map(str::to_string),
        ),
        MetaCommit::CopyOf(src) => (
            src.size,
            src.mime_type.clone(),
            src.etag.clone(),
            src.custom_meta.clone(),
        ),
    };
    ObjectMetadata {
        bucket: bucket.to_string(),
        key: key.to_string(),
        size,
        mime_type: content_type,
        etag,
        created_at: now,
        updated_at: now,
        custom_meta,
        deleted_at: None,
        storage_class: None,
        origin_node: None,
    }
}
