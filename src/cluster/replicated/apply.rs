use std::path::{Path, PathBuf};

use crate::storage::engine::StorageEngine;
use crate::storage::error::{internal, StorageError};
use crate::storage::streaming::{open_object_body_stream, verify_wire_checksum};
use crate::storage::WriteConditions;

use super::hooks::RemoteChange;
use super::log::{ReplicationEvent, ReplicationLog, ReplicationOp};

/// Human: Apply a peer replication event locally: idempotent on event_id, and only when the change is newer
/// than this node's version of the key (an older one is acknowledged and dropped).
/// Agent: IF has_event THEN no-op Ok; ELSE put/delete via StorageEngine under RemoteChange; records applied.
pub async fn apply_replication_event_bytes(
    engine: &StorageEngine,
    log: &ReplicationLog,
    event: &ReplicationEvent,
    blob: Option<Vec<u8>>,
) -> Result<(), StorageError> {
    if let (Some(bytes), Some(expected)) = (&blob, event.wire_checksum.as_deref()) {
        verify_wire_checksum(bytes, expected)?;
    }
    apply_event(engine, log, event, blob.map(Payload::Bytes)).await
}

/// Human: Apply a replicated PUT whose content was streamed to `content` (already size/checksum-verified
/// by the caller), so objects of any size replicate without being held in memory.
pub async fn apply_replication_event_file(
    engine: &StorageEngine,
    log: &ReplicationLog,
    event: &ReplicationEvent,
    content: &Path,
) -> Result<(), StorageError> {
    apply_event(engine, log, event, Some(Payload::File(content))).await
}

enum Payload<'a> {
    Bytes(Vec<u8>),
    File(&'a Path),
}

async fn apply_event(
    engine: &StorageEngine,
    log: &ReplicationLog,
    event: &ReplicationEvent,
    payload: Option<Payload<'_>>,
) -> Result<(), StorageError> {
    if log.has_event(&event.event_id).await? {
        return Ok(());
    }
    // Human: Cheap early exit; the check that counts runs again under the key's write lock (RemoteChange).
    let incoming = event.effective_version();
    if log
        .key_version(&event.bucket, &event.key)
        .await?
        .is_some_and(|current| current.version >= incoming)
    {
        log.record_applied(event).await?;
        return Ok(());
    }

    let hook = RemoteChange { log, event };
    let conditions = WriteConditions {
        hook: Some(&hook),
        ..WriteConditions::default()
    };
    let content_type = event.content_type.as_deref();
    let custom_meta = event.custom_meta.as_deref();
    let applied = match (event.op, payload) {
        (ReplicationOp::Delete, _) => {
            engine
                .delete_object_conditional(&event.bucket, &event.key, conditions)
                .await
        }
        (ReplicationOp::Put, Some(Payload::Bytes(bytes))) => engine
            .put_object_conditional(
                &event.bucket,
                &event.key,
                content_type,
                custom_meta,
                std::io::Cursor::new(bytes),
                conditions,
            )
            .await
            .map(drop),
        (ReplicationOp::Put, Some(Payload::File(path))) => {
            let file = tokio::fs::File::open(path).await.map_err(internal)?;
            engine
                .put_object_conditional(
                    &event.bucket,
                    &event.key,
                    content_type,
                    custom_meta,
                    tokio::io::BufReader::new(file),
                    conditions,
                )
                .await
                .map(drop)
        }
        (ReplicationOp::Put, None) => {
            let path = event
                .payload_path
                .as_ref()
                .map(|rel| PathBuf::from(log.data_dir()).join(rel))
                .ok_or(StorageError::NotFound)?;
            // Human: The payload file is a stored blob — read its logical content, never the container bytes.
            let size = event
                .size
                .and_then(|s| u64::try_from(s).ok())
                .ok_or_else(|| internal(anyhow::anyhow!("put event without size")))?;
            let content = open_object_body_stream(&path, size, 0, size, &engine.read_context()).await?;
            engine
                .put_object_conditional(
                    &event.bucket,
                    &event.key,
                    content_type,
                    custom_meta,
                    tokio_util::io::StreamReader::new(content),
                    conditions,
                )
                .await
                .map(drop)
        }
    };
    match applied {
        Ok(()) => Ok(()),
        // Human: The key changed here meanwhile to this version or a newer one: nothing to apply.
        Err(StorageError::PreconditionFailed) => {
            log.record_applied(event).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}
