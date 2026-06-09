use std::path::PathBuf;

use tokio::fs::File;
use tokio::io::BufReader;

use crate::storage::engine::StorageEngine;
use crate::storage::error::{internal, StorageError};

use super::log::{ReplicationEvent, ReplicationLog, ReplicationOp};

/// Human: Apply a peer replication event locally (idempotent on event_id).
/// Agent: IF has_event THEN no-op Ok; ELSE put/delete via StorageEngine; record_applied.
pub async fn apply_replication_event_bytes(
    engine: &StorageEngine,
    log: &ReplicationLog,
    event: &ReplicationEvent,
    blob: Option<Vec<u8>>,
) -> Result<(), StorageError> {
    if log.has_event(&event.event_id).await? {
        return Ok(());
    }

    match event.op {
        ReplicationOp::Delete => {
            engine
                .delete_object(&event.bucket, &event.key, None)
                .await?;
        }
        ReplicationOp::Put => {
            let content_type = event.content_type.as_deref();
            let custom_meta = event.custom_meta.as_deref();
            if let Some(bytes) = blob {
                engine
                    .put_object(
                        &event.bucket,
                        &event.key,
                        content_type,
                        custom_meta,
                        std::io::Cursor::new(bytes),
                    )
                    .await?;
            } else {
                let path = event
                    .payload_path
                    .as_ref()
                    .map(|rel| PathBuf::from(log.data_dir()).join(rel))
                    .ok_or(StorageError::NotFound)?;
                let file = BufReader::new(File::open(&path).await.map_err(internal)?);
                engine
                    .put_object(
                        &event.bucket,
                        &event.key,
                        content_type,
                        custom_meta,
                        file,
                    )
                    .await?;
            }
        }
    }

    log.record_applied(event).await?;
    Ok(())
}
