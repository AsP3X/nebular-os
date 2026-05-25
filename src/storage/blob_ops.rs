use std::path::Path;

use tokio::fs;

use super::error::{internal, StorageError};

// Human: Cross-device link failures use EXDEV (18) on Unix when src/dst are on different mounts.
// Agent: EXDEV=18; hard_link fallback to fs::copy preserves copy_object behavior off-volume.
#[cfg(unix)]
const ERR_CROSS_DEVICE: i32 = 18;

// Human: Prefer a hard link for server-side copy so identical bytes share one inode on the same volume.
// Agent: TRY hard_link(src,dst); ON EXDEV OR non-unix USE fs::copy; dst parent created; existing dst removed first.
pub async fn link_or_copy_blob(src: &Path, dst: &Path) -> Result<(), StorageError> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).await.map_err(internal)?;
    }
    if dst.exists() {
        fs::remove_file(dst).await.map_err(internal)?;
    }

    #[cfg(unix)]
    {
        match std::fs::hard_link(src, dst) {
            Ok(()) => return Ok(()),
            Err(e) if e.raw_os_error() == Some(ERR_CROSS_DEVICE) => {}
            Err(e) => return Err(internal(e)),
        }
    }

    fs::copy(src, dst).await.map_err(internal)?;
    Ok(())
}

#[cfg(unix)]
pub fn same_inode(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(ma) = std::fs::metadata(a) else {
        return false;
    };
    let Ok(mb) = std::fs::metadata(b) else {
        return false;
    };
    ma.dev() == mb.dev() && ma.ino() == mb.ino()
}

#[cfg(not(unix))]
pub fn same_inode(_a: &Path, _b: &Path) -> bool {
    false
}
