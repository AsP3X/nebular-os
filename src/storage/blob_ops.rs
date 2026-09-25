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

/// Human: Flush a file's bytes to stable storage before it becomes visible under an object path.
pub async fn sync_file(path: &Path) -> Result<(), StorageError> {
    let path = path.to_path_buf();
    // Human: Windows' FlushFileBuffers needs a handle with write access; Unix fsync works on any fd.
    tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .read(true)
            .write(cfg!(windows))
            .open(&path)?
            .sync_all()
    })
        .await
        .map_err(internal)?
        .map_err(internal)
}

/// Human: Flush a directory so a rename/create/unlink inside it survives a crash (no-op off Unix).
pub async fn sync_dir(dir: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        let dir = dir.to_path_buf();
        tokio::task::spawn_blocking(move || std::fs::File::open(&dir)?.sync_all())
            .await
            .map_err(internal)?
            .map_err(internal)?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Human: Rename `src` over `dst`; when they sit on different filesystems (EXDEV — e.g. a bucket directory
/// mounted elsewhere), copy into a scratch file beside `dst` first and rename that, so `dst` still flips atomically.
pub async fn rename_into_place(src: &Path, dst: &Path) -> std::io::Result<()> {
    match fs::rename(src, dst).await {
        Err(e) if is_cross_device(&e) => {
            let beside = dst
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(format!(".xdev-{}", uuid::Uuid::new_v4()));
            let copied = async {
                fs::copy(src, &beside).await?;
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(cfg!(windows))
                    .open(&beside)?
                    .sync_all()?;
                fs::rename(&beside, dst).await
            }
            .await;
            if copied.is_err() {
                let _ = fs::remove_file(&beside).await;
            } else {
                let _ = fs::remove_file(src).await;
            }
            copied
        }
        other => other,
    }
}

fn is_cross_device(e: &std::io::Error) -> bool {
    #[cfg(unix)]
    return e.raw_os_error() == Some(ERR_CROSS_DEVICE);
    // Human: Windows reports ERROR_NOT_SAME_DEVICE (17) for cross-volume moves.
    #[cfg(not(unix))]
    return e.raw_os_error() == Some(17);
}

/// Human: Identity of the file currently at a path; a rewrite by rename or re-create changes it.
/// Agent: COMPARE before/after to detect that an object was replaced while maintenance was re-encoding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStamp {
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    inode: (u64, u64),
}

impl FileStamp {
    pub fn of(path: &Path) -> Option<Self> {
        let meta = std::fs::metadata(path).ok()?;
        #[cfg(unix)]
        let inode = {
            use std::os::unix::fs::MetadataExt;
            (meta.dev(), meta.ino())
        };
        Some(Self {
            len: meta.len(),
            modified: meta.modified().ok(),
            #[cfg(unix)]
            inode,
        })
    }
}
