//! Human: Upgrading from storage written by earlier builds (see tests/fixtures/README.md):
//! - `v0.1.4`: the last release — NOS2 blobs with and without the trained zstd dictionary, NOSD dedup manifests
//!   with their blocks, raw blobs, a multipart object and a soft-deleted object;
//! - `nosb`: an unreleased `master` build (da75495) that wrote NOSB block-compressed blobs.
//!
//! These tests open a copy with the current engine (running its schema upgrades) and check that every object
//! reads back, scrubs clean, and survives recompression and migration to the current format.

use std::path::Path;

use futures_util::StreamExt;
use nebular_os::storage::engine::{EngineOptions, StorageEngine};
use nebular_os::storage::error::StorageError;
use nebular_os::storage::scrub::{ScrubMode, ScrubOptions};
use nebular_os::storage::types::ObjectMetadata;
use nebular_os::storage::GetObjectOutcome;
use sha2::{Digest, Sha256};

#[derive(serde::Deserialize)]
struct Manifest {
    objects: Vec<FixtureObject>,
    soft_deleted: Vec<FixtureRef>,
}

#[derive(serde::Deserialize)]
struct FixtureObject {
    bucket: String,
    key: String,
    size: u64,
    sha256: String,
    content_type: Option<String>,
    custom_meta_origin: Option<String>,
}

#[derive(serde::Deserialize)]
struct FixtureRef {
    bucket: String,
    key: String,
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// A private copy of fixture `name` opened with the current engine.
async fn open_fixture(name: &str, dict_enabled: bool) -> (StorageEngine, tempfile::TempDir, Manifest) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name);
    let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(fixture.join("manifest.json")).unwrap()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&fixture.join("data"), &tmp.path().join("data"));
    std::fs::copy(fixture.join("metadata.db"), tmp.path().join("metadata.db")).unwrap();
    let data_dir = tmp.path().join("data").to_string_lossy().replace('\\', "/");
    let meta_path = tmp.path().join("metadata.db").to_string_lossy().replace('\\', "/");
    let engine = StorageEngine::with_full_options(
        &meta_path,
        &data_dir,
        EngineOptions {
            zstd_dict_enabled: dict_enabled,
            dedup_enabled: true,
            ..EngineOptions::default()
        },
    )
    .await
    .expect("the current engine opens a v0.1.4 data directory");
    (engine, tmp, manifest)
}

async fn read(
    engine: &StorageEngine,
    bucket: &str,
    key: &str,
    range: Option<&str>,
) -> Result<(Vec<u8>, Box<ObjectMetadata>), String> {
    match engine.get_object(bucket, key, range, None, None).await {
        Ok(GetObjectOutcome::Content {
            mut stream,
            content_length,
            meta,
            ..
        }) => {
            let mut body = Vec::new();
            while let Some(chunk) = stream.next().await {
                body.extend_from_slice(&chunk.map_err(|e| format!("body error: {e}"))?);
            }
            if body.len() as u64 != content_length {
                return Err(format!("{} bytes, expected {content_length}", body.len()));
            }
            Ok((body, meta))
        }
        Ok(GetObjectOutcome::NotModified(_)) => Err("unexpected 304".into()),
        Err(e) => Err(format!("{e:?}")),
    }
}

async fn assert_every_object_reads_back(engine: &StorageEngine, manifest: &Manifest, label: &str) {
    for obj in &manifest.objects {
        let name = format!("{label}: {}/{}", obj.bucket, obj.key);
        let (body, meta) = read(engine, &obj.bucket, &obj.key, None)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(body.len() as u64, obj.size, "{name}");
        assert_eq!(hex::encode(Sha256::digest(&body)), obj.sha256, "{name}");
        if let Some(content_type) = &obj.content_type {
            assert_eq!(meta.mime_type.as_deref(), Some(content_type.as_str()), "{name}");
        }
        if let Some(origin) = &obj.custom_meta_origin {
            assert!(
                meta.custom_meta.as_deref().unwrap_or_default().contains(origin.as_str()),
                "{name}: {:?}",
                meta.custom_meta
            );
        }
        let (start, end) = (obj.size / 3, obj.size * 2 / 3);
        let range = format!("bytes={start}-{}", end - 1);
        let (slice, _) = read(engine, &obj.bucket, &obj.key, Some(&range))
            .await
            .unwrap_or_else(|e| panic!("{name} {range}: {e}"));
        assert!(slice == body[start as usize..end as usize], "{name} {range}");
    }
    for gone in &manifest.soft_deleted {
        let outcome = engine.get_object(&gone.bucket, &gone.key, None, None, None).await;
        assert!(
            matches!(outcome, Err(StorageError::NotFound)),
            "{label}: soft-deleted {}/{} is visible",
            gone.bucket,
            gone.key
        );
    }
}

async fn assert_scrubs_clean(engine: &StorageEngine, manifest: &Manifest, label: &str) {
    for mode in [ScrubMode::Light, ScrubMode::Deep] {
        let report = engine
            .scrub_objects(ScrubOptions {
                limit: 1_000,
                mode,
                ..ScrubOptions::default()
            })
            .await
            .unwrap();
        assert_eq!(
            (report.corrupted, report.verified),
            (0, manifest.objects.len() as u64),
            "{label} {mode:?}: {:?}",
            report.corrupted_keys
        );
    }
}

/// Reads, scrubs, migrates to the current format, and reads and scrubs again.
async fn read_scrub_migrate(engine: &StorageEngine, manifest: &Manifest, label: &str) {
    assert_every_object_reads_back(engine, manifest, label).await;
    assert_scrubs_clean(engine, manifest, label).await;
    let migrated = engine.migrate_blobs(1_000, None).await.unwrap();
    assert_eq!(migrated.failed, 0, "{label}: {migrated:?}");
    let label = format!("{label}, after migrate_blobs");
    assert_every_object_reads_back(engine, manifest, &label).await;
    assert_scrubs_clean(engine, manifest, &label).await;
}

async fn recompress_everything(engine: &StorageEngine, manifest: &Manifest, label: &str) {
    let mut passes = 0;
    loop {
        let report = engine.recompress_blobs(7).await.unwrap();
        passes += 1;
        if report.scanned < 7 || passes > 10 {
            break;
        }
    }
    let label = format!("{label}, after recompression");
    assert_every_object_reads_back(engine, manifest, &label).await;
    assert_scrubs_clean(engine, manifest, &label).await;
}

#[tokio::test]
async fn v0_1_4_objects_read_scrub_and_migrate() {
    // Human: The dictionary must be loaded for reads whatever NOS_ZSTD_DICT_ENABLED says now.
    for dict_enabled in [true, false] {
        let label = if dict_enabled { "v0.1.4, dictionary on" } else { "v0.1.4, dictionary off" };
        let (engine, _tmp, manifest) = open_fixture("v0.1.4", dict_enabled).await;
        read_scrub_migrate(&engine, &manifest, label).await;

        // Human: Dedup refcounts carried over: deleting one twin leaves the other's blocks in place.
        engine.delete_object("media", "dedup/a.txt", None).await.unwrap();
        let twin = manifest.objects.iter().find(|o| o.key == "dedup/b.txt").unwrap();
        let (body, _) = read(&engine, "media", "dedup/b.txt", None).await.unwrap();
        assert_eq!(hex::encode(Sha256::digest(&body)), twin.sha256, "{label}");
    }
}

#[tokio::test]
async fn v0_1_4_objects_survive_recompression() {
    let (engine, _tmp, manifest) = open_fixture("v0.1.4", true).await;
    recompress_everything(&engine, &manifest, "v0.1.4").await;
}

#[tokio::test]
async fn nosb_objects_read_scrub_and_migrate() {
    let (engine, _tmp, manifest) = open_fixture("nosb", false).await;
    read_scrub_migrate(&engine, &manifest, "nosb").await;
}

#[tokio::test]
async fn nosb_objects_survive_recompression() {
    let (engine, _tmp, manifest) = open_fixture("nosb", false).await;
    recompress_everything(&engine, &manifest, "nosb").await;
}
