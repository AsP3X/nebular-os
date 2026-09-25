use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, RwLock};

use super::error::{internal, StorageError};

/// Human: Every dictionary this process has loaded, by the ID zstd writes into the header of each frame
/// compressed with it — so any frame can be decoded with exactly its own dictionary, whichever is current.
static BY_FRAME_ID: LazyLock<RwLock<HashMap<u32, Arc<Vec<u8>>>>> = LazyLock::new(RwLock::default);

/// Data directories whose dictionaries this process has registered.
static REGISTERED_DIRS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Mutex::default);

/// Serializes choosing a new dictionary id and saving it, so two trainings can't claim the same id.
static TRAINING: Mutex<()> = Mutex::new(());

fn register(dict: &Arc<Vec<u8>>) {
    if let Some(id) = zstd::zstd_safe::get_dict_id_from_dict(dict)
        && let Ok(mut map) = BY_FRAME_ID.write()
    {
        map.insert(id.get(), dict.clone());
    }
}

/// Human: The dictionary a zstd frame needs: `Ok(None)` when its header names none, the registered dictionary
/// with that ID, or `hint` when it is that dictionary. `Err` when the frame names a dictionary that isn't
/// available — reported up front rather than as a decode failure halfway through.
/// Agent: READS the frame header's dictID; LOOKS UP BY_FRAME_ID, then `hint`; Err when neither matches.
pub fn dictionary_for_frame(
    frame: &[u8],
    hint: Option<&[u8]>,
) -> Result<Option<Arc<Vec<u8>>>, StorageError> {
    let Some(id) = zstd::zstd_safe::get_dict_id_from_frame(frame) else {
        return Ok(None);
    };
    if let Some(dict) = BY_FRAME_ID.read().ok().and_then(|map| map.get(&id.get()).cloned()) {
        return Ok(Some(dict));
    }
    match hint {
        Some(hint) if zstd::zstd_safe::get_dict_id_from_dict(hint) == Some(id) => Ok(Some(Arc::new(hint.to_vec()))),
        _ => Err(internal(anyhow::anyhow!(
            "data was compressed with zstd dictionary {id}, which is not in the data directory's .dict/"
        ))),
    }
}

/// Human: Make sure the dictionaries in `data_dir` are registered for `dictionary_for_frame`, reading `.dict/`
/// only if this process hasn't yet (`DictStore::new` registers them; dictionaries saved later register
/// themselves).
/// Agent: ONCE per data_dir per process (REGISTERED_DIRS); CALLS DictStore::new, which registers every file.
pub fn register_data_dir(data_dir: &str) {
    let registered = REGISTERED_DIRS
        .lock()
        .is_ok_and(|dirs| dirs.contains(data_dir));
    if !registered {
        DictStore::new(data_dir);
    }
}

/// A dictionary and its id.
pub type IdentifiedDict = (u16, Arc<Vec<u8>>);

/// Human: Trained zstd dictionaries (`.dict/{id}.zdict`). New writes use the one with the highest id; older ones
/// stay on disk because blobs compressed with them name them in their frames (`dictionary_for_frame`).
/// Agent: `current` caches the highest id and is shared by clones; `new` registers every dictionary for reads.
#[derive(Clone)]
pub struct DictStore {
    data_dir: String,
    current: Arc<RwLock<Option<IdentifiedDict>>>,
}

impl DictStore {
    pub fn new(data_dir: &str) -> Self {
        let store = Self {
            data_dir: data_dir.to_string(),
            current: Arc::default(),
        };
        store.load_all();
        if let Ok(mut dirs) = REGISTERED_DIRS.lock() {
            dirs.insert(data_dir.to_string());
        }
        store
    }

    pub fn dict_path(&self, id: u16) -> PathBuf {
        PathBuf::from(&self.data_dir)
            .join(".dict")
            .join(format!("{id}.zdict"))
    }

    /// Ids of the non-empty dictionaries on disk, ascending.
    fn stored_ids(&self) -> Vec<u16> {
        let mut ids: Vec<u16> = std::fs::read_dir(self.dict_dir())
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name().into_string().ok()?;
                let id = name.strip_suffix(".zdict")?.parse().ok()?;
                (entry.metadata().ok()?.len() > 0).then_some(id)
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Human: Load and register every dictionary on disk, and remember the newest for writes.
    /// Agent: READS .dict/*.zdict ascending; REGISTERS each by frame id; SETS current = highest id loaded.
    pub fn load_all(&self) {
        let mut newest = None;
        for id in self.stored_ids() {
            match self.load(id) {
                Ok(Some(dict)) => newest = Some((id, dict)),
                Ok(None) => {}
                Err(e) => tracing::warn!(id, error = %e, "cannot load zstd dictionary"),
            }
        }
        if let Ok(mut current) = self.current.write() {
            *current = newest;
        }
    }

    pub fn load(&self, id: u16) -> Result<Option<Arc<Vec<u8>>>, StorageError> {
        let path = self.dict_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let dict = Arc::new(bytes);
        register(&dict);
        Ok(Some(dict))
    }

    /// Human: Save dictionary `id`. An existing dictionary is never replaced: blobs compressed with it would
    /// become undecodable once the replacement is loaded.
    /// Agent: Err when `id` exists; WRITES tmp + fsync + rename; REGISTERS; SETS current when id ≥ current id.
    pub fn save(&self, id: u16, dict: &[u8]) -> Result<(), StorageError> {
        let path = self.dict_path(id);
        if self.has_dictionary(id) {
            return Err(internal(anyhow::anyhow!("zstd dictionary {id} already exists")));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
        // Human: Write, fsync, then rename — a crash or a concurrent reader never sees a torn dictionary.
        let tmp = path.with_extension(format!("zdict.tmp-{}", uuid::Uuid::new_v4()));
        let written = std::fs::File::create(&tmp).and_then(|mut f| {
            f.write_all(dict)?;
            f.sync_all()
        });
        if let Err(e) = written.and_then(|()| std::fs::rename(&tmp, &path)) {
            let _ = std::fs::remove_file(&tmp);
            return Err(internal(anyhow::anyhow!(e)));
        }
        let dict = Arc::new(dict.to_vec());
        register(&dict);
        if let Ok(mut current) = self.current.write()
            && current.as_ref().is_none_or(|(current_id, _)| *current_id <= id)
        {
            *current = Some((id, dict));
        }
        Ok(())
    }

    /// Human: Train a dictionary from `samples` and save it under the next free id, which it returns; `None`
    /// when there isn't enough sample data.
    /// Agent: HOLDS TRAINING (process-wide) across next_id + save, so concurrent trainings get distinct ids.
    pub fn train_next(&self, samples: &[Vec<u8>], max_dict_bytes: usize) -> Result<Option<u16>, StorageError> {
        let _training = TRAINING.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = self.next_id()?;
        Ok(self.train_and_save(id, samples, max_dict_bytes, 0)?.then_some(id))
    }

    /// Human: Train a dictionary from logical object samples and save it as `id`.
    /// Agent: RETURNS false when there isn't enough sample data to train on.
    pub fn train_and_save(
        &self,
        id: u16,
        samples: &[Vec<u8>],
        max_dict_bytes: usize,
        _level: i32,
    ) -> Result<bool, StorageError> {
        if samples.len() < 2 {
            return Ok(false);
        }
        let total: usize = samples.iter().map(|s| s.len()).sum();
        if total < 256 {
            return Ok(false);
        }
        let max_size = max_dict_bytes.min(total.saturating_mul(3) / 10).max(256);
        let dict = zstd::dict::from_samples(samples, max_size)
            .map_err(|e| internal(anyhow::anyhow!("dict training failed: {e}")))?;
        if dict.is_empty() {
            return Ok(false);
        }
        self.save(id, &dict)?;
        Ok(true)
    }

    /// The dictionary new writes use (highest id) and its id.
    pub fn current(&self) -> Option<IdentifiedDict> {
        self.current.read().ok().and_then(|current| current.clone())
    }

    /// Human: The id the next trained dictionary gets. Ids start at 1: blob headers record 0 both for "no
    /// dictionary" and for the single dictionary earlier releases kept as `0.zdict`, which stays valid.
    pub fn next_id(&self) -> Result<u16, StorageError> {
        match self.stored_ids().last() {
            None => Ok(1),
            Some(&last) => last
                .checked_add(1)
                .ok_or_else(|| internal(anyhow::anyhow!("no dictionary ids left"))),
        }
    }

    /// The current dictionary's bytes (see `current`).
    pub fn global_dict(&self) -> Option<Arc<Vec<u8>>> {
        self.current().map(|(_, dict)| dict)
    }

    pub fn dict_dir(&self) -> PathBuf {
        PathBuf::from(&self.data_dir).join(".dict")
    }

    pub fn exists_on_disk(&self, id: u16) -> bool {
        self.dict_path(id).exists()
    }

    /// True when a non-empty dictionary file is stored for `id` (empty files are ignored by `load`).
    pub fn has_dictionary(&self, id: u16) -> bool {
        std::fs::metadata(self.dict_path(id)).is_ok_and(|m| m.len() > 0)
    }

    /// True when any dictionary is stored.
    pub fn has_any_dictionary(&self) -> bool {
        !self.stored_ids().is_empty()
    }

    pub fn remove(&self, id: u16) -> Result<(), StorageError> {
        let path = self.dict_path(id);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
        self.load_all();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn samples(word: &str) -> Vec<Vec<u8>> {
        (0..8)
            .map(|i| format!("{word} log entry {i} with shared vocabulary {word}\n").repeat(120))
            .map(String::into_bytes)
            .collect()
    }

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = DictStore::new(tmp.path().to_str().unwrap());
        store.save(0, b"fake-dict-bytes").unwrap();
        let loaded = store.load(0).unwrap().unwrap();
        assert_eq!(&loaded[..], b"fake-dict-bytes");
        assert_eq!(store.current().unwrap().0, 0);
    }

    #[test]
    fn empty_file_is_not_a_dictionary() {
        let tmp = TempDir::new().unwrap();
        let store = DictStore::new(tmp.path().to_str().unwrap());
        std::fs::create_dir_all(store.dict_dir()).unwrap();
        std::fs::write(store.dict_path(0), b"").unwrap();
        assert!(store.exists_on_disk(0));
        assert!(!store.has_dictionary(0));
        assert!(!store.has_any_dictionary());
        store.save(0, b"dict").unwrap();
        assert!(store.has_dictionary(0));
        let leftovers = std::fs::read_dir(store.dict_dir()).unwrap().count();
        assert_eq!(leftovers, 1, "save must not leave temp files behind");
    }

    #[test]
    fn a_saved_dictionary_is_never_replaced() {
        let tmp = TempDir::new().unwrap();
        let store = DictStore::new(tmp.path().to_str().unwrap());
        store.save(1, b"first").unwrap();
        assert!(store.save(1, b"second").is_err());
        assert_eq!(&store.load(1).unwrap().unwrap()[..], b"first");
        assert_eq!(store.train_next(&samples("CHARLIE"), 4096).unwrap(), Some(2));
    }

    #[test]
    fn clones_share_the_current_dictionary() {
        let tmp = TempDir::new().unwrap();
        let store = DictStore::new(tmp.path().to_str().unwrap());
        let trainer = store.clone();
        assert_eq!(trainer.train_next(&samples("DELTA"), 4096).unwrap(), Some(1));
        assert_eq!(store.current().map(|(id, _)| id), Some(1));
    }

    #[test]
    fn frames_find_their_own_dictionary_after_retraining() {
        let tmp = TempDir::new().unwrap();
        let store = DictStore::new(tmp.path().to_str().unwrap());
        assert_eq!(store.train_next(&samples("ALPHA"), 4096).unwrap(), Some(1));
        let (first_id, first) = store.current().unwrap();
        let frame_a = zstd::bulk::Compressor::with_dictionary(3, &first)
            .unwrap()
            .compress(&samples("ALPHA")[0])
            .unwrap();
        assert_eq!(store.train_next(&samples("BRAVO"), 4096).unwrap(), Some(2));
        let (second_id, second) = store.current().unwrap();
        assert_eq!((first_id, second_id), (1, 2));
        let frame_b = zstd::bulk::Compressor::with_dictionary(3, &second)
            .unwrap()
            .compress(&samples("BRAVO")[0])
            .unwrap();

        // Human: A fresh store (a restart) resolves both frames by the IDs in their headers.
        let reopened = DictStore::new(tmp.path().to_str().unwrap());
        assert_eq!(reopened.current().unwrap().0, 2);
        for (frame, original) in [(&frame_a, &samples("ALPHA")[0]), (&frame_b, &samples("BRAVO")[0])] {
            let dict = dictionary_for_frame(frame, None).unwrap().unwrap();
            let decoded = zstd::bulk::Decompressor::with_dictionary(&dict)
                .unwrap()
                .decompress(frame, original.len())
                .unwrap();
            assert_eq!(&decoded, original);
        }
        let plain = zstd::bulk::compress(b"no dictionary here", 3).unwrap();
        assert!(dictionary_for_frame(&plain, None).unwrap().is_none());
    }
}
