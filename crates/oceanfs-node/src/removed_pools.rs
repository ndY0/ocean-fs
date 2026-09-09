//! Durable node-local store of **removed pools** (d5, ADR-0036 D8).
//!
//! Detach survives restart. The pool set at boot is `config − removed`,
//! where `removed` is this store's content — a small TOML file of
//! [`PoolRemovedRecord`]s written crash-safely (temp → fsync → rename)
//! under the node's data directory, the same state-dir family as
//! `membership_state.toml` and g7's wal-pool replacement marker.
//!
//! The store is **node-local and advisory**: it is never gossiped (peers
//! only see the manifest the node re-gossips after a detach), and the
//! topology config remains the authoritative operator intent. Losing the
//! file cannot lose data — a pool is only ever recorded after it was
//! drained empty — it only resurrects an *empty* pool on the next boot
//! (visible in `/admin/pools`; the operator re-drains + re-detaches or
//! edits the config). A record suppresses a config-declared pool only
//! while both `name` and `root` match; re-attaching the same name+root via
//! the admin API clears the record.

use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use oceanfs_storage::PoolRemovedRecord;
use serde::{Deserialize, Serialize};

/// File name of the removed-pool record inside the node's data directory.
const REMOVED_POOLS_FILE: &str = "removed_pools.toml";

/// On-disk shape of the removed-pool record file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RemovedPoolsFile {
    /// Format version (1). Kept so a future format change can be detected.
    #[serde(default = "default_version")]
    version: u32,
    /// The removed pools, one per detached pool.
    #[serde(default)]
    removed: Vec<PoolRemovedRecord>,
}

fn default_version() -> u32 {
    1
}

/// Crash-safe file store of removed-pool records under a node data dir.
#[derive(Debug, Clone)]
pub(crate) struct RemovedPoolStore {
    /// Path of the record file (`{data_dir}/removed_pools.toml`).
    path: PathBuf,
}

impl RemovedPoolStore {
    /// Binds a store to a node's data directory.
    ///
    /// The file is created lazily on the first write.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let store = RemovedPoolStore::at("/var/lib/oceanfs");
    /// store.record("data-0", "/mnt/nvme0")?;
    /// ```
    pub(crate) fn at(data_dir: &Path) -> Self {
        Self { path: data_dir.join(REMOVED_POOLS_FILE) }
    }

    /// Loads the removed-pool records.
    ///
    /// Returns an empty list when the file does not exist. A file that
    /// exists but cannot be read or parsed is an **error** (boot refuses):
    /// a corrupt removal record is ambiguous — silently ignoring it would
    /// resurrect a detached pool, and guessing the operator's intent is
    /// worse than a loud stop (ADR-0036 D8).
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the file exists but cannot be read or
    /// parsed as TOML.
    pub(crate) fn load(&self) -> io::Result<Vec<PoolRemovedRecord>> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => {
                let file: RemovedPoolsFile = toml::from_str(&contents).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "corrupt removed-pool record '{}' ({}): refusing to guess the \
                             operator's removal intent — fix or remove the file, or drop the \
                             pool from the config (ADR-0036 D8)",
                            self.path.display(),
                            e
                        ),
                    )
                })?;
                Ok(file.removed)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    /// Persists a detach: records a removed pool (name+root).
    ///
    /// Idempotent — recording the same name+root twice is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the record file cannot be written.
    pub(crate) fn record(&self, name: &str, root: &Path) -> io::Result<()> {
        let mut removed = self.load()?;
        if removed.iter().any(|r| r.matches(name, root)) {
            return Ok(());
        }
        removed.push(PoolRemovedRecord::new(name.to_string(), root.to_path_buf()));
        self.write_all(&removed)
    }

    /// Clears a removed-pool record (re-attach round-trip).
    ///
    /// Idempotent — clearing an absent record is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the record file cannot be written.
    pub(crate) fn clear(&self, name: &str, root: &Path) -> io::Result<()> {
        let removed = self.load()?;
        let before = removed.len();
        let remaining: Vec<PoolRemovedRecord> =
            removed.into_iter().filter(|r| !r.matches(name, root)).collect();
        if remaining.len() == before {
            return Ok(()); // nothing matched — no rewrite needed.
        }
        self.write_all(&remaining)
    }

    /// Rewrites the whole record file crash-safely: temp file in the same
    /// directory → fsync → rename over the target. A crash mid-write
    /// leaves either the old or the new file — never a truncated mix.
    fn write_all(&self, removed: &[PoolRemovedRecord]) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = RemovedPoolsFile { version: default_version(), removed: removed.to_vec() };
        let serialized = toml::to_string_pretty(&file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let tmp_path = self.path.with_extension("toml.tmp");
        {
            let mut tmp = fs::File::create(&tmp_path)?;
            tmp.write_all(serialized.as_bytes())?;
            tmp.flush()?;
            tmp.sync_all()?;
        }
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::Path;

    use super::*;

    fn test_store() -> (tempfile::TempDir, RemovedPoolStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = RemovedPoolStore::at(dir.path());
        (dir, store)
    }

    #[test]
    fn missing_file_loads_empty() {
        let (_dir, store) = test_store();
        assert!(store.load().unwrap().is_empty());
    }

    #[test]
    fn record_persists_and_loads_back() {
        let (_dir, store) = test_store();
        store.record("data-0", Path::new("/mnt/nvme0")).unwrap();
        let records = store.load().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "data-0");
        assert_eq!(records[0].root, PathBuf::from("/mnt/nvme0"));
    }

    #[test]
    fn record_is_idempotent() {
        let (_dir, store) = test_store();
        store.record("data-0", Path::new("/mnt/nvme0")).unwrap();
        store.record("data-0", Path::new("/mnt/nvme0")).unwrap();
        store.record("data-1", Path::new("/mnt/nvme1")).unwrap();
        assert_eq!(store.load().unwrap().len(), 2);
    }

    #[test]
    fn clear_removes_only_the_matching_record() {
        let (_dir, store) = test_store();
        store.record("data-0", Path::new("/mnt/nvme0")).unwrap();
        store.record("data-1", Path::new("/mnt/nvme1")).unwrap();
        store.clear("data-0", Path::new("/mnt/nvme0")).unwrap();
        let records = store.load().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "data-1");
    }

    #[test]
    fn clear_on_absent_record_is_a_noop() {
        let (_dir, store) = test_store();
        store.record("data-0", Path::new("/mnt/nvme0")).unwrap();
        store.clear("data-9", Path::new("/mnt/nope")).unwrap();
        assert_eq!(store.load().unwrap().len(), 1);
    }

    #[test]
    fn corrupt_file_is_a_load_error() {
        let (_dir, store) = test_store();
        std::fs::write(&store.path, "not toml {{{").unwrap();
        assert!(store.load().is_err());
    }

    #[test]
    fn write_leaves_no_tmp_file_behind() {
        let (_dir, store) = test_store();
        store.record("data-0", Path::new("/mnt/nvme0")).unwrap();
        let tmp_path = store.path.with_extension("toml.tmp");
        assert!(!tmp_path.exists(), "crash-safe write must clean up its temp file");
        assert!(store.path.exists());
    }
}
