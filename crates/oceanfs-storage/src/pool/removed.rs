//! Durable record of a **detached** pool (ADR-0036 D8, d5).
//!
//! Detach is persistent: a restart must honor the removal without a
//! config edit, or placement would silently refill the empty pool the
//! operator just removed — the footgun in exactly the disk-replacement
//! workflow detach exists for. The pool set at boot is `config − removed`,
//! where `removed` is a small node-local record of detached pools keyed by
//! **name + root** (never pool id — ids are config-order and shift when
//! the operator edits/reorders the config file).
//!
//! This module defines the pure record type only. The crash-safe file
//! store that persists it under the node's state directory lives in
//! `oceanfs-node` (`removed_pools.rs`, node-local state — the record is
//! never gossiped; peers only see the manifest the node re-gossips after
//! detach).
//!
//! Reconciliation rules (ADR-0036 D8):
//!
//! - a record suppresses a config-declared pool **only while both name and
//!   root match** (`[`PoolRemovedRecord::matches`]`); a stale record whose
//!   name/root matches no config pool is inert;
//! - re-attaching the same name+root via the admin API clears the record
//!   (a hot-swap round-trip removes the tombstone);
//! - `PoolRegistry::from_config_skipping` applies `config − removed`
//!   **before** role/cardinality validation of the *remaining* set, so a
//!   restart of a node that drained + detached its last data pool refuses
//!   boot (ADR-0031 on the post-overlay set; the documented retirement
//!   sequence is detach → leave, never restart-fully-detached).

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

/// A detached pool's identity: `name` + `root` exactly as declared in the
/// topology config.
///
/// Keyed by name+root (never pool id): ids are config-order and shift when
/// the operator removes or reorders config entries, while a pool's
/// identity — its configured name and its root directory — is stable
/// across those edits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolRemovedRecord {
    /// The pool's configured name.
    pub name: String,
    /// The pool's configured root directory.
    pub root: PathBuf,
    /// Epoch milliseconds at which the detach was recorded (informational).
    pub removed_at: u64,
}

impl PoolRemovedRecord {
    /// Creates a record for `name`/`root`, stamped with the current time.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::PoolRemovedRecord;
    ///
    /// let record = PoolRemovedRecord::new("data-0".into(), std::path::PathBuf::from("/mnt/nvme0"));
    /// assert_eq!(record.name, "data-0");
    /// assert!(record.matches("data-0", std::path::Path::new("/mnt/nvme0")));
    /// ```
    pub fn new(name: String, root: PathBuf) -> Self {
        let removed_at =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
        Self { name, root, removed_at }
    }

    /// Whether this record suppresses a config pool with the given
    /// `name` and `root` — true only when **both** match (ADR-0036 D8).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::PoolRemovedRecord;
    ///
    /// let record =
    ///     PoolRemovedRecord::new("data-0".into(), std::path::PathBuf::from("/mnt/nvme0"));
    /// assert!(record.matches("data-0", std::path::Path::new("/mnt/nvme0")));
    /// // Same name, different root: the record does not suppress it.
    /// assert!(!record.matches("data-0", std::path::Path::new("/mnt/nvme1")));
    /// // Same root, different name: ditto.
    /// assert!(!record.matches("data-1", std::path::Path::new("/mnt/nvme0")));
    /// ```
    pub fn matches(&self, name: &str, root: &Path) -> bool {
        self.name == name && self.root == root
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn record_requires_both_name_and_root_to_match() {
        let record = PoolRemovedRecord::new("data-0".into(), PathBuf::from("/mnt/nvme0"));
        assert!(record.matches("data-0", Path::new("/mnt/nvme0")));
        assert!(!record.matches("data-0", Path::new("/mnt/nvme0/child")));
        assert!(!record.matches("data-0", Path::new("/mnt/NVME0")));
        assert!(!record.matches("other", Path::new("/mnt/nvme0")));
    }

    #[test]
    fn record_serializes_and_deserializes() {
        let record = PoolRemovedRecord::new("data-0".into(), PathBuf::from("/mnt/nvme0"));
        let json = serde_json::to_string(&record).expect("serialize");
        let round: PoolRemovedRecord = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(round, record);
    }

    #[test]
    fn record_stamps_removed_at_from_the_clock() {
        let record = PoolRemovedRecord::new("data-0".into(), PathBuf::from("/mnt/nvme0"));
        // removed_at is epoch millis and should be non-zero and close to now.
        let now =
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after epoch").as_millis()
                as u64;
        assert!(record.removed_at > 0 && now.abs_diff(record.removed_at) < 60_000);
    }
}
