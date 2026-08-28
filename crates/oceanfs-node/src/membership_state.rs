//! Durable membership state for restart rejoin (ADR-0022).
//!
//! Persists the two values a node must remember across restarts:
//!
//! - `self_incarnation`: the last incarnation the node announced with.
//!   On the next start the node announces with `persisted + 1`, so peers
//!   accept the announcement as authoritative and update its address.
//! - `fallback_seeds`: the last-known member addresses, re-contacted on
//!   startup when the configured `seed_nodes` are unreachable or empty
//!   (covers the seedless bootstrap-node restart, t43).
//!
//! Stored as a small TOML file under `{data_dir}/membership_state.toml`,
//! written atomically (temp file + rename) so a crash mid-write never
//! corrupts the previous state. This keeps the membership crate free of
//! any storage dependency: the composition root owns durability and
//! passes plain values into `Membership::join`.

use std::{
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// Maximum number of fallback seed addresses persisted.
///
/// Keeps the file bounded: the list only needs enough entries to
/// re-contact the cluster once.
const MAX_FALLBACK_SEEDS: usize = 16;

/// The durable content of the membership state file.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DurableMembershipState {
    /// Last announced incarnation; `None` on first boot.
    #[serde(default)]
    pub self_incarnation: Option<u64>,
    /// Last-known member addresses (`"ip:port"`).
    #[serde(default)]
    pub fallback_seeds: Vec<String>,
}

/// File-backed store for [`DurableMembershipState`].
///
/// # Examples
///
/// ```ignore
/// use oceanfs_node::membership_state::MembershipStateStore;
///
/// let store = MembershipStateStore::new("data/membership_state.toml");
/// store.save_incarnation(5)?;
/// store.save_fallback_seeds(&["10.0.0.1:9000"])?;
/// let state = store.load()?;
/// ```
#[derive(Debug, Clone)]
pub(crate) struct MembershipStateStore {
    /// Path of the state file.
    path: PathBuf,
}
// [review][architecture][high]
// if ever, for whatever reason, the file get corrupted, we have no mean of reconnecting a node to
// the cluster, we should discuss afallback approaches, maybe gossip based ? i need your honest input on this.
// [end]
impl MembershipStateStore {
    /// Creates a store bound to the given file path.
    ///
    /// The file is created lazily on the first write.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Loads the persisted state.
    ///
    /// Returns the default (empty) state when the file does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn load(&self) -> io::Result<DurableMembershipState> {
        match std::fs::read_to_string(&self.path) {
            Ok(contents) => toml::from_str(&contents)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(DurableMembershipState::default()),
            Err(e) => Err(e),
        }
    }

    /// Persists the whole state atomically.
    fn save(&self, state: &DurableMembershipState) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let serialized = toml::to_string_pretty(state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        // Atomic write: write to a temp file in the same directory, then
        // rename over the target. A crash between the two leaves either
        // the old or the new state — never a truncated mix.
        let tmp_path = self.path.with_extension("toml.tmp");
        std::fs::write(&tmp_path, serialized)?;
        std::fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    /// Write-through persists the self incarnation.
    ///
    /// # Errors
    ///
    /// Returns an error if the state file cannot be written.
    pub fn save_incarnation(&self, incarnation: u64) -> io::Result<()> {
        let mut state = self.load()?;
        state.self_incarnation = Some(incarnation);
        self.save(&state)
    }

    /// Persists the fallback seed list, capped at [`MAX_FALLBACK_SEEDS`].
    ///
    /// # Errors
    ///
    /// Returns an error if the state file cannot be written.
    pub fn save_fallback_seeds(&self, seeds: &[String]) -> io::Result<()> {
        let mut state = self.load()?;
        state.fallback_seeds = seeds.iter().take(MAX_FALLBACK_SEEDS).cloned().collect();
        self.save(&state)
    }

    /// Adds a fallback seed address to the persisted list.
    ///
    /// Deduplicates and keeps the list capped at [`MAX_FALLBACK_SEEDS`].
    ///
    /// # Errors
    ///
    /// Returns an error if the state file cannot be written.
    pub fn add_fallback_seed(&self, seed: &str) -> io::Result<()> {
        let mut state = self.load()?;
        if !state.fallback_seeds.iter().any(|s| s == seed) {
            state.fallback_seeds.insert(0, seed.to_string());
            state.fallback_seeds.truncate(MAX_FALLBACK_SEEDS);
            self.save(&state)?;
        }
        Ok(())
    }
}

/// Builds the default state-file path for a node data directory.
pub(crate) fn default_state_path(data_dir: &Path) -> PathBuf {
    data_dir.join("membership_state.toml")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn test_store() -> (tempfile::TempDir, MembershipStateStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = MembershipStateStore::new(dir.path().join("membership_state.toml"));
        (dir, store)
    }

    #[test]
    fn load_without_file_returns_defaults() {
        let (_dir, store) = test_store();
        let state = store.load().unwrap();
        assert_eq!(state.self_incarnation, None);
        assert!(state.fallback_seeds.is_empty());
    }

    #[test]
    fn incarnation_roundtrip() {
        let (_dir, store) = test_store();
        store.save_incarnation(5).unwrap();

        let state = store.load().unwrap();
        assert_eq!(state.self_incarnation, Some(5));

        // Bump again — write-through on every bump.
        store.save_incarnation(6).unwrap();
        let state = store.load().unwrap();
        assert_eq!(state.self_incarnation, Some(6));
    }

    #[test]
    fn fallback_seeds_roundtrip() {
        let (_dir, store) = test_store();
        let seeds: Vec<String> = vec!["10.0.0.1:9000".into(), "10.0.0.2:9000".into()];
        store.save_fallback_seeds(&seeds).unwrap();

        let state = store.load().unwrap();
        assert_eq!(state.fallback_seeds, seeds);
    }

    #[test]
    fn fallback_seeds_are_capped() {
        let (_dir, store) = test_store();
        let seeds: Vec<String> =
            (0..(MAX_FALLBACK_SEEDS + 10)).map(|i| format!("10.0.0.{i}:9000")).collect();
        store.save_fallback_seeds(&seeds).unwrap();

        let state = store.load().unwrap();
        assert_eq!(state.fallback_seeds.len(), MAX_FALLBACK_SEEDS);
    }

    #[test]
    fn saving_incarnation_preserves_seeds() {
        let (_dir, store) = test_store();
        store.save_fallback_seeds(&["10.0.0.1:9000".into()]).unwrap();
        store.save_incarnation(4).unwrap();

        let state = store.load().unwrap();
        assert_eq!(state.self_incarnation, Some(4));
        assert_eq!(state.fallback_seeds, vec!["10.0.0.1:9000".to_string()]);
    }

    #[test]
    fn corrupted_file_returns_error() {
        let (dir, store) = test_store();
        std::fs::write(dir.path().join("membership_state.toml"), "not valid toml {{{").unwrap();

        assert!(store.load().is_err());
    }

    #[test]
    fn no_temp_file_left_after_save() {
        let (dir, store) = test_store();
        store.save_incarnation(1).unwrap();

        assert!(!dir.path().join("membership_state.toml.tmp").exists());
    }
}
