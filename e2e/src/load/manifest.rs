//! PUT recording and post-run verification for load tests.
//!
//! The [`Manifest`] tracks every PUT operation's (bucket, key, BLAKE3
//! hash) during a load test run. After the run, [`verify`](Manifest::verify)
//! GETs every non-deleted key from the cluster and checks the response
//! hash against the recorded value.
//!
//! ## Concurrent same-key writes (LWW)
//!
//! Under concurrent same-key PUTs, Last-Write-Wins resolves the final
//! content to *one of* the versions written. The manifest therefore
//! records the **set** of version hashes per key, and verification
//! passes when the response hash matches any recorded version — a
//! mismatch means the content matches none of them (real corruption or
//! loss), not merely that another writer won the race.
//!
//! ## Concurrency
//!
//! [`Manifest::record`] and [`Manifest::record_delete`] are called from
//! multiple concurrent tokio worker tasks. The underlying [`DashMap`]
//! provides shard-level internal locking for concurrent safety.

use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use dashmap::DashMap;
use serde::Serialize;

use crate::harness::LoadTarget;

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

/// A concurrent-safe tracker for written objects during a load test.
///
/// Stores the set of BLAKE3 hashes of every successfully PUT version of
/// an object. During post-run verification, every non-deleted entry is
/// GET'd from the cluster and the response hash is compared against the
/// recorded version set.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use e2e::load::Manifest;
///
/// # fn example() {
/// let manifest = Arc::new(Manifest::new());
/// manifest.record("bucket", "key-1", b"hello world");
/// assert_eq!(manifest.active_count(), 1);
/// # }
/// ```
pub struct Manifest {
    /// Map from `"{bucket}/{key}"` to `(version_hash_set, is_deleted)`.
    entries: DashMap<String, (HashSet<[u8; 32]>, AtomicBool)>,
    /// Total number of keys ever inserted (including deleted ones).
    total_count: AtomicUsize,
}

impl Manifest {
    /// Creates a new, empty Manifest.
    pub fn new() -> Self {
        Self { entries: DashMap::new(), total_count: AtomicUsize::new(0) }
    }

    /// Records a successful PUT operation.
    ///
    /// Computes the BLAKE3 hash of `body` and adds it to the set of
    /// versions written for `"{bucket}/{key}"`. A re-PUT of an existing
    /// key (including one previously deleted) clears any delete marker
    /// and adds the new version to the set.
    pub fn record(&self, bucket: &str, key: &str, body: &[u8]) {
        let composite_key = format!("{bucket}/{key}");
        let hash = *blake3::hash(body).as_bytes();
        let mut entry = self.entries.entry(composite_key).or_insert_with(|| {
            self.total_count.fetch_add(1, Ordering::Relaxed);
            (HashSet::new(), AtomicBool::new(false))
        });
        entry.0.insert(hash);
        entry.1.store(false, Ordering::Relaxed);
    }

    /// Marks a key as deleted so that [`verify`](Self::verify) skips it.
    ///
    /// If the key was never recorded (DELETE of a key we never PUT),
    /// this is a no-op and does not increment the count.
    pub fn record_delete(&self, bucket: &str, key: &str) {
        let composite_key = format!("{bucket}/{key}");
        if let Some(entry) = self.entries.get(&composite_key) {
            entry.1.store(true, Ordering::Relaxed);
        }
    }

    /// Returns the total number of entries (including deleted ones).
    pub fn len(&self) -> usize {
        self.total_count.load(Ordering::Relaxed)
    }

    /// Returns `true` if the manifest contains no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of non-deleted (active) entries.
    pub fn active_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.1.load(Ordering::Relaxed)).count()
    }

    /// Verifies every non-deleted entry against the load target.
    ///
    /// Runs sequentially (single-threaded) after all workers stop.
    /// For each key: GET from a random alive node, compute BLAKE3
    /// hash of the response body, and check it against the set of
    /// versions recorded for the key (any recorded version passes —
    /// concurrent LWW writes may legitimately resolve to any of them).
    /// On connection errors, retries with exponential backoff
    /// (100ms, 200ms, 400ms, 800ms).
    ///
    /// The target is generic over [`LoadTarget`] so verification works
    /// against both spawned `Cluster`s and remote `RemoteCluster`s.
    ///
    /// Returns a vector of [`Mismatch`] entries for any keys whose
    /// final content matches none of the written versions.
    pub async fn verify<C: LoadTarget>(&self, target: &C) -> Vec<Mismatch> {
        let mut mismatches = Vec::new();

        for entry in self.entries.iter() {
            let key = entry.key().clone();
            let (versions, deleted) = entry.value();
            if deleted.load(Ordering::Relaxed) {
                continue;
            }

            if let Some(mismatch) = verify_one(target, &key, versions).await {
                mismatches.push(mismatch);
            }
        }

        mismatches
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Mismatch
// ---------------------------------------------------------------------------

/// A single verification failure: the GET response matched none of the
/// recorded version hashes.
#[derive(Debug, Clone, Serialize)]
pub struct Mismatch {
    /// The composite key `"{bucket}/{key}"`.
    pub key: String,
    /// Description of the recorded versions, e.g.
    /// `"one of 3 recorded versions"`.
    pub expected_hash: String,
    /// The actual hash from the response body (hex), or `"unreachable"` if
    /// all retry attempts failed.
    pub actual_hash: String,
    /// The HTTP address of the node that was queried.
    pub node: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Verifies a single manifest entry against the load target.
///
/// Returns `None` on success (the response hash matched one of the
/// recorded versions), or `Some(Mismatch)` on failure.
///
/// Retries with exponential backoff on transient errors. Reports the key
/// as `"unreachable"` if all retries are exhausted.
async fn verify_one<C: LoadTarget>(
    target: &C,
    composite_key: &str,
    expected: &HashSet<[u8; 32]>,
) -> Option<Mismatch> {
    let backoffs = [100u64, 200, 400, 800];

    for &backoff_ms in &backoffs {
        if target.is_empty() {
            return Some(Mismatch {
                key: composite_key.to_string(),
                expected_hash: format!("one of {} recorded versions", expected.len()),
                actual_hash: "unreachable".to_string(),
                node: "(no nodes)".to_string(),
            });
        }

        let node_idx = rand::random::<usize>() % target.len();
        let node_addr = target.node_addr(node_idx).to_string();

        match target.get(node_idx, &format!("/{composite_key}")).await {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.bytes().await.unwrap_or_default();
                let actual_hash = blake3::hash(&body);
                if expected.contains(actual_hash.as_bytes()) {
                    return None; // Success — matched a written version.
                }
                return Some(Mismatch {
                    key: composite_key.to_string(),
                    expected_hash: format!("one of {} recorded versions", expected.len()),
                    actual_hash: hex::encode(actual_hash.as_bytes()),
                    node: node_addr,
                });
            }
            Ok(resp) => {
                // Non-success status (e.g., 404, 500) — may be transient.
                if resp.status().as_u16() == 404 || (500..600).contains(&resp.status().as_u16()) {
                    // Key not found (404) or transient server error (5xx
                    // — e.g. the node is still settling after a restart)
                    // — retry with backoff.
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                return Some(Mismatch {
                    key: composite_key.to_string(),
                    expected_hash: format!("one of {} recorded versions", expected.len()),
                    actual_hash: format!("HTTP {}", resp.status()),
                    node: node_addr,
                });
            }
            Err(_) => {
                // Connection error — retry with backoff.
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                continue;
            }
        }
    }

    // All retries exhausted.
    let node =
        if target.is_empty() { "(no nodes)".to_string() } else { target.node_addr(0).to_string() };
    Some(Mismatch {
        key: composite_key.to_string(),
        expected_hash: format!("one of {} recorded versions", expected.len()),
        actual_hash: "unreachable".to_string(),
        node,
    })
}

// ---------------------------------------------------------------------------
// ManifestSummary
// ---------------------------------------------------------------------------

/// Aggregate summary of manifest verification results.
///
/// Serializable for inclusion in the load test report.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestSummary {
    /// Number of objects written during the test.
    pub objects_written: usize,
    /// Number of objects successfully verified.
    pub objects_verified: usize,
    /// Number of hash mismatches detected.
    pub mismatches: usize,
    /// Detailed mismatch information (capped to first 100).
    pub mismatch_details: Vec<Mismatch>,
}

impl Manifest {
    /// Runs verification and returns a serializable summary.
    ///
    /// This is a convenience wrapper around [`verify`](Self::verify).
    pub async fn verify_summary<C: LoadTarget>(&self, target: &C) -> ManifestSummary {
        let objects_written = self.len();
        let mismatches = self.verify(target).await;
        let objects_verified = objects_written.saturating_sub(mismatches.len());
        ManifestSummary {
            objects_written,
            objects_verified,
            mismatches: mismatches.len(),
            mismatch_details: mismatches,
        }
    }
}

// ---------------------------------------------------------------------------
// hex helper (no external crate needed)
// ---------------------------------------------------------------------------

mod hex {
    /// Encodes bytes as a lowercase hex string.
    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push(HEX_CHARS[(b >> 4) as usize] as char);
            s.push(HEX_CHARS[(b & 0x0f) as usize] as char);
        }
        s
    }

    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_inserts_entry() {
        let manifest = Manifest::new();
        manifest.record("bucket", "key1", b"hello");
        assert_eq!(manifest.len(), 1);
        assert_eq!(manifest.active_count(), 1);
    }

    #[test]
    fn record_overwrites_existing_key() {
        let manifest = Manifest::new();
        manifest.record("bucket", "key1", b"hello");
        manifest.record("bucket", "key1", b"world");
        assert_eq!(manifest.len(), 1); // count doesn't increase
    }

    #[test]
    fn record_delete_marks_as_deleted() {
        let manifest = Manifest::new();
        manifest.record("bucket", "key1", b"hello");
        assert_eq!(manifest.active_count(), 1);
        manifest.record_delete("bucket", "key1");
        assert_eq!(manifest.active_count(), 0);
        assert_eq!(manifest.len(), 1); // total count unchanged
    }

    #[test]
    fn record_delete_nonexistent_is_noop() {
        let manifest = Manifest::new();
        manifest.record_delete("bucket", "nonexistent");
        assert_eq!(manifest.len(), 0);
    }

    #[test]
    fn record_after_delete_reactivates_key() {
        let manifest = Manifest::new();
        manifest.record("bucket", "key1", b"hello");
        manifest.record_delete("bucket", "key1");
        assert_eq!(manifest.active_count(), 0);
        // A re-PUT after a delete must clear the delete marker; the
        // key count stays stable.
        manifest.record("bucket", "key1", b"world");
        assert_eq!(manifest.active_count(), 1);
        assert_eq!(manifest.len(), 1);
    }

    #[test]
    fn record_multiple_versions_keeps_single_key() {
        // LWW-aware recording: concurrent same-key writes accumulate
        // versions under one key without inflating the count.
        let manifest = Manifest::new();
        manifest.record("bucket", "key1", b"v1");
        manifest.record("bucket", "key1", b"v2");
        manifest.record("bucket", "key1", b"v3");
        assert_eq!(manifest.len(), 1);
        assert_eq!(manifest.active_count(), 1);
    }

    #[test]
    fn concurrent_records_no_data_race() {
        use std::sync::Arc;

        let manifest = Arc::new(Manifest::new());
        let num_tasks = 16;
        let num_keys_per_task = 100;
        let barrier = Arc::new(std::sync::Barrier::new(num_tasks));

        std::thread::scope(|s| {
            for t in 0..num_tasks {
                let manifest = Arc::clone(&manifest);
                let barrier = Arc::clone(&barrier);
                s.spawn(move || {
                    barrier.wait();
                    for i in 0..num_keys_per_task {
                        let key = format!("task{t}_key{i}");
                        manifest.record("bucket", &key, b"data");
                    }
                });
            }
        });

        assert_eq!(manifest.len(), num_tasks * num_keys_per_task, "all inserts must be visible");
    }

    #[test]
    fn concurrent_record_and_delete_no_data_race() {
        use std::sync::Arc;

        let manifest = Arc::new(Manifest::new());
        let num_tasks = 8;
        let keys_per_task = 50;
        let barrier = Arc::new(std::sync::Barrier::new(num_tasks));

        // First, insert all keys.
        for t in 0..num_tasks {
            for i in 0..keys_per_task {
                manifest.record("bucket", &format!("task{t}_key{i}"), b"data");
            }
        }

        // Then concurrently mark half as deleted.
        std::thread::scope(|s| {
            for t in 0..num_tasks {
                let manifest = Arc::clone(&manifest);
                let barrier = Arc::clone(&barrier);
                s.spawn(move || {
                    barrier.wait();
                    for i in 0..keys_per_task {
                        if i % 2 == 0 {
                            manifest.record_delete("bucket", &format!("task{t}_key{i}"));
                        }
                    }
                });
            }
        });

        let expected_active = num_tasks * keys_per_task / 2; // half deleted
        assert_eq!(manifest.active_count(), expected_active);
    }

    #[test]
    fn hex_encode_produces_lowercase() {
        let result = hex::encode(&[0xab, 0xcd, 0xef]);
        assert_eq!(result, "abcdef");
    }

    #[test]
    fn hex_encode_empty_is_empty() {
        assert_eq!(hex::encode(&[]), "");
    }

    // ── Mismatch / hash tests ─────

    #[test]
    fn mismatch_populated_with_correct_hex_values() {
        let m = Mismatch {
            key: "bucket/key1".to_string(),
            expected_hash: "abc123".to_string(),
            actual_hash: "def456".to_string(),
            node: "127.0.0.1:9000".to_string(),
        };
        assert_eq!(m.key, "bucket/key1");
        assert_eq!(m.expected_hash, "abc123");
        assert_eq!(m.actual_hash, "def456");
        assert_eq!(m.node, "127.0.0.1:9000");
    }

    #[test]
    fn record_stores_correct_blake3_hash() {
        let manifest = Manifest::new();
        let body = b"test data for hashing";
        manifest.record("bucket", "key1", body);

        let expected = blake3::hash(body);
        let entry = manifest.entries.get("bucket/key1").expect("entry should exist");
        let (versions, deleted) = entry.value();
        assert!(
            versions.contains(expected.as_bytes()),
            "recorded version set must contain the hash"
        );
        assert!(!deleted.load(Ordering::Relaxed));
    }

    #[test]
    fn record_delete_skips_entry_during_active_count() {
        let manifest = Manifest::new();
        // Record 10 keys.
        for i in 0..10 {
            manifest.record("bucket", &format!("key{i}"), b"data");
        }
        assert_eq!(manifest.active_count(), 10);
        assert_eq!(manifest.len(), 10);

        // Delete keys 0-4.
        for i in 0..5 {
            manifest.record_delete("bucket", &format!("key{i}"));
        }

        // Active count should be 5, total count still 10.
        assert_eq!(manifest.active_count(), 5, "5 active keys remaining");
        assert_eq!(manifest.len(), 10, "total count unchanged by delete");
    }

    #[test]
    fn verify_skips_deleted_entries() {
        // This test validates the internal logic: deleted entries are
        // skipped by the verify loop. Since verify() requires a live
        // Cluster (integration-level), we verify the skip logic
        // indirectly by checking deleted flags and active_count.
        let manifest = Manifest::new();

        // Record 100 keys.
        for i in 0..100 {
            manifest.record("bucket", &format!("key{i}"), b"data");
        }
        assert_eq!(manifest.active_count(), 100);

        // Delete 30 of them.
        for i in 0..30 {
            manifest.record_delete("bucket", &format!("key{i}"));
        }

        // 70 active, 100 total.
        assert_eq!(manifest.active_count(), 70);
        assert_eq!(manifest.len(), 100);

        // Verify each deleted key is flagged.
        for i in 0..30 {
            let entry = manifest.entries.get(&format!("bucket/key{i}")).expect("entry exists");
            let (_hash, deleted) = entry.value();
            assert!(deleted.load(Ordering::Relaxed), "key key{i} should be deleted");
        }

        // Verify active keys are NOT flagged.
        for i in 30..100 {
            let entry = manifest.entries.get(&format!("bucket/key{i}")).expect("entry exists");
            let (_hash, deleted) = entry.value();
            assert!(!deleted.load(Ordering::Relaxed), "key key{i} should be active");
        }
    }

    #[test]
    fn manifest_is_empty_after_creation() {
        let manifest = Manifest::new();
        assert!(manifest.is_empty());
        assert_eq!(manifest.len(), 0);
        assert_eq!(manifest.active_count(), 0);
    }

    #[test]
    fn manifest_default_creates_empty() {
        let manifest = Manifest::default();
        assert!(manifest.is_empty());
    }
}
