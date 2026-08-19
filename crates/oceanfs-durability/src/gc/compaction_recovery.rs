//! Compaction crash recovery — ADR-0025 Decision 4's recovery shape.
//!
//! The compactor (see `segment_compactor.rs`) is a state machine whose
//! durable checkpoints are events; a crash can leave a compaction unit
//! between milestones. Recovery is **fold + one objects-CF read per
//! unit**: the folded registry tells which new segments are sealed
//! (with their `repacked_from` marker) and which old segments are
//! deleted; one objects-CF read per unit tells which side the objects
//! point at. The startup dispatcher performs the returned actions
//! (through the coordinator for deletes, through the shard store for
//! sweeps — see `startup-rebuild-from-machine`).
//!
//! Crash-window rows 7–9 of ADR-0025 §Crash-window table:
//!
//! | Crash between | Folded state | Recovery action |
//! |---|---|---|
//! | NewSealed → ObjectsMoved | New sealed, objects→old | `SweepNewOrphan(new)` — the new `.dat` is an orphan (row 7) |
//! | ObjectsMoved → OldDeleted | Objects→new, old sealed | `FinishOldDeletion(old)` (row 8) |
//! | OldDeleted → OldRemoved | Old deleted, `.dat` present | `SweepOldDat(old)` (row 9) |
//!
//! The pre-`NewSealed` windows need no action here: a crash before the
//! `SealEvent` leaves the new segment `Reserved`, and the data-WAL
//! pass adopts its durable `.dat` (crash-window row 3) or drops the
//! empty reserve (row 1); the unreferenced replacement is then reaped
//! like any orphan.

use std::sync::Arc;

use oceanfs_core::{SegmentId, SizeTier};
use oceanfs_storage::segment::lifecycle::{SegmentLifecycleRegistry, SegmentState};

use crate::Result;

/// The compactor's in-memory progress through one compaction unit.
///
/// The durable checkpoints are the **events**, not this enum (ADR-0025
/// Decision 4): `NewSealed` is durable when the `SealEvent(new)` is
/// appended, `OldDeleted` when the `DeleteEvent(old)` is appended. The
/// enum exists for observability and tests — crash recovery never reads
/// it (it reads the fold).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionState {
    /// New `.dat` being written (no durable event yet).
    Copying,
    /// `SealEvent(new)` appended — the new segment is real `[durable]`.
    NewSealed,
    /// `PutObject(new refs)` committed in the objects CF `[RocksDB]`.
    ObjectsMoved,
    /// `DeleteEvent(old)` appended — the old segment is gone `[durable]`.
    OldDeleted,
    /// Old `.dat` unlinked.
    OldRemoved,
}

/// One compaction unit: repack `old_segment_id` into `new_segment_id`.
///
/// The tier/EC shape never changes across a repack (the `SealEvent(new)`
/// carries the same tier/EC as the source — a repack that dropped the
/// storage shape would be rejected at the seal transition).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionUnit {
    /// The source segment being repacked away.
    pub old_segment_id: SegmentId,
    /// The replacement segment produced by the repack.
    pub new_segment_id: SegmentId,
    /// The storage tier (unchanged by the repack).
    pub tier: SizeTier,
    /// Erasure-coding data shard count (unchanged by the repack).
    pub ec_k: u8,
    /// Erasure-coding parity shard count (unchanged by the repack).
    pub ec_m: u8,
}

/// The one objects-CF read per unit — the only cross-store hop in
/// compaction recovery (RocksDB stays the objects' store; ADR-0025
/// Decision 4: "fold + one objects-CF read").
///
/// Production implementation wraps the metadata store's object scan;
/// tests use an instrumented double that counts reads (the DoD asserts
/// exactly one read per unit, no per-chunk scans).
pub trait ObjectLookup: Send + Sync {
    /// Returns whether any object references `segment_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the objects store cannot be read.
    fn is_referenced(&self, segment_id: SegmentId) -> Result<bool>;
}

/// Production [`ObjectLookup`] over the objects store: one
/// `list_objects_all_with_bucket` scan answers the reference question
/// in a single store call — the DoD's "one objects-CF read per unit,
/// no per-chunk scans".
pub struct StoreObjectLookup(pub Arc<dyn oceanfs_storage_api::MetadataStore>);

impl ObjectLookup for StoreObjectLookup {
    fn is_referenced(&self, segment_id: SegmentId) -> Result<bool> {
        Ok(self
            .0
            .list_objects_all_with_bucket()
            .into_iter()
            .flatten()
            .any(|(_, meta)| meta.chunks.iter().any(|c| c.segment_id == segment_id)))
    }
}

/// A startup recovery action for an incomplete compaction unit.
///
/// The startup dispatcher performs each action: deletes through the
/// coordinator (`request_delete` — durable before unlink), sweeps
/// through the shard store (idempotent — a missing file is `Ok(0)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionRecoveryAction {
    /// Row 8: objects point at the new segment, the old segment is
    /// still `Sealed` — finish the unit's deletion:
    /// `request_delete(old)` then sweep the old `.dat`.
    FinishOldDeletion(SegmentId),
    /// Row 7: objects still point at the old segment, the new segment
    /// is `Sealed` and unreferenced — its `.dat` is an orphan:
    /// `request_delete(new)` then sweep the new `.dat`.
    SweepNewOrphan(SegmentId),
    /// Row 9: the old segment is already `Deleted` (its `DeleteEvent`
    /// is durable) — only its `.dat` residue remains: sweep it.
    SweepOldDat(SegmentId),
}

/// Scans the folded registry for incomplete compaction units and
/// returns the recovery action per unit (rows 7–9 of ADR-0025
/// §Crash-window table).
///
/// The scan is O(marked units): every `Sealed` entry carrying the
/// `repacked_from` marker (ADR-0025 Decision 4) is examined exactly
/// once, with exactly **one** objects-CF read (`is_referenced`) per
/// unit. No per-chunk scans. A unit whose `SealEvent` was truncated
/// away before `NewSealed` has no marker and is handled by the
/// data-WAL pass (row 3 adoption / row 1 drop) plus the orphan reaper.
///
/// # Errors
///
/// Returns an error if the objects-CF read fails (the caller aborts
/// startup — a wrong action could delete live data).
pub fn recover_incomplete_compactions(
    registry: &SegmentLifecycleRegistry,
    objects: &dyn ObjectLookup,
) -> Result<Vec<CompactionRecoveryAction>> {
    // Collect the marked units under the registry's read guards
    // (perf 7.1: no lock held across the objects-CF reads).
    let mut units: Vec<(SegmentId, SegmentId)> = Vec::new();
    registry.for_each(|id, entry| {
        if let Some(old) = entry.repacked_from {
            units.push((id, old));
        }
    });

    let mut actions = Vec::with_capacity(units.len());
    for (new_id, old_id) in units {
        // The one objects-CF read per unit (the DoD's instrumented
        // assertion): do objects point at the new segment or the old?
        if objects.is_referenced(new_id)? {
            // ObjectsMoved is durable: the new segment is authoritative.
            // The old side decides the action: still Sealed → finish
            // the deletion (row 8); Deleted/evicted → only the `.dat`
            // residue can remain (row 9 — the sweep is idempotent).
            match registry.get(old_id) {
                Some(entry) if entry.state == SegmentState::Sealed => {
                    actions.push(CompactionRecoveryAction::FinishOldDeletion(old_id));
                }
                Some(entry) if entry.state == SegmentState::Reserved => {
                    // Unreachable by construction (GC compacts only
                    // sealed segments; the old side was Sealed when the
                    // unit started). Treating it as a deletion target
                    // could sweep a live segment — skip it instead.
                    tracing::warn!(
                        old_segment_id = %old_id,
                        new_segment_id = %new_id,
                        "compaction unit's old segment is Reserved; skipping (unreachable state)"
                    );
                }
                _ => {
                    actions.push(CompactionRecoveryAction::SweepOldDat(old_id));
                }
            }
        } else {
            // The unit never reached ObjectsMoved: objects still point
            // at the old segment, so the new segment's `.dat` is an
            // orphan (row 7).
            actions.push(CompactionRecoveryAction::SweepNewOrphan(new_id));
        }
    }
    Ok(actions)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashSet;

    use oceanfs_core::{LifecycleConfig, SegmentMetadata, SizeTier};
    use oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry;

    use super::*;

    /// Instrumented objects-CF double: counts reads, answers from a set.
    struct CountingLookup {
        referenced: HashSet<SegmentId>,
        reads: std::sync::atomic::AtomicUsize,
    }

    impl CountingLookup {
        fn new(referenced: impl IntoIterator<Item = SegmentId>) -> Self {
            Self { referenced: referenced.into_iter().collect(), reads: 0.into() }
        }
        fn reads(&self) -> usize {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl ObjectLookup for CountingLookup {
        fn is_referenced(&self, segment_id: SegmentId) -> Result<bool> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.referenced.contains(&segment_id))
        }
    }

    fn sealed_meta(id: SegmentId) -> SegmentMetadata {
        SegmentMetadata {
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1_700_000_000_000),
        }
    }

    fn reserved_meta(id: SegmentId) -> SegmentMetadata {
        SegmentMetadata { merkle_root: None, sealed_at: None, ..sealed_meta(id) }
    }

    /// Registers a Sealed entry carrying the compaction marker.
    fn seed_marked(registry: &SegmentLifecycleRegistry, new: SegmentId, old: SegmentId) {
        registry.reserve(new, reserved_meta(new)).unwrap();
        registry.seal_with(new, sealed_meta(new), Some(old)).unwrap();
    }

    #[test]
    fn row7_objects_still_point_at_old_yields_sweep_new_orphan() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let new = SegmentId::new();
        let old = SegmentId::new();
        seed_marked(&registry, new, old);
        let lookup = CountingLookup::new([old]);

        let actions = recover_incomplete_compactions(&registry, &lookup).unwrap();
        assert_eq!(actions, vec![CompactionRecoveryAction::SweepNewOrphan(new)]);
        assert_eq!(lookup.reads(), 1, "exactly one objects-CF read per unit");
    }

    #[test]
    fn row8_objects_point_at_new_with_old_sealed_yields_finish_old_deletion() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let new = SegmentId::new();
        let old = SegmentId::new();
        seed_marked(&registry, new, old);
        registry.reserve(old, reserved_meta(old)).unwrap();
        registry.seal(old, sealed_meta(old)).unwrap();
        let lookup = CountingLookup::new([new]);

        let actions = recover_incomplete_compactions(&registry, &lookup).unwrap();
        assert_eq!(actions, vec![CompactionRecoveryAction::FinishOldDeletion(old)]);
        assert_eq!(lookup.reads(), 1, "exactly one objects-CF read per unit");
    }

    #[test]
    fn row9_old_deleted_and_evicted_yields_sweep_old_dat() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig {
            lifecycle_registry_shards: 8,
            delete_grace_ms: 0,
        });
        let new = SegmentId::new();
        let old = SegmentId::new();
        seed_marked(&registry, new, old);
        registry.reserve(old, reserved_meta(old)).unwrap();
        registry.seal(old, sealed_meta(old)).unwrap();
        registry.delete(old).unwrap(); // grace 0 → evicted
        let lookup = CountingLookup::new([new]);

        let actions = recover_incomplete_compactions(&registry, &lookup).unwrap();
        assert_eq!(actions, vec![CompactionRecoveryAction::SweepOldDat(old)]);
        assert_eq!(lookup.reads(), 1, "exactly one objects-CF read per unit");
    }

    #[test]
    fn complete_units_and_unmarked_entries_produce_no_actions() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let new = SegmentId::new();
        let old = SegmentId::new();
        seed_marked(&registry, new, old);
        // The unit is complete: objects→new, old deleted + swept.
        let lookup = CountingLookup::new([new]);
        let actions = recover_incomplete_compactions(&registry, &lookup).unwrap();
        // old is missing from the registry → SweepOldDat is the residue
        // action (idempotent even when the file is already gone).
        assert_eq!(actions, vec![CompactionRecoveryAction::SweepOldDat(old)]);
        assert_eq!(lookup.reads(), 1);

        // An ordinary sealed segment (no marker) never produces actions.
        let plain = SegmentId::new();
        let registry2 = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        registry2.reserve(plain, reserved_meta(plain)).unwrap();
        registry2.seal(plain, sealed_meta(plain)).unwrap();
        let lookup2 = CountingLookup::new([plain]);
        let actions2 = recover_incomplete_compactions(&registry2, &lookup2).unwrap();
        assert!(actions2.is_empty(), "no marker → no unit → no actions");
        assert_eq!(lookup2.reads(), 0, "no unit → no objects-CF read");
    }

    #[test]
    fn multiple_units_read_once_each() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let units: Vec<(SegmentId, SegmentId)> =
            (0..3).map(|_| (SegmentId::new(), SegmentId::new())).collect();
        for (new, old) in &units {
            seed_marked(&registry, *new, *old);
            registry.reserve(*old, reserved_meta(*old)).unwrap();
            registry.seal(*old, sealed_meta(*old)).unwrap();
        }
        let referenced: HashSet<SegmentId> = units.iter().map(|(new, _)| *new).collect();
        let lookup = CountingLookup::new(referenced);

        let actions = recover_incomplete_compactions(&registry, &lookup).unwrap();
        assert_eq!(actions.len(), units.len(), "one action per incomplete unit");
        assert_eq!(
            lookup.reads(),
            units.len(),
            "exactly one objects-CF read per unit, no per-chunk scans"
        );
    }
}
