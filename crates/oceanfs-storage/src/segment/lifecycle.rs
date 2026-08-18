//! Segment lifecycle machine — in-memory registry + single coordinator.
//!
//! ADR-0025 Decision 1, migration phase 1. This module is the runtime
//! half of the segment-lifecycle redesign: a sharded in-memory
//! [`SegmentLifecycleRegistry`] holding exactly one entry per **live**
//! segment, and a single [`SegmentLifecycleCoordinator`] that is the
//! **only writer** of segment lifecycle state.
//!
//! In phase 1 the RocksDB `segments` CF write remains as the
//! coordinator's durable side-effect (no behavior change), but every
//! CF writer is routed through the coordinator — the pool, the seal
//! worker's persistence path, the orphan reaper, and WAL replay stop
//! touching state directly; they *request* transitions. The
//! phantom-downgrade race and the idle-seal gap die here, by
//! construction, before any event log exists:
//!
//! - **No downgrade.** The transition API is typed: `reserve` accepts
//!   absent/`Reserved`, `seal` accepts `Reserved` only, `delete`
//!   accepts `Reserved`/`Sealed`. There is **no method that assigns a
//!   lower state**, so a `sealed_at: None` re-write over a `Sealed`
//!   entry (the phantom-downgrade race) is not expressible.
//! - **Reserve before data.** `request_reserve` returns `Ok` only
//!   after its durable CF write; the write path calls it before the
//!   first `DataEntry` (WAL entry) of its segment.
//! - **Idle-seal.** The coordinator owns the idle-seal timer:
//!   [`SegmentLifecycleCoordinator::seal_idle_segments`] sweeps the
//!   pools for partially-filled segments that stopped receiving
//!   writes, sealing them within `seal_timeout_ms` (empty segments are
//!   never sealed — recovery drops empty reserves).
//!
//! # LOCK ORDER
//!
//! A registry shard is a **leaf** lock in the coordinator's own
//! transitions: `validate (shard read, released) → durable I/O (no
//! locks) → fold (shard write)` — the durable CF write and the fold
//! are separate critical sections, so no I/O ever runs under a shard
//! lock (performance §7.1), and the coordinator never acquires a slot
//! lock while holding a shard.
//!
//! The one legal multi-lock order in the crate is **slot lock →
//! registry shard write**, on the pool's fill path: the frozen buffer
//! is attached to the entry in the same critical section as the slot
//! freeze (the read window is continuous — `lifecycle-read-path`). It
//! is safe because no code path acquires a shard and then a slot lock:
//! `read_source` releases the shard before returning (the caller's
//! slot scan happens after), `request_*` never touch slots, and
//! `seal_idle_segments` sweeps slots and retries in-flight seals in
//! separate steps. If a future feature introduces another order, this
//! comment must document the full ordering.
//!
//! Memory bound (ADR-0025 Decision 5 — stated at TB scale, not
//! load-test scale): ~300 B/entry × ~170K live segments/TB → **~50 MB
//! at 1 TB, ~500 MB at 10 TB (1.7M segments), ~5 GB at 100 TB**. The
//! bound is O(live segments), not O(lifetime writes): `delete()`
//! evicts. The `oceanfs_lifecycle_registry_entries` and
//! `oceanfs_lifecycle_registry_bytes_estimate` gauges make the
//! actual cost visible continuously.

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use oceanfs_core::{
    Gauge, LabelSet, LifecycleConfig, MetricRegistrar, SegmentId, SegmentMetadata, SizeTier,
};
use oceanfs_storage_api::MetadataStore;
use parking_lot::RwLock;

use crate::{error::Result, segment::pool::SegmentPool};

/// Estimated in-memory cost of one live registry entry, in bytes
/// (ADR-0025 Decision 5: ~300 B/entry including `HashMap` overhead).
const ESTIMATED_BYTES_PER_ENTRY: u64 = 300;

/// The lifecycle states a segment can be in.
///
/// The only three states — no sub-states, no "sealed but downgradable"
/// representation. A transition may never move a segment from a higher
/// state to a lower one; the transition API shape makes a downgrade
/// unrepresentable (ADR-0025 Decision 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SegmentState {
    /// Segment reserved (phantom registered / `ReserveEvent` appended);
    /// data may be in flight but the segment is not yet durable on disk.
    Reserved,
    /// Segment sealed: its `.dat` file is durable and its full metadata
    /// (tier, ec_k/m, seal-time `merkle_root`) is committed.
    Sealed,
    /// Segment deleted: the durable deletion happened; the entry remains
    /// only for the (configurable) delete grace, then is evicted.
    Deleted,
}

/// One live segment's lifecycle entry: its state and full metadata.
///
/// The full `SegmentMetadata` lives with the state (tier, ec_k/ec_m,
/// `merkle_root` filled at seal). `data_wal_pos` is added by the
/// `event-wal-format` feature (epic 2), not here.
#[derive(Debug)]
pub struct LifecycleEntry {
    /// The segment's current lifecycle state.
    pub state: SegmentState,
    /// The segment's full metadata as last committed by a transition.
    pub metadata: SegmentMetadata,
    /// When a `Deleted` entry's grace expires (entry eviction time).
    /// Meaningful only for `Deleted` entries; set at construction.
    evict_at: Instant,
    /// The frozen buffer between fill and durable seal (was the
    /// `sealing_data` side-map; ADR-0025 Decision 2 — the read window
    /// is owned by the machine's entry). Attached by the pool's fill
    /// transition in the same critical section as the freeze; cleared
    /// by the seal transition's fold. `Bytes::clone` — refcount only.
    in_flight: Option<Bytes>,
    /// Whether the seal enqueue is taken care of: `true` from the
    /// freeze (the fill path owns the enqueue) until the seal
    /// completes; reset to `false` when an enqueue attempt fails so
    /// the idle-seal driver retries the seal (a full seal queue delays
    /// the seal but never removes the read window).
    seal_queued: bool,
}

impl LifecycleEntry {
    /// Creates a new lifecycle entry in the given state.
    pub(crate) fn new(state: SegmentState, metadata: SegmentMetadata) -> Self {
        Self { state, metadata, evict_at: Instant::now(), in_flight: None, seal_queued: false }
    }
}

/// Where a segment's data can be read from — the answer to "where do I
/// read this segment?" in one registry lookup (ADR-0025 Decision 2).
///
/// NOTE: this type is distinct from
/// `oceanfs_storage::io::SegmentReadSource` (the HTTP handler's
/// memory-vs-file source tracking). The two live in different modules
/// and serve different purposes; this one is the machine's resolution.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SegmentReadSource {
    /// `Reserved` entry with no in-flight data: the data lives in an
    /// active pool slot buffer (append-mode, mutable). The caller
    /// performs the slot scan for this case only.
    ActiveSlot,
    /// `Reserved`/`Sealed` entry carrying the frozen buffer between
    /// fill and durable seal (was `sealing_data`). The `Bytes` is a
    /// refcounted handle — serving is a slice, no copy.
    InFlight(Bytes),
    /// Durable `.dat` — the read falls through to the disk reader.
    Sealed,
    /// Not this node's segment (replica fallback) or gone (404 path).
    Missing,
}

/// A lifecycle transition that could not be applied.
///
/// Every variant means **no state was mutated**: illegal transitions
/// return an error and leave the registry unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransitionError {
    /// The segment is not present in the registry.
    #[error("segment not present in the lifecycle registry")]
    Missing,
    /// `reserve` on an id that is already `Reserved` — idempotent
    /// re-reserves return `Ok`; this error is not produced for them.
    #[error("segment is already reserved")]
    AlreadyReserved,
    /// A transition required a non-`Sealed` entry but the segment is
    /// already `Sealed`. In particular: `reserve` on a `Sealed` id
    /// (the phantom-downgrade write) is rejected here.
    #[error("segment is already sealed")]
    AlreadySealed,
    /// A transition required a non-`Deleted` entry but the segment is
    /// already `Deleted` (within the delete grace).
    #[error("segment is already deleted")]
    AlreadyDeleted,
    /// `seal` on an entry that is not `Reserved`.
    #[error("segment is not in the Reserved state")]
    NotReserved,
    /// The durable side-effect (phase 1: the CF write) failed; the
    /// registry fold was skipped.
    #[error("durable lifecycle write failed: {0}")]
    DurableWriteFailed(String),
}

/// Returns the number of registry shards for the given configuration.
///
/// The shard count is `lifecycle_registry_shards`, clamped to at
/// least 1 (a zero shard count is treated as the default of 1 so the
/// registry is always usable).
///
/// # Examples
///
/// ```
/// use oceanfs_core::LifecycleConfig;
/// use oceanfs_storage::segment::lifecycle::shard_count;
///
/// let config = LifecycleConfig::default();
/// assert_eq!(shard_count(&config), 64);
/// ```
pub fn shard_count(config: &LifecycleConfig) -> usize {
    config.lifecycle_registry_shards.max(1)
}

/// In-memory segment lifecycle registry — a sharded map.
///
/// Holds exactly one entry per **live** segment (`Reserved` or
/// `Sealed`, not yet `Deleted`), sharded across
/// `lifecycle_registry_shards` independent
/// `parking_lot::RwLock<HashMap<SegmentId, LifecycleEntry>>` shards;
/// the shard for a segment is chosen by hashing its `SegmentId`. Reads
/// (GET-path resolution, GC/scrub enumeration) never block each other;
/// writes are once-per-lifecycle (fill / seal / delete).
///
/// All methods take `&self`. Illegal transitions return
/// [`TransitionError`] variants and **never mutate state** — there is
/// no method that assigns a lower state, so a downgrade is not
/// expressible (ADR-0025 Decision 1).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{LifecycleConfig, SegmentId, SegmentMetadata, SizeTier};
/// use oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry;
///
/// let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
/// let id = SegmentId::new();
/// let meta = SegmentMetadata {
///     segment_id: id,
///     ec_k: 4,
///     ec_m: 2,
///     size_tier: SizeTier::Standard,
///     merkle_root: None,
///     storage_locations: smallvec::SmallVec::new(),
///     sealed_at: None,
/// };
/// assert!(registry.reserve(id, meta).is_ok());
/// assert_eq!(registry.len(), 1);
/// ```
pub struct SegmentLifecycleRegistry {
    shards: Box<[RwLock<HashMap<SegmentId, LifecycleEntry>>]>,
    config: LifecycleConfig,
}

impl SegmentLifecycleRegistry {
    /// Creates a new sharded registry from the given configuration.
    pub fn new(config: &LifecycleConfig) -> Self {
        let shard_count = shard_count(config);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self { shards: shards.into_boxed_slice(), config: config.clone() }
    }

    /// Returns the shard index for a segment id.
    fn shard_for(&self, id: SegmentId) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        id.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    /// Returns the delete grace configured for this registry.
    fn delete_grace(&self) -> Duration {
        Duration::from_millis(self.config.delete_grace_ms)
    }

    /// Whether a `Deleted` entry's grace has expired.
    fn deleted_expired(entry: &LifecycleEntry, now: Instant) -> bool {
        entry.state == SegmentState::Deleted && entry.evict_at <= now
    }

    /// Removes expired `Deleted` entries from one shard.
    ///
    /// Callers must hold the shard's WRITE lock. Read paths never
    /// evict — they skip expired entries (treating them as absent), so
    /// reads stay on read locks (performance §7.2).
    fn evict_expired_locked(shard: &mut HashMap<SegmentId, LifecycleEntry>, now: Instant) {
        shard.retain(|_, entry| !Self::deleted_expired(entry, now));
    }

    // ------------------------------------------------------------------
    // Public transitions (validate + fold in one critical section)
    // ------------------------------------------------------------------

    /// Reserves a segment: `Ok` only when the id is absent or already
    /// `Reserved` (idempotent re-reserve — the existing entry is kept
    /// unchanged). On a `Sealed`/`Deleted` id → `Err(AlreadySealed)`
    /// / `Err(AlreadyDeleted)`, no mutation.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError::AlreadySealed`] or
    /// [`TransitionError::AlreadyDeleted`] when the id holds a higher
    /// state — the phantom-downgrade write is rejected here.
    pub fn reserve(&self, id: SegmentId, metadata: SegmentMetadata) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        Self::evict_expired_locked(&mut guard, Instant::now());
        match guard.get(&id) {
            None => {
                guard.insert(id, LifecycleEntry::new(SegmentState::Reserved, metadata));
                Ok(())
            }
            Some(entry) if entry.state == SegmentState::Reserved => Ok(()),
            Some(entry) if entry.state == SegmentState::Sealed => {
                Err(TransitionError::AlreadySealed)
            }
            Some(_) => Err(TransitionError::AlreadyDeleted),
        }
    }

    /// Seals a segment: `Reserved` → `Sealed` only, taking the full
    /// sealed metadata (incl. the seal-time `merkle_root`).
    /// `Err(AlreadySealed)` / `Err(NotReserved)` / `Err(Missing)`
    /// otherwise — no mutation.
    ///
    /// # Errors
    ///
    /// Returns a [`TransitionError`] when the entry is not `Reserved`.
    pub fn seal(&self, id: SegmentId, metadata: SegmentMetadata) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        Self::evict_expired_locked(&mut guard, Instant::now());
        match guard.get_mut(&id) {
            Some(entry) if entry.state == SegmentState::Reserved => {
                entry.state = SegmentState::Sealed;
                entry.metadata = metadata;
                // The seal transition closes the read window: the `.dat`
                // is authoritative once sealed (the in-flight buffer is
                // released — the memory-bound test pins this).
                entry.in_flight = None;
                entry.seal_queued = false;
                Ok(())
            }
            Some(entry) if entry.state == SegmentState::Sealed => {
                Err(TransitionError::AlreadySealed)
            }
            Some(_) => Err(TransitionError::NotReserved),
            None => Err(TransitionError::Missing),
        }
    }

    /// Deletes a segment: `Reserved` | `Sealed` → `Deleted`;
    /// `Err(AlreadyDeleted)` / `Err(Missing)` otherwise. The entry is
    /// evicted after the configured grace (default: immediate),
    /// keeping the registry O(live segments).
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError::AlreadyDeleted`] or
    /// [`TransitionError::Missing`] when no live entry exists.
    pub fn delete(&self, id: SegmentId) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        let now = Instant::now();
        Self::evict_expired_locked(&mut guard, now);
        match guard.get_mut(&id) {
            Some(entry) if entry.state == SegmentState::Deleted => {
                Err(TransitionError::AlreadyDeleted)
            }
            Some(entry) => {
                entry.state = SegmentState::Deleted;
                entry.evict_at = now + self.delete_grace();
                if self.delete_grace().is_zero() {
                    guard.remove(&id);
                }
                Ok(())
            }
            None => Err(TransitionError::Missing),
        }
    }

    // ------------------------------------------------------------------
    // Read accessors
    // ------------------------------------------------------------------

    /// Returns the entry for a live segment, or `None`.
    ///
    /// Entries whose delete grace has expired are treated as absent
    /// (they are evicted lazily by the next write to their shard).
    pub fn get(&self, id: SegmentId) -> Option<LifecycleEntry> {
        let shard = &self.shards[self.shard_for(id)];
        let guard = shard.read();
        guard.get(&id).filter(|entry| !Self::deleted_expired(entry, Instant::now())).map(|entry| {
            LifecycleEntry {
                state: entry.state,
                metadata: entry.metadata.clone(),
                evict_at: entry.evict_at,
                in_flight: entry.in_flight.clone(),
                seal_queued: entry.seal_queued,
            }
        })
    }

    /// Resolves where a segment's data can be read from — one registry
    /// lookup (ADR-0025 Decision 2).
    ///
    /// The resolution: an entry carrying the frozen in-flight buffer
    /// (`Reserved`/`Sealed`) → [`SegmentReadSource::InFlight`]; a
    /// `Reserved` entry without one → [`SegmentReadSource::ActiveSlot`]
    /// (the caller's slot scan serves it); `Sealed` → the durable
    /// `.dat` (the caller falls through to the disk reader);
    /// `Missing`/`Deleted` → not resolvable here (replica fallback or
    /// 404). Holds a shard read lock only — no I/O, no allocation
    /// (performance §7.1).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{LifecycleConfig, SegmentId, SegmentMetadata, SizeTier};
    /// use oceanfs_storage::segment::lifecycle::{
    ///     SegmentLifecycleRegistry, SegmentReadSource,
    /// };
    ///
    /// let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
    /// let id = SegmentId::new();
    /// assert!(matches!(registry.read_source(id), SegmentReadSource::Missing));
    /// let meta = SegmentMetadata {
    ///     segment_id: id,
    ///     ec_k: 4,
    ///     ec_m: 2,
    ///     size_tier: SizeTier::Standard,
    ///     merkle_root: None,
    ///     storage_locations: smallvec::SmallVec::new(),
    ///     sealed_at: None,
    /// };
    /// registry.reserve(id, meta).unwrap();
    /// assert!(matches!(registry.read_source(id), SegmentReadSource::ActiveSlot));
    /// ```
    pub fn read_source(&self, id: SegmentId) -> SegmentReadSource {
        let shard = &self.shards[self.shard_for(id)];
        let guard = shard.read();
        match guard.get(&id) {
            None => SegmentReadSource::Missing,
            Some(entry) if Self::deleted_expired(entry, Instant::now()) => {
                SegmentReadSource::Missing
            }
            Some(entry) if entry.state == SegmentState::Deleted => SegmentReadSource::Missing,
            Some(entry) => {
                if let Some(data) = &entry.in_flight {
                    SegmentReadSource::InFlight(data.clone())
                } else if entry.state == SegmentState::Sealed {
                    SegmentReadSource::Sealed
                } else {
                    SegmentReadSource::ActiveSlot
                }
            }
        }
    }

    /// Attaches a frozen buffer to a `Reserved` entry — the pool's fill
    /// transition calls this in the SAME critical section as the slot
    /// freeze, so the read window is continuous (ADR-0025 Decision 2).
    ///
    /// Also marks the seal as queued: the fill path owns the enqueue
    /// from the freeze on; only an enqueue failure resets that flag so
    /// the idle-seal driver retries (a full seal queue delays the seal
    /// but never removes the read window).
    ///
    /// When the entry does not exist yet — the **fill-before-reserve
    /// window**: the write path's durable reserve lands AFTER the append
    /// that filled the segment (the segment id is only known once the
    /// append returns) — a registry-only `Reserved` entry is inserted
    /// with the pool's tier/EC metadata. This is a pure in-memory fold:
    /// the coordinator remains the only DURABLE writer, and its
    /// `request_reserve` lands µs later as an idempotent no-op.
    ///
    /// Returns `false` only when the entry holds a higher state
    /// (`Sealed`/`Deleted` — impossible by construction; the caller
    /// logs).
    pub(crate) fn attach_in_flight(
        &self,
        id: SegmentId,
        tier: SizeTier,
        ec_k: u8,
        ec_m: u8,
        data: Bytes,
    ) -> bool {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        match guard.get_mut(&id) {
            Some(entry) if entry.state == SegmentState::Reserved => {
                entry.in_flight = Some(data);
                entry.seal_queued = true;
                true
            }
            None => {
                let meta = SegmentMetadata {
                    segment_id: id,
                    ec_k,
                    ec_m,
                    size_tier: tier,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                };
                guard.insert(
                    id,
                    LifecycleEntry {
                        state: SegmentState::Reserved,
                        metadata: meta,
                        evict_at: Instant::now(),
                        in_flight: Some(data),
                        seal_queued: true,
                    },
                );
                true
            }
            Some(_) => false,
        }
    }

    /// Marks an entry's seal as queued (the idle-seal driver's retry
    /// succeeded). No-op when the entry is gone or already sealed.
    pub(crate) fn mark_seal_queued(&self, id: SegmentId) {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        if let Some(entry) = guard.get_mut(&id) {
            if entry.in_flight.is_some() {
                entry.seal_queued = true;
            }
        }
    }

    /// Marks an entry's seal as NOT queued: an enqueue attempt failed
    /// (seal queue at capacity or closed), so the idle-seal driver must
    /// retry the seal. The in-flight window stays readable.
    pub(crate) fn mark_seal_unqueued(&self, id: SegmentId) {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        if let Some(entry) = guard.get_mut(&id) {
            entry.seal_queued = false;
        }
    }

    /// Returns the number of entries currently carrying a frozen
    /// in-flight buffer (the seal pipeline's in-flight set).
    pub(crate) fn in_flight_count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let guard = shard.read();
                guard.values().filter(|entry| entry.in_flight.is_some()).count()
            })
            .sum()
    }

    /// Returns the number of frozen entries whose seal enqueue FAILED
    /// (`seal_queued == false`) — the set the idle-seal driver must
    /// retry. Test-only: the production gate bounds the TOTAL
    /// in-flight set (`in_flight_count`) at `IN_FLIGHT_CAP`.
    #[cfg(test)]
    pub(crate) fn in_flight_unqueued_count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let guard = shard.read();
                guard
                    .values()
                    .filter(|entry| entry.in_flight.is_some() && !entry.seal_queued)
                    .count()
            })
            .sum()
    }

    /// Enumerates a snapshot of the **live** entries (`Reserved` /
    /// `Sealed`; `Deleted` entries — including those still inside the
    /// delete grace — are skipped).
    ///
    /// The closure runs under each shard's read lock, one shard at a
    /// time. It must not call back into this registry (a mutating call
    /// would deadlock on the shard it holds).
    pub fn for_each(&self, mut f: impl FnMut(SegmentId, &LifecycleEntry)) {
        let now = Instant::now();
        for shard in self.shards.iter() {
            let guard = shard.read();
            for (id, entry) in guard.iter() {
                if entry.state != SegmentState::Deleted && !Self::deleted_expired(entry, now) {
                    f(*id, entry);
                }
            }
        }
    }

    /// Returns the number of **live** entries (`Reserved` + `Sealed`;
    /// `Deleted` entries — even inside the delete grace — are not
    /// counted). O(live segments), not O(lifetime writes).
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let guard = shard.read();
                guard.values().filter(|entry| entry.state != SegmentState::Deleted).count()
            })
            .sum()
    }

    /// Returns `true` when the registry holds no live entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimates the registry's in-memory footprint in bytes:
    /// `len() × ~300 B/entry` (ADR-0025 Decision 5).
    pub fn mem_estimate_bytes(&self) -> u64 {
        self.len() as u64 * ESTIMATED_BYTES_PER_ENTRY
    }

    /// Seeds the registry with one durable entry (startup population).
    ///
    /// Insert-if-absent only: entries already present (e.g. a segment
    /// reserved by WAL replay before the seed ran) are left untouched.
    /// No durable write happens here — the caller's store already holds
    /// the entry; this is a pure registry fold so the coordinator is the
    /// complete single writer over EXISTING data too (the reaper's
    /// `request_delete` validates against the registry).
    pub(crate) fn seed_entry(&self, id: SegmentId, state: SegmentState, metadata: SegmentMetadata) {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        let now = Instant::now();
        Self::evict_expired_locked(&mut guard, now);
        guard.entry(id).or_insert_with(|| LifecycleEntry::new(state, metadata));
    }

    // ------------------------------------------------------------------
    // Split validate/fold primitives (coordinator-only)
    //
    // The coordinator validates against the registry (read-only),
    // releases the shard lock, performs the durable CF write, then
    // folds the transition into the registry (write). The split keeps
    // I/O out of the lock bodies (performance §7.1) while preserving
    // "no mutation on illegal transitions".
    // ------------------------------------------------------------------

    /// Validates a `reserve` without mutating: `Ok` when absent or
    /// `Reserved`; `Err(AlreadySealed)` / `Err(AlreadyDeleted)`
    /// otherwise. Expired `Deleted` entries count as absent.
    pub(crate) fn validate_reserve(&self, id: SegmentId) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let guard = shard.read();
        match guard.get(&id) {
            None => Ok(()),
            Some(entry) if entry.state == SegmentState::Reserved => Ok(()),
            Some(entry) if entry.state == SegmentState::Sealed => {
                Err(TransitionError::AlreadySealed)
            }
            Some(entry) if Self::deleted_expired(entry, Instant::now()) => Ok(()),
            Some(_) => Err(TransitionError::AlreadyDeleted),
        }
    }

    /// Folds a validated `reserve` into the registry. The durable
    /// side-effect must have succeeded before this is called. Absent →
    /// insert; already `Reserved` → idempotent no-op (the existing
    /// entry is kept); anything else → error without mutation.
    pub(crate) fn fold_reserve(
        &self,
        id: SegmentId,
        metadata: SegmentMetadata,
    ) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        let now = Instant::now();
        Self::evict_expired_locked(&mut guard, now);
        match guard.get(&id) {
            None => {
                guard.insert(id, LifecycleEntry::new(SegmentState::Reserved, metadata));
                Ok(())
            }
            Some(entry) if entry.state == SegmentState::Reserved => Ok(()),
            Some(entry) if entry.state == SegmentState::Sealed => {
                Err(TransitionError::AlreadySealed)
            }
            Some(_) => Err(TransitionError::AlreadyDeleted),
        }
    }

    /// Validates a `seal` without mutating: `Ok` only when the entry
    /// is `Reserved`.
    pub(crate) fn validate_seal(&self, id: SegmentId) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let guard = shard.read();
        match guard.get(&id) {
            Some(entry) if entry.state == SegmentState::Reserved => Ok(()),
            Some(entry) if entry.state == SegmentState::Sealed => {
                Err(TransitionError::AlreadySealed)
            }
            Some(entry) if Self::deleted_expired(entry, Instant::now()) => {
                Err(TransitionError::Missing)
            }
            Some(_) => Err(TransitionError::NotReserved),
            None => Err(TransitionError::Missing),
        }
    }

    /// Folds a validated `seal` into the registry: `Reserved` → `Sealed`
    /// with the full sealed metadata.
    pub(crate) fn fold_seal(
        &self,
        id: SegmentId,
        metadata: SegmentMetadata,
    ) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        let now = Instant::now();
        Self::evict_expired_locked(&mut guard, now);
        match guard.get_mut(&id) {
            Some(entry) if entry.state == SegmentState::Reserved => {
                entry.state = SegmentState::Sealed;
                entry.metadata = metadata;
                // The seal transition closes the read window: once the
                // `.dat` is durable, the in-flight buffer is released —
                // the memory-bound test pins this, and failing to clear
                // it would leak the frozen buffer forever and keep the
                // in-flight cap permanently engaged (lifecycle-read-path).
                entry.in_flight = None;
                entry.seal_queued = false;
                Ok(())
            }
            Some(entry) if entry.state == SegmentState::Sealed => {
                Err(TransitionError::AlreadySealed)
            }
            Some(_) => Err(TransitionError::NotReserved),
            None => Err(TransitionError::Missing),
        }
    }

    /// Validates a `delete` without mutating: `Ok` when the entry is
    /// `Reserved` or `Sealed`.
    pub(crate) fn validate_delete(&self, id: SegmentId) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let guard = shard.read();
        match guard.get(&id) {
            Some(entry)
                if entry.state == SegmentState::Deleted
                    && !Self::deleted_expired(entry, Instant::now()) =>
            {
                Err(TransitionError::AlreadyDeleted)
            }
            Some(_) => Ok(()),
            None => Err(TransitionError::Missing),
        }
    }

    /// Folds a validated `delete` into the registry: `Reserved` |
    /// `Sealed` → `Deleted`, then evicts after the delete grace
    /// (immediately when the grace is zero).
    pub(crate) fn fold_delete(&self, id: SegmentId) -> Result<(), TransitionError> {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        let now = Instant::now();
        Self::evict_expired_locked(&mut guard, now);
        match guard.get_mut(&id) {
            Some(entry) if entry.state == SegmentState::Deleted => {
                Err(TransitionError::AlreadyDeleted)
            }
            Some(entry) => {
                entry.state = SegmentState::Deleted;
                entry.evict_at = now + self.delete_grace();
                if self.delete_grace().is_zero() {
                    guard.remove(&id);
                }
                Ok(())
            }
            None => Err(TransitionError::Missing),
        }
    }
}

/// The single writer of segment lifecycle state.
///
/// The **only** writer of the registry and the **only** writer of the
/// durable segment lifecycle state (phase 1: the `segments` CF via
/// `MetadataStore`). The pool, the seal worker's persistence path, the
/// orphan reaper, and WAL replay do not touch state directly — they
/// *request* transitions. Each `request_*` method is strictly ordered:
/// **validate (registry) → durable side-effect (CF write) → fold into
/// the registry** — the fold happens only after the durable write
/// returns, and no I/O runs under a shard lock (performance §7.1).
///
/// The coordinator also owns the **idle-seal timer**: every `Reserved`
/// segment that stops receiving writes for `seal_timeout_ms` is sealed
/// (the pool slots are the idle detectors — they track per-segment
/// last-append — and the coordinator's `seal_idle_segments` tick drives
/// the sweep; empty segments are never sealed).
pub struct SegmentLifecycleCoordinator {
    registry: Arc<SegmentLifecycleRegistry>,
    metadata: Arc<dyn MetadataStore>,
    /// Pools swept by the idle-seal driver (empty until
    /// [`with_idle_seal`](Self::with_idle_seal) is called).
    idle_pools: Vec<Arc<SegmentPool>>,
    /// Idle-seal timeout in milliseconds (the sealer's
    /// `seal_timeout_ms` config).
    idle_seal_timeout_ms: u64,
    /// Live registry entry count gauge (`oceanfs_lifecycle_registry_entries`).
    entries_gauge: Gauge,
    /// Registry memory estimate gauge
    /// (`oceanfs_lifecycle_registry_bytes_estimate`).
    bytes_gauge: Gauge,
}

impl SegmentLifecycleCoordinator {
    /// Creates a new lifecycle coordinator over the given metadata
    /// store (the phase-1 durable side-effect target) and registry
    /// configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::LifecycleConfig;
    /// use oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator;
    /// use oceanfs_storage::RocksDbMetadataStore;
    /// use oceanfs_core::MetadataConfig;
    /// use std::sync::Arc;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = tempfile::tempdir()?;
    /// let store = Arc::new(RocksDbMetadataStore::open(&MetadataConfig {
    ///     data_dir: dir.path().join("meta"),
    ///     ..Default::default()
    /// })?);
    /// let coordinator = SegmentLifecycleCoordinator::new(store, &LifecycleConfig::default());
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(metadata: Arc<dyn MetadataStore>, config: &LifecycleConfig) -> Self {
        Self::with_registry(metadata, Arc::new(SegmentLifecycleRegistry::new(config)))
    }

    /// Creates a new lifecycle coordinator over the given metadata
    /// store (the phase-1 durable side-effect target), sharing a
    /// caller-provided registry.
    ///
    /// The composition root uses this when the registry must exist
    /// BEFORE the coordinator — the segment pools hold
    /// `Arc<SegmentLifecycleRegistry>` for the read path and the
    /// in-flight attach, and the node constructs the registry first
    /// and hands it to both (construction order: registry → pools →
    /// coordinator).
    pub fn with_registry(
        metadata: Arc<dyn MetadataStore>,
        registry: Arc<SegmentLifecycleRegistry>,
    ) -> Self {
        let entries_gauge = Gauge::new(
            "oceanfs_lifecycle_registry_entries".into(),
            "Live segment lifecycle registry entries (Reserved + Sealed, not Deleted)".into(),
            LabelSet::empty(),
        );
        let bytes_gauge = Gauge::new(
            "oceanfs_lifecycle_registry_bytes_estimate".into(),
            "Estimated lifecycle registry memory footprint (entries × ~300 B)".into(),
            LabelSet::empty(),
        );
        let coordinator = Self {
            registry,
            metadata,
            idle_pools: Vec::new(),
            idle_seal_timeout_ms: 0,
            entries_gauge,
            bytes_gauge,
        };
        coordinator.update_gauges();
        coordinator
    }

    /// Wires the idle-seal driver: the pools whose slots
    /// [`seal_idle_segments`](Self::seal_idle_segments) sweeps, and the
    /// seal timeout they honor (the sealer's `seal_timeout_ms`).
    #[must_use]
    pub fn with_idle_seal(mut self, pools: Vec<Arc<SegmentPool>>, seal_timeout_ms: u64) -> Self {
        self.idle_pools = pools;
        self.idle_seal_timeout_ms = seal_timeout_ms;
        self
    }

    /// Returns the underlying registry (read-only access for
    /// consumers; all writes go through this coordinator).
    pub fn registry(&self) -> &SegmentLifecycleRegistry {
        &self.registry
    }

    /// Seeds the registry from the durable store (phase 1: the
    /// `segments` CF).
    ///
    /// Called once at node startup so the registry mirrors every
    /// pre-existing segment — the coordinator must be the complete
    /// single writer over existing data too (the reaper's
    /// `request_delete` validates against the registry, and the
    /// ADR-0025 Decision 5 memory bound presupposes one entry per live
    /// segment). Pure registry folds: no CF writes happen here (the
    /// entries already are durable), so the only-writer invariant is
    /// preserved. Entries inserted later by the write path or WAL
    /// replay are never overwritten.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError::DurableWriteFailed`] when the store
    /// cannot be enumerated.
    pub fn seed_from_metadata_store(&self) -> Result<(), TransitionError> {
        for meta in self.metadata.list_segments() {
            let meta = meta.map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
            let state = if meta.sealed_at.is_some() {
                SegmentState::Sealed
            } else {
                SegmentState::Reserved
            };
            self.registry.seed_entry(meta.segment_id, state, meta);
        }
        self.update_gauges();
        Ok(())
    }

    /// Reserves a segment durably: validate (absent | `Reserved`) →
    /// CF `put_segment` (`sealed_at: None`) → fold into the registry.
    ///
    /// Returns `Ok` **only after** the durable CF write — the write
    /// path calls this before the first `DataEntry` (WAL entry) of its
    /// segment, so the WAL cleanup can never mistake an in-flight
    /// segment for garbage.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError::AlreadySealed`] /
    /// [`TransitionError::AlreadyDeleted`] when the segment already
    /// holds a higher state (the phantom-downgrade write is rejected —
    /// no CF write and no fold happen), or
    /// [`TransitionError::DurableWriteFailed`] when the CF write fails.
    pub async fn request_reserve(
        &self,
        id: SegmentId,
        tier: oceanfs_core::SizeTier,
        ec_k: u8,
        ec_m: u8,
    ) -> Result<(), TransitionError> {
        self.registry.validate_reserve(id)?;
        let meta = SegmentMetadata {
            segment_id: id,
            ec_k,
            ec_m,
            size_tier: tier,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: None,
        };
        self.metadata
            .put_segment(meta.clone())
            .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
        self.registry.fold_reserve(id, meta)?;
        self.update_gauges();
        Ok(())
    }

    /// Seals a segment durably: validate (`Reserved` only) → CF
    /// `put_segment` with the full sealed metadata (incl. the seal-time
    /// `merkle_root`) → fold into the registry.
    ///
    /// Callers invoke this only after the `.dat` fsync returns (the
    /// seal worker's operation sequence); the durable write and the
    /// fold are strictly ordered after validation.
    ///
    /// # Errors
    ///
    /// Returns a [`TransitionError`] when the entry is not `Reserved`
    /// (no CF write, no fold), or
    /// [`TransitionError::DurableWriteFailed`] when the CF write fails.
    pub async fn request_seal(
        &self,
        id: SegmentId,
        metadata: SegmentMetadata,
    ) -> Result<(), TransitionError> {
        self.registry.validate_seal(id)?;
        self.metadata
            .put_segment(metadata.clone())
            .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
        self.registry.fold_seal(id, metadata)?;
        self.update_gauges();
        Ok(())
    }

    /// Deletes a segment durably: validate (`Reserved` | `Sealed`) →
    /// CF deleted-marker write → fold into the registry (the entry is
    /// evicted after the delete grace).
    ///
    /// The caller (the orphan reaper) invokes this **before** the
    /// `.dat` unlink — the durable deletion precedes the data removal
    /// (ADR-0024 invariant 3: "Delete before unlink").
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError::AlreadyDeleted`] /
    /// [`TransitionError::Missing`] when no live entry exists (no CF
    /// write, no fold), or
    /// [`TransitionError::DurableWriteFailed`] when the CF write fails.
    pub async fn request_delete(&self, id: SegmentId) -> Result<(), TransitionError> {
        self.registry.validate_delete(id)?;
        self.metadata
            .delete_segment(id)
            .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
        self.registry.fold_delete(id)?;
        self.update_gauges();
        Ok(())
    }

    /// Seals a batch of segments whose `.dat` files are already
    /// durable (fsynced + finalized by the flush coordinator).
    ///
    /// Preserves the flush coordinator's one-RocksDB-batch-per-cycle
    /// property: every accepted id is validated first (read-only, no
    /// locks held across I/O — performance §7.1), the accepted
    /// metadata is written in **one** `batch_write`, then each accepted
    /// entry is folded. Returns one result per input, aligned by
    /// index: a validation failure for one segment does not fail the
    /// others.
    ///
    /// # Errors
    ///
    /// Each element is `Ok` on success, or a [`TransitionError`] for
    /// that segment.
    pub(crate) fn seal_finalized_batch(
        &self,
        metas: Vec<SegmentMetadata>,
    ) -> Vec<std::result::Result<(), TransitionError>> {
        // Phase 1 — validate every id (read-only shard visits; the
        // shard locks are released before any durable I/O).
        let mut out: Vec<std::result::Result<(), TransitionError>> =
            std::iter::repeat_with(|| Ok(())).take(metas.len()).collect();
        let mut accepted: Vec<SegmentMetadata> = Vec::with_capacity(metas.len());
        for (i, meta) in metas.into_iter().enumerate() {
            match self.registry.validate_seal(meta.segment_id) {
                Ok(()) => accepted.push(meta),
                Err(e) => out[i] = Err(e),
            }
        }
        if accepted.is_empty() {
            return out;
        }
        // Phase 2 — one durable batch write for the accepted entries.
        let ops: Vec<oceanfs_storage_api::BatchOp> =
            accepted.iter().cloned().map(oceanfs_storage_api::BatchOp::PutSegment).collect();
        if let Err(e) = self.metadata.batch_write(ops) {
            for slot in &mut out {
                if slot.is_ok() {
                    *slot = Err(TransitionError::DurableWriteFailed(e.to_string()));
                }
            }
            return out;
        }
        // Phase 3 — fold each accepted entry (write locks, once per
        // segment, strictly after the durable write returned).
        for meta in accepted {
            let id = meta.segment_id;
            if let Err(e) = self.registry.fold_seal(id, meta) {
                // A fold can lose a race only to a concurrent delete of
                // the same segment (unreachable in phase 1: the reaper
                // deletes only unreferenced segments, and a segment
                // being sealed is referenced). The durable write already
                // happened; the registry converges on the next
                // transition.
                tracing::warn!(
                    segment_id = %id,
                    error = ?e,
                    "seal fold lost a transition race; registry entry unchanged"
                );
            }
        }
        self.update_gauges();
        out
    }

    /// The idle-seal driver tick: sweeps every wired pool for
    /// partially-filled segments that stopped receiving writes for
    /// `seal_timeout_ms` and seals them (the fill path sealed only on
    /// `is_full()`, leaving such segments `Reserved` forever and
    /// pinning their WAL files — the `wal_not_unbounded` leak).
    ///
    /// Empty segments are never sealed (recovery drops empty
    /// reserves). The tick also **retries deferred seals**: frozen
    /// segments whose enqueue failed while the seal queue was at
    /// capacity stay readable through the registry's in-flight window
    /// and are re-enqueued here — a full seal queue delays the seal
    /// but never removes the read window (lifecycle-read-path). Once
    /// the in-flight count drops below the cap, slots re-arm.
    ///
    /// A timeout of zero means "idle immediately" (used by tests;
    /// production wires the sealer's `seal_timeout_ms`).
    pub async fn seal_idle_segments(&self) {
        let timeout = Duration::from_millis(self.idle_seal_timeout_ms);
        for pool in &self.idle_pools {
            pool.sweep_idle_segments(timeout).await;
        }
        self.retry_deferred_seals().await;
        // Re-arm slots whose in-flight count dropped below the cap
        // (seals completed during the sweep/retry).
        for pool in &self.idle_pools {
            pool.try_activate_slot();
        }
    }

    /// Re-enqueues seal work for frozen segments whose enqueue failed
    /// while the seal queue was at capacity (their `seal_queued` flag
    /// was reset). The in-flight window stays readable throughout.
    async fn retry_deferred_seals(&self) {
        // Collect under the registry's read locks (the closure must not
        // call back into the registry).
        let mut pending: Vec<(SegmentId, SizeTier, Bytes)> = Vec::new();
        self.registry.for_each(|id, entry| {
            if entry.state == SegmentState::Reserved
                && entry.in_flight.is_some()
                && !entry.seal_queued
            {
                if let Some(data) = &entry.in_flight {
                    pending.push((id, entry.metadata.size_tier, data.clone()));
                }
            }
        });
        for (id, tier, data) in pending {
            let Some(pool) = self.idle_pools.iter().find(|p| p.storage_tier() == tier) else {
                continue;
            };
            if pool.enqueue_inflight_work(id, tier, data).await {
                self.registry.mark_seal_queued(id);
            }
        }
    }

    /// Registers the registry-size gauges with a metrics registrar.
    ///
    /// The gauges are updated by the coordinator after every fold, so
    /// they reflect the live registry continuously
    /// (performance §11.1 — atomic gauge stores, no lock on the
    /// transition path beyond the registry shards themselves).
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_gauge(self.entries_gauge.clone());
        registrar.register_gauge(self.bytes_gauge.clone());
    }

    /// Refreshes the registry-size gauges from the live registry.
    fn update_gauges(&self) {
        self.entries_gauge.set(self.registry.len() as u64);
        self.bytes_gauge.set(self.registry.mem_estimate_bytes());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use oceanfs_core::{MetadataConfig, PoolConfig, SegmentSizeConfig, SizeTier};
    use oceanfs_storage_api::MetadataStore;
    use tempfile::TempDir;

    use super::*;
    use crate::{buffer_pool::BufferPool, metadata::RocksDbMetadataStore};

    /// A registry config with a non-zero delete grace, so the
    /// `Deleted` state is observable (with the default immediate
    /// eviction it never survives a transition).
    fn grace_config(grace_ms: u64) -> LifecycleConfig {
        LifecycleConfig { lifecycle_registry_shards: 8, delete_grace_ms: grace_ms }
    }

    fn test_metadata(segment_id: SegmentId, sealed: bool) -> SegmentMetadata {
        SegmentMetadata {
            segment_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: sealed.then_some(1_700_000_000_000),
        }
    }

    // ------------------------------------------------------------------
    // Registry — pure transitions
    // ------------------------------------------------------------------

    #[test]
    fn reserve_absent_creates_reserved_entry() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        let entry = registry.get(id).expect("entry present");
        assert_eq!(entry.state, SegmentState::Reserved);
        assert_eq!(entry.metadata.segment_id, id);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn reserve_idempotent_on_reserved_keeps_existing_entry() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        let first = test_metadata(id, false);
        let mut second = test_metadata(id, false);
        second.ec_k = 8; // a re-reserve must NOT clobber the entry
        registry.reserve(id, first).unwrap();
        registry.reserve(id, second).unwrap();
        let entry = registry.get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Reserved);
        assert_eq!(entry.metadata.ec_k, 4, "existing entry kept on idempotent re-reserve");
    }

    #[test]
    fn reserve_on_sealed_returns_already_sealed_and_does_not_mutate() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.seal(id, test_metadata(id, true)).unwrap();
        // The phantom-downgrade write: reserve over a Sealed entry.
        let err = registry.reserve(id, test_metadata(id, false)).unwrap_err();
        assert_eq!(err, TransitionError::AlreadySealed);
        let entry = registry.get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Sealed, "downgrade must not mutate");
        assert!(entry.metadata.sealed_at.is_some());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn reserve_on_deleted_returns_already_deleted() {
        let registry = SegmentLifecycleRegistry::new(&grace_config(10_000));
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.delete(id).unwrap();
        let err = registry.reserve(id, test_metadata(id, false)).unwrap_err();
        assert_eq!(err, TransitionError::AlreadyDeleted);
        let entry = registry.get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Deleted, "reserve over Deleted must not mutate");
    }

    #[test]
    fn seal_on_reserved_marks_sealed_with_full_metadata() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.seal(id, test_metadata(id, true)).unwrap();
        let entry = registry.get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Sealed);
        assert!(entry.metadata.sealed_at.is_some());
    }

    #[test]
    fn seal_on_sealed_returns_already_sealed_and_does_not_mutate() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.seal(id, test_metadata(id, true)).unwrap();
        let mut re_seal = test_metadata(id, true);
        re_seal.ec_k = 9; // must not land
        let err = registry.seal(id, re_seal).unwrap_err();
        assert_eq!(err, TransitionError::AlreadySealed);
        assert_eq!(registry.get(id).unwrap().metadata.ec_k, 4, "re-seal must not mutate");
    }

    #[test]
    fn seal_on_deleted_returns_not_reserved() {
        let registry = SegmentLifecycleRegistry::new(&grace_config(10_000));
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.delete(id).unwrap();
        let err = registry.seal(id, test_metadata(id, true)).unwrap_err();
        assert_eq!(err, TransitionError::NotReserved);
        assert_eq!(registry.get(id).unwrap().state, SegmentState::Deleted, "no mutation");
    }

    #[test]
    fn seal_on_missing_returns_missing() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        let err = registry.seal(id, test_metadata(id, true)).unwrap_err();
        assert_eq!(err, TransitionError::Missing);
        assert!(registry.is_empty());
    }

    #[test]
    fn delete_on_reserved_marks_deleted() {
        let registry = SegmentLifecycleRegistry::new(&grace_config(10_000));
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.delete(id).unwrap();
        let entry = registry.get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Deleted);
    }

    #[test]
    fn delete_on_sealed_marks_deleted() {
        let registry = SegmentLifecycleRegistry::new(&grace_config(10_000));
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.seal(id, test_metadata(id, true)).unwrap();
        registry.delete(id).unwrap();
        assert_eq!(registry.get(id).unwrap().state, SegmentState::Deleted);
    }

    #[test]
    fn delete_on_deleted_returns_already_deleted() {
        let registry = SegmentLifecycleRegistry::new(&grace_config(10_000));
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.delete(id).unwrap();
        let err = registry.delete(id).unwrap_err();
        assert_eq!(err, TransitionError::AlreadyDeleted);
    }

    #[test]
    fn delete_on_missing_returns_missing() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let err = registry.delete(SegmentId::new()).unwrap_err();
        assert_eq!(err, TransitionError::Missing);
    }

    #[test]
    fn delete_with_default_grace_evicts_immediately() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.delete(id).unwrap();
        assert!(registry.get(id).is_none(), "immediate eviction");
        assert!(registry.is_empty(), "registry stays O(live segments)");
    }

    #[test]
    fn expired_deleted_entries_are_treated_as_absent() {
        // After the grace lapses, get/len/for_each/validate_reserve
        // must all treat the entry as gone (lazy eviction on read).
        let registry = SegmentLifecycleRegistry::new(&grace_config(1));
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.delete(id).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(registry.get(id).is_none(), "expired deleted entry is absent");
        assert_eq!(registry.len(), 0, "expired deleted entry not counted");
        assert!(registry.validate_reserve(id).is_ok(), "expired deleted entry is reservable");
    }

    #[test]
    fn get_returns_none_for_unknown_segment() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        assert!(registry.get(SegmentId::new()).is_none());
    }

    #[test]
    fn for_each_enumerates_all_live_entries() {
        let registry = SegmentLifecycleRegistry::new(&grace_config(10_000));
        let ids: Vec<SegmentId> = (0..10).map(|_| SegmentId::new()).collect();
        for id in &ids {
            registry.reserve(*id, test_metadata(*id, false)).unwrap();
        }
        let mut seen: Vec<SegmentId> = Vec::new();
        registry.for_each(|id, entry| {
            assert_eq!(entry.state, SegmentState::Reserved);
            seen.push(id);
        });
        seen.sort();
        let mut expected = ids.clone();
        expected.sort();
        assert_eq!(seen, expected);
    }

    #[test]
    fn len_counts_live_entries_across_shards() {
        let registry = SegmentLifecycleRegistry::new(&grace_config(10_000));
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
        let ids: Vec<SegmentId> = (0..100).map(|_| SegmentId::new()).collect();
        for id in &ids {
            registry.reserve(*id, test_metadata(*id, false)).unwrap();
        }
        assert_eq!(registry.len(), 100);
        registry.delete(ids[0]).unwrap();
        assert_eq!(registry.len(), 99, "deleted entries are not live");
    }

    #[test]
    fn mem_estimate_is_bounded_by_350_bytes_per_entry() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let ids: Vec<SegmentId> = (0..50).map(|_| SegmentId::new()).collect();
        for id in &ids {
            registry.reserve(*id, test_metadata(*id, false)).unwrap();
        }
        let len = registry.len() as u64;
        assert!(registry.mem_estimate_bytes() <= 350 * len, "per-entry bound (ADR-0025 D5)");
        assert!(registry.mem_estimate_bytes() > 0);
    }

    #[test]
    fn shard_count_returns_configured_count_clamped_to_one() {
        let config = LifecycleConfig { lifecycle_registry_shards: 128, ..Default::default() };
        assert_eq!(shard_count(&config), 128);
        let zero = LifecycleConfig { lifecycle_registry_shards: 0, ..Default::default() };
        assert_eq!(shard_count(&zero), 1, "zero shards must not produce an unusable registry");
    }

    #[test]
    fn churn_10k_reserve_seal_delete_ends_empty() {
        // O(live), not O(lifetime): after 10K full lifecycles the
        // registry must be back to zero entries.
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        for _ in 0..10_000 {
            let id = SegmentId::new();
            registry.reserve(id, test_metadata(id, false)).unwrap();
            registry.seal(id, test_metadata(id, true)).unwrap();
            registry.delete(id).unwrap();
        }
        assert_eq!(registry.len(), 0, "churn must not accumulate entries");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_transitions_never_downgrade_and_never_panic() {
        // Stress the sharded registry: concurrent reserve/seal/delete
        // on the same ids. The invariant under test: a Sealed entry is
        // never observed as Reserved afterwards (no downgrade).
        let registry = Arc::new(SegmentLifecycleRegistry::new(&grace_config(10_000)));
        let ids: Vec<SegmentId> = (0..32).map(|_| SegmentId::new()).collect();
        let mut handles = Vec::new();
        for worker in 0..8 {
            let registry = Arc::clone(&registry);
            let ids = ids.clone();
            handles.push(tokio::spawn(async move {
                for round in 0..200usize {
                    let id = ids[(worker * 40 + round) % ids.len()];
                    match round % 3 {
                        0 => {
                            let _ = registry.reserve(id, test_metadata(id, false));
                        }
                        1 => {
                            let _ = registry.seal(id, test_metadata(id, true));
                        }
                        _ => {
                            let _ = registry.delete(id);
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Poison probe: after the churn, any entry that is Sealed must
        // stay Sealed when a downgrade is attempted. The sealed ids are
        // collected first — `for_each` holds the shard's read lock, and
        // the probe writes, so the probe must run after enumeration.
        let mut sealed_ids: Vec<SegmentId> = Vec::new();
        registry.for_each(|id, entry| {
            if entry.state == SegmentState::Sealed {
                sealed_ids.push(id);
            }
        });
        for id in sealed_ids {
            let err = registry.reserve(id, test_metadata(id, false)).unwrap_err();
            assert!(matches!(err, TransitionError::AlreadySealed), "no downgrade allowed");
            let after = registry.get(id).unwrap();
            assert_eq!(after.state, SegmentState::Sealed, "poison probe must not mutate");
        }
    }

    // ------------------------------------------------------------------
    // Coordinator — validate → durable → fold
    // ------------------------------------------------------------------

    async fn test_store() -> (Arc<RocksDbMetadataStore>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        (store, dir)
    }

    #[tokio::test]
    async fn seed_from_metadata_store_populates_registry_without_cf_writes() {
        let (store, _dir) = test_store().await;
        // Two durable entries written OUTSIDE the coordinator (as a
        // pre-upgrade deployment would have them): one sealed, one
        // phantom.
        let sealed_id = SegmentId::new();
        let phantom_id = SegmentId::new();
        store.put_segment(test_metadata(sealed_id, true)).unwrap();
        store.put_segment(test_metadata(phantom_id, false)).unwrap();

        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        assert!(coordinator.registry().is_empty(), "registry starts empty");
        coordinator.seed_from_metadata_store().unwrap();

        assert_eq!(coordinator.registry().len(), 2);
        assert_eq!(
            coordinator.registry().get(sealed_id).unwrap().state,
            SegmentState::Sealed,
            "sealed CF entry seeds as Sealed"
        );
        assert_eq!(
            coordinator.registry().get(phantom_id).unwrap().state,
            SegmentState::Reserved,
            "phantom CF entry seeds as Reserved"
        );
        // The seeded entries participate in transitions: a delete of
        // the seeded sealed segment writes the marker through the
        // coordinator (the reaper path).
        coordinator.request_delete(sealed_id).await.unwrap();
        assert!(
            store.get_segment(sealed_id).unwrap().is_none(),
            "coordinator deletes seeded entry"
        );
        let deleted: Vec<SegmentId> = store
            .list_deleted_segments()
            .into_iter()
            .filter_map(|r| r.ok().map(|(sid, _)| sid))
            .collect();
        assert!(deleted.contains(&sealed_id), "deleted-marker written for seeded sealed segment");
    }

    // ------------------------------------------------------------------
    // Read source resolution + in-flight window (lifecycle-read-path)
    // ------------------------------------------------------------------

    #[test]
    fn read_source_missing_for_absent_and_deleted_entries() {
        let registry = SegmentLifecycleRegistry::new(&grace_config(10_000));
        let id = SegmentId::new();
        assert!(matches!(registry.read_source(id), SegmentReadSource::Missing));
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.delete(id).unwrap();
        assert!(matches!(registry.read_source(id), SegmentReadSource::Missing));
    }

    #[test]
    fn read_source_active_slot_for_reserved_without_in_flight() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        assert!(matches!(registry.read_source(id), SegmentReadSource::ActiveSlot));
    }

    #[test]
    fn read_source_in_flight_after_attach_and_sealed_after_seal() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        // Attach the frozen buffer (the pool fill's job).
        assert!(registry.attach_in_flight(
            id,
            SizeTier::Standard,
            4,
            2,
            Bytes::from_static(b"frozen")
        ));
        match registry.read_source(id) {
            SegmentReadSource::InFlight(data) => assert_eq!(&data[..], b"frozen"),
            other => panic!("expected InFlight, got {other:?}"),
        }
        assert_eq!(registry.in_flight_count(), 1);
        // The seal transition closes the window: after request_seal the
        // entry resolves as Sealed and the in-flight count drops.
        registry.seal(id, test_metadata(id, true)).unwrap();
        assert!(matches!(registry.read_source(id), SegmentReadSource::Sealed));
        assert_eq!(registry.in_flight_count(), 0, "seal must clear the in-flight window");
    }

    #[test]
    fn attach_on_missing_entry_inserts_registry_only_reserve() {
        // The fill-before-reserve window: the write path's durable
        // reserve lands AFTER the append that filled the segment, so
        // the freeze's attach may find no entry. It inserts a
        // registry-only Reserved entry (pure in-memory fold — the
        // coordinator's later request_reserve is an idempotent no-op).
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        assert!(
            registry.attach_in_flight(id, SizeTier::Small, 2, 1, Bytes::from_static(b"data")),
            "attach must self-heal the missing entry"
        );
        let entry = registry.get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Reserved);
        assert!(matches!(registry.read_source(id), SegmentReadSource::InFlight(_)));
        // The coordinator's durable reserve afterwards: idempotent.
        assert!(registry.reserve(id, test_metadata(id, false)).is_ok());
        assert!(registry.get(id).unwrap().metadata.sealed_at.is_none());
    }

    #[test]
    fn attach_on_sealed_entry_is_rejected() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        registry.seal(id, test_metadata(id, true)).unwrap();
        assert!(
            !registry.attach_in_flight(id, SizeTier::Standard, 4, 2, Bytes::from_static(b"x")),
            "a sealed entry must never accept an in-flight attach"
        );
    }

    #[test]
    fn seal_queued_flag_roundtrip() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        registry.reserve(id, test_metadata(id, false)).unwrap();
        assert!(registry.attach_in_flight(id, SizeTier::Standard, 4, 2, Bytes::from_static(b"d")));
        assert_eq!(registry.in_flight_unqueued_count(), 0, "attach marks the seal queued");
        registry.mark_seal_unqueued(id);
        assert_eq!(registry.in_flight_unqueued_count(), 1, "failed enqueue → retry set");
        registry.mark_seal_queued(id);
        assert_eq!(registry.in_flight_unqueued_count(), 0);
        // The seal clears everything.
        registry.seal(id, test_metadata(id, true)).unwrap();
        assert_eq!(registry.in_flight_count(), 0);
        assert_eq!(registry.in_flight_unqueued_count(), 0);
    }

    #[tokio::test]
    async fn request_reserve_writes_cf_then_folds() {
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let id = SegmentId::new();
        coordinator.request_reserve(id, SizeTier::Small, 2, 1).await.unwrap();
        // Durable side-effect first: the CF entry exists, unsealed.
        let cf = store.get_segment(id).unwrap().expect("CF phantom written");
        assert!(cf.sealed_at.is_none());
        assert_eq!(cf.size_tier, SizeTier::Small);
        // Registry folded.
        let entry = coordinator.registry().get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Reserved);
        assert_eq!(coordinator.registry().len(), 1);
    }

    #[tokio::test]
    async fn request_reserve_is_idempotent_at_coordinator_level() {
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let id = SegmentId::new();
        coordinator.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        coordinator.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        let entry = coordinator.registry().get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Reserved);
    }

    #[tokio::test]
    async fn request_reserve_on_sealed_is_rejected_without_downgrade() {
        // The phantom-downgrade race, at the coordinator level: a
        // reserve attempt landing on an already-sealed segment must be
        // rejected, and neither the registry nor the CF may regress.
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let id = SegmentId::new();
        coordinator.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        coordinator.request_seal(id, test_metadata(id, true)).await.unwrap();
        // Poison probe: the old register_phantom_before_wal would have
        // re-written sealed_at: None here.
        let err = coordinator.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap_err();
        assert_eq!(err, TransitionError::AlreadySealed);
        let cf = store.get_segment(id).unwrap().expect("CF entry still present");
        assert!(cf.sealed_at.is_some(), "CF must not be downgraded to unsealed");
        let entry = coordinator.registry().get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Sealed, "registry must not be downgraded");
    }

    #[tokio::test]
    async fn coordinator_seal_clears_in_flight_via_the_fold() {
        // The production seal-complete paths (request_seal and
        // seal_finalized_batch) fold through `fold_seal` — the seal
        // transition must clear the in-flight window there, not only in
        // the direct registry `seal()` used by tests. A mutation that
        // drops the clear leaks the frozen buffer and keeps the
        // in-flight cap permanently engaged.
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let id = SegmentId::new();
        coordinator.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        // Attach a frozen buffer (the pool fill's job).
        assert!(coordinator.registry().attach_in_flight(
            id,
            SizeTier::Standard,
            4,
            2,
            Bytes::from_static(b"frozen-payload"),
        ));
        assert_eq!(coordinator.registry().in_flight_count(), 1);

        // request_seal → fold_seal: the window must close.
        coordinator.request_seal(id, test_metadata(id, true)).await.unwrap();
        assert_eq!(
            coordinator.registry().in_flight_count(),
            0,
            "request_seal must clear the in-flight window"
        );
        assert!(matches!(coordinator.registry().read_source(id), SegmentReadSource::Sealed));

        // seal_finalized_batch → fold_seal: the same clearing.
        let id2 = SegmentId::new();
        coordinator.request_reserve(id2, SizeTier::Standard, 4, 2).await.unwrap();
        assert!(coordinator.registry().attach_in_flight(
            id2,
            SizeTier::Standard,
            4,
            2,
            Bytes::from_static(b"frozen-payload-2"),
        ));
        let results = coordinator.seal_finalized_batch(vec![test_metadata(id2, true)]);
        assert!(results[0].is_ok());
        assert_eq!(coordinator.registry().in_flight_count(), 0, "batch seal must clear the window");
        assert!(matches!(coordinator.registry().read_source(id2), SegmentReadSource::Sealed));
    }

    #[tokio::test]
    async fn request_seal_writes_cf_then_folds() {
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let id = SegmentId::new();
        coordinator.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        coordinator.request_seal(id, test_metadata(id, true)).await.unwrap();
        let cf = store.get_segment(id).unwrap().expect("CF sealed entry");
        assert!(cf.sealed_at.is_some());
        let entry = coordinator.registry().get(id).unwrap();
        assert_eq!(entry.state, SegmentState::Sealed);
    }

    #[tokio::test]
    async fn request_seal_on_missing_returns_missing_without_cf_write() {
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let id = SegmentId::new();
        let err = coordinator.request_seal(id, test_metadata(id, true)).await.unwrap_err();
        assert_eq!(err, TransitionError::Missing);
        assert!(store.get_segment(id).unwrap().is_none(), "no CF write for an illegal seal");
        assert!(coordinator.registry().is_empty());
    }

    #[tokio::test]
    async fn request_delete_writes_marker_then_folds() {
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let id = SegmentId::new();
        coordinator.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        coordinator.request_seal(id, test_metadata(id, true)).await.unwrap();
        coordinator.request_delete(id).await.unwrap();
        // Durable: CF entry gone, deleted-marker present (the segment
        // was sealed, so the marker must exist for WAL retention).
        assert!(store.get_segment(id).unwrap().is_none());
        let deleted: Vec<SegmentId> = store
            .list_deleted_segments()
            .into_iter()
            .filter_map(|r| r.ok().map(|(sid, _)| sid))
            .collect();
        assert!(deleted.contains(&id), "deleted-marker written for a sealed segment");
        // Registry: evicted (default immediate grace).
        assert!(coordinator.registry().get(id).is_none());
        assert_eq!(coordinator.registry().len(), 0);
    }

    #[tokio::test]
    async fn request_delete_on_missing_returns_missing() {
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let err = coordinator.request_delete(SegmentId::new()).await.unwrap_err();
        assert_eq!(err, TransitionError::Missing);
    }

    #[tokio::test]
    async fn request_reserve_durable_failure_skips_the_fold() {
        let (store, _dir) = test_store().await;
        let failing = Arc::new(FailingPutStore::new(store.clone()));
        let coordinator = SegmentLifecycleCoordinator::new(failing, &LifecycleConfig::default());
        let id = SegmentId::new();
        let err = coordinator.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap_err();
        assert!(matches!(err, TransitionError::DurableWriteFailed(_)));
        assert!(coordinator.registry().is_empty(), "fold must be skipped on durable failure");
    }

    #[tokio::test]
    async fn seal_finalized_batch_folds_all_and_writes_cf() {
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let ids: Vec<SegmentId> = (0..16).map(|_| SegmentId::new()).collect();
        for id in &ids {
            coordinator.request_reserve(*id, SizeTier::Standard, 4, 2).await.unwrap();
        }
        let metas: Vec<SegmentMetadata> = ids.iter().map(|id| test_metadata(*id, true)).collect();
        let results = coordinator.seal_finalized_batch(metas);
        assert!(results.iter().all(|r| r.is_ok()), "all 16 must seal: {results:?}");
        assert_eq!(coordinator.registry().len(), 16);
        for id in &ids {
            let cf = store.get_segment(*id).unwrap().expect("sealed in CF");
            assert!(cf.sealed_at.is_some());
            assert_eq!(coordinator.registry().get(*id).unwrap().state, SegmentState::Sealed);
        }
    }

    #[tokio::test]
    async fn seal_finalized_batch_rejects_invalid_entries_isolated() {
        let (store, _dir) = test_store().await;
        let coordinator =
            SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default());
        let reserved_id = SegmentId::new();
        coordinator.request_reserve(reserved_id, SizeTier::Standard, 4, 2).await.unwrap();
        let missing_id = SegmentId::new();
        let sealed_id = SegmentId::new();
        coordinator.request_reserve(sealed_id, SizeTier::Standard, 4, 2).await.unwrap();
        coordinator.request_seal(sealed_id, test_metadata(sealed_id, true)).await.unwrap();

        let metas = vec![
            test_metadata(reserved_id, true),
            test_metadata(missing_id, true),
            test_metadata(sealed_id, true),
        ];
        let results = coordinator.seal_finalized_batch(metas);
        assert_eq!(results[0], Ok(()));
        assert_eq!(results[1], Err(TransitionError::Missing));
        assert_eq!(results[2], Err(TransitionError::AlreadySealed));
        // The valid entry folded; the invalid ones did not mutate.
        assert_eq!(coordinator.registry().get(reserved_id).unwrap().state, SegmentState::Sealed);
        assert_eq!(coordinator.registry().get(sealed_id).unwrap().state, SegmentState::Sealed);
        assert!(store.get_segment(missing_id).unwrap().is_none(), "no CF write for invalid entry");
    }

    #[tokio::test]
    async fn register_metrics_registers_lifecycle_gauges() {
        struct TestRegistrar {
            gauges: parking_lot::Mutex<Vec<String>>,
        }
        impl MetricRegistrar for TestRegistrar {
            fn register_counter(&self, _: oceanfs_core::Counter) {}
            fn register_gauge(&self, gauge: Gauge) {
                self.gauges.lock().push(gauge.name().to_string());
            }
            fn register_histogram(&self, _: std::sync::Arc<oceanfs_core::Histogram>) {}
        }

        let (store, _dir) = test_store().await;
        let coordinator = SegmentLifecycleCoordinator::new(store, &LifecycleConfig::default());
        let reg = TestRegistrar { gauges: parking_lot::Mutex::new(Vec::new()) };
        coordinator.register_metrics(&reg);
        let names = reg.gauges.lock().clone();
        assert!(names.contains(&"oceanfs_lifecycle_registry_entries".to_string()));
        assert!(names.contains(&"oceanfs_lifecycle_registry_bytes_estimate".to_string()));
    }

    // ------------------------------------------------------------------
    // Idle-seal driver (pool-driven detection, coordinator-owned timer)
    // ------------------------------------------------------------------

    fn test_pool_config() -> (PoolConfig, SegmentSizeConfig, Arc<BufferPool>) {
        let pool_config = PoolConfig {
            active_pool_size: 2,
            shard_count: 1,
            max_inflight_encodes: 4,
            encode_queue_capacity: 16,
            ec_streaming_encode: false,
        };
        let size_config =
            SegmentSizeConfig { default_target_size: 4 * 1024 * 1024, ..Default::default() };
        let buf_pool = Arc::new(BufferPool::new(65536, 16));
        (pool_config, size_config, buf_pool)
    }

    async fn drain_seal_queue(pool: &SegmentPool) {
        // Drains on a background thread: `recv()` can only return None
        // once every sender is dropped, and the pool keeps its sender
        // alive — awaiting inline would block forever.
        if let Some(mut rx) = pool.take_seal_rx() {
            std::thread::spawn(move || while rx.blocking_recv().is_some() {});
        }
    }

    #[tokio::test]
    async fn seal_idle_segments_seals_partially_filled_segment() {
        let (pool_config, size_config, buf_pool) = test_pool_config();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let pool = Arc::new(
            SegmentPool::new(
                pool_config,
                SizeTier::Standard,
                &size_config,
                buf_pool,
                None,
                None,
                Arc::clone(&registry),
            )
            .unwrap(),
        );
        drain_seal_queue(&pool).await;

        let (store, _dir) = test_store().await;
        let coordinator = SegmentLifecycleCoordinator::with_registry(store, registry)
            .with_idle_seal(vec![pool.clone()], 0); // 0 ms timeout = idle immediately

        let (seg_id, offset, length) = pool.append(b"partial").unwrap();
        assert_eq!((offset, length), (0, 7));

        // The driver tick seals the idle partial segment.
        coordinator.seal_idle_segments().await;
        // The old segment is no longer active: the slot was re-armed
        // with a FRESH segment, so the next append gets a new id.
        let (new_seg_id, new_offset, _) = pool.append(b"more").unwrap();
        assert_eq!(new_offset, 0);
        assert_ne!(new_seg_id, seg_id, "idle segment must have been sealed");
    }

    #[tokio::test]
    async fn idle_driver_retries_deferred_seals_under_queue_backpressure() {
        // A full seal queue delays the seal but never removes the read
        // window: when an enqueue fails, the entry stays in flight and
        // the idle driver's tick re-enqueues it once the queue drains.
        let pool_config = PoolConfig {
            active_pool_size: 2,
            shard_count: 1,
            max_inflight_encodes: 4,
            encode_queue_capacity: 1, // stalls after one item
            ec_streaming_encode: false,
        };
        let size_config = SegmentSizeConfig {
            default_target_size: 1024,
            small_target_size: 1024,
            ..Default::default()
        };
        let buf_pool = Arc::new(BufferPool::new(65536, 16));
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let pool = Arc::new(
            SegmentPool::new(
                pool_config,
                SizeTier::Standard,
                &size_config,
                buf_pool,
                None,
                None,
                Arc::clone(&registry),
            )
            .unwrap(),
        );
        let (store, _dir) = test_store().await;
        let coordinator = SegmentLifecycleCoordinator::with_registry(store, Arc::clone(&registry))
            .with_idle_seal(vec![pool.clone()], 0);

        // Stall the queue: take the receiver without draining.
        let mut rx = pool.take_seal_rx().expect("seal rx");

        // Fill the first segment — the enqueue fills the one-slot queue.
        let data = vec![0xEEu8; 2048]; // fills (target 1024)
        let (seg_id, _, _) = pool.append(&data).unwrap();

        // Fill the second segment — its enqueue stalls (queue at
        // capacity) and times out: the write is REJECTED (never acked)
        // but the frozen entry stays in flight (readable) and is marked
        // for retry.
        let data2 = vec![0xFFu8; 2048];
        let err = pool
            .append_with_hook_async(&data2, |_, _, _| {}, std::time::Duration::from_millis(20))
            .await
            .expect_err("the stalled queue must reject the write");
        assert!(matches!(err, crate::error::Error::WriteBackpressureTimeout));

        // Both frozen entries are in the registry; the second is NOT
        // queued (its enqueue failed) and both stay readable.
        let mut in_flight_ids: Vec<SegmentId> = Vec::new();
        registry.for_each(|id, entry| {
            if entry.in_flight.is_some() {
                in_flight_ids.push(id);
            }
        });
        assert_eq!(in_flight_ids.len(), 2, "both fills' entries are in flight");
        let seg2_id =
            in_flight_ids.iter().copied().find(|id| *id != seg_id).expect("second segment id");
        assert!(matches!(registry.read_source(seg2_id), SegmentReadSource::InFlight(_)));
        assert_eq!(registry.in_flight_unqueued_count(), 1, "enqueue failed → retry set");
        // ... and it stays readable under the backpressure.
        let chunk = pool.try_read(seg2_id, 0, 2048).expect("readable during the stall");
        assert_eq!(&chunk[..], &data2[..]);

        // Drain the queue (the seal worker's job), then tick the idle
        // driver: the deferred seal must be re-enqueued.
        let _first = rx.try_recv().expect("first seal queued");
        drop(rx);
        let drain_pool = Arc::clone(&pool);
        let drain = std::thread::spawn(move || {
            while let Some(_w) = drain_pool.take_seal_rx().and_then(|mut r| r.blocking_recv()) {}
        });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        coordinator.seal_idle_segments().await;
        assert!(
            registry.in_flight_unqueued_count() == 0,
            "the idle driver must retry the deferred seal"
        );
        drain.join().unwrap();
        let _ = seg_id;
    }

    #[tokio::test]
    async fn seal_idle_segments_does_not_seal_empty_segment() {
        let (pool_config, size_config, buf_pool) = test_pool_config();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let pool = Arc::new(
            SegmentPool::new(
                pool_config,
                SizeTier::Standard,
                &size_config,
                buf_pool,
                None,
                None,
                Arc::clone(&registry),
            )
            .unwrap(),
        );
        drain_seal_queue(&pool).await;

        let (store, _dir) = test_store().await;
        let coordinator = SegmentLifecycleCoordinator::with_registry(store, Arc::clone(&registry))
            .with_idle_seal(vec![pool.clone()], 0);

        // No appends: the slot's segment is empty and must NOT be
        // sealed (recovery drops empty reserves — ADR-0024 retention).
        coordinator.seal_idle_segments().await;
        let (seg_id, offset, _) = pool.append(b"first").unwrap();
        assert_eq!(offset, 0, "empty segment was not sealed; append lands in it");
        let _ = seg_id;
    }

    // ------------------------------------------------------------------
    // Test helpers
    // ------------------------------------------------------------------

    /// A `MetadataStore` wrapper that fails `put_segment` on demand
    /// (delegating everything else through the trait), for
    /// durable-failure tests.
    struct FailingPutStore {
        inner: Arc<RocksDbMetadataStore>,
        fail: AtomicBool,
    }

    impl FailingPutStore {
        fn new(inner: Arc<RocksDbMetadataStore>) -> Self {
            Self { inner, fail: AtomicBool::new(true) }
        }
    }

    impl MetadataStore for FailingPutStore {
        fn list_object_keys(
            &self,
            bucket: &oceanfs_core::BucketId,
        ) -> std::io::Result<Vec<(oceanfs_core::BucketId, oceanfs_core::ObjectKey)>> {
            MetadataStore::list_object_keys(&*self.inner, bucket)
        }
        fn get_object_metadata(
            &self,
            bucket: &oceanfs_core::BucketId,
            key: &oceanfs_core::ObjectKey,
        ) -> std::io::Result<Option<oceanfs_core::ObjectMetadata>> {
            MetadataStore::get_object_metadata(&*self.inner, bucket, key)
        }
        fn list_objects(
            &self,
            bucket: &oceanfs_core::BucketId,
            prefix: &str,
        ) -> Vec<std::io::Result<oceanfs_core::ObjectMetadata>> {
            MetadataStore::list_objects(&*self.inner, bucket, prefix)
        }
        fn list_objects_all(&self) -> Vec<std::io::Result<oceanfs_core::ObjectMetadata>> {
            MetadataStore::list_objects_all(&*self.inner)
        }
        fn get_segment(&self, id: SegmentId) -> std::io::Result<Option<SegmentMetadata>> {
            MetadataStore::get_segment(&*self.inner, id)
        }
        fn list_segments(&self) -> Vec<std::io::Result<SegmentMetadata>> {
            MetadataStore::list_segments(&*self.inner)
        }
        fn list_tombstones(
            &self,
            bucket: &oceanfs_core::BucketId,
        ) -> Vec<std::io::Result<(oceanfs_core::ObjectKey, oceanfs_core::Tombstone)>> {
            MetadataStore::list_tombstones(&*self.inner, bucket)
        }
        fn delete_tombstone(
            &self,
            bucket: &oceanfs_core::BucketId,
            key: &oceanfs_core::ObjectKey,
        ) -> std::io::Result<()> {
            MetadataStore::delete_tombstone(&*self.inner, bucket, key)
        }
        fn put_segment(&self, _meta: SegmentMetadata) -> std::io::Result<()> {
            if self.fail.load(Ordering::Relaxed) {
                Err(std::io::Error::new(std::io::ErrorKind::Other, "test seam: put_segment failed"))
            } else {
                Ok(())
            }
        }
        fn delete_segment(&self, id: SegmentId) -> std::io::Result<()> {
            MetadataStore::delete_segment(&*self.inner, id)
        }
        fn put_object(
            &self,
            bucket: &oceanfs_core::BucketId,
            meta: oceanfs_core::ObjectMetadata,
        ) -> std::io::Result<()> {
            MetadataStore::put_object(&*self.inner, bucket, meta)
        }
        fn delete_object(
            &self,
            bucket: &oceanfs_core::BucketId,
            key: &oceanfs_core::ObjectKey,
            hlc: oceanfs_core::Hlc,
        ) -> std::io::Result<()> {
            MetadataStore::delete_object(&*self.inner, bucket, key, hlc)
        }
        fn batch_write(&self, ops: Vec<oceanfs_storage_api::BatchOp>) -> std::io::Result<()> {
            MetadataStore::batch_write(&*self.inner, ops)
        }
    }
}
