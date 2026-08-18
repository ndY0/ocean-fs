//! Segment lifecycle machine — in-memory registry + single coordinator.
//!
//! ADR-0025 Decision 1. This module is the runtime half of the
//! segment-lifecycle redesign: a sharded in-memory
//! [`SegmentLifecycleRegistry`] holding exactly one entry per **live**
//! segment, and a single [`SegmentLifecycleCoordinator`] that is the
//! **only writer** of segment lifecycle state.
//!
//! In phase 1 the RocksDB `segments` CF write is the coordinator's
//! durable side-effect (no behavior change), but every CF writer is
//! routed through the coordinator — the pool, the seal worker's
//! persistence path, the orphan reaper, and WAL replay stop touching
//! state directly; they *request* transitions. In phase 2 (the event
//! WAL wired via [`SegmentLifecycleCoordinator::with_event_wal`], ADR-
//! 0024) the durable side-effect becomes the event append and the CF
//! write is demoted to a **derived mirror** performed after the event
//! (dual-read verification surface). The phantom-downgrade race and
//! the idle-seal gap die here, by construction:
//!
//! - **No downgrade.** The transition API is typed: `reserve` accepts
//!   absent/`Reserved`, `seal` accepts `Reserved` only, `delete`
//!   accepts `Reserved`/`Sealed`. There is **no method that assigns a
//!   lower state**, so a `sealed_at: None` re-write over a `Sealed`
//!   entry (the phantom-downgrade race) is not expressible.
//! - **Reserve before data.** `request_reserve` returns `Ok` only
//!   after its durable side-effect; the write path calls it before the
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
    EventWalConfig, Gauge, HashOutput, LabelSet, LifecycleConfig, MetricRegistrar, SegmentId,
    SegmentMetadata, SizeTier,
};
use oceanfs_storage_api::MetadataStore;
use parking_lot::RwLock;

use crate::{
    error::{Error, Result},
    segment::{
        event_checkpoint::EventCheckpoint,
        event_wal::{
            DataWalPos, DeleteEvent, EventWal, EventWalPos, ReserveEvent, SealEvent, SegmentEvent,
        },
        pool::SegmentPool,
    },
    wal::{WalReader, WalWriter},
    SegmentHeader, SegmentSealer,
};

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
/// `merkle_root` filled at seal).
#[derive(Debug)]
pub struct LifecycleEntry {
    /// The segment's current lifecycle state.
    pub state: SegmentState,
    /// The segment's full metadata as last committed by a transition.
    pub metadata: SegmentMetadata,
    /// The data-WAL position (file sequence + offset) of the segment's
    /// LAST data entry, recorded by the write path on every append
    /// (ADR-0024 Decision 2). Consumed at seal time to build the
    /// `SealEvent`; `None` until the first data entry is appended (and
    /// always for replayed segments whose WAL entries were truncated).
    pub data_wal_pos: Option<DataWalPos>,
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
        Self {
            state,
            metadata,
            data_wal_pos: None,
            evict_at: Instant::now(),
            in_flight: None,
            seal_queued: false,
        }
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

/// The machine as a single type — the spec's `SegmentLifecycle` name.
///
/// The lifecycle machine is the [`SegmentLifecycleCoordinator`]: the
/// only writer of segment lifecycle state (ADR-0025 Decision 1). The
/// alias keeps the spec's `SegmentLifecycle::rebuild_from_events` /
/// `rebuild_with_data_wal` API resolvable for downstream features
/// (`event-wal-checkpoint`, `startup-rebuild-from-machine`) — the
/// methods exist exactly once, on the coordinator.
pub type SegmentLifecycle = SegmentLifecycleCoordinator;

/// The observable result of a startup rebuild (ADR-0025 Decision 3 —
/// `state = fold(events)`).
///
/// Every crash-window row asserts a specific outcome vector; the fold
/// and the data-WAL pass are observable through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RebuildOutcome {
    /// Number of distinct segments whose state was established by the
    /// event fold.
    pub folded_segments: usize,
    /// Number of `Reserved` segments with zero data entries, dropped by
    /// the data-WAL pass (idle-seal never seals empty — crash-window
    /// row 1).
    pub dropped_empty_reserves: usize,
    /// Number of `Reserved`-unsealed segments rebuilt from the data WAL
    /// and re-sealed (crash-window row 2).
    pub re_sealed_segments: usize,
    /// Number of `Reserved`-unsealed segments whose durable `.dat` was
    /// adopted: root recomputed, `SealEvent` appended, no re-seal I/O
    /// (crash-window row 3).
    pub adopted_segments: usize,
    /// Number of data-WAL entries skipped during the pass: entries for
    /// sealed/deleted segments and orphan entries whose segment has no
    /// `ReserveEvent` (the reserve-before-entry invariant — they are
    /// logged and swept, never replayed).
    pub swept_entries: u64,
}

/// The machine's retention rule (ADR-0024 §Retention, phase 2): an
/// entry at position `p` of segment `S` is garbage iff `S` is `Sealed`
/// with `data_wal_pos ≥ p`, or `S` is `Deleted`.
///
/// `Reserved` entries are always live (the WAL is their only durable
/// copy); a `Sealed` entry without a recorded `data_wal_pos` is
/// conservatively live (nothing to sweep — replayed segments whose WAL
/// entries were truncated have no entries left).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{LifecycleConfig, SegmentId, SegmentMetadata, SizeTier};
/// use oceanfs_storage::segment::lifecycle::{
///     entry_is_garbage, SegmentLifecycleRegistry, SegmentState,
/// };
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
/// registry.reserve(id, meta).unwrap();
/// let entry = registry.get(id).unwrap();
/// let pos = oceanfs_storage::DataWalPos { file_seq: 0, offset: 100 };
/// assert!(!entry_is_garbage(&entry, &pos), "Reserved entries are always live");
/// ```
pub fn entry_is_garbage(entry: &LifecycleEntry, pos: &DataWalPos) -> bool {
    match entry.state {
        SegmentState::Deleted => true,
        SegmentState::Sealed => matches!(entry.data_wal_pos, Some(p) if p >= *pos),
        SegmentState::Reserved => false,
    }
}

// The recovery pass buffers only the `Reserved`-unsealed residue —
// bounded by the data WAL's retention window by construction (the WAL
// cannot grow unbounded: rotation + sweep keep ~4 × 64 MB files). No
// mid-stream drain: a group sealed before the stream ends could miss
// later entries (a segment's final `data_wal_pos` must be recorded
// before its re-seal reads it).

/// Reads a segment's data section from its durable `.dat` and computes
/// the recovery merkle root via the caller's root builder — the same
/// construction the seal worker uses, so adopted segments carry
/// matching roots (crash-window row 3).
///
/// `None` when the file is missing or unparsable (the segment falls
/// back to WAL replay).
fn read_segment_data_root(
    segments_dir: &std::path::Path,
    id: SegmentId,
    merkle_root_fn: &(dyn Fn(&[u8]) -> Option<HashOutput> + Send + Sync),
) -> Option<HashOutput> {
    let path = segments_dir.join(format!("{id}.dat"));
    let raw = std::fs::read(path).ok()?;
    let header = SegmentHeader::from_bytes(&raw)?;
    let hdr_size = SegmentHeader::header_size(header.version);
    let data_end = (hdr_size as u64 + header.size) as usize;
    if data_end > raw.len() {
        return None; // truncated tail — leave to WAL replay
    }
    merkle_root_fn(&raw[hdr_size..data_end])
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
                data_wal_pos: entry.data_wal_pos,
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
                        data_wal_pos: None,
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
    ///
    /// `data_wal_pos` restores the entry's recorded position (checkpoint
    /// snapshots carry it — retention needs it to survive
    /// checkpointing).
    pub(crate) fn seed_entry(
        &self,
        id: SegmentId,
        state: SegmentState,
        metadata: SegmentMetadata,
        data_wal_pos: Option<DataWalPos>,
    ) {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        let now = Instant::now();
        Self::evict_expired_locked(&mut guard, now);
        guard.entry(id).or_insert_with(|| {
            let mut entry = LifecycleEntry::new(state, metadata);
            entry.data_wal_pos = data_wal_pos;
            entry
        });
    }

    /// Hints every shard's map at the expected live-entry count
    /// (perf 1.3 — the recovery fold pre-sizes from the CF mirror /
    /// checkpoint estimate, avoiding reallocation cascades).
    pub(crate) fn reserve_hint(&self, entries: usize) {
        let per_shard = entries / self.shards.len() + 1;
        for shard in self.shards.iter() {
            shard.write().reserve(per_shard);
        }
    }

    /// Removes a `Reserved` entry without any durable side-effect — the
    /// recovery pass's drop of an empty reserve (crash-window row 1:
    /// idle-seal never seals empty).
    ///
    /// No `DeleteEvent` is appended: the `ReserveEvent` stays in the
    /// event log and a restart re-folds it and re-drops it (idempotent).
    pub(crate) fn drop_reserve(&self, id: SegmentId) {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        if let Some(entry) = guard.get(&id) {
            if entry.state == SegmentState::Reserved {
                guard.remove(&id);
            }
        }
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

    /// Records the data-WAL position of a segment's latest data entry.
    ///
    /// Called by the write path after every data-WAL append (through
    /// the coordinator — the registry's only writer). Only `Reserved`
    /// entries are updated: the position feeds the `SealEvent` built at
    /// seal time (ADR-0024 Decision 2 — the recovery fold seeks by it).
    /// No-op when the entry is absent or no longer `Reserved` (the
    /// reserve always precedes the first data entry, so in the write
    /// path the entry exists — this guard only covers races with a
    /// concurrent delete).
    pub(crate) fn record_data_wal_pos(&self, id: SegmentId, pos: DataWalPos) {
        let shard = &self.shards[self.shard_for(id)];
        let mut guard = shard.write();
        if let Some(entry) = guard.get_mut(&id) {
            if entry.state == SegmentState::Reserved {
                entry.data_wal_pos = Some(pos);
            }
        }
    }

    /// Returns the recorded data-WAL position of a segment's last data
    /// entry, or `None` when nothing was recorded (no appends, or the
    /// entry is absent).
    pub(crate) fn last_data_wal_pos(&self, id: SegmentId) -> Option<DataWalPos> {
        let shard = &self.shards[self.shard_for(id)];
        let guard = shard.read();
        guard.get(&id).and_then(|entry| entry.data_wal_pos)
    }

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
    /// The event WAL (ADR-0024, migration phase 2): when wired, every
    /// `request_*` appends its event as the durable side-effect and the
    /// CF write becomes a derived mirror performed AFTER the event
    /// (dual-read verification surface). `None` keeps the phase-1
    /// behavior (CF write only) — tests and minimal embeddings.
    event_wal: Option<Arc<EventWal>>,
    /// The event log's checkpoint manager (ADR-0024 Decision 3): when
    /// wired, `maybe_checkpoint` runs after every event append, triggered
    /// only by the byte threshold. `None` disables checkpointing.
    checkpoint: Option<Arc<EventCheckpoint>>,
    /// The checkpoint trigger's configuration (the byte threshold).
    checkpoint_config: Option<EventWalConfig>,
    /// Latch: exactly one checkpoint task in flight (a burst past the
    /// threshold produces exactly one checkpoint — DoD).
    checkpoint_latch: Arc<std::sync::atomic::AtomicBool>,
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
            event_wal: None,
            checkpoint: None,
            checkpoint_config: None,
            checkpoint_latch: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            idle_pools: Vec::new(),
            idle_seal_timeout_ms: 0,
            entries_gauge,
            bytes_gauge,
        };
        coordinator.update_gauges();
        coordinator
    }

    /// Arms the event-appender arm (ADR-0025 migration phase 2): the
    /// coordinator appends Reserve/Seal/Delete events to the event WAL
    /// as its durable side-effect, and the CF write becomes a
    /// **derived-mirror** write performed AFTER the event append
    /// (dual-read verification surface — the event log is the source of
    /// truth, ADR-0024 Decision 1).
    ///
    /// Without this (phase 1 — tests, minimal embeddings), the
    /// coordinator keeps the CF write as the durable side-effect.
    #[must_use]
    pub fn with_event_wal(mut self, event_wal: Arc<EventWal>) -> Self {
        self.event_wal = Some(event_wal);
        self
    }

    /// Arms the checkpoint trigger (ADR-0024 Decision 3): after every
    /// event append, `maybe_checkpoint` runs — triggered **only** by the
    /// byte threshold (`event_wal_checkpoint_bytes`; no time-based
    /// fallback), spawning the snapshot + truncate off the append path.
    #[must_use]
    pub fn with_checkpoint(
        mut self,
        checkpoint: Arc<EventCheckpoint>,
        config: EventWalConfig,
    ) -> Self {
        self.checkpoint = Some(checkpoint);
        self.checkpoint_config = Some(config);
        self
    }

    /// The threshold-only checkpoint trigger (ADR-0024 Decision 3):
    /// when the event log has grown past the byte threshold since the
    /// last checkpoint and no checkpoint is in flight, spawn the
    /// snapshot + truncate off the append path.
    ///
    /// The latch guarantees a burst past the threshold produces exactly
    /// one checkpoint (the DoD invariant); `up_to` is the position
    /// captured at spawn time — appends landing during the checkpoint
    /// are folded on top at startup (exactly-once by position coverage).
    async fn maybe_checkpoint(&self) {
        let (Some(checkpoint), Some(event_wal), Some(config)) =
            (&self.checkpoint, &self.event_wal, &self.checkpoint_config)
        else {
            return;
        };
        if !checkpoint.needs_checkpoint(config) {
            return;
        }
        if self.checkpoint_latch.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return; // a checkpoint is already in flight
        }
        let checkpoint = Arc::clone(checkpoint);
        let event_wal = Arc::clone(event_wal);
        let registry = Arc::clone(&self.registry);
        let latch = Arc::clone(&self.checkpoint_latch);
        tokio::spawn(async move {
            let up_to = event_wal.latest_pos();
            match checkpoint.write_checkpoint(&registry, up_to) {
                Ok(info) => {
                    if let Err(e) = checkpoint.truncate_before(info.covered_pos).await {
                        tracing::warn!(error = %e, "event WAL checkpoint truncation failed");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "event WAL checkpoint failed"),
            }
            latch.store(false, std::sync::atomic::Ordering::Release);
        });
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

    /// Records the data-WAL position of a segment's latest data entry
    /// (ADR-0024 Decision 2).
    ///
    /// Called by the write path after every data-WAL append (through the
    /// sealer's `append_wal_entry`); the coordinator is the registry's
    /// only writer. The last recorded position per segment is embedded
    /// in the `SealEvent` at seal time.
    pub(crate) fn record_data_wal_pos(&self, id: SegmentId, pos: DataWalPos) {
        self.registry.record_data_wal_pos(id, pos);
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
            self.registry.seed_entry(meta.segment_id, state, meta, None);
        }
        self.update_gauges();
        Ok(())
    }

    /// Seeds the registry from a checkpoint snapshot (ADR-0024
    /// Decision 3): every live entry's state, full metadata, and
    /// recorded `data_wal_pos` (retention needs it to survive
    /// checkpointing) is restored. The fold then runs from the
    /// checkpoint's covered position.
    pub fn seed_from_checkpoint(&self, snapshot: &SegmentLifecycleRegistry) {
        snapshot.for_each(|id, entry| {
            self.registry.seed_entry(id, entry.state, entry.metadata.clone(), entry.data_wal_pos);
        });
    }

    /// Folds the event log into the registry — the deterministic
    /// recovery core (`state = fold(events)`, ADR-0025 Decision 3).
    ///
    /// Applies every event in the iterator's order through the typed
    /// transition API (`reserve` / `seal` / `delete` — the registry's
    /// pure transitions; no event is re-appended during recovery). The
    /// fold is deterministic: the same event sequence folded twice
    /// yields identical registries (the `SealEvent` carries no
    /// timestamp, so the folded `sealed_at` is the deterministic
    /// sentinel `Some(0)`).
    ///
    /// The registry is pre-sized from the CF mirror estimate
    /// (perf 1.3); the lock bodies contain only map ops (perf 7.1).
    ///
    /// A torn tail ([`Error::TornEventRecord`]) ends the fold at the
    /// last good record — the crash window's residue, folded state is
    /// authoritative. A mid-log corruption ([`Error::CorruptEventLog`])
    /// or a rejected transition ([`Error::EventFoldError`]) aborts with
    /// the record position — never a silent partial fold.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptEventLog`] for a corrupt mid-log record,
    /// [`Error::EventFoldError`] for a rejected transition (with the
    /// record position), or an I/O error from the iterator.
    pub fn rebuild_from_events(
        &self,
        events: impl Iterator<Item = Result<(EventWalPos, SegmentEvent)>>,
    ) -> Result<RebuildOutcome> {
        let mut outcome = RebuildOutcome::default();
        // Pre-size the registry and the folded-id set (perf 1.3): the CF
        // mirror holds the same live set as the fold, modulo the
        // event→mirror lag.
        let mirror_estimate = self.metadata.list_segments().len();
        let mut folded: std::collections::HashSet<SegmentId> =
            std::collections::HashSet::with_capacity(mirror_estimate);
        self.registry.reserve_hint(mirror_estimate);

        for item in events {
            let (pos, evt) = match item {
                Ok(item) => item,
                // A torn tail ends the fold at the last good record —
                // the crash window's residue, folded state is
                // authoritative (the open-time truncation normally
                // removes it before the fold runs; this is the
                // defensive path).
                Err(Error::TornEventRecord { .. }) => break,
                Err(e) => return Err(e),
            };
            let segment_id = evt.segment_id();
            let fold_result = match evt {
                SegmentEvent::Reserve(evt) => {
                    let meta = SegmentMetadata {
                        segment_id: evt.segment_id,
                        ec_k: evt.ec_k,
                        ec_m: evt.ec_m,
                        size_tier: evt.tier,
                        merkle_root: None,
                        storage_locations: smallvec::SmallVec::new(),
                        sealed_at: None,
                    };
                    self.registry.reserve(evt.segment_id, meta)
                }
                SegmentEvent::Seal(evt) => {
                    // Record the SealEvent's data_wal_pos BEFORE the seal
                    // transition (record_data_wal_pos updates Reserved
                    // entries only; the seal keeps the position).
                    self.registry.record_data_wal_pos(evt.segment_id, evt.data_wal_pos);
                    let meta = SegmentMetadata {
                        segment_id: evt.segment_id,
                        ec_k: evt.ec_k,
                        ec_m: evt.ec_m,
                        size_tier: evt.tier,
                        merkle_root: Some(evt.merkle_root),
                        storage_locations: smallvec::SmallVec::new(),
                        sealed_at: Some(0), // deterministic sentinel — the event carries no timestamp
                    };
                    self.registry.seal(evt.segment_id, meta)
                }
                SegmentEvent::Delete(evt) => self.registry.delete(evt.segment_id),
            };
            fold_result.map_err(|e| Error::EventFoldError {
                pos,
                detail: format!("{e} (segment {segment_id})"),
            })?;
            folded.insert(segment_id);
        }
        outcome.folded_segments = folded.len();
        Ok(outcome)
    }

    /// Full startup recovery: fold the events, verify the CF mirror
    /// (phase 2 dual-read), then run the data-WAL pass for
    /// `Reserved`-unsealed segments (ADR-0024 Decision 1).
    ///
    /// The pass, per crash-window row:
    /// - **row 1** — `Reserved` with zero data entries: the reserve is
    ///   dropped (idle-seal never seals empty);
    /// - **row 2** — `Reserved`-unsealed with data entries: entries are
    ///   replayed into the pools in position order, the last entry's
    ///   position is recorded, and the segment is re-sealed through the
    ///   seal worker (the `SealEvent` carries the recomputed
    ///   `merkle_root` and the recorded `data_wal_pos`);
    /// - **row 3** — `Reserved`-unsealed with a durable `.dat` (the
    ///   crash happened after the `.dat` fsync, before the `SealEvent`):
    ///   the root is recomputed from the `.dat` and a `SealEvent` is
    ///   appended via `request_seal` — no re-seal I/O;
    /// - a data entry whose segment has **no** `ReserveEvent` is a
    ///   corruption signal (the reserve-before-entry invariant): logged
    ///   with its position and swept, never replayed.
    ///
    /// The spec's `seed` (checkpoint snapshot) is realized by pre-seeding
    /// the coordinator's registry (`seed_entry`) and starting the event
    /// iterator at the checkpoint position — the checkpoint feature
    /// composes that; `None` here means fold from the earliest retained
    /// event.
    ///
    /// The data WAL is consumed and truncated after the pass. The pass
    /// requires the pools wired via
    /// [`with_idle_seal`](Self::with_idle_seal) and the sealer's `.dat`
    /// directory.
    ///
    /// # Errors
    ///
    /// Returns the fold's corruption errors, a
    /// [`Error::MirrorDivergence`] when the CF mirror contradicts the
    /// fold in the impossible direction, or an I/O error from the data
    /// WAL / sealer.
    pub async fn rebuild_with_data_wal(
        &self,
        events: impl Iterator<Item = Result<(EventWalPos, SegmentEvent)>>,
        data_wal: &WalReader,
        sealer: &SegmentSealer,
        merkle_root_fn: impl Fn(&[u8]) -> Option<HashOutput> + Send + Sync + 'static,
        data_wal_writer: &WalWriter,
    ) -> Result<RebuildOutcome> {
        let mut outcome = self.rebuild_from_events(events)?;
        self.verify_and_repair_mirror()?;
        self.recover_reserved_unsealed(
            data_wal,
            sealer,
            &merkle_root_fn,
            &mut outcome,
            data_wal_writer,
        )
        .await?;
        Ok(outcome)
    }

    /// Phase-2 dual-read verification: the folded registry must agree
    /// with the CF mirror, and the mirror must not hold anything the
    /// fold cannot produce.
    ///
    /// The mirror write always follows its event append, so a crash
    /// between the two leaves the mirror **lagging** the fold — a normal
    /// crash window, repaired from the fold (the event log is
    /// authoritative; phase-2 consumers such as GC still read the CF,
    /// so the mirror must be consistent after rebuild). The impossible
    /// direction — the mirror holding an entry or a sealed state the
    /// fold lacks — fails startup with a structured error
    /// ([`Error::MirrorDivergence`]); a stale reserve mirror for a
    /// dropped empty reserve is removed.
    fn verify_and_repair_mirror(&self) -> Result<()> {
        // Snapshot the live entries under the shard read locks (the
        // closure must not call back into the registry).
        let mut live: Vec<(SegmentId, SegmentState, SegmentMetadata)> = Vec::new();
        self.registry.for_each(|id, entry| {
            live.push((id, entry.state, entry.metadata.clone()));
        });

        for (id, state, meta) in live {
            match state {
                SegmentState::Deleted => {
                    // The delete mirror must eventually remove the entry;
                    // a lagging mirror (crash between DeleteEvent and the
                    // mirror write) is repaired.
                    if self.metadata.get_segment(id)?.is_some() {
                        self.metadata.delete_segment(id).map_err(|e| Error::MirrorDivergence {
                            segment_id: id,
                            detail: format!("failed to repair lagging delete mirror: {e}"),
                        })?;
                    }
                }
                SegmentState::Reserved | SegmentState::Sealed => {
                    match self.metadata.get_segment(id)? {
                        Some(cf_meta) => {
                            let cf_sealed = cf_meta.sealed_at.is_some();
                            let fold_sealed = state == SegmentState::Sealed;
                            if cf_sealed != fold_sealed {
                                return Err(Error::MirrorDivergence {
                                    segment_id: id,
                                    detail: format!(
                                        "mirror sealed_at={cf_sealed} but the fold says {state:?}"
                                    ),
                                });
                            }
                        }
                        None => {
                            // Mirror lag (crash between the event append
                            // and the mirror write) — repair from the
                            // fold.
                            self.metadata.put_segment(meta).map_err(|e| {
                                Error::MirrorDivergence {
                                    segment_id: id,
                                    detail: format!("failed to repair lagging mirror: {e}"),
                                }
                            })?;
                        }
                    }
                }
            }
        }

        // The impossible direction: mirror entries the fold lacks.
        for cf_meta in self.metadata.list_segments() {
            let cf_meta = cf_meta.map_err(|e| {
                Error::Io(std::io::Error::other(format!("CF mirror scan failed: {e}")))
            })?;
            let id = cf_meta.segment_id;
            if self.registry.get(id).is_some() {
                continue;
            }
            if cf_meta.sealed_at.is_some() {
                // A sealed mirror entry requires a durable SealEvent,
                // which the fold must have seen (the mirror write
                // follows the event append).
                return Err(Error::MirrorDivergence {
                    segment_id: id,
                    detail: "mirror holds a sealed entry the fold lacks".into(),
                });
            }
            // Stale reserve mirror for a dropped empty reserve — remove
            // it (the fold is authoritative).
            self.metadata.delete_segment(id).map_err(|e| Error::MirrorDivergence {
                segment_id: id,
                detail: format!("failed to remove stale reserve mirror: {e}"),
            })?;
        }
        Ok(())
    }

    /// The data-WAL pass: rebuild every `Reserved`-unsealed segment
    /// (adopt the durable `.dat` or replay the entries), drop empty
    /// reserves, sweep orphan entries (ADR-0024 Decision 1).
    async fn recover_reserved_unsealed(
        &self,
        data_wal: &WalReader,
        sealer: &SegmentSealer,
        merkle_root_fn: &(dyn Fn(&[u8]) -> Option<HashOutput> + Send + Sync),
        outcome: &mut RebuildOutcome,
        data_wal_writer: &WalWriter,
    ) -> Result<()> {
        // 1. Collect the Reserved-unsealed set (the fold's residue) with
        //    the registry's authoritative tier.
        let mut reserved: Vec<(SegmentId, SizeTier)> = Vec::new();
        self.registry.for_each(|id, entry| {
            if entry.state == SegmentState::Reserved {
                reserved.push((id, entry.metadata.size_tier));
            }
        });
        let reserved_ids: std::collections::HashSet<SegmentId> =
            reserved.iter().map(|(id, _)| *id).collect();
        let segments_dir = sealer.segment_data_dir().to_path_buf();

        // 2. Split adopt vs replay by the durable `.dat` presence. The
        //    probe validates the FULL data section (header + data_end
        //    within the file), mirroring `read_segment_data_root`: a
        //    `.dat` with a valid header but a truncated data section
        //    falls back to replay — its entries must be buffered during
        //    the stream (silently adopting it would orphan the segment
        //    and then truncate its only durable copy).
        let mut adopt: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
        for (id, _) in &reserved {
            let path = segments_dir.join(format!("{id}.dat"));
            if let Ok(raw) = std::fs::read(&path) {
                if let Some(header) = SegmentHeader::from_bytes(&raw) {
                    let hdr_size = SegmentHeader::header_size(header.version);
                    let data_end = (hdr_size as u64 + header.size) as usize;
                    if data_end <= raw.len() {
                        adopt.insert(*id);
                        continue;
                    }
                }
                tracing::warn!(
                    segment_id = %id,
                    "interrupted-seal .dat unparsable or truncated; removing it and falling back to WAL replay"
                );
                // The file is untrustworthy (an interrupted seal's
                // artifact is only valid if fully written). Remove it
                // so the re-seal's readiness wait actually waits for
                // the fresh .dat instead of seeing the corrupt one.
                let _ = std::fs::remove_file(&path);
            }
        }

        // 3. Stream the data WAL ONCE in position order (perf 3.1 — no
        //    per-entry seeks): buffer replay data per segment (bounded
        //    by the WAL retention window — see the module note), record
        //    the last entry position per reserved segment, count swept
        //    entries (sealed/deleted/orphan).
        let mut groups: std::collections::HashMap<SegmentId, Vec<Bytes>> =
            std::collections::HashMap::with_capacity(reserved.len());
        let mut last_pos: std::collections::HashMap<SegmentId, DataWalPos> =
            std::collections::HashMap::with_capacity(reserved.len());
        for item in data_wal.replay_positions() {
            let (pos, entry) = item?;
            let id = entry.segment_id();
            if reserved_ids.contains(&id) {
                last_pos.insert(id, pos);
                if adopt.contains(&id) {
                    // The entry is garbage once the .dat is adopted.
                    outcome.swept_entries += 1;
                } else {
                    groups.entry(id).or_default().push(entry.data);
                }
            } else {
                if self.registry.get(id).is_none() {
                    // A data entry without any ReserveEvent: the
                    // reserve-before-entry invariant (ADR-0024
                    // Decision 1) is by construction — its violation is
                    // a corruption signal. Logged and swept, never
                    // replayed.
                    tracing::warn!(
                        segment_id = %id,
                        pos = ?pos,
                        "data WAL entry without a ReserveEvent; swept (reserve-before-entry invariant)"
                    );
                }
                outcome.swept_entries += 1;
            }
        }

        // 4. Record every segment's LAST entry position FIRST (the
        //    re-seal's SealEvent reads it at seal time), then drain all
        //    replay groups sequentially (the pool's slot model bounds
        //    concurrency).
        for (id, pos) in &last_pos {
            self.record_data_wal_pos(*id, *pos);
        }
        for id in groups.keys().cloned().collect::<Vec<_>>() {
            self.drain_replay_group(id, &mut groups, &reserved).await?;
        }

        // 5. Adopt the durable `.dat` segments (row 3): recompute the
        //    root and append the SealEvent via request_seal — no re-seal
        //    I/O.
        for &id in &adopt {
            let Some(entry) = self.registry.get(id) else { continue };
            let Some(root) = read_segment_data_root(&segments_dir, id, merkle_root_fn) else {
                tracing::warn!(segment_id = %id, "adopt failed to read .dat; leaving Reserved");
                continue;
            };
            let meta = SegmentMetadata {
                segment_id: id,
                ec_k: entry.metadata.ec_k,
                ec_m: entry.metadata.ec_m,
                size_tier: entry.metadata.size_tier,
                merkle_root: Some(root),
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64,
                ),
            };
            self.request_seal(id, meta)
                .await
                .map_err(|e| Error::Io(std::io::Error::other(format!("adopt seal failed: {e}"))))?;
            outcome.adopted_segments += 1;
        }

        // 6. Drop empty reserves (row 1): Reserved with no data entries
        //    and no `.dat`.
        let mut remaining_reserved: Vec<SegmentId> = Vec::new();
        self.registry.for_each(|id, entry| {
            if entry.state == SegmentState::Reserved {
                remaining_reserved.push(id);
            }
        });
        for id in remaining_reserved {
            if !last_pos.contains_key(&id) && !adopt.contains(&id) {
                self.registry.drop_reserve(id);
                outcome.dropped_empty_reserves += 1;
            }
        }

        // 7. The replayed seals complete asynchronously on the seal
        //    worker; wait for their .dat files (reads must never race a
        //    partially-written segment — the node's pre-bind readiness).
        let replayed_ids: Vec<SegmentId> =
            last_pos.keys().filter(|id| !adopt.contains(id)).copied().collect();
        if !replayed_ids.is_empty() {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            for id in &replayed_ids {
                let path = segments_dir.join(format!("{id}.dat"));
                while !path.exists() {
                    if std::time::Instant::now() > deadline {
                        tracing::warn!(
                            segment_id = %id,
                            "re-sealed segment .dat not durable within 30s; startup continues"
                        );
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
            outcome.re_sealed_segments = replayed_ids.len();
        }

        // 8. The data WAL is fully consumed (replayed or swept) —
        //    truncate it, exactly like the WAL replay path.
        data_wal_writer.truncate(0).await?;
        Ok(())
    }

    /// Drains one replay group through its pool: appends every buffered
    /// entry in position order (append order == offset order), then
    /// seals the segment through the pool's replay-seal path (the seal
    /// worker computes the root and `request_seal` embeds the recorded
    /// `data_wal_pos`).
    async fn drain_replay_group(
        &self,
        id: SegmentId,
        groups: &mut std::collections::HashMap<SegmentId, Vec<Bytes>>,
        reserved: &[(SegmentId, SizeTier)],
    ) -> Result<()> {
        let Some(data) = groups.remove(&id) else { return Ok(()) };
        let tier = reserved
            .iter()
            .find(|(rid, _)| *rid == id)
            .map(|(_, tier)| *tier)
            .unwrap_or(SizeTier::Standard);
        let Some(pool) = self.idle_pools.iter().find(|p| p.storage_tier() == tier) else {
            return Err(Error::Io(std::io::Error::other(format!(
                "recovery: no pool wired for tier {tier:?} of segment {id}"
            ))));
        };
        for chunk in &data {
            pool.append_replayed(id, chunk).await.map_err(|e| {
                Error::Io(std::io::Error::other(format!("replay append for {id} failed: {e}")))
            })?;
        }
        pool.seal_replayed_partial(id).await.map_err(|e| {
            Error::Io(std::io::Error::other(format!("replay seal for {id} failed: {e}")))
        })?;
        Ok(())
    }

    /// Reserves a segment durably: validate (absent | `Reserved`) →
    /// durable side-effect → fold into the registry.
    ///
    /// **Phase 1** (no event WAL wired): the CF `put_segment`
    /// (`sealed_at: None`) is the durable side-effect.
    ///
    /// **Phase 2** (event WAL wired): the `ReserveEvent` is appended
    /// first (durable via the event group — ADR-0024 Decision 1), then
    /// the fold; the CF write becomes a **derived mirror** performed
    /// AFTER the event append (dual-read verification surface). A
    /// mirror-write failure is logged, not fatal — the event log is
    /// authoritative.
    ///
    /// Returns `Ok` **only after** the durable side-effect — the write
    /// path calls this before the first `DataEntry` (WAL entry) of its
    /// segment, so the WAL cleanup can never mistake an in-flight
    /// segment for garbage.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError::AlreadySealed`] /
    /// [`TransitionError::AlreadyDeleted`] when the segment already
    /// holds a higher state (the phantom-downgrade write is rejected —
    /// no durable write and no fold happen), or
    /// [`TransitionError::DurableWriteFailed`] when the durable
    /// side-effect (event append in phase 2, CF write in phase 1) fails.
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
        if let Some(event_wal) = &self.event_wal {
            // Phase 2: the event append is the durable side-effect.
            let evt = SegmentEvent::Reserve(ReserveEvent { segment_id: id, tier, ec_k, ec_m });
            event_wal
                .append(evt)
                .await
                .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
            self.registry.fold_reserve(id, meta.clone())?;
            if let Err(e) = self.metadata.put_segment(meta) {
                tracing::warn!(
                    segment_id = %id,
                    error = %e,
                    "lifecycle CF mirror write failed after reserve event; event log is authoritative"
                );
            }
        } else {
            // Phase 1: the CF write is the durable side-effect.
            self.metadata
                .put_segment(meta.clone())
                .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
            self.registry.fold_reserve(id, meta)?;
        }
        self.maybe_checkpoint().await;
        self.update_gauges();
        Ok(())
    }

    /// Seals a segment durably: validate (`Reserved` only) → durable
    /// side-effect → fold into the registry.
    ///
    /// **Phase 1** (no event WAL wired): the CF `put_segment` with the
    /// full sealed metadata (incl. the seal-time `merkle_root`) is the
    /// durable side-effect.
    ///
    /// **Phase 2** (event WAL wired): the `SealEvent` — carrying the
    /// full repacked metadata: `merkle_root` is a seal input (the
    /// BadDigest defect is impossible) and `data_wal_pos` is the LAST
    /// data entry's position (ADR-0024 Decision 2) — is appended first,
    /// then the fold, then the CF mirror write.
    ///
    /// Callers invoke this only after the `.dat` fsync returns (the
    /// seal worker's operation sequence); the durable write and the
    /// fold are strictly ordered after validation.
    ///
    /// # Errors
    ///
    /// Returns a [`TransitionError`] when the entry is not `Reserved`
    /// (no durable write, no fold), or
    /// [`TransitionError::DurableWriteFailed`] when the durable
    /// side-effect fails. In phase 2 a missing `merkle_root` is a
    /// durable-write failure: a `SealEvent` without the seal-time root
    /// would fabricate the anchor recovery trusts.
    pub async fn request_seal(
        &self,
        id: SegmentId,
        metadata: SegmentMetadata,
    ) -> Result<(), TransitionError> {
        self.registry.validate_seal(id)?;
        if let Some(event_wal) = &self.event_wal {
            // Phase 2: the event append is the durable side-effect.
            let merkle_root = metadata.merkle_root.ok_or_else(|| {
                TransitionError::DurableWriteFailed(
                    "seal without merkle_root (the event log requires the seal-time root)".into(),
                )
            })?;
            let data_wal_pos = self
                .registry
                .last_data_wal_pos(id)
                .unwrap_or(DataWalPos { file_seq: 0, offset: 0 });
            let evt = SegmentEvent::Seal(SealEvent {
                segment_id: id,
                tier: metadata.size_tier,
                ec_k: metadata.ec_k,
                ec_m: metadata.ec_m,
                merkle_root,
                data_wal_pos,
            });
            event_wal
                .append(evt)
                .await
                .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
            self.registry.fold_seal(id, metadata.clone())?;
            if let Err(e) = self.metadata.put_segment(metadata) {
                tracing::warn!(
                    segment_id = %id,
                    error = %e,
                    "lifecycle CF mirror write failed after seal event; event log is authoritative"
                );
            }
        } else {
            // Phase 1: the CF write is the durable side-effect.
            self.metadata
                .put_segment(metadata.clone())
                .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
            self.registry.fold_seal(id, metadata)?;
        }
        self.maybe_checkpoint().await;
        self.update_gauges();
        Ok(())
    }

    /// Deletes a segment durably: validate (`Reserved` | `Sealed`) →
    /// durable side-effect → fold into the registry (the entry is
    /// evicted after the delete grace).
    ///
    /// **Phase 1** (no event WAL wired): the CF deleted-marker write is
    /// the durable side-effect.
    ///
    /// **Phase 2** (event WAL wired): the `DeleteEvent` is appended
    /// first (durable via the event group), then the fold, then the CF
    /// mirror `delete_segment`.
    ///
    /// The caller (the orphan reaper) invokes this **before** the
    /// `.dat` unlink — the durable deletion precedes the data removal
    /// (ADR-0024 invariant 3: "Delete before unlink").
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError::AlreadyDeleted`] /
    /// [`TransitionError::Missing`] when no live entry exists (no
    /// durable write, no fold), or
    /// [`TransitionError::DurableWriteFailed`] when the durable
    /// side-effect fails.
    pub async fn request_delete(&self, id: SegmentId) -> Result<(), TransitionError> {
        self.registry.validate_delete(id)?;
        if let Some(event_wal) = &self.event_wal {
            // Phase 2: the event append is the durable side-effect.
            let evt = SegmentEvent::Delete(DeleteEvent { segment_id: id });
            event_wal
                .append(evt)
                .await
                .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
            self.registry.fold_delete(id)?;
            if let Err(e) = self.metadata.delete_segment(id) {
                tracing::warn!(
                    segment_id = %id,
                    error = %e,
                    "lifecycle CF mirror write failed after delete event; event log is authoritative"
                );
            }
        } else {
            // Phase 1: the CF write is the durable side-effect.
            self.metadata
                .delete_segment(id)
                .map_err(|e| TransitionError::DurableWriteFailed(e.to_string()))?;
            self.registry.fold_delete(id)?;
        }
        self.maybe_checkpoint().await;
        self.update_gauges();
        Ok(())
    }

    /// Seals a batch of segments whose `.dat` files are already
    /// durable (fsynced + finalized by the flush coordinator).
    ///
    /// Preserves the flush coordinator's one-RocksDB-batch-per-cycle
    /// property: every accepted id is validated first (read-only, no
    /// locks held across I/O — performance §7.1), the accepted metadata
    /// is committed (phase 1: one `batch_write`; phase 2: one
    /// `SealEvent` append per id — the event group's group commit
    /// batches the fsyncs — then the folds, then one mirror
    /// `batch_write`), and each accepted entry is folded. Returns one
    /// result per input, aligned by index: a validation failure for one
    /// segment does not fail the others.
    ///
    /// # Errors
    ///
    /// Each element is `Ok` on success, or a [`TransitionError`] for
    /// that segment.
    pub(crate) async fn seal_finalized_batch(
        &self,
        metas: Vec<SegmentMetadata>,
    ) -> Vec<std::result::Result<(), TransitionError>> {
        // Phase 1 — validate every id (read-only shard visits; the
        // shard locks are released before any durable I/O).
        let mut out: Vec<std::result::Result<(), TransitionError>> =
            std::iter::repeat_with(|| Ok(())).take(metas.len()).collect();
        let mut accepted: Vec<(usize, SegmentMetadata)> = Vec::with_capacity(metas.len());
        for (i, meta) in metas.into_iter().enumerate() {
            match self.registry.validate_seal(meta.segment_id) {
                Ok(()) => accepted.push((i, meta)),
                Err(e) => out[i] = Err(e),
            }
        }
        if accepted.is_empty() {
            return out;
        }

        if let Some(event_wal) = &self.event_wal {
            // Phase 2 — per-id event append (durable via the event
            // group; the group commit batches the fsyncs), then fold,
            // then ONE mirror batch write for the folded entries. A
            // single event failure fails only that id; mirror failures
            // are logged (the event log is authoritative).
            let mut mirror_ops: Vec<oceanfs_storage_api::BatchOp> =
                Vec::with_capacity(accepted.len());
            for (i, meta) in accepted {
                let id = meta.segment_id;
                let Some(merkle_root) = meta.merkle_root else {
                    out[i] = Err(TransitionError::DurableWriteFailed(
                        "seal without merkle_root (the event log requires the seal-time root)"
                            .into(),
                    ));
                    continue;
                };
                let data_wal_pos = self
                    .registry
                    .last_data_wal_pos(id)
                    .unwrap_or(DataWalPos { file_seq: 0, offset: 0 });
                let evt = SegmentEvent::Seal(SealEvent {
                    segment_id: id,
                    tier: meta.size_tier,
                    ec_k: meta.ec_k,
                    ec_m: meta.ec_m,
                    merkle_root,
                    data_wal_pos,
                });
                let mirror_meta = meta.clone();
                match event_wal.append(evt).await {
                    Ok(_) => match self.registry.fold_seal(id, meta) {
                        Ok(()) => {
                            mirror_ops.push(oceanfs_storage_api::BatchOp::PutSegment(mirror_meta));
                        }
                        Err(e) => {
                            // A fold can lose a race only to a concurrent
                            // delete of the same segment (unreachable in
                            // phase 1: the reaper deletes only
                            // unreferenced segments, and a segment being
                            // sealed is referenced). The event is
                            // durable; the registry converged elsewhere;
                            // no mirror write for this id.
                            tracing::warn!(
                                segment_id = %id,
                                error = ?e,
                                "seal fold lost a transition race; event durable, registry entry unchanged"
                            );
                        }
                    },
                    Err(e) => out[i] = Err(TransitionError::DurableWriteFailed(e.to_string())),
                }
            }
            if !mirror_ops.is_empty() {
                if let Err(e) = self.metadata.batch_write(mirror_ops) {
                    tracing::warn!(
                        error = %e,
                        "lifecycle CF mirror batch write failed after seal events; event log is authoritative"
                    );
                }
            }
        } else {
            // Phase 1 — one durable batch write for the accepted entries.
            let ops: Vec<oceanfs_storage_api::BatchOp> = accepted
                .iter()
                .cloned()
                .map(|(_, meta)| oceanfs_storage_api::BatchOp::PutSegment(meta))
                .collect();
            if let Err(e) = self.metadata.batch_write(ops) {
                for (i, _) in accepted {
                    out[i] = Err(TransitionError::DurableWriteFailed(e.to_string()));
                }
                return out;
            }
            // Phase 3 — fold each accepted entry (write locks, once per
            // segment, strictly after the durable write returned).
            for (_, meta) in accepted {
                let id = meta.segment_id;
                if let Err(e) = self.registry.fold_seal(id, meta) {
                    tracing::warn!(
                        segment_id = %id,
                        error = ?e,
                        "seal fold lost a transition race; registry entry unchanged"
                    );
                }
            }
        }
        self.maybe_checkpoint().await;
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

    use oceanfs_core::{
        EventWalConfig, HashOutput, MetadataConfig, PoolConfig, SegmentSizeConfig, SizeTier,
        WalConfig,
    };
    use oceanfs_storage_api::MetadataStore;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        buffer_pool::BufferPool,
        metadata::RocksDbMetadataStore,
        segment::event_wal::{EventWal, EventWalPos},
        wal::{WalEntry, WalWriter},
        SegmentSealer,
    };

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
        let results = coordinator.seal_finalized_batch(vec![test_metadata(id2, true)]).await;
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
        let results = coordinator.seal_finalized_batch(metas).await;
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
        let results = coordinator.seal_finalized_batch(metas).await;
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

    // ------------------------------------------------------------------
    // Event WAL phase 2 — full coordinator sequence (ADR-0024, DoD)
    // ------------------------------------------------------------------

    /// The DoD integration test: a reserve→data→seal→delete sequence
    /// through the coordinator produces exactly three events whose
    /// replay fold reproduces the registry exactly; the CF mirror
    /// matches (dual-read); the seal's `data_wal_pos` equals the data
    /// WAL writer's returned position of the LAST data entry.
    #[tokio::test]
    async fn event_wal_phase2_full_sequence_folds_and_mirrors() {
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
        let event_wal_config = EventWalConfig {
            event_wal_dir: dir.path().join("event-wal"),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024 * 1024,
        };
        let event_wal = Arc::new(
            EventWal::open(event_wal_config.event_wal_dir.clone(), &event_wal_config)
                .await
                .unwrap(),
        );

        // The coordinator registry uses a non-zero delete grace so the
        // `Deleted` state is observable in both the live and the folded
        // registries (the DoD's "replay fold reproduces the registry
        // exactly").
        let lifecycle_config = grace_config(1_000);
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(
                store.clone(),
                Arc::new(SegmentLifecycleRegistry::new(&lifecycle_config)),
            )
            .with_event_wal(event_wal.clone()),
        );

        // The data WAL writer + sealer — the write path's position
        // source (`append_wal_entry` records each entry's position with
        // the coordinator).
        let data_config = WalConfig {
            data_dir: dir.path().join("wal"),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            wal_use_sync_file_range: false,
        };
        let data_wal = Arc::new(WalWriter::open(&data_config).await.unwrap());
        let sealer = Arc::new(SegmentSealer::new(
            crate::SealConfig { data_dir: dir.path().join("segments"), ..Default::default() },
            data_wal.clone(),
            lifecycle.clone(),
        ));

        // reserve → N data appends → seal → delete
        let id = SegmentId::new();
        lifecycle.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();

        let mut last_pos: Option<DataWalPos> = None;
        for i in 0..3u32 {
            let entry = WalEntry::new(
                id,
                (i * 100) as u64,
                3,
                3,
                1, // standard pool tier byte
                0,
                0,
                HashOutput::from_bytes([0u8; 32]),
                vec![1, 2, 3].into(),
            );
            last_pos = Some(sealer.append_wal_entry(entry).await.unwrap());
        }
        let last_pos = last_pos.expect("three data entries appended");
        // Each data entry is the 88-byte header + 3 payload bytes.
        assert_eq!(
            data_wal.global_position().await,
            3 * (WalEntry::header_size() as u64 + 3),
            "data WAL holds the three entries"
        );

        let sealed_meta = SegmentMetadata {
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(HashOutput::from_bytes([0xAB; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1_700_000_000_000),
        };
        lifecycle.request_seal(id, sealed_meta.clone()).await.unwrap();
        lifecycle.request_delete(id).await.unwrap();

        // --- Exactly three events, in order: Reserve, Seal, Delete ---
        let events: Vec<(EventWalPos, SegmentEvent)> = event_wal
            .read_from(EventWalPos { file_seq: 0, offset: 0 })
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(events.len(), 3, "the sequence must produce exactly three events");

        match &events[0].1 {
            SegmentEvent::Reserve(evt) => {
                assert_eq!(evt.segment_id, id);
                assert_eq!(evt.tier, SizeTier::Standard);
                assert_eq!(evt.ec_k, 4);
                assert_eq!(evt.ec_m, 2);
            }
            other => panic!("first event must be Reserve, got {other:?}"),
        }
        match &events[1].1 {
            SegmentEvent::Seal(evt) => {
                assert_eq!(evt.segment_id, id);
                assert_eq!(evt.tier, SizeTier::Standard);
                assert_eq!(evt.ec_k, 4);
                assert_eq!(evt.ec_m, 2);
                assert_eq!(evt.merkle_root, HashOutput::from_bytes([0xAB; 32]));
                assert_eq!(
                    evt.data_wal_pos, last_pos,
                    "SealEvent.data_wal_pos must equal the LAST data entry's position"
                );
            }
            other => panic!("second event must be Seal, got {other:?}"),
        }
        match &events[2].1 {
            SegmentEvent::Delete(evt) => assert_eq!(evt.segment_id, id),
            other => panic!("third event must be Delete, got {other:?}"),
        }

        // --- Replay fold reproduces the registry exactly ---
        let folded = SegmentLifecycleRegistry::new(&lifecycle_config);
        for (_, evt) in &events {
            match evt {
                SegmentEvent::Reserve(evt) => {
                    let meta = SegmentMetadata {
                        segment_id: evt.segment_id,
                        ec_k: evt.ec_k,
                        ec_m: evt.ec_m,
                        size_tier: evt.tier,
                        merkle_root: None,
                        storage_locations: smallvec::SmallVec::new(),
                        sealed_at: None,
                    };
                    folded.reserve(evt.segment_id, meta).unwrap();
                }
                SegmentEvent::Seal(evt) => {
                    let meta = SegmentMetadata {
                        segment_id: evt.segment_id,
                        ec_k: evt.ec_k,
                        ec_m: evt.ec_m,
                        size_tier: evt.tier,
                        merkle_root: Some(evt.merkle_root),
                        storage_locations: smallvec::SmallVec::new(),
                        sealed_at: Some(1_700_000_000_000),
                    };
                    folded.seal(evt.segment_id, meta).unwrap();
                }
                SegmentEvent::Delete(evt) => {
                    folded.delete(evt.segment_id).unwrap();
                }
            }
        }
        // Both registries hold the same Deleted entry (grace not yet
        // expired in either).
        let live = lifecycle.registry().get(id).expect("live registry entry present");
        let folded_entry = folded.get(id).expect("folded registry entry present");
        assert_eq!(live.state, SegmentState::Deleted);
        assert_eq!(folded_entry.state, SegmentState::Deleted);
        assert_eq!(folded.len(), lifecycle.registry().len(), "fold reproduces the live registry");

        // --- Dual-read: the CF mirror matches the event fold ---
        // reserve put sealed_at:None; seal updated it; delete removed it.
        assert!(
            store.get_segment(id).unwrap().is_none(),
            "CF mirror must reflect the delete (event log authoritative)"
        );
    }
}
