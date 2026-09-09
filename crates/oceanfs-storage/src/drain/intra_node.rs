//! The intra-node drain worker (d3, ADR-0036 C1a).
//!
//! A paced, operator-visible background mover that empties a `Draining`
//! data pool by relocating its sealed segments to **sibling data pools on
//! the same node** (no cluster traffic, `storage_locations` untouched).
//! The node runs one instance under the ADR-0017 Tier-1 housekeeping
//! budget; each [`IntraNodeDrain::run_cycle`] relocates up to
//! [`IntraNodeDrainConfig::max_bytes_per_tick`] across every draining
//! pool.
//!
//! ## Correctness rules (ADR-0036 D6, ADR-0034)
//!
//! - **Registry enumeration only** — the worker walks
//!   [`SegmentLifecycleRegistry::for_each`] (in-memory, bounded metadata
//!   discipline); it never scans a disk.
//! - **Largest-first, park-when-the-largest-cannot-fit.** Candidates of a
//!   source pool are relocated in descending `total_bytes` order. When the
//!   largest remaining segment fits no eligible sibling, the pool is
//!   parked with a blocked reason and **smaller segments are not moved**:
//!   a sealed `.dat` cannot straddle pools, so moving a smaller segment
//!   only consumes the siblings' headroom and cannot make the larger one
//!   fit — partial progress would waste I/O and shrink the window.
//! - **No-destructive-failure** — a parked drain deletes nothing and leaves
//!   the pool `Draining`; a later capacity change clears the reason and the
//!   next cycle retries.
//! - **Empty ⇒ `Detachable`** — when no live (`Reserved` *or* `Sealed`)
//!   registry entry carries the source `pool_id`, the worker calls
//!   [`PoolRegistry::set_pool_empty`]; d5 detaches from there. A
//!   `Reserved` entry (an in-flight seal that must still land on the
//!   source, d1 decision) keeps the pool non-empty.
//! - **Pause** — a pool whose drain record is paused
//!   ([`PoolRegistry::set_drain_paused`]) is skipped for the whole cycle;
//!   pausing never aborts a relocation already holding the per-segment
//!   lock.
//! - **Dead mid-drain** — genuine confirmed loss (`PoolStatus::Dead`)
//!   beats the operator flag (d1); a dead pool is never a drain source or
//!   a relocation target.
//!
//! Each relocation goes through the d2 [`SegmentRelocator`]
//! (copy → commit → unlink under the per-segment write lock), so the two
//! crash windows stay safe and a re-run after a pre-commit crash is
//! idempotent.

use std::sync::Arc;

use oceanfs_core::SegmentId;

use crate::{
    pool::{drain::DrainState, placement::PlacementPolicy, PoolRegistry, PoolStatus},
    segment::{
        lifecycle::{SegmentLifecycleRegistry, SegmentState},
        relocate::{RelocateError, SegmentRelocator},
    },
};

/// Per-task pacing knob for the intra-node drain worker (ADR-0036 D5 —
/// its own configurable byte budget; no shared budget framework).
///
/// # Examples
///
/// ```
/// use oceanfs_storage::IntraNodeDrainConfig;
///
/// let config = IntraNodeDrainConfig { max_bytes_per_tick: 16 * 1024 * 1024 };
/// assert_eq!(config.max_bytes_per_tick, 16 * 1024 * 1024);
/// // Defaults to the operator-facing 256 MiB per tick.
/// assert_eq!(IntraNodeDrainConfig::default().max_bytes_per_tick, 256 * 1024 * 1024);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntraNodeDrainConfig {
    /// Bytes of relocation performed per cycle across all draining pools
    /// (a post-relocate cap — a single segment larger than the budget
    /// overshoots one tick).
    pub max_bytes_per_tick: u64,
}

impl Default for IntraNodeDrainConfig {
    fn default() -> Self {
        Self { max_bytes_per_tick: 256 * 1024 * 1024 }
    }
}

/// One cycle's outcome, aggregated over every draining data pool.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::DrainCycleStats;
///
/// let stats = DrainCycleStats {
///     segments_moved: 2,
///     bytes_moved: 1024,
///     emptied: vec![0],
///     blocked: Vec::new(),
/// };
/// assert_eq!(stats.segments_moved, 2);
/// assert!(stats.blocked.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainCycleStats {
    /// Sealed segments relocated off draining pools this cycle.
    pub segments_moved: u64,
    /// Logical bytes relocated (`SegmentMetadata.total_bytes` summed).
    pub bytes_moved: u64,
    /// Pools that transitioned to `Detachable` this cycle (drained empty).
    pub emptied: Vec<u32>,
    /// Pools parked with a blocked reason this cycle
    /// (`(pool_id, reason)` — no-destructive-failure surfacing).
    pub blocked: Vec<(u32, String)>,
}

/// The intra-node drain mover (d3).
///
/// Pure orchestration over storage-owned primitives: it enumerates the
/// lifecycle registry (never the disk), picks sibling targets through
/// [`PlacementPolicy`], and relocates through the d2
/// [`SegmentRelocator`]. Constructed by the node's composition root from
/// the `Arc`s it already holds; the durability module wraps one instance
/// in the `"drain_intra"` ADR-0017 Tier-1 task.
///
/// # Examples
///
/// ```ignore
/// // Requires a fully wired 2+ data-pool node (registry + lifecycle
/// // registry + coordinator/store relocator) — see the drain unit tests
/// // and the node integration test.
/// let drain = IntraNodeDrain::new(config, pool_registry, lifecycle_registry, relocator);
/// let stats = drain.run_cycle().await?;
/// ```
pub struct IntraNodeDrain {
    config: IntraNodeDrainConfig,
    registry: Arc<PoolRegistry>,
    lifecycle_registry: Arc<SegmentLifecycleRegistry>,
    relocator: SegmentRelocator,
    placement: PlacementPolicy,
}

impl IntraNodeDrain {
    /// Creates the intra-node drain worker.
    ///
    /// `registry` carries the pool set + drain state; `lifecycle_registry`
    /// is the bounded segment enumeration source; `relocator` is the d2
    /// copy→commit→unlink mover bound to the node's lifecycle coordinator
    /// and concrete store.
    pub fn new(
        config: IntraNodeDrainConfig,
        registry: Arc<PoolRegistry>,
        lifecycle_registry: Arc<SegmentLifecycleRegistry>,
        relocator: SegmentRelocator,
    ) -> Self {
        Self { config, registry, lifecycle_registry, relocator, placement: PlacementPolicy::new() }
    }

    /// Runs one drain cycle: every active (non-paused, non-dead) draining
    /// pool is drained in pool-id order until `max_bytes_per_tick` is
    /// consumed.
    ///
    /// Returns the aggregate [`DrainCycleStats`]. A cycle with no draining
    /// pool is a cheap no-op (one registry snapshot read).
    ///
    /// # Errors
    ///
    /// The cycle never returns an error: per-segment failures park the
    /// source pool with a surfaced reason (no-destructive-failure rule)
    /// instead of aborting the whole cycle. A caller that needs the
    /// error taxonomy inspects [`DrainCycleStats::blocked`].
    pub async fn run_cycle(&self) -> DrainCycleStats {
        let mut stats = DrainCycleStats::default();
        // Pools the worker may never write into: every data pool that is
        // draining or already detachable (belt-and-braces on top of the
        // Healthy-only placement filter). Taken once per cycle; a pool that
        // flips mid-cycle is still excluded by its status atomic.
        let exclude = self.non_target_data_pool_ids();
        // Deterministic order: ascending pool id.
        let mut sources = self.draining_source_ids();
        sources.sort_unstable();

        'sources: for source in sources {
            let live = self.collect_live_entries(source);

            // Empty (no Sealed and no Reserved entry still carries the
            // source pool_id) → Detachable, d5's precondition.
            if live.is_empty() {
                if self.registry.set_pool_empty(source).is_ok() {
                    stats.emptied.push(source);
                }
                continue;
            }

            // Sealed candidates only, largest-first (bytes desc, id asc for
            // determinism).
            let mut candidates: Vec<(SegmentId, u64)> = live
                .iter()
                .filter(|(_, state, _)| *state == SegmentState::Sealed)
                .map(|(id, _, total_bytes)| (*id, *total_bytes))
                .collect();
            candidates.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

            let mut parked = false;
            for (segment_id, total_bytes) in candidates {
                if stats.bytes_moved >= self.config.max_bytes_per_tick {
                    // Budget consumed — stop the cycle; the next cycle
                    // resumes with the remaining candidates.
                    break 'sources;
                }

                let Some(target) = self.placement.select_data_pool_with_headroom(
                    &self.registry,
                    &exclude,
                    total_bytes,
                ) else {
                    // No sibling with headroom for the largest remaining
                    // segment: park the pool (never move smaller ones —
                    // they cannot make the largest fit) and surface the
                    // reason. Nothing is deleted.
                    let reason = format!(
                        "no sibling data pool has capacity for segment {segment_id} \
                         (requires {total_bytes} bytes free)"
                    );
                    if self.registry.set_drain_blocked(source, Some(&reason)).is_ok() {
                        stats.blocked.push((source, reason));
                    }
                    parked = true;
                    break;
                };

                match self.relocator.relocate(segment_id, target.id()).await {
                    Ok(()) => {
                        stats.segments_moved += 1;
                        stats.bytes_moved = stats.bytes_moved.saturating_add(total_bytes);
                        // Fresh statvfs so the next selection sees the space
                        // this copy consumed (cheap relative to the copy).
                        self.registry.refresh_capacity();
                        // A successful move proves capacity appeared —
                        // clear a stale blocked reason.
                        if self.registry.drain_state(source).is_blocked() {
                            let _ = self.registry.set_drain_blocked(source, None);
                        }
                    }
                    Err(
                        RelocateError::SamePool { .. }
                        | RelocateError::NotSealed(_)
                        | RelocateError::NotHeldLocally(_),
                    ) => {
                        // The entry changed concurrently (sealed elsewhere /
                        // deleted); skip it — the next cycle re-enumerates.
                        continue;
                    }
                    Err(err) => {
                        // Deterministic worker-visible failure (missing
                        // source file, commit/store I/O): park the pool with
                        // the reason surfaced and retry next cycle. Nothing
                        // is deleted and nothing half-removed.
                        let reason = format!("relocating segment {segment_id} failed: {err}");
                        if self.registry.set_drain_blocked(source, Some(&reason)).is_ok() {
                            stats.blocked.push((source, reason));
                        }
                        parked = true;
                        break;
                    }
                }
            }

            if parked {
                continue;
            }

            // The pool's whole candidate list was drained this cycle. Close
            // the reserve→seal race before marking Detachable: re-enumerate
            // so a segment that sealed onto the source between the first
            // snapshot and the last relocate keeps the pool non-empty.
            if self.collect_live_entries(source).is_empty()
                && self.registry.set_pool_empty(source).is_ok()
            {
                stats.emptied.push(source);
            }
        }

        stats
    }

    /// Data-pool ids the worker may never write into this cycle: any pool
    /// whose drain record is `Draining` or `Detachable`. (Dead and
    /// Degraded/write-degraded pools are additionally excluded by the
    /// Healthy-only placement filter.)
    fn non_target_data_pool_ids(&self) -> Vec<u32> {
        self.registry
            .data_pools()
            .iter()
            .filter(|pool| self.registry.drain_state(pool.id()) != DrainState::Idle)
            .map(|pool| pool.id())
            .collect()
    }

    /// Data-pool ids whose drain is currently active: status `Draining`,
    /// record `Draining`, not operator-paused, and owned by the intra-node
    /// mover (a pool begun in [`crate::DrainMode::Cluster`] is the d4
    /// controller's, never this worker's).
    fn draining_source_ids(&self) -> Vec<u32> {
        self.registry
            .data_pools()
            .iter()
            .filter(|pool| {
                pool.status() == PoolStatus::Draining
                    && self.registry.drain_mode(pool.id()) != crate::DrainMode::Cluster
                    && matches!(
                        self.registry.drain_state(pool.id()),
                        DrainState::Draining { paused: false, .. }
                    )
            })
            .map(|pool| pool.id())
            .collect()
    }

    /// Snapshot of the live registry entries (`Reserved`/`Sealed`) that
    /// carry `pool_id == source` — the worker's only segment enumeration
    /// source (ADR-0034; never a disk scan).
    fn collect_live_entries(&self, pool_id: u32) -> Vec<(SegmentId, SegmentState, u64)> {
        let mut live = Vec::new();
        self.lifecycle_registry.for_each(|id, entry| {
            if entry.metadata.pool_id == pool_id {
                live.push((id, entry.state, entry.metadata.total_bytes));
            }
        });
        live
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use bytes::Bytes;
    use oceanfs_core::{LifecycleConfig, PoolRole, SegmentId, SizeTier, StorageConfig};
    use oceanfs_storage_api::SegmentDataStore;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        io::{IoBackend, IoObserver, IoReadMode, SegmentReader},
        segment::{
            data_store::DiskSegmentStore, event_wal::EventWal,
            lifecycle::SegmentLifecycleCoordinator,
        },
    };

    const KIB: usize = 1024;

    /// A reader that never reads chunks (the drain tests assert file
    /// placement, not chunk reads); `purge_cache` is a no-op.
    struct NoopReader;

    #[async_trait::async_trait]
    impl SegmentReader for NoopReader {
        async fn read_chunk(
            &self,
            _segment_id: &SegmentId,
            _offset: u64,
            _length: u32,
        ) -> std::result::Result<Bytes, String> {
            Err("the drain test env never reads chunks through the injected reader".into())
        }
    }

    /// The drain env (default 2 data pools): registry + lifecycle
    /// registry + coordinator over a real event-WAL + the unified store,
    /// all built over sibling roots under one tempdir.
    struct DrainEnv {
        registry: Arc<PoolRegistry>,
        lifecycle_registry: Arc<SegmentLifecycleRegistry>,
        lifecycle: Arc<SegmentLifecycleCoordinator>,
        store: Arc<DiskSegmentStore>,
        roots: Vec<PathBuf>,
    }

    fn pool_config(
        name: &str,
        role: PoolRole,
        root: &std::path::Path,
    ) -> oceanfs_core::StoragePoolConfig {
        oceanfs_core::StoragePoolConfig {
            name: name.to_string(),
            role,
            root: root.to_path_buf(),
            weight: Some(1),
            tech: oceanfs_core::PoolTech::Auto,
            health: Default::default(),
        }
    }

    async fn build_env(tmp: &TempDir) -> DrainEnv {
        build_env_with_data_pools(tmp, 2).await
    }

    /// Builds the drain env with `data_pools` sibling data roots (the
    /// multi-drain serialization test needs a third healthy sibling to
    /// accept both sources' segments).
    async fn build_env_with_data_pools(tmp: &TempDir, data_pools: usize) -> DrainEnv {
        let roots: Vec<PathBuf> =
            (0..data_pools).map(|i| tmp.path().join(format!("pool-data-{i}"))).collect();
        let mut pools = Vec::new();
        for (i, root) in roots.iter().enumerate() {
            pools.push(pool_config(&format!("data-{i}"), PoolRole::Data, root));
        }
        pools.push(pool_config("wal-0", PoolRole::Wal, &tmp.path().join("pool-wal")));
        pools.push(pool_config("meta-0", PoolRole::Metadata, &tmp.path().join("pool-meta")));
        pools.push(pool_config("hints-0", PoolRole::Hints, &tmp.path().join("pool-hints")));
        let storage =
            StorageConfig { pools, missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal };
        let registry =
            Arc::new(PoolRegistry::from_config(&storage, &tmp.path().join("meta")).unwrap());
        let lifecycle_registry =
            Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let event_wal_config = oceanfs_core::EventWalConfig {
            event_wal_dir: tmp.path().join("event-wal"),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024 * 1024,
        };
        let event_wal = Arc::new(
            EventWal::open(tmp.path().join("event-wal"), &event_wal_config).await.unwrap(),
        );
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&lifecycle_registry))
                .with_event_wal(event_wal),
        );
        let observer = Arc::new(IoObserver::new());
        for pool_id in 0..data_pools as u32 {
            observer.register_pool(pool_id, None);
        }
        let reader: Arc<dyn SegmentReader> = Arc::new(NoopReader);
        let store = Arc::new(DiskSegmentStore::new(
            Arc::clone(&registry),
            Arc::clone(&lifecycle_registry),
            reader,
            IoReadMode::Buffered,
            Arc::new(IoBackend::default()),
            observer,
        ));
        DrainEnv { registry, lifecycle_registry, lifecycle, store, roots }
    }

    /// Seeds a sealed segment whose `.dat` lives on `pool_id`'s root and
    /// holds `size` deterministic bytes.
    async fn seed_on(env: &DrainEnv, pool_id: u32, size: usize) -> SegmentId {
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let id = SegmentId::new();
        env.lifecycle.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        let merkle_root = oceanfs_core::HashOutput::from_bytes(*blake3::hash(&data).as_bytes());
        let meta = oceanfs_core::SegmentMetadata {
            pool_id,
            total_bytes: size as u64,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1_700_000_000_000),
        };
        env.lifecycle.request_seal(id, meta, None).await.unwrap();
        env.store.write_segment_data(&id, &data).await.unwrap();
        assert!(env.roots[pool_id as usize].join(format!("{id}.dat")).exists());
        id
    }

    fn worker(env: &DrainEnv, budget: u64) -> IntraNodeDrain {
        IntraNodeDrain::new(
            IntraNodeDrainConfig { max_bytes_per_tick: budget },
            Arc::clone(&env.registry),
            Arc::clone(&env.lifecycle_registry),
            SegmentRelocator::new(Arc::clone(&env.lifecycle), Arc::clone(&env.store)),
        )
    }

    fn set_free(env: &DrainEnv, pool_id: u32, free: u64) {
        env.registry.set_pool_capacity(pool_id, free, free);
    }

    fn dat_on(env: &DrainEnv, pool_id: u32, id: SegmentId) -> bool {
        env.roots[pool_id as usize].join(format!("{id}.dat")).exists()
    }

    #[tokio::test]
    async fn drains_sealed_segments_to_sibling_and_empties_pool() {
        let tmp = TempDir::new().unwrap();
        let env = build_env(&tmp).await;
        let a = seed_on(&env, 0, 4 * KIB).await;
        let b = seed_on(&env, 0, 8 * KIB).await;
        env.registry.begin_drain(0).unwrap();

        let stats = worker(&env, 1 << 30).run_cycle().await;
        assert_eq!(stats.segments_moved, 2);
        assert_eq!(stats.bytes_moved, 12 * KIB as u64);
        assert_eq!(stats.emptied, vec![0]);
        assert!(stats.blocked.is_empty());

        // All .dat moved to the sibling; the source root is empty.
        assert!(!dat_on(&env, 0, a) && !dat_on(&env, 0, b));
        assert!(dat_on(&env, 1, a) && dat_on(&env, 1, b));
        assert_eq!(env.registry.drain_state(0), DrainState::Detachable);
        // Status stays Draining so placement never refills it.
        assert_eq!(env.registry.pool_by_id(0).unwrap().status(), PoolStatus::Draining);
    }

    #[tokio::test]
    async fn tick_stops_at_byte_budget_and_resumes_next_cycle() {
        let tmp = TempDir::new().unwrap();
        let env = build_env(&tmp).await;
        let size = 256 * KIB;
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(seed_on(&env, 0, size).await);
        }
        env.registry.begin_drain(0).unwrap();
        let drain = worker(&env, (2 * size) as u64);
        let first = drain.run_cycle().await;
        assert_eq!(first.segments_moved, 2, "budget allows exactly two segments");
        assert_eq!(first.bytes_moved, (2 * size) as u64);
        assert!(first.emptied.is_empty(), "one segment remains");
        assert!(dat_on(&env, 0, ids[2]), "the third segment waits for the next tick");
        assert!(dat_on(&env, 1, ids[0]) && dat_on(&env, 1, ids[1]));

        let second = drain.run_cycle().await;
        assert_eq!(second.segments_moved, 1);
        assert_eq!(second.emptied, vec![0]);
        assert!(!dat_on(&env, 0, ids[2]));
        assert!(dat_on(&env, 1, ids[2]));
    }

    #[tokio::test]
    async fn parks_when_no_sibling_headroom_and_deletes_nothing() {
        let tmp = TempDir::new().unwrap();
        let env = build_env(&tmp).await;
        let id = seed_on(&env, 0, 2 * 1024 * KIB).await;
        // The only sibling cannot fit the segment (1 MiB free < 2 MiB +
        // 64 MiB headroom).
        set_free(&env, 1, 1024 * 1024);
        env.registry.begin_drain(0).unwrap();

        let stats = worker(&env, 1 << 30).run_cycle().await;
        assert_eq!(stats.segments_moved, 0);
        assert_eq!(stats.blocked.len(), 1);
        assert_eq!(stats.blocked[0].0, 0);
        assert!(
            stats.blocked[0].1.contains("no sibling data pool has capacity"),
            "reason surfaced: {}",
            stats.blocked[0].1
        );
        // Nothing deleted, nothing half-removed: the pool stays Draining
        // with the source .dat intact.
        assert!(dat_on(&env, 0, id));
        assert!(!dat_on(&env, 1, id));
        let state = env.registry.drain_state(0);
        assert!(state.is_blocked());
        assert_eq!(env.registry.pool_by_id(0).unwrap().status(), PoolStatus::Draining);
    }

    #[tokio::test]
    async fn paused_pool_cycle_is_noop_and_resume_continues() {
        let tmp = TempDir::new().unwrap();
        let env = build_env(&tmp).await;
        let id = seed_on(&env, 0, 4 * KIB).await;
        env.registry.begin_drain(0).unwrap();
        env.registry.set_drain_paused(0, true).unwrap();

        // Paused: the cycle is a no-op for this pool.
        let stats = worker(&env, 1 << 30).run_cycle().await;
        assert_eq!(stats.segments_moved, 0);
        assert!(dat_on(&env, 0, id), "paused drain moves nothing");

        // Resume: the next cycle drains to Detachable.
        env.registry.set_drain_paused(0, false).unwrap();
        let stats = worker(&env, 1 << 30).run_cycle().await;
        assert_eq!(stats.segments_moved, 1);
        assert_eq!(stats.emptied, vec![0]);
        assert!(dat_on(&env, 1, id));
    }

    #[tokio::test]
    async fn two_draining_pools_serialize_under_the_global_budget() {
        let tmp = TempDir::new().unwrap();
        // Three data pools: pool 0 and pool 1 drain simultaneously into
        // the healthy sibling (pool 2); the global budget serializes the
        // two sources across cycles.
        let env = build_env_with_data_pools(&tmp, 3).await;
        let size = 256 * KIB;
        let a = seed_on(&env, 0, size).await;
        let b = seed_on(&env, 1, size).await;
        env.registry.begin_drain(0).unwrap();
        env.registry.begin_drain(1).unwrap();

        // Budget for exactly one segment: pool 0 (lower id) drains first,
        // pool 1 waits for the next cycle.
        let drain = worker(&env, size as u64);
        let first = drain.run_cycle().await;
        assert_eq!(first.segments_moved, 1);
        assert_eq!(first.emptied, vec![0], "pool 0 emptied first");
        assert!(!dat_on(&env, 0, a) && dat_on(&env, 2, a), "a moved to the healthy sibling");
        assert!(dat_on(&env, 1, b), "pool 1 untouched by the first cycle");

        let second = drain.run_cycle().await;
        assert_eq!(second.segments_moved, 1);
        assert_eq!(second.emptied, vec![1], "pool 1 emptied on the second cycle");
        assert!(!dat_on(&env, 1, b) && dat_on(&env, 2, b), "b moved to the healthy sibling");
    }

    #[tokio::test]
    async fn reserved_entry_keeps_pool_non_empty() {
        let tmp = TempDir::new().unwrap();
        let env = build_env(&tmp).await;
        // A reserved (not yet sealed) entry — an in-flight seal that must
        // still land (d1 decision 4). A Reserved entry's metadata carries
        // the default pool_id 0 until seal chooses the pool, so while the
        // drain source is pool 0 it is a live entry the worker must not
        // declare empty over.
        let id = SegmentId::new();
        env.lifecycle.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        env.registry.begin_drain(0).unwrap();

        let stats = worker(&env, 1 << 30).run_cycle().await;
        assert!(stats.emptied.is_empty(), "a Reserved entry keeps the pool non-empty");
        assert!(stats.blocked.is_empty(), "no relocation is attempted for Reserved entries");
        assert_eq!(stats.segments_moved, 0);
        // Not Detachable yet — the seal can still land on this pool.
        assert_eq!(
            env.registry.drain_state(0),
            DrainState::Draining { blocked_reason: None, paused: false }
        );
    }

    #[tokio::test]
    async fn ghost_sealed_entry_parks_with_reason_never_silently_detaches() {
        let tmp = TempDir::new().unwrap();
        let env = build_env(&tmp).await;
        // A Sealed entry with no .dat on disk — the registry says we hold
        // it but the file is gone. Draining must park with the reason
        // surfaced (no-destructive-failure: never detach over a ghost).
        let id = SegmentId::new();
        env.lifecycle.request_reserve(id, SizeTier::Standard, 4, 2).await.unwrap();
        env.lifecycle
            .request_seal(
                id,
                oceanfs_core::SegmentMetadata {
                    pool_id: 0,
                    total_bytes: 0,
                    segment_id: id,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: SizeTier::Standard,
                    merkle_root: Some(oceanfs_core::HashOutput::from_bytes([7; 32])),
                    storage_locations: smallvec::smallvec![],
                    sealed_at: Some(1_700_000_000_000),
                },
                None,
            )
            .await
            .unwrap();
        env.registry.begin_drain(0).unwrap();

        let stats = worker(&env, 1 << 30).run_cycle().await;
        assert!(stats.emptied.is_empty(), "a live entry keeps the pool non-empty");
        assert_eq!(stats.blocked.len(), 1);
        assert!(stats.blocked[0].1.contains("failed"), "reason surfaced: {}", stats.blocked[0].1);
        assert!(env.registry.drain_state(0).is_blocked());
        assert_eq!(env.registry.pool_by_id(0).unwrap().status(), PoolStatus::Draining);
    }

    #[tokio::test]
    async fn dead_source_pool_is_skipped() {
        let tmp = TempDir::new().unwrap();
        let env = build_env(&tmp).await;
        let id = seed_on(&env, 0, 4 * KIB).await;
        env.registry.begin_drain(0).unwrap();
        // Genuine confirmed loss beats the operator flag (d1).
        env.registry.set_status(0, PoolStatus::Dead);

        let stats = worker(&env, 1 << 30).run_cycle().await;
        assert_eq!(stats.segments_moved, 0, "a dead pool is never a drain source");
        assert!(stats.emptied.is_empty());
        assert!(dat_on(&env, 0, id), "a dead pool's data is untouched");
    }

    #[tokio::test]
    async fn cluster_mode_pool_is_not_this_workers_source() {
        let tmp = TempDir::new().unwrap();
        let env = build_env(&tmp).await;
        let id = seed_on(&env, 0, 4 * KIB).await;
        // The d4 cluster drain owns this pool (DrainMode::Cluster); the d3
        // intra-node worker must not chase it.
        env.registry.begin_drain_with_mode(0, crate::DrainMode::Cluster).unwrap();

        let stats = worker(&env, 1 << 30).run_cycle().await;
        assert_eq!(stats.segments_moved, 0, "cluster-owned pools are never intra-node sources");
        assert!(stats.emptied.is_empty());
        assert!(stats.blocked.is_empty(), "skipped, not blocked");
        assert!(dat_on(&env, 0, id));
        assert_eq!(
            env.registry.drain_state(0),
            DrainState::Draining { blocked_reason: None, paused: false }
        );
    }
}
