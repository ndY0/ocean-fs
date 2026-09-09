//! Cluster drain controller (d4, ADR-0036 C1b).
//!
//! The **off-node mover**: a paced, pausable, terminal controller that
//! empties a `Draining` pool (or a node's whole footprint) by moving
//! copies to **other nodes** through the ADR-0030 target-pull machinery,
//! then **source-releasing** the local copy.
//!
//! Ownership: a data pool whose drain record says [`DrainMode::Cluster`]
//! belongs to this controller; pools begun in [`DrainMode::IntraNode`]
//! belong to the d3 worker. The controller walks the lifecycle registry
//! (ADR-0034 — never a disk scan), and for every segment it still holds:
//!
//! - if dropping self leaves `>= replication_factor` live holders, it
//!   **source-releases directly** (no new copy is needed);
//! - otherwise it dispatches [`RepairDispatcher::dispatch_drain`] — a
//!   synchronous `RequestReReplication` (reason `Drain`) that returns only
//!   after the target confirms a **durable** copy (the target-side
//!   materialization gate) — converges the target into its holder view,
//!   then source-releases.
//!
//! Source-release = a durable refresh of the segment's `storage_locations`
//! to `holders − self` (the payload is a set replacement; the coordinator
//! performs no holder-set validation) followed by the local `.dat` unlink.
//! The lifecycle entry stays `Sealed` with `holders − self`, so the g4
//! reconciler's live-count (read from this node's registry) stops counting
//! self immediately and never re-adds it.
//!
//! ## Interim ring-share behavior (recorded, no C2a)
//!
//! Freed capacity from a drained node is reclaimed through new writes and
//! repair-target selection only: this epic does not re-weight ring
//! ownership (C2a / d6 is optional and late). Until `leave(None)`, the
//! ring still maps ranges to the draining node; the node keeps serving
//! reads of still-held segments and its peers route writes elsewhere
//! (its manifest shows no healthy data pool). Operators drain off-peak,
//! before `leave`.
//!
//! ## No-destructive-failure (ADR-0036 D6)
//!
//! A segment that needs a copy but finds no eligible target anywhere
//! parks its source pool with a surfaced blocked reason; nothing is
//! deleted and nothing half-removed. The drain resumes when capacity
//! appears.

use std::sync::Arc;

use oceanfs_core::{NodeId, NodeState, SegmentId, SizeTier};
use oceanfs_durability::healing_service::{ReRepRequest, RepairReason};
use oceanfs_membership::Membership;
use oceanfs_storage::{
    segment::lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry, SegmentState},
    DrainMode, PoolRegistry, PoolStatus,
};
use oceanfs_storage_api::SegmentDataStore;
use tracing::{debug, warn};

use crate::repair::{DrainDispatchError, RepairDispatcher};

/// Per-tick pacing knob for the cluster drain controller (ADR-0036 D5 —
/// its own configurable byte budget; no shared budget framework).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainClusterConfig {
    /// Bytes of off-node re-replication dispatched per cycle across all
    /// cluster-draining pools (counted on release; a single segment larger
    /// than the budget overshoots one tick).
    pub max_bytes_per_tick: u64,
}

impl Default for DrainClusterConfig {
    fn default() -> Self {
        Self { max_bytes_per_tick: 64 * 1024 * 1024 }
    }
}

/// One cluster-drain cycle's outcome, aggregated over every
/// cluster-mode draining pool.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainCycleStats {
    /// Segments dispatched to an off-node target this cycle (each will be
    /// released once its copy is durable).
    pub dispatched: u64,
    /// Segments source-released this cycle (local copy unlinked, self
    /// dropped from `storage_locations`).
    pub released: u64,
    /// Logical bytes released this cycle (`SegmentMetadata.total_bytes`).
    pub bytes_released: u64,
    /// Pools that transitioned to `Detachable` this cycle (drained empty).
    pub emptied: Vec<u32>,
    /// Pools parked with a blocked reason this cycle (no eligible target
    /// for a segment that needed one — nothing deleted).
    pub blocked: Vec<(u32, String)>,
}

/// A sealed segment the node still holds on a cluster-draining source
/// pool.
#[derive(Clone)]
struct HeldSegment {
    segment_id: SegmentId,
    total_bytes: u64,
    merkle_root: Option<oceanfs_core::HashOutput>,
    tier: SizeTier,
    ec_k: u8,
    ec_m: u8,
    holders: Vec<NodeId>,
}

/// The cluster drain controller (d4, ADR-0036 C1b) — one per node,
/// registered as the `"drain_cluster"` Tier-1 `DurabilityTask`.
///
/// # Examples
///
/// ```ignore
/// // Requires a fully wired node (registry + lifecycle registry +
/// // coordinator + store + repair dispatcher + membership) — see the
/// // durability-module wiring and the 3-node integration test.
/// let controller = DrainClusterController::new(config, self_id, membership,
///     repair_dispatcher, registry, lifecycle_registry, lifecycle, data_store,
///     replication_factor, interval);
/// let stats = controller.run_drain_cycle().await?;
/// ```
pub struct DrainClusterController {
    config: DrainClusterConfig,
    self_id: NodeId,
    membership: Arc<Membership>,
    repair_dispatcher: Arc<RepairDispatcher>,
    registry: Arc<PoolRegistry>,
    lifecycle_registry: Arc<SegmentLifecycleRegistry>,
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    data_store: Arc<dyn SegmentDataStore>,
    replication_factor: u32,
    interval: std::time::Duration,
}

impl DrainClusterController {
    /// Creates the cluster drain controller.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: DrainClusterConfig,
        self_id: NodeId,
        membership: Arc<Membership>,
        repair_dispatcher: Arc<RepairDispatcher>,
        registry: Arc<PoolRegistry>,
        lifecycle_registry: Arc<SegmentLifecycleRegistry>,
        lifecycle: Arc<SegmentLifecycleCoordinator>,
        data_store: Arc<dyn SegmentDataStore>,
        replication_factor: u32,
        interval: std::time::Duration,
    ) -> Self {
        Self {
            config,
            self_id,
            membership,
            repair_dispatcher,
            registry,
            lifecycle_registry,
            lifecycle,
            data_store,
            replication_factor,
            interval,
        }
    }

    /// Runs one drain cycle: every cluster-mode draining pool is drained
    /// in pool-id order until `max_bytes_per_tick` is consumed.
    ///
    /// Returns the aggregate [`DrainCycleStats`]. A cycle with no
    /// cluster-mode draining pool is a cheap no-op.
    pub async fn run_drain_cycle(&self) -> DrainCycleStats {
        let mut stats = DrainCycleStats::default();
        let mut sources = self.cluster_draining_sources();
        sources.sort_unstable();

        for source in sources {
            // Release-then-dispatch is serialized per segment; a full cycle
            // budget is checked before every new segment.
            loop {
                if stats.bytes_released >= self.config.max_bytes_per_tick {
                    return stats;
                }
                let (held, has_reserved) = self.held_segments(source);
                // Drain-progress gauge (d4's `oceanfs_drain_*` close): the
                // number of held segments left on the source pool.
                self.registry.set_drain_remaining(source, held.len() as u64);
                if held.is_empty() && !has_reserved {
                    // No segment we still hold on this pool (released
                    // entries keep a pool_id but drop self) → Detachable.
                    if self.registry.set_pool_empty(source).is_ok() {
                        stats.emptied.push(source);
                    }
                    break;
                }
                let Some(candidate) = self.next_releasable(&held, &stats) else {
                    // Every remaining candidate needs an off-node copy but
                    // the budget is spent → resume next cycle.
                    if stats.bytes_released >= self.config.max_bytes_per_tick {
                        return stats;
                    }
                    break;
                };

                if self.live_holders_excluding_self(&candidate) >= self.replication_factor as usize
                {
                    // Dropping self keeps RF satisfied — release without a
                    // new copy.
                    self.release(&candidate, source).await;
                    stats.released += 1;
                    stats.bytes_released =
                        stats.bytes_released.saturating_add(candidate.total_bytes);
                    self.registry.note_drain_released(source);
                    self.clear_blocked_if_any(source);
                } else {
                    // Need a new off-node copy first; the synchronous
                    // dispatch returns only after the target's copy is
                    // durable (or fails).
                    match self.dispatch_and_release(&candidate, source).await {
                        Ok(()) => {
                            stats.dispatched += 1;
                            stats.released += 1;
                            stats.bytes_released =
                                stats.bytes_released.saturating_add(candidate.total_bytes);
                            // Drain-throughput counters (d4's
                            // `oceanfs_drain_*` close).
                            self.registry.note_drain_dispatched(source);
                            self.registry.note_drain_released(source);
                            self.clear_blocked_if_any(source);
                        }
                        Err(DrainDispatchError::NoEligibleTarget) => {
                            let reason = format!(
                                "no eligible cluster target for segment {} \
                                 (needs a copy before release)",
                                candidate.segment_id
                            );
                            if self.registry.set_drain_blocked(source, Some(&reason)).is_ok() {
                                stats.blocked.push((source, reason));
                            }
                            break;
                        }
                        Err(err) => {
                            // Not-durable / RPC / timeout: the drain retries
                            // next cycle; nothing is deleted.
                            let reason = format!(
                                "dispatching segment {} failed: {err}",
                                candidate.segment_id
                            );
                            if self.registry.set_drain_blocked(source, Some(&reason)).is_ok() {
                                stats.blocked.push((source, reason));
                            }
                            break;
                        }
                    }
                }
            }
        }

        stats
    }

    /// Cluster-mode draining pools (status `Draining`, record `Draining`,
    /// not paused, mode `Cluster`).
    fn cluster_draining_sources(&self) -> Vec<u32> {
        self.registry
            .data_pools()
            .iter()
            .filter(|pool| {
                pool.status() == PoolStatus::Draining
                    && self.registry.drain_mode(pool.id()) == DrainMode::Cluster
                    && matches!(
                        self.registry.drain_state(pool.id()),
                        oceanfs_storage::DrainState::Draining { paused: false, .. }
                    )
            })
            .map(|pool| pool.id())
            .collect()
    }

    /// All sealed segments the node still holds on `pool_id` (self is in
    /// `storage_locations`), plus whether any `Reserved` in-flight entry
    /// targets the pool (keeps it non-empty until it seals).
    fn held_segments(&self, pool_id: u32) -> (Vec<HeldSegment>, bool) {
        let mut held = Vec::new();
        let mut has_reserved = false;
        self.lifecycle_registry.for_each(|id, entry| {
            if entry.metadata.pool_id != pool_id {
                return;
            }
            match entry.state {
                SegmentState::Reserved => {
                    has_reserved = true;
                }
                SegmentState::Sealed => {
                    if entry.metadata.storage_locations.iter().any(|loc| loc == &self.self_id) {
                        let meta = &entry.metadata;
                        held.push(HeldSegment {
                            segment_id: id,
                            total_bytes: meta.total_bytes,
                            merkle_root: meta.merkle_root,
                            tier: meta.size_tier,
                            ec_k: meta.ec_k,
                            ec_m: meta.ec_m,
                            holders: meta.storage_locations.to_vec(),
                        });
                    }
                }
                SegmentState::Deleted => {}
                _ => {}
            }
        });
        (held, has_reserved)
    }

    /// Picks the largest remaining held segment to process this tick.
    ///
    /// Largest-first keeps progress visible (and matches d3's ordering
    /// rationale). The byte budget is a *post-process* cap: a segment
    /// larger than the remaining budget is still taken when the tick has
    /// not yet moved anything (one oversized segment overshoots the tick,
    /// exactly like the intra-node worker); once bytes have been moved the
    /// budget stops the cycle and the rest waits for the next tick.
    fn next_releasable(
        &self,
        held: &[HeldSegment],
        stats: &DrainCycleStats,
    ) -> Option<HeldSegment> {
        let mut best: Option<&HeldSegment> = None;
        for candidate in held {
            let projected = stats.bytes_released.saturating_add(candidate.total_bytes);
            if stats.bytes_released > 0 && projected > self.config.max_bytes_per_tick {
                continue;
            }
            let replace = match best {
                None => true,
                Some(current) => candidate.total_bytes > current.total_bytes,
            };
            if replace {
                best = Some(candidate);
            }
        }
        best.cloned()
    }

    /// Live holder count excluding self, from the membership view (node
    /// Alive/Suspect and not data-dead — same filter the dispatcher's
    /// live-holder set applies).
    fn live_holders_excluding_self(&self, segment: &HeldSegment) -> usize {
        segment
            .holders
            .iter()
            .filter(|node| {
                **node != self.self_id
                    && matches!(
                        self.membership.state_of(node),
                        Some(NodeState::Alive | NodeState::Suspect)
                    )
                    && !self.is_data_dead(node)
            })
            .count()
    }

    /// Whether the node's manifest reports it data-dead (has data pools
    /// and every one is `dead`) — it cannot serve a fetch.
    fn is_data_dead(&self, node: &NodeId) -> bool {
        let Some(manifest) = self.membership.manifest_of(node) else {
            return false;
        };
        let data_pools: Vec<_> = manifest.pools().iter().filter(|p| p.role() == "data").collect();
        !data_pools.is_empty() && data_pools.iter().all(|p| p.status() == "dead")
    }

    /// Dispatches an off-node copy (when needed) and source-releases the
    /// local copy.
    ///
    /// `dispatch_drain` already waits for the target's durable stamp and
    /// converges the target into this node's holder view; release then
    /// refreshes `storage_locations` to `holders − self` and unlinks the
    /// local `.dat`.
    async fn dispatch_and_release(
        &self,
        candidate: &HeldSegment,
        source_pool: u32,
    ) -> Result<(), DrainDispatchError> {
        let request = ReRepRequest {
            origin: self.self_id.clone(),
            segment_id: candidate.segment_id,
            holders: candidate.holders.clone(),
            reason: RepairReason::Drain,
            retry_count: 0,
            merkle_root: candidate.merkle_root,
            tier: candidate.tier,
            ec_k: candidate.ec_k,
            ec_m: candidate.ec_m,
        };
        self.repair_dispatcher.dispatch_drain(&request).await?;
        self.release(candidate, source_pool).await;
        Ok(())
    }

    /// Clears a stale blocked reason after a successful release (an
    /// eligible target / capacity appeared).
    fn clear_blocked_if_any(&self, pool_id: u32) {
        if self.registry.drain_state(pool_id).is_blocked() {
            let _ = self.registry.set_drain_blocked(pool_id, None);
        }
    }

    /// Source-release: durable refresh to the CURRENT holder set minus
    /// self, then unlink the local `.dat` from the source pool root.
    ///
    /// The current set is read from the live registry entry at release
    /// time so a just-converged target (added by `dispatch_drain`'s
    /// converge step) is kept in the released view — releasing from a
    /// stale pre-dispatch snapshot would drop the new holder and force an
    /// extra g4 repair round. Falls back to the candidate snapshot only if
    /// the entry vanished (deleted concurrently).
    ///
    /// Crash windows: refresh-durable/unlink-pending leaves the `.dat` as
    /// an unregistered file → boot-reapable residue (d2 rule);
    /// dispatch-in-flight is re-run idempotently by the next cycle.
    async fn release(&self, candidate: &HeldSegment, source_pool: u32) {
        let current_holders: Vec<NodeId> = self
            .lifecycle_registry
            .get(candidate.segment_id)
            .map(|entry| entry.metadata.storage_locations.to_vec())
            .unwrap_or_else(|| candidate.holders.clone());
        let mut locations: smallvec::SmallVec<[NodeId; 16]> =
            current_holders.iter().filter(|node| **node != self.self_id).cloned().collect();
        locations.sort();
        locations.dedup();
        if let Err(e) = self
            .lifecycle
            .request_refresh_metadata(
                candidate.segment_id,
                candidate.merkle_root,
                Some(locations),
                None, // no pool_id change on source-release
            )
            .await
        {
            warn!(
                segment_id = %candidate.segment_id,
                error = ?e,
                "cluster drain: source-release refresh failed; the entry still counts self \
                 and the next cycle re-dispatches"
            );
            return;
        }
        debug!(
            segment_id = %candidate.segment_id,
            pool = source_pool,
            "cluster drain: source released (self dropped from storage_locations)"
        );
        if let Err(e) =
            self.data_store.delete_shards_with_pool(&candidate.segment_id, source_pool).await
        {
            warn!(
                segment_id = %candidate.segment_id,
                error = %e,
                "cluster drain: source .dat unlink failed (boot residue sweep will reap it)"
            );
        }
    }
}

#[async_trait::async_trait]
impl oceanfs_durability::DurabilityTask for DrainClusterController {
    fn name(&self) -> &'static str {
        "drain_cluster"
    }

    fn interval(&self) -> std::time::Duration {
        self.interval
    }

    fn keyspace_fraction(&self) -> f64 {
        1.0
    }

    async fn run_cycle(
        &self,
        window: oceanfs_durability::KeyspaceWindow,
    ) -> oceanfs_durability::Result<u64> {
        match window {
            oceanfs_durability::KeyspaceWindow::Full => {
                let stats = self.run_drain_cycle().await;
                Ok(stats.released)
            }
            oceanfs_durability::KeyspaceWindow::Shard { index, total } => {
                Err(oceanfs_durability::Error::Internal(format!(
                    "{} is registered with keyspace_fraction = 1.0 (full pass) but received \
                     KeyspaceWindow::Shard {{ index: {index}, total: {total} }}",
                    self.name()
                )))
            }
        }
    }
}
