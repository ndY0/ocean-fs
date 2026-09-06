//! Health-consequence wiring (g2 `failure-state-machine`, ADR-0029 §D3).
//!
//! The [`HealthMonitor`](oceanfs_storage::pool::health::HealthMonitor)
//! (storage) drives status transitions on the registry and emits bounded
//! [`HealthEvent`]s. This module is the
//! node layer's half of the D3 role-consequence matrix:
//!
//! - **metadata** Dead → the node serves nothing (surfaced lazily via
//!   `PoolRegistry::node_serves_requests`; the S3/read + write gates
//!   reject 503 in g6);
//! - **data** Dead → the affected segment set is derived from the
//!   lifecycle registry (`pool_id == dead_pool`, Phase A f5) — handed to
//!   g3 for the loss announcement;
//! - **hints** Dead → the hint enqueue path (server write coordinator)
//!   rejects new debt (reconciliation rebuilds lost debt);
//! - **wal** Dead → `write_degraded` (the monitor drives this directly
//!   on the registry).
//!
//! Every status change also re-declares the node's [`NodeManifest`] so
//! peers observe the new pool state (f6: the manifest version bump
//! propagates via gossip; f7's routing cache updates through the
//! membership event subscriber).

use std::sync::Arc;

use oceanfs_core::{NodeId, PoolRole, SegmentId};
use oceanfs_membership::{manifest::NodeManifest, Membership};
use oceanfs_storage::{
    pool::{health::HealthEvent, PoolRegistry},
    PoolStatus, SegmentLifecycleRegistry,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{pool_manifest::build_node_manifest, routing_cache::ManifestCache};

/// Derives the segment set owned by a dead pool — every live lifecycle
/// entry whose durable `pool_id` matches (Phase A f5 mapping).
///
/// This is the "range set" that g3's loss announcement must carry
/// (ADR-0029 §D4 correction note: peers cannot derive a node's local
/// segment→pool mapping, so the exact set rides the announcement).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{LifecycleConfig, SegmentId, SegmentMetadata, SizeTier};
/// use oceanfs_storage::SegmentLifecycleRegistry;
/// use oceanfs_node::health::derive_affected_segments;
///
/// let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
/// let id = SegmentId::new();
/// registry
///     .reserve(
///         id,
///         SegmentMetadata {
///             pool_id: 3,
///             total_bytes: 0,
///             segment_id: id,
///             ec_k: 0,
///             ec_m: 0,
///             size_tier: SizeTier::Standard,
///             merkle_root: None,
///             storage_locations: smallvec::SmallVec::new(),
///             sealed_at: None,
///         },
///     )
///     .expect("reserve");
/// let affected = derive_affected_segments(&registry, 3);
/// assert_eq!(affected, vec![id]);
/// ```
pub fn derive_affected_segments(
    lifecycle: &SegmentLifecycleRegistry,
    pool_id: u32,
) -> Vec<SegmentId> {
    let mut affected = Vec::new();
    lifecycle.for_each(|segment_id, entry| {
        if entry.metadata.pool_id == pool_id {
            affected.push(segment_id);
        }
    });
    affected
}

// [review][architecture][critical]
// this functionnality could benefit from a reactor implementation.
// rather than spawning a worker just to keep track of the health consequences, this could react to the events and keep the state within a more generic reactor.
// as a matter of fact, losts of handlers in the project could. their state could be ram, rocksdb or disk backed, does not matter, we would have a cleaner way of updating
// a state. worker pattern would remain for the real workers, handling workload.
// [end]
/// The g3 loss-announcement fan-out closure: `(pool_id, affected
/// segments)` → announce to the affected segments' replica holders
/// (ADR-0029 §D4 fast path).
pub type LossAnnouncer = Arc<dyn Fn(u32, Vec<SegmentId>) + Send + Sync>;

/// Spawns the node's health-consequence applier task.
///
/// Consumes the monitor's status events and applies the D3 role matrix
/// (metadata Dead → the node serves nothing — surfaced to readers via
/// `PoolRegistry::node_serves_requests`, data Dead → affected-segment
/// derivation), then re-declares the node manifest so peers see the
/// change. Returns the join handle.
///
/// The task exits on `shutdown` cancellation (the monitor's event
/// sender is retained by the storage module until the node drops, so
/// channel close alone cannot stop the applier during shutdown).
///
/// `loss_announcer` (g3 `loss-announcement`, ADR-0029 §D4 fast path):
/// when a **data** pool is confirmed Dead, the applier derives the
/// affected segment set and hands it to the closure, which fans the
/// announcement out to `union(storage_locations − self)` over the set
/// (bounded retries; the g4 reconciliation loop is the failsafe). `None`
/// (tests) skips the announcement.
#[allow(clippy::too_many_arguments)]
pub fn spawn_health_consequences(
    events: mpsc::Receiver<HealthEvent>,
    registry: Arc<PoolRegistry>,
    membership: Arc<Membership>,
    lifecycle: Arc<SegmentLifecycleRegistry>,
    self_id: NodeId,
    boot_incarnation: u64,
    manifest_cache: Arc<ManifestCache>,
    loss_announcer: Option<LossAnnouncer>,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut events = events;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::debug!("health consequence applier cancelled");
                    break;
                }
                event = events.recv() => {
                    let Some(event) = event else { break };
                    let HealthEvent::StatusChanged { pool_id, status } = event else {
                        // Non-exhaustive: unknown future events are ignored.
                        continue;
                    };
                    let Some(pool) = registry.pool_by_id(pool_id) else { continue };
                    match pool.role() {
                        // The metadata-Dead consequence (node serves nothing) is
                        // derived lazily from the REGISTRY by the read/write
                        // gates (`PoolRegistry::node_serves_requests`) — the
                        // monitor already set the pool Dead before this event, so
                        // there is no separate flag to maintain (one source of
                        // truth). Only the manifest re-declaration below matters.
                        PoolRole::Metadata => {
                            tracing::info!(pool_id, "metadata pool status change observed");
                        }
                        PoolRole::Data if status == PoolStatus::Dead => {
                            let affected = derive_affected_segments(&lifecycle, pool_id);
                            tracing::warn!(
                                pool_id,
                                affected_segments = affected.len(),
                                "data pool Dead: affected segments derived"
                            );
                            if let Some(announcer) = &loss_announcer {
                                announcer(pool_id, affected);
                            }
                        }
                        // wal (write_degraded is driven by the monitor) and
                        // hints (rejection is checked at enqueue time) need no
                        // node-level consequence here.
                        _ => {}
                    }
                    // Re-declare the manifest: peers must see the new status
                    // (and write_degraded) immediately — the f6 version bump
                    // propagates it via gossip.
                    let incarnation = membership
                        .incarnation_of(&self_id)
                        .map(|inc| inc.value())
                        .unwrap_or(boot_incarnation);
                    let manifest: Arc<NodeManifest> =
                        Arc::new(build_node_manifest(incarnation, &registry));
                    membership.set_self_manifest((*manifest).clone());
                    manifest_cache.update(self_id.clone(), manifest);
                }
            }
        }
    })
}
