//! Peer-side cached routing state (ADR-0029 §D5) — a hint, never a
//! dependency.
//!
//! The read path (replica selection for a GET) and the write path
//! (replica target selection for a PUT) consult the node's cached
//! storage-pool manifests to avoid routing to nodes that cannot serve
//! the operation: zero Healthy data pools for reads, `write_degraded`
//! or zero Healthy data pools for writes.
//!
//! The trait is defined in the CONSUMING crate (`oceanfs-server`),
//! per architecture §2.1: the implementation lives in the composition
//! root's `ManifestCache` (`oceanfs-node::routing_cache`), which owns
//! the manifest data, the exclusion policy, and the routing metrics.
//! The coordinators hold `Option<Arc<dyn RoutingHint>>` — `None`
//! disables the hint entirely (Phase-A neutral: every manifest is
//! Healthy, so the filters never exclude, but the structure, the
//! error fallback, and the metrics are in place for Phase B's status
//! transitions).
//!
//! Failover semantics: the cache optimizes; the error path guarantees.
//! An I/O error on a candidate replica falls through to the next
//! replica regardless of what the cache said — `on_failover` only
//! records the event.

use oceanfs_core::NodeId;

/// The routing-hint source consulted by the read/write coordinators.
///
/// Implementations must be cheap and lock-free on the hot path (perf
/// 2.4: `ArcSwap`-backed, wholesale-replaced map — no lock in the
/// read/write path, perf 7.2).
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_core::NodeId;
/// use oceanfs_server::routing_hint::RoutingHint;
///
/// /// A test hint: never excludes, counts failovers.
/// struct NoopHint(std::sync::atomic::AtomicU64);
///
/// impl RoutingHint for NoopHint {
///     fn exclude_read_candidate(&self, _node_id: &NodeId) -> bool { false }
///     fn exclude_write_target(&self, _node_id: &NodeId) -> bool { false }
///     fn on_failover(&self) {
///         self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
///     }
/// }
///
/// let hint: Arc<dyn RoutingHint> = Arc::new(NoopHint(Default::default()));
/// assert!(!hint.exclude_read_candidate(&NodeId::new("peer")));
/// ```
pub trait RoutingHint: Send + Sync {
    /// Whether `node_id` must be excluded as a read candidate: its
    /// cached manifest reports zero Healthy data pools (the node cannot
    /// serve segment reads).
    ///
    /// Returns `false` when the cache has no entry (unknown peer = no
    /// pool info): the caller proceeds and relies on the error-driven
    /// fallback to the next replica — the cache is a hint, the error is
    /// the truth.
    fn exclude_read_candidate(&self, node_id: &NodeId) -> bool;

    /// Whether `node_id` must be excluded as a write target: its cached
    /// manifest reports `write_degraded` (role consequence, ADR-0029
    /// §D3) or zero Healthy data pools.
    ///
    /// Returns `false` when the cache has no entry (unknown peer stays
    /// eligible; write failures become hinted-handoff debt).
    fn exclude_write_target(&self, node_id: &NodeId) -> bool;

    /// Records an error-driven failover: a candidate replica failed at
    /// I/O time (timeout, connection error, disk error) and the caller
    /// is falling through to the next replica. Backs the
    /// `oceanfs_routing_failover_total` metric.
    fn on_failover(&self);
}
