//! Cluster membership with SWIM failure detection and gossip.
//!
//! The [`Membership`] struct is the main entry point for membership
//! operations. It manages the local node's view of the cluster,
//! spawns background tasks for failure detection and gossip, and
//! emits state-change events via a broadcast channel.

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::{Counter, Gauge, GossipConfig, Histogram, Incarnation, NodeId, NodeState};
use oceanfs_network::ConnectionPool;
use oceanfs_routing::RingCache;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use self::state::MembershipState;
use crate::{failure_detector::DetectorCommand, gossip::GossipCommand};

mod accessors;
pub mod manager;
pub mod state;

/// An event emitted when a node's state changes.
///
/// Carries the node's incarnation and, when known, its address so that
/// consumers (the membership event handler) can apply the transition
/// without re-deriving them from local state — a re-admitted node is
/// absent from local state, so the event is the only carrier of the
/// fresh address (ADR-0022 Decision 2).
///
/// Attribution (ADR-0028 D3): `version` is the emitter's logical clock
/// for this node (bumped on every emission) and `origin` is the emitter
/// itself — the authority-class merge rules in `upsert_node` use them to
/// order facts without last-writer-wins heuristics.
#[derive(Debug, Clone)]
pub struct MembershipEvent {
    /// The node whose state changed.
    pub node_id: NodeId,
    /// Previous state.
    pub old_state: NodeState,
    /// New state.
    pub new_state: NodeState,
    /// The node's incarnation for this transition.
    pub incarnation: Incarnation,
    /// The node's address, when the emitter knows it.
    pub address: Option<SocketAddr>,
    /// The emitter's version for this node (per-(node, origin) clock).
    pub version: u64,
    /// The observer that emitted this event.
    pub origin: NodeId,
}

/// The authority-class table (ADR-0028 D3).
///
/// For an entry about `target` from `origin` as seen by `self_id`, at
/// the SAME incarnation, higher class wins regardless of version; within
/// the same class and origin, higher version wins.
///
/// | Class | Meaning |
/// |---|---|
/// | 4 | The leaver's own Left/Leaving claim (target == origin) |
/// | 3 | My own detector's observation / my own facts |
/// | 2 | Another member's detector facts (Suspect/Dead/recovery) |
/// | 1 | The target's own Alive announcement (replayable history) |
/// | 0 | Rejected: entries about SELF not originating from self |
///
/// Class 3 > 2 implements "ping-verified Alive beats remote Suspect"
/// and "DEAD is detector-local"; class 2 > 1 implements remote
/// suspicion; class 4 > 3 lets a graceful leave propagate over stale
/// detector facts.
pub(crate) fn authority_class(
    target: &NodeId,
    origin: &NodeId,
    state: NodeState,
    self_id: &NodeId,
) -> u8 {
    if target == self_id {
        // Self-liveness authority: only the node itself may describe
        // its own state. Any other origin is rejected outright.
        return if origin == self_id { 4 } else { 0 };
    }
    match state {
        NodeState::Leaving | NodeState::Left => {
            if origin == target {
                4
            } else if origin == self_id {
                3
            } else {
                2
            }
        }
        NodeState::Suspect | NodeState::Dead => {
            if origin == self_id {
                3
            } else if origin == target {
                // A target cannot legitimately suspect itself; treat a
                // replayed Suspect with the target's origin as its
                // announcement class (beatable by any detector fact).
                1
            } else {
                2
            }
        }
        NodeState::Alive => {
            if origin == self_id {
                3
            } else if origin == target {
                1
            } else {
                2
            }
        }
    }
}

/// Cluster membership tracker with SWIM failure detection and gossip.
///
/// Manages the lifecycle of cluster membership: join, leave, failure
/// detection, and state-change broadcasting. Background tasks run
/// the SWIM ping loop and gossip protocol.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use oceanfs_core::{GossipConfig, NodeId};
/// use oceanfs_membership::Membership;
/// use oceanfs_routing::{Ring, RingCache};
///
/// # async fn example() {
/// let config = GossipConfig::default();
/// let ring_cache = Arc::new(RingCache::new(Ring::new(Default::default())));
/// let membership = Membership::new(
///     NodeId::new("node-1"),
///     "127.0.0.1:9001".parse().unwrap(),
///     config,
///     ring_cache,
/// );
///
/// // Subscribe to membership changes.
/// let mut events = membership.subscribe();
///
/// // Trigger a join (async).
/// // membership.join().await.unwrap();
/// # }
/// ```
pub struct Membership {
    /// This node's identifier.
    pub(crate) node_id: NodeId,
    /// This node's gRPC address.
    pub(crate) address: SocketAddr,
    /// Gossip configuration.
    pub(crate) config: GossipConfig,
    /// Current membership state.
    pub(crate) state: RwLock<MembershipState>,
    /// Ring cache for topology updates on membership changes.
    pub(crate) ring: Arc<RingCache>,
    /// Broadcast channel for state-change events.
    pub(crate) event_tx: broadcast::Sender<MembershipEvent>,
    /// Sender for failure detector commands (set during start()).
    pub(crate) detector_tx: RwLock<Option<tokio::sync::mpsc::Sender<DetectorCommand>>>,
    /// Sender for gossip protocol commands (set during start()).
    pub(crate) gossip_tx: RwLock<Option<tokio::sync::mpsc::Sender<GossipCommand>>>,
    /// Connection pool for gRPC calls (join, gossip push).
    pub(crate) pool: RwLock<Option<Arc<ConnectionPool>>>,
    /// Whether the membership has been started.
    pub(crate) started: RwLock<bool>,
    /// Cancellation token for graceful shutdown of background tasks.
    pub(crate) shutdown: CancellationToken,
    /// Gossip messages sent (set during start).
    pub(crate) gossip_sent: RwLock<Option<Counter>>,
    /// Gossip messages received (set during start).
    pub(crate) gossip_received: RwLock<Option<Counter>>,
    /// Gossip messages dropped (set during start).
    pub(crate) gossip_dropped: RwLock<Option<Counter>>,
    /// Gossip round duration histogram (set during start).
    pub(crate) gossip_round_duration: RwLock<Option<Arc<Histogram>>>,
    /// Gossip push duration histogram (set during start) — the
    /// dissemination push latency (ADR-0028: no longer the liveness
    /// signal; probes have their own plane and histogram).
    pub(crate) gossip_push_duration: RwLock<Option<Arc<Histogram>>>,
    /// SWIM probe cycle duration histogram (set during start,
    /// ADR-0028 D2).
    pub(crate) probe_duration: RwLock<Option<Arc<Histogram>>>,
    /// SWIM probe cycles that ended in failure (set during start).
    pub(crate) probe_failures: RwLock<Option<Counter>>,
    /// SWIM indirect (relayed) probes sent (set during start).
    pub(crate) indirect_probes: RwLock<Option<Counter>>,
    /// The local node's version clock for its own entries (ADR-0028 D3):
    /// bumped on every self-announcement / leave event so peers can
    /// order same-incarnation self-origin entries.
    pub(crate) self_version: std::sync::atomic::AtomicU64,
    /// Ring version gauge — increments on each ring topology change.
    pub(crate) ring_version: Gauge,
}
