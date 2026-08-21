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
    /// Ring version gauge — increments on each ring topology change.
    pub(crate) ring_version: Gauge,
}
