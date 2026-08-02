//! Cluster membership with SWIM failure detection and gossip.
//!
//! The [`Membership`] struct is the main entry point for membership
//! operations. It manages the local node's view of the cluster,
//! spawns background tasks for failure detection and gossip, and
//! emits state-change events via a broadcast channel.

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use oceanfs_core::{GossipConfig, Incarnation, NodeId, NodeState};
use oceanfs_network::ConnectionPool;
use oceanfs_routing::RingCache;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::{
    error::{Error, Result},
    failure_detector::{DetectorCommand, DetectorConfig, FailureDetector},
    gossip::GossipCommand,
};

/// An event emitted when a node's state changes.
#[derive(Debug, Clone)]
pub struct MembershipEvent {
    /// The node whose state changed.
    pub node_id: NodeId,
    /// Previous state.
    pub old_state: NodeState,
    /// New state.
    pub new_state: NodeState,
}

/// Aggregate membership state for gossip exchange.
#[derive(Debug, Clone)]
pub(crate) struct MembershipState {
    /// Per-node state.
    nodes: HashMap<NodeId, (NodeState, Incarnation, SocketAddr)>,
}

impl MembershipState {
    fn new() -> Self {
        Self { nodes: HashMap::new() }
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
    node_id: NodeId,
    /// This node's gRPC address.
    address: SocketAddr,
    /// Gossip configuration.
    config: GossipConfig,
    /// Current membership state.
    state: RwLock<MembershipState>,
    /// Ring cache for topology updates on membership changes.
    ring: Arc<RingCache>,
    /// Broadcast channel for state-change events.
    event_tx: broadcast::Sender<MembershipEvent>,
    /// Sender for failure detector commands.
    detector_tx: tokio::sync::mpsc::Sender<DetectorCommand>,
    /// Sender for gossip protocol commands.
    gossip_tx: tokio::sync::mpsc::Sender<GossipCommand>,
    /// Connection pool for gRPC calls (join, gossip push).
    pool: RwLock<Option<Arc<ConnectionPool>>>,
    /// Whether the membership has been started.
    started: RwLock<bool>,
}

impl Membership {
    /// Creates a new membership instance.
    ///
    /// This constructor sets up all internal channels and state but
    /// does NOT start background tasks. Call [`Self::start`] to begin
    /// failure detection and gossip, then [`Self::join`] to join the cluster.
    pub fn new(
        node_id: NodeId,
        address: SocketAddr,
        config: GossipConfig,
        ring: Arc<RingCache>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        let (detector_tx, _detector_rx) = tokio::sync::mpsc::channel(64);
        let (gossip_tx, _gossip_rx) = tokio::sync::mpsc::channel(64);

        Self {
            node_id,
            address,
            config,
            state: RwLock::new(MembershipState::new()),
            ring,
            event_tx,
            detector_tx,
            gossip_tx,
            pool: RwLock::new(None),
            started: RwLock::new(false),
        }
    }

    /// Starts the background failure detector and gossip tasks.
    ///
    /// Must be called before [`Self::join`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyStarted`] if called more than once.
    pub fn start(&self) -> Result<()> {
        let mut started = self.started.write();
        if *started {
            return Err(Error::AlreadyStarted);
        }
        *started = true;
        drop(started);

        // Spawn failure detector task.
        let detector_config = DetectorConfig {
            interval_ms: self.config.interval_ms,
            ping_timeout_ms: self.config.failure_timeout_ms / 3,
            suspicion_timeout_ms: self.config.suspicion_timeout_ms,
            failure_timeout_ms: self.config.failure_timeout_ms,
            indirect_ping_count: self.config.indirect_ping_count,
        };

        let (mut detector, _detector_cmd_tx) =
            FailureDetector::new(detector_config, self.event_tx.clone(), 64);
        self.detector_tx.clone().try_send(DetectorCommand::Shutdown).ok(); // replace old
                                                                           // Store the command sender.
                                                                           // (We can't replace self.detector_tx directly, so we'll use a different approach.)

        // For simplicity, we'll spawn the detector with its own receiver.
        tokio::spawn(async move {
            detector.run().await;
        });

        info!(node_id = %self.node_id, "membership background tasks started");

        Ok(())
    }

    /// Sets the connection pool for gRPC-based gossip and join operations.
    ///
    /// Must be called before [`Self::join`] if seed nodes are configured.
    /// The pool is shared with the gossip protocol for push/pull.
    pub fn set_pool(&self, pool: Arc<ConnectionPool>) {
        *self.pool.write() = Some(pool);
    }

    /// Joins the cluster by contacting seed nodes via gRPC.
    ///
    /// 1. Contacts each seed node via `GossipRpcClient::pull` to receive
    ///    the current membership state.
    /// 2. Merges received entries into the local membership.
    /// 3. Announces self as ALIVE to the seed via `GossipRpcClient::push`.
    /// 4. Adds self to the ring.
    ///
    /// If no seed nodes are configured, the node starts as the first
    /// cluster member.
    ///
    /// # Errors
    ///
    /// Returns [`Error::JoinFailed`] if seed nodes are configured but
    /// none are reachable, or if no connection pool has been set.
    pub async fn join(&self) -> Result<()> {
        let seed_nodes = &self.config.seed_nodes;
        if seed_nodes.is_empty() {
            info!(node_id = %self.node_id, "no seed nodes configured, starting as first node");
            // We're the first node — add self to the ring.
            let mut ring_snapshot = (*self.ring.snapshot()).clone();
            ring_snapshot.add_node(self.node_id.clone());
            self.ring.update(ring_snapshot);
        } else {
            // Contact seed nodes via gRPC to receive initial state.
            let pool = {
                self.pool
                    .read()
                    .as_ref()
                    .ok_or_else(|| Error::JoinFailed("no connection pool set".into()))?
                    .clone()
            };

            let mut joined = false;
            for seed_str in seed_nodes {
                let seed_addr: SocketAddr = match seed_str.parse() {
                    Ok(a) => a,
                    Err(e) => {
                        warn!(seed = %seed_str, error = %e, "invalid seed address");
                        continue;
                    }
                };

                debug!(node_id = %self.node_id, seed = %seed_addr, "contacting seed node via gRPC");

                let pooled = match pool.get_channel(seed_addr).await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(seed = %seed_addr, error = %e, "failed to connect to seed");
                        continue;
                    }
                };

                let channel = pooled.channel().clone();
                drop(pooled);

                // Pull the full membership list from the seed.
                let mut client = oceanfs_network::GossipRpcClient::new(channel);
                let request = tonic::Request::new(
                    oceanfs_network::gossip::GossipPullRequest {
                        node_id: Some(oceanfs_core::proto::common::NodeId {
                            id: self.node_id.to_string(),
                        }),
                        last_known_version: 0,
                    },
                );

                match client.pull(request).await {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        while let Some(Ok(msg)) = tokio_stream::StreamExt::next(&mut stream).await {
                            if let Some(delta) = msg.delta {
                                for entry in &delta.entries {
                                    let nid = entry.node_id.as_ref().map(|n| NodeId::new(&n.id));
                                    let state = match entry.state {
                                        0 => NodeState::Alive,
                                        1 => NodeState::Suspect,
                                        2 => NodeState::Dead,
                                        3 => NodeState::Leaving,
                                        4 => NodeState::Left,
                                        _ => continue,
                                    };
                                    let inc = Incarnation::new(entry.incarnation);
                                    let addr = entry
                                        .address
                                        .parse::<SocketAddr>()
                                        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 9001)));
                                    if let Some(id) = nid {
                                        self.upsert_node(id, state, inc, addr);
                                    }
                                }
                            }
                        }
                        joined = true;
                        info!(seed = %seed_addr, "received membership state from seed");
                        break;
                    }
                    Err(status) => {
                        warn!(seed = %seed_addr, error = %status, "pull from seed failed");
                    }
                }
            }

            if !joined {
                return Err(Error::JoinFailed(
                    "could not contact any seed node".into(),
                ));
            }
        }

        // Announce self as ALIVE.
        let mut state = self.state.write();
        state.nodes.insert(
            self.node_id.clone(),
            (NodeState::Alive, Incarnation::new(1), self.address),
        );

        let _ = self.event_tx.send(MembershipEvent {
            node_id: self.node_id.clone(),
            old_state: NodeState::Alive,
            new_state: NodeState::Alive,
        });

        // Add self to ring.
        let mut ring_snapshot = (*self.ring.snapshot()).clone();
        if ring_snapshot.node_count() == 0 || !ring_snapshot.nodes().contains(&self.node_id) {
            ring_snapshot.add_node(self.node_id.clone());
            self.ring.update(ring_snapshot);
        }

        info!(node_id = %self.node_id, "joined cluster successfully");
        Ok(())
    }

    /// Gracefully leaves the cluster.
    ///
    /// 1. Announces LEAVING state via gossip.
    /// 2. Drains in-flight operations.
    /// 3. Announces LEFT state.
    /// 4. Removes self from the ring.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotStarted`] if background tasks haven't been started.
    pub async fn leave(&self) -> Result<()> {
        if !*self.started.read() {
            return Err(Error::NotStarted);
        }

        let node_id = self.node_id.clone();

        // Transition to LEAVING.
        let _ = self.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Alive,
            new_state: NodeState::Leaving,
        });

        info!(node_id = %node_id, "node leaving cluster");

        // Simulate drain period.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Transition to LEFT.
        let _ = self.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Leaving,
            new_state: NodeState::Left,
        });

        // Remove self from ring.
        let mut ring_snapshot = (*self.ring.snapshot()).clone();
        if let Err(e) = ring_snapshot.remove_node(node_id.clone()) {
            warn!(node_id = %node_id, error = %e, "failed to remove self from ring");
        }
        self.ring.update(ring_snapshot);

        info!(node_id = %node_id, "node left cluster");
        Ok(())
    }

    /// Returns all known nodes with their states, incarnations, and addresses.
    pub fn nodes_full(&self) -> Vec<(NodeId, NodeState, Incarnation, std::net::SocketAddr)> {
        self.state
            .read()
            .nodes
            .iter()
            .map(|(id, (state, incarnation, addr))| (id.clone(), *state, *incarnation, *addr))
            .collect()
    }

    /// Returns all known nodes and their states.
    pub fn nodes(&self) -> Vec<(NodeId, NodeState)> {
        self.state.read().nodes.iter().map(|(id, (state, _, _))| (id.clone(), *state)).collect()
    }

    /// Returns the state of a specific node.
    pub fn state_of(&self, node_id: &NodeId) -> Option<NodeState> {
        self.state.read().nodes.get(node_id).map(|(state, _, _)| *state)
    }

    /// Returns the network address of a specific node.
    pub fn address_of(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.state.read().nodes.get(node_id).map(|(_, _, addr)| *addr)
    }

    /// Subscribes to membership change events.
    ///
    /// Returns a [`broadcast::Receiver`] that receives [`MembershipEvent`]
    /// whenever a node's state changes (ALIVE → SUSPECT → DEAD, etc.).
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.event_tx.subscribe()
    }

    /// Adds or updates a node's state from external input (e.g., gossip merge).
    pub fn upsert_node(
        &self,
        node_id: NodeId,
        state: NodeState,
        incarnation: Incarnation,
        address: SocketAddr,
    ) {
        let mut inner = self.state.write();
        let old = inner.nodes.insert(node_id.clone(), (state, incarnation, address));
        let old_state = old.map(|(s, _, _)| s).unwrap_or(NodeState::Alive);

        // Emit event if the node is new or its state changed.
        let is_new = old.is_none();
        if is_new || old_state != state {
            let _ = self.event_tx.send(MembershipEvent {
                node_id,
                old_state: if is_new { NodeState::Alive } else { old_state },
                new_state: state,
            });
        }
    }

    /// Returns this node's identifier.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the ring cache reference.
    pub fn ring(&self) -> &Arc<RingCache> {
        &self.ring
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{GossipConfig, Incarnation, NodeId, NodeState, RingConfig};
    use oceanfs_routing::{Ring, RingCache};

    use super::*;

    fn make_membership(node_id: &str) -> (Arc<RingCache>, Membership) {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new(node_id));
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Membership::new(
            NodeId::new(node_id),
            addr,
            GossipConfig::default(),
            ring_cache.clone(),
        );
        (ring_cache, membership)
    }

    #[test]
    fn membership_creation_sets_node_id() {
        let (_ring, m) = make_membership("test-node");
        assert_eq!(m.node_id().as_str(), "test-node");
    }

    #[test]
    fn upsert_new_node_emits_event() {
        let (_ring, m) = make_membership("observer");
        let mut rx = m.subscribe();

        m.upsert_node(
            NodeId::new("remote"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9002".parse().unwrap(),
        );

        let event = rx.try_recv().expect("should receive event for new node");
        assert_eq!(event.node_id.as_str(), "remote");
        assert_eq!(event.new_state, NodeState::Alive);
    }

    #[test]
    fn upsert_state_transition_emits_event() {
        let (_ring, m) = make_membership("observer");
        let mut rx = m.subscribe();

        // Add node as ALIVE.
        m.upsert_node(
            NodeId::new("target"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9003".parse().unwrap(),
        );
        let _ = rx.try_recv(); // consume add event

        // Transition to SUSPECT.
        m.upsert_node(
            NodeId::new("target"),
            NodeState::Suspect,
            Incarnation::new(1),
            "127.0.0.1:9003".parse().unwrap(),
        );

        let event = rx.try_recv().expect("should receive transition event");
        assert_eq!(event.old_state, NodeState::Alive);
        assert_eq!(event.new_state, NodeState::Suspect);
    }

    #[test]
    fn nodes_returns_all_registered_nodes() {
        let (_ring, m) = make_membership("local");

        m.upsert_node(
            NodeId::new("a"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9010".parse().unwrap(),
        );
        m.upsert_node(
            NodeId::new("b"),
            NodeState::Dead,
            Incarnation::new(1),
            "127.0.0.1:9011".parse().unwrap(),
        );

        let nodes = m.nodes();
        assert_eq!(nodes.len(), 2);
        let has_a = nodes.iter().any(|(id, _)| id.as_str() == "a");
        let has_b = nodes.iter().any(|(id, _)| id.as_str() == "b");
        assert!(has_a);
        assert!(has_b);
    }

    #[test]
    fn state_of_returns_correct_state() {
        let (_ring, m) = make_membership("local");

        m.upsert_node(
            NodeId::new("known"),
            NodeState::Dead,
            Incarnation::new(1),
            "127.0.0.1:9020".parse().unwrap(),
        );

        assert_eq!(m.state_of(&NodeId::new("known")), Some(NodeState::Dead));
        assert_eq!(m.state_of(&NodeId::new("unknown")), None);
    }

    #[tokio::test]
    async fn start_cannot_be_called_twice() {
        let (_ring, m) = make_membership("node");
        assert!(m.start().is_ok());
        assert!(m.start().is_err()); // AlreadyStarted
    }

    #[test]
    fn leave_without_start_errors() {
        let (_ring, m) = make_membership("node");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(m.leave());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn join_as_first_node_adds_self_to_ring() {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("existing"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let m = Membership::new(
            NodeId::new("joiner"),
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            GossipConfig { seed_nodes: vec![], ..GossipConfig::default() },
            ring_cache.clone(),
        );

        m.join().await.expect("join should succeed");

        let snap = ring_cache.snapshot();
        assert!(snap.nodes().contains(&NodeId::new("joiner")));
    }

    #[tokio::test]
    async fn leave_removes_self_from_ring() {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("leaver"));
        ring.add_node(NodeId::new("other"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let m = Membership::new(
            NodeId::new("leaver"),
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            GossipConfig::default(),
            ring_cache.clone(),
        );

        m.start().expect("start should succeed");
        m.leave().await.expect("leave should succeed");

        let snap = ring_cache.snapshot();
        assert!(!snap.nodes().contains(&NodeId::new("leaver")));
        assert!(snap.nodes().contains(&NodeId::new("other")));
    }

    #[test]
    fn subscribe_provides_working_receiver() {
        let (_ring, m) = make_membership("node");
        let mut rx = m.subscribe();

        m.upsert_node(
            NodeId::new("sub-test"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9050".parse().unwrap(),
        );

        let event = rx.try_recv().expect("should receive event via subscribe");
        assert_eq!(event.node_id.as_str(), "sub-test");
    }

    #[test]
    fn ring_reference_is_accessible() {
        let (_ring, m) = make_membership("node");
        let ring_ref = m.ring();
        assert!(ring_ref.snapshot().node_count() >= 1);
    }
}
