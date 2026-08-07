//! Membership lifecycle management.
//!
//! Contains the constructor, start-up, join, leave, and state mutation
//! logic for the [`Membership`] coordinator.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use oceanfs_core::{Gauge, Incarnation, LabelSet, NodeId, NodeState};
use oceanfs_network::ConnectionPool;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use super::{
    state::{MembershipState, NodeEntry},
    Membership, MembershipEvent,
};
use crate::{
    error::{Error, Result},
    failure_detector::{DetectorCommand, DetectorConfig, FailureDetector},
    gossip::{GossipCommand, GossipProtocol},
};

impl Membership {
    /// Creates a new membership instance.
    ///
    /// Sets up internal channels and state but does NOT start background
    /// tasks. Call [`Self::start`] then [`Self::join`] to join the cluster.
    pub fn new(
        node_id: NodeId,
        address: SocketAddr,
        config: oceanfs_core::GossipConfig,
        ring: Arc<oceanfs_routing::RingCache>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(256);

        Self {
            node_id,
            address,
            config,
            state: RwLock::new(MembershipState::new()),
            ring,
            event_tx,
            detector_tx: RwLock::new(None),
            gossip_tx: RwLock::new(None),
            pool: RwLock::new(None),
            started: RwLock::new(false),
            shutdown: tokio_util::sync::CancellationToken::new(),
            gossip_sent: RwLock::new(None),
            gossip_received: RwLock::new(None),
            gossip_dropped: RwLock::new(None),
            ring_version: Gauge::new(
                "ring_version".into(),
                "Ring topology version, incremented on each change".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Starts the background failure detector and gossip tasks.
    /// Must be called before [`Self::join`].
    ///
    /// # Errors
    /// Returns [`Error::AlreadyStarted`] if called more than once.
    pub fn start(self: &Arc<Self>) -> Result<()> {
        let mut started = self.started.write();
        if *started {
            return Err(Error::AlreadyStarted);
        }
        *started = true;
        drop(started);
        // ---- Failure detector ----
        let detector_config = DetectorConfig {
            interval_ms: self.config.interval_ms,
            ping_timeout_ms: self.config.failure_timeout_ms / 3,
            suspicion_timeout_ms: self.config.suspicion_timeout_ms,
            failure_timeout_ms: self.config.failure_timeout_ms,
            indirect_ping_count: self.config.indirect_ping_count,
        };
        let (mut detector, detector_cmd_tx) = FailureDetector::new(
            detector_config,
            self.event_tx.clone(),
            self.node_id.clone(),
            Incarnation::new(1),
            64,
        );

        // Store the command sender so other methods can control the detector.
        let detector_tx_for_gossip = detector_cmd_tx.clone();
        *self.detector_tx.write() = Some(detector_cmd_tx);

        let detector_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = detector.run() => {},
                _ = detector_shutdown.cancelled() => {},
            }
        });

        // ---- Alive-nodes sync ----
        // Periodically feed the failure detector with the current set of alive
        // nodes from the membership state. Without this, the detector has no
        // peers to ping and cannot detect failures.
        let sync_membership = Arc::clone(self);
        let sync_detector_tx = detector_tx_for_gossip.clone();
        let sync_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(sync_membership.config.interval_ms));
            // Don't fire immediately — wait for the first interval.
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let alive: Vec<_> = sync_membership
                            .state
                            .read()
                            .nodes
                            .iter()
                            .map(|(id, (state, inc, addr))| (id.clone(), *state, *addr, *inc))
                            .collect();
                        let _ = sync_detector_tx.try_send(
                            DetectorCommand::UpdateAliveNodes { nodes: alive },
                        );
                    }
                    _ = sync_shutdown.cancelled() => break,
                }
            }
        });
        // ---- Gossip protocol ----
        let (gossip_cmd_tx, gossip_cmd_rx) = tokio::sync::mpsc::channel(64);
        let (gossip_event_tx, _gossip_event_rx) = broadcast::channel(16);

        let mut gossip_protocol = GossipProtocol::new(
            gossip_cmd_rx,
            gossip_event_tx,
            detector_tx_for_gossip,
            self.event_tx.clone(),
            self.config.interval_ms,
            self.node_id.clone(),
        );

        // If the connection pool is already set, pass it to the gossip protocol.
        if let Some(pool) = self.pool.read().as_ref() {
            gossip_protocol.set_pool(pool.clone());
        }
        // Extract gossip counters for metrics registration.
        {
            let mut sent = self.gossip_sent.write();
            let mut recv = self.gossip_received.write();
            let mut dropped = self.gossip_dropped.write();
            *sent = Some(gossip_protocol.messages_sent.clone());
            *recv = Some(gossip_protocol.messages_received.clone());
            *dropped = Some(gossip_protocol.messages_dropped.clone());
        }
        *self.gossip_tx.write() = Some(gossip_cmd_tx);
        let gossip_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = gossip_protocol.run() => {},
                _ = gossip_shutdown.cancelled() => {},
            }
        });

        // ---- Event handler: apply state changes to membership and ring ----
        // The failure detector and gossip protocol emit MembershipEvent via
        // event_tx. This task subscribes and calls upsert_node() to keep the
        // membership state and ring consistent with detected state changes.
        let mut event_rx = self.event_tx.subscribe();
        let event_membership = Arc::clone(self);
        let event_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        match event {
                            Ok(MembershipEvent { node_id, new_state, .. }) => {
                                // Look up the node's current incarnation and address.
                                let (incarnation, address) = {
                                    let state = event_membership.state.read();
                                    state.nodes.get(&node_id)
                                        .map(|(_, inc, addr)| (*inc, *addr))
                                        .unwrap_or((Incarnation::new(1),
                                            std::net::SocketAddr::from(([127, 0, 0, 1], 9001))))
                                };
                                event_membership.upsert_node(
                                    node_id, new_state, incarnation, address,
                                );
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(skipped = n, "membership event handler lagged");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = event_shutdown.cancelled() => break,
                }
            }
            tracing::debug!("membership event handler shut down");
        });

        info!(node_id = %self.node_id, "membership background tasks started");

        Ok(())
    }

    /// Sets the connection pool for gRPC-based gossip and join operations.
    ///
    /// Must be called before [`Self::join`] if seed nodes are configured.
    /// The pool is shared with the gossip protocol for push/pull.
    ///
    /// If the gossip protocol has already been started, this sends a
    /// `SetPool` command to update it asynchronously.
    pub fn set_pool(&self, pool: Arc<ConnectionPool>) {
        // Update the gossip protocol if it's already running.
        if let Some(tx) = self.gossip_tx.read().as_ref() {
            let _ = tx.try_send(GossipCommand::SetPool { pool: pool.clone() });
        }
        *self.pool.write() = Some(pool);
    }

    /// Registers gossip counters with a metrics registrar.
    pub fn register_gossip_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        if let Some(ref c) = *self.gossip_sent.read() {
            registrar.register_counter(c.clone());
        }
        if let Some(ref c) = *self.gossip_received.read() {
            registrar.register_counter(c.clone());
        }
        if let Some(ref c) = *self.gossip_dropped.read() {
            registrar.register_counter(c.clone());
        }
        registrar.register_gauge(self.ring_version.clone());
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
            info!(
                node_id = %self.node_id,
                "no seed nodes configured, starting as first node"
            );
            // Self is added via upsert_node() below.
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
            let mut joined_seed_addr: Option<SocketAddr> = None;

            for seed_str in seed_nodes {
                let seed_addr: SocketAddr = match seed_str.parse() {
                    Ok(a) => a,
                    Err(e) => {
                        warn!(seed = %seed_str, error = %e, "invalid seed address");
                        continue;
                    }
                };

                debug!(
                    node_id = %self.node_id,
                    seed = %seed_addr,
                    "contacting seed node via gRPC"
                );

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
                let request = tonic::Request::new(oceanfs_network::gossip::GossipPullRequest {
                    node_id: Some(oceanfs_core::proto::common::NodeId {
                        id: self.node_id.to_string(),
                    }),
                    last_known_version: 0,
                });

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
                                    let addr =
                                        entry.address.parse::<SocketAddr>().unwrap_or_else(|_| {
                                            SocketAddr::from(([127, 0, 0, 1], 9001))
                                        });
                                    if let Some(id) = nid {
                                        self.upsert_node(id, state, inc, addr);
                                    }
                                }
                            }
                        }
                        joined = true;
                        joined_seed_addr = Some(seed_addr);
                        info!(seed = %seed_addr, "received membership state from seed");
                        break;
                    }
                    Err(status) => {
                        warn!(
                            seed = %seed_addr,
                            error = %status,
                            "pull from seed failed"
                        );
                    }
                }
            }

            if !joined {
                return Err(Error::JoinFailed("could not contact any seed node".into()));
            }

            // PR5: After receiving membership list, announce self to the seed
            // via push. This lets the seed learn about the joiner and add it
            // to its ring.
            if let Some(seed_addr) = joined_seed_addr {
                debug!(seed = %seed_addr, "announcing self to seed via gossip push");

                let pooled = pool.get_channel(seed_addr).await.map_err(|e| {
                    Error::JoinFailed(format!("failed to connect to seed for push: {e}"))
                })?;
                let channel = pooled.channel().clone();
                drop(pooled);

                let mut push_client = oceanfs_network::GossipRpcClient::new(channel);
                let proto_entry = oceanfs_core::proto::membership::MembershipEntry {
                    node_id: Some(oceanfs_core::proto::common::NodeId {
                        id: self.node_id.to_string(),
                    }),
                    state: 0, // ALIVE
                    incarnation: 1,
                    address: self.address.to_string(),
                    last_seen: None,
                };
                let delta =
                    oceanfs_core::proto::membership::MembershipList { entries: vec![proto_entry] };
                let msg = oceanfs_network::gossip::GossipMessage {
                    delta: Some(delta),
                    ring_version: 0,
                    hlc: None,
                };
                let stream = tokio_stream::iter(vec![msg]);

                match push_client.push(tonic::Request::new(stream)).await {
                    Ok(response) => {
                        let ack = response.into_inner();
                        if ack.accepted {
                            info!(
                                seed = %seed_addr,
                                "seed accepted self-announcement"
                            );
                        }
                    }
                    Err(status) => {
                        warn!(
                            seed = %seed_addr,
                            error = %status,
                            "failed to announce self to seed"
                        );
                    }
                }
            }
        }

        // Announce self as ALIVE via upsert_node so the gossip protocol is
        // notified.
        self.upsert_node(self.node_id.clone(), NodeState::Alive, Incarnation::new(1), self.address);

        info!(node_id = %self.node_id, "joined cluster successfully");
        Ok(())
    }

    /// Gracefully leaves the cluster.
    ///
    /// 1. Announces LEAVING state via gossip.
    /// 2. Determines the ring successor for data handoff.
    /// 3. If a `leave_handler` is provided, seals and transfers WAL
    ///    data and segment shards to the successor.
    /// 4. Announces LEFT state and removes self from the ring.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotStarted`] if background tasks haven't been started.
    /// Returns [`Error::Leave`] if the leave handler reports a failure.
    pub async fn leave(
        &self,
        leave_handler: Option<&dyn crate::GracefulLeaveHandler>,
    ) -> Result<()> {
        if !*self.started.read() {
            return Err(Error::NotStarted);
        }

        let node_id = self.node_id.clone();

        // Determine the ring successor for data handoff.
        let successor =
            self.ring.snapshot().successor_of(&node_id).unwrap_or_else(|| node_id.clone());

        // Transition to LEAVING.
        let _ = self.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Alive,
            new_state: NodeState::Leaving,
        });

        info!(
            node_id = %node_id,
            successor = %successor,
            "node leaving cluster"
        );

        // Execute graceful leave data handoff.
        if let Some(handler) = leave_handler {
            if successor != node_id {
                info!(successor = %successor, "handing off WAL to successor");
                handler
                    .handoff_wal_to(&successor)
                    .await
                    .map_err(|e| Error::Leave(format!("WAL handoff failed: {e}")))?;

                info!(successor = %successor, "transferring segment shards to successor");
                let count = handler
                    .transfer_segment_shards_to(&successor)
                    .await
                    .map_err(|e| Error::Leave(format!("segment shard transfer failed: {e}")))?;
                info!(count, "segment shard transfer complete");
            } else {
                info!("no successor found; skipping data handoff");
            }
        } else {
            // No leave handler provided — drain period only.
            info!("no leave handler configured; draining in-flight requests");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Transition to LEFT.
        let _ = self.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Leaving,
            new_state: NodeState::Left,
        });

        // Remove self from ring.
        let mut ring_snapshot = (*self.ring.snapshot()).clone();
        if let Err(e) = ring_snapshot.remove_node(node_id.clone()) {
            warn!(
                node_id = %node_id,
                error = %e,
                "failed to remove self from ring"
            );
        }
        self.ring.update(ring_snapshot);
        self.ring_version.inc();

        info!(node_id = %node_id, "node left cluster");
        Ok(())
    }

    /// Adds or updates a node's state from external input (e.g., gossip merge).
    /// New ALIVE nodes are added to the ring; Dead/Left nodes are removed.
    pub fn upsert_node(
        &self,
        node_id: NodeId,
        state: NodeState,
        incarnation: Incarnation,
        address: SocketAddr,
    ) {
        let mut inner = self.state.write();

        // Capture old state before modifying.
        let old = inner.nodes.get(&node_id).map(|(s, _, _)| *s);
        let old_state = old.unwrap_or(NodeState::Alive);
        let is_new = old.is_none();

        // Remove dead/left nodes from state so they don't appear in cluster views.
        if state == NodeState::Dead || state == NodeState::Left {
            inner.nodes.remove(&node_id);
        } else {
            inner.nodes.insert(node_id.clone(), (state, incarnation, address));
        }
        drop(inner);
        if is_new || old_state != state {
            let _ = self.event_tx.send(MembershipEvent {
                node_id: node_id.clone(),
                old_state: if is_new { NodeState::Alive } else { old_state },
                new_state: state,
            });

            // PR4: Update ring synchronously on membership changes.
            let mut ring_snapshot = (*self.ring.snapshot()).clone();

            if is_new {
                // New node — add to ring.
                ring_snapshot.add_node(node_id.clone());
                debug!(node_id = %node_id, "ring: added new node");
            } else if state == NodeState::Dead || state == NodeState::Left {
                // Transitioned to dead/left — remove from ring.
                if ring_snapshot.remove_node(node_id.clone()).is_ok() {
                    debug!(
                        node_id = %node_id,
                        state = ?state,
                        "ring: removed dead/left node"
                    );
                }
            }

            self.ring.update(ring_snapshot);
            self.ring_version.inc();

            // Notify the gossip protocol of membership changes.
            if let Some(tx) = self.gossip_tx.read().as_ref() {
                let entry = NodeEntry { node_id: node_id.clone(), incarnation, state, address };
                let _ = tx.try_send(GossipCommand::AddNode { entry });
                debug!(
                    node_id = %node_id,
                    state = ?state,
                    "gossip: enqueued AddNode"
                );
            }
        }
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
            NodeState::Suspect,
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
            NodeState::Suspect,
            Incarnation::new(1),
            "127.0.0.1:9020".parse().unwrap(),
        );

        assert_eq!(m.state_of(&NodeId::new("known")), Some(NodeState::Suspect));
        assert_eq!(m.state_of(&NodeId::new("unknown")), None);
    }

    #[tokio::test]
    async fn start_cannot_be_called_twice() {
        let (_ring, m) = make_membership("node");
        let m = std::sync::Arc::new(m);
        assert!(m.start().is_ok());
        assert!(m.start().is_err()); // AlreadyStarted
    }

    #[test]
    fn leave_without_start_errors() {
        let (_ring, m) = make_membership("node");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(m.leave(None));
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

        let m = std::sync::Arc::new(Membership::new(
            NodeId::new("leaver"),
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            GossipConfig::default(),
            ring_cache.clone(),
        ));

        m.start().expect("start should succeed");
        m.leave(None).await.expect("leave should succeed");

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

    // --- Ring version gauge tests ---

    #[test]
    fn ring_version_starts_at_zero() {
        let (_ring, m) = make_membership("node");
        assert_eq!(m.ring_version.get(), 0);
    }

    #[test]
    fn ring_version_increments() {
        let (_ring, m) = make_membership("node");
        m.ring_version.inc();
        assert_eq!(m.ring_version.get(), 1);
        m.ring_version.inc();
        assert_eq!(m.ring_version.get(), 2);
    }

    #[test]
    fn ring_version_gauge_name_is_correct() {
        let (_ring, m) = make_membership("node");
        assert!(m.ring_version.name().contains("ring_version"));
    }

    #[test]
    fn ring_version_is_registered_in_gossip_metrics() {
        use oceanfs_core::MetricRegistrar;

        struct TestRegistrar {
            gauge_names: std::sync::Mutex<Vec<String>>,
        }
        impl MetricRegistrar for TestRegistrar {
            fn register_counter(&self, _: oceanfs_core::Counter) {}
            fn register_gauge(&self, gauge: oceanfs_core::Gauge) {
                self.gauge_names.lock().unwrap().push(gauge.name().to_string());
            }
            fn register_histogram(&self, _: std::sync::Arc<oceanfs_core::Histogram>) {}
        }

        let (_ring, m) = make_membership("node");
        let reg = TestRegistrar { gauge_names: std::sync::Mutex::new(Vec::new()) };

        m.register_gossip_metrics(&reg);

        let names = reg.gauge_names.lock().unwrap();
        assert!(
            names.contains(&"ring_version".to_string()),
            "ring_version gauge should be registered, got: {names:?}"
        );
    }
}
