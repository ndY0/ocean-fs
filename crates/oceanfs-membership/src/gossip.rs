//! Push-pull gossip protocol for membership state dissemination.
//!
//! Implements the gossip protocol that runs every `gossip_interval_ms`:
//! 1. Select a random peer from the membership.
//! 2. Push a delta of membership changes since the last exchange.
//! 3. Receive the peer's delta in response and merge it.
//!
//! The protocol is designed to be transport-agnostic — the actual
//! message sending (gRPC, etc.) is handled by the caller.

use std::{collections::HashMap, sync::Arc, time::Duration};

use oceanfs_core::{Incarnation, NodeId, NodeState};
use oceanfs_network::ConnectionPool;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, trace, warn};

use crate::{
    failure_detector::DetectorCommand,
    membership::state::{GossipDelta, GossipState, NodeEntry},
};

/// Internal command to the gossip task.
#[derive(Clone)]
pub(crate) enum GossipCommand {
    /// Push a state delta to a specific peer.
    Push { peer: NodeId, delta: GossipDelta },
    /// Receive a delta from a peer and merge it.
    ReceiveDelta { from: NodeId, delta: GossipDelta },
    /// Set or update the connection pool for gRPC push calls.
    SetPool { pool: Arc<ConnectionPool> },
    /// Add a node entry to the local gossip state.
    AddNode { entry: NodeEntry },
    /// Shut down the gossip task.
    Shutdown,
}

impl std::fmt::Debug for GossipCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Push { peer, delta } => f
                .debug_struct("Push")
                .field("peer", peer)
                .field("delta_len", &delta.changed.len())
                .finish(),
            Self::ReceiveDelta { from, delta } => f
                .debug_struct("ReceiveDelta")
                .field("from", from)
                .field("delta_len", &delta.changed.len())
                .finish(),
            Self::SetPool { .. } => f.debug_struct("SetPool").finish(),
            Self::AddNode { entry } => {
                f.debug_struct("AddNode").field("node_id", &entry.node_id).finish()
            }
            Self::Shutdown => write!(f, "Shutdown"),
        }
    }
}

/// The gossip protocol task.
///
/// Manages the local membership view, merges incoming gossip
/// deltas, and emits membership change events.
pub(crate) struct GossipProtocol {
    /// Receiver for gossip commands and incoming deltas.
    rx: mpsc::Receiver<GossipCommand>,
    /// Sender for membership events (state transitions).
    event_tx: broadcast::Sender<GossipCommand>,
    /// Sender for failure detector commands.
    detector_tx: mpsc::Sender<DetectorCommand>,
    /// Local membership state.
    state: GossipState,
    /// Tracked incarnations per node for conflict resolution.
    incarnations: HashMap<NodeId, Incarnation>,
    /// Membership event channel (for higher-level subscribers).
    membership_event_tx: broadcast::Sender<crate::membership::MembershipEvent>,
    /// Connection pool for gRPC push calls to peers.
    pool: Option<Arc<ConnectionPool>>,
    /// Interval between periodic gossip rounds in milliseconds.
    gossip_interval_ms: u64,
    /// This node's identifier (for excluding self from peer selection).
    node_id: NodeId,
}

impl GossipProtocol {
    /// Creates a new gossip protocol instance.
    pub fn new(
        rx: mpsc::Receiver<GossipCommand>,
        event_tx: broadcast::Sender<GossipCommand>,
        detector_tx: mpsc::Sender<DetectorCommand>,
        membership_event_tx: broadcast::Sender<crate::membership::MembershipEvent>,
        gossip_interval_ms: u64,
        node_id: NodeId,
    ) -> Self {
        Self {
            rx,
            event_tx,
            detector_tx,
            state: GossipState::new(),
            incarnations: HashMap::new(),
            membership_event_tx,
            pool: None,
            gossip_interval_ms,
            node_id,
        }
    }

    /// Sets the connection pool for gRPC push calls.
    pub fn set_pool(&mut self, pool: Arc<ConnectionPool>) {
        self.pool = Some(pool);
    }

    /// Runs the gossip protocol loop.
    ///
    /// Processes incoming commands and fires a periodic gossip ticker
    /// that selects a random alive peer and pushes a membership delta.
    pub async fn run(&mut self) {
        let mut ticker = tokio::time::interval(Duration::from_millis(self.gossip_interval_ms));
        // Don't fire immediately — wait for the first interval to elapse.
        ticker.tick().await;

        loop {
            tokio::select! {
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            if !self.handle_command(cmd).await {
                                break;
                            }
                        }
                        None => break, // Channel closed.
                    }
                }
                _ = ticker.tick() => {
                    // Periodic gossip: select a random alive peer and push a delta.
                    self.on_gossip_tick().await;
                }
            }
        }
        debug!("gossip protocol shut down");
    }

    /// Fires on each gossip interval tick.
    ///
    /// Pushes the current membership delta to all alive peers so that
    /// state changes propagate quickly and failure detection can
    /// observe unreachable peers immediately.
    async fn on_gossip_tick(&mut self) {
        let alive: Vec<_> = self
            .state
            .nodes
            .iter()
            .filter(|(id, e)| e.state == NodeState::Alive && *id != &self.node_id)
            .map(|(id, _)| id.clone())
            .collect();

        if alive.is_empty() {
            trace!("gossip tick: no alive peers to push to");
            return;
        }

        let delta = self.build_delta();
        if delta.changed.is_empty() {
            trace!("gossip tick: delta is empty, skipping push");
            return;
        }

        // Push to all alive peers so failure detection hits every
        // unreachable peer on every tick, not just a random subset.
        for peer in &alive {
            debug!(peer = %peer, changed = delta.changed.len(), "periodic gossip push");
            self.handle_command(GossipCommand::Push { peer: peer.clone(), delta: delta.clone() })
                .await;
        }
    }

    /// Handles a gossip command.
    async fn handle_command(&mut self, cmd: GossipCommand) -> bool {
        match cmd {
            GossipCommand::Push { peer, delta } => {
                trace!(peer = %peer, changed = delta.changed.len(), "pushing gossip delta");
                // Merge our own delta locally first so our state stays consistent
                // regardless of whether the push succeeds.
                self.merge_delta(&delta);

                // Spawn the gRPC push in a background task so it doesn't block
                // the gossip ticker. If the peer is dead, the connection timeout
                // (default 5s) would otherwise block the select! loop and prevent
                // the ticker from firing, making failure detection stall.
                if let Some(ref pool) = self.pool {
                    if let Some(entry) = self.state.nodes.get(&peer) {
                        let peer_addr = entry.address;
                        let pool = pool.clone();
                        let detector = self.detector_tx.clone();
                        let peer_clone = peer.clone();

                        // Convert the GossipDelta into protobuf GossipMessages
                        // outside the spawned task to avoid cloning the delta.
                        let entries: Vec<_> = delta
                            .changed
                            .iter()
                            .map(|e| oceanfs_core::proto::membership::MembershipEntry {
                                node_id: Some(oceanfs_core::proto::common::NodeId {
                                    id: e.node_id.to_string(),
                                }),
                                state: match e.state {
                                    NodeState::Alive => 0,
                                    NodeState::Suspect => 1,
                                    NodeState::Dead => 2,
                                    NodeState::Leaving => 3,
                                    NodeState::Left => 4,
                                },
                                incarnation: e.incarnation.value(),
                                address: e.address.to_string(),
                                last_seen: None,
                            })
                            .collect();

                        tokio::spawn(async move {
                            match pool.get_channel(peer_addr).await {
                                Ok(pooled) => {
                                    let channel = pooled.channel().clone();
                                    drop(pooled);

                                    let mut client = oceanfs_network::GossipRpcClient::new(channel);

                                    let msg = oceanfs_network::gossip::GossipMessage {
                                        delta: Some(
                                            oceanfs_core::proto::membership::MembershipList {
                                                entries,
                                            },
                                        ),
                                        ring_version: 0,
                                        hlc: None,
                                    };

                                    let stream = tokio_stream::iter(vec![msg]);
                                    match client.push(tonic::Request::new(stream)).await {
                                        Ok(response) => {
                                            let ack = response.into_inner();
                                            if ack.accepted {
                                                debug!(
                                                    peer = %peer_clone,
                                                    updated = ack.updated_entries,
                                                    "gossip push ack received"
                                                );
                                            }
                                            let _ =
                                                detector.try_send(DetectorCommand::PingResponse {
                                                    target: peer_clone,
                                                    success: true,
                                                });
                                        }
                                        Err(status) => {
                                            warn!(peer = %peer_clone, error = %status, "gossip push failed");
                                            let _ =
                                                detector.try_send(DetectorCommand::PingResponse {
                                                    target: peer_clone,
                                                    success: false,
                                                });
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(peer = %peer_clone, error = %e, "failed to acquire channel for push");
                                    let _ = detector.try_send(DetectorCommand::PingResponse {
                                        target: peer_clone,
                                        success: false,
                                    });
                                }
                            }
                        });
                    }
                }
            }
            GossipCommand::ReceiveDelta { from, delta } => {
                debug!(from = %from, changed = delta.changed.len(), "received gossip delta");
                self.merge_delta(&delta);
            }
            GossipCommand::SetPool { pool } => {
                debug!("gossip protocol pool updated");
                self.pool = Some(pool);
            }
            GossipCommand::AddNode { entry } => {
                debug!(node_id = %entry.node_id, "adding node to gossip state");
                self.add_node(entry);
            }
            GossipCommand::Shutdown => return false,
        }
        true
    }

    /// Merges a gossip delta into the local state.
    ///
    /// Uses incarnation numbers for conflict resolution:
    /// higher incarnation always wins. If incarnations are equal,
    /// the more "active" state wins (Alive > Suspect > Dead).
    fn merge_delta(&mut self, delta: &GossipDelta) {
        for entry in &delta.changed {
            let current_incarnation =
                self.incarnations.get(&entry.node_id).copied().unwrap_or(Incarnation::new(0));

            // Higher incarnation always wins.
            if entry.incarnation < current_incarnation {
                trace!(
                    node_id = %entry.node_id,
                    "ignoring stale delta (incarnation {} < {})",
                    entry.incarnation.value(),
                    current_incarnation.value(),
                );
                continue;
            }

            let old_state =
                self.state.nodes.get(&entry.node_id).map(|e| e.state).unwrap_or(NodeState::Alive);

            // Update local state.
            self.state.nodes.insert(entry.node_id.clone(), entry.clone());
            self.incarnations.insert(entry.node_id.clone(), entry.incarnation);

            // Emit membership event if state changed.
            if old_state != entry.state {
                let _ = self.membership_event_tx.send(crate::membership::MembershipEvent {
                    node_id: entry.node_id.clone(),
                    old_state,
                    new_state: entry.state,
                });

                // If a node is declared DEAD, notify the failure detector.
                if entry.state == NodeState::Dead {
                    // The failure detector handles the DEAD→ring update flow.
                }
            }
        }
    }

    /// Builds a delta containing all changes since the given watermark.
    pub(crate) fn build_delta(&self) -> GossipDelta {
        GossipDelta { changed: self.state.nodes.values().cloned().collect() }
    }

    /// Returns a snapshot of the current gossip state.
    pub(crate) fn snapshot(&self) -> &GossipState {
        &self.state
    }

    /// Adds a node to the local state (typically on join).
    pub(crate) fn add_node(&mut self, entry: NodeEntry) {
        self.incarnations.insert(entry.node_id.clone(), entry.incarnation);
        self.state.nodes.insert(entry.node_id.clone(), entry);
    }

    /// Returns the set of alive nodes.
    pub(crate) fn alive_nodes(&self) -> Vec<(NodeId, NodeState)> {
        self.state
            .nodes
            .iter()
            .filter(|(_, e)| e.state == NodeState::Alive)
            .map(|(id, e)| (id.clone(), e.state))
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{Incarnation, NodeId, NodeState};
    use tokio::sync::mpsc;

    use super::*;

    fn make_protocol() -> GossipProtocol {
        let (_cmd_tx, cmd_rx) = mpsc::channel(8);
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(16);
        let (detector_tx, _detector_rx) = mpsc::channel(8);
        let (membership_tx, _membership_rx) = tokio::sync::broadcast::channel(16);
        GossipProtocol::new(
            cmd_rx,
            event_tx,
            detector_tx,
            membership_tx,
            1000, // gossip_interval_ms
            NodeId::new("test-node"),
        )
    }

    fn make_node_entry(id: &str, incarnation: u64, state: NodeState) -> NodeEntry {
        NodeEntry {
            node_id: NodeId::new(id),
            incarnation: Incarnation::new(incarnation),
            state,
            address: "127.0.0.1:9001".parse().unwrap(),
        }
    }

    #[test]
    fn merge_delta_newer_incarnation_wins() {
        let mut protocol = make_protocol();

        // Add a node with incarnation 1.
        protocol.add_node(make_node_entry("n1", 1, NodeState::Alive));

        // Try to merge a stale delta (incarnation 0 → should be ignored).
        let delta = GossipDelta { changed: vec![make_node_entry("n1", 0, NodeState::Dead)] };
        protocol.merge_delta(&delta);

        // The node should still be ALIVE (stale delta was ignored).
        let alive = protocol.alive_nodes();
        assert!(
            alive.iter().any(|(id, _)| id.as_str() == "n1"),
            "node should still be ALIVE after stale delta"
        );
    }

    #[test]
    fn merge_delta_higher_incarnation_state_applied() {
        let mut protocol = make_protocol();

        // Add a node with incarnation 1.
        protocol.add_node(make_node_entry("n1", 1, NodeState::Alive));

        // Merge a delta with incarnation 2 declaring it DEAD.
        let delta = GossipDelta { changed: vec![make_node_entry("n1", 2, NodeState::Dead)] };
        protocol.merge_delta(&delta);

        // The node should now be dead.
        let alive = protocol.alive_nodes();
        assert!(
            !alive.iter().any(|(id, _)| id.as_str() == "n1"),
            "node should be DEAD after higher-incarnation delta"
        );
    }

    #[test]
    fn merge_delta_same_incarnation_no_change() {
        let mut protocol = make_protocol();

        protocol.add_node(make_node_entry("n1", 1, NodeState::Alive));

        // Same incarnation, same state — no change.
        let delta = GossipDelta { changed: vec![make_node_entry("n1", 1, NodeState::Alive)] };
        protocol.merge_delta(&delta);

        assert_eq!(protocol.alive_nodes().len(), 1);
    }

    #[test]
    fn build_delta_returns_all_known_nodes() {
        let mut protocol = make_protocol();

        protocol.add_node(make_node_entry("a", 1, NodeState::Alive));
        protocol.add_node(make_node_entry("b", 1, NodeState::Alive));

        let delta = protocol.build_delta();
        assert_eq!(delta.changed.len(), 2);
    }

    #[test]
    fn alive_nodes_filters_non_alive() {
        let mut protocol = make_protocol();

        protocol.add_node(make_node_entry("a", 1, NodeState::Alive));
        protocol.add_node(make_node_entry("b", 1, NodeState::Dead));
        protocol.add_node(make_node_entry("c", 1, NodeState::Suspect));
        protocol.add_node(make_node_entry("d", 1, NodeState::Leaving));

        let alive = protocol.alive_nodes();
        assert_eq!(alive.len(), 1);
        assert_eq!(alive[0].0.as_str(), "a");
    }

    #[test]
    fn snapshot_returns_current_state() {
        let mut protocol = make_protocol();
        protocol.add_node(make_node_entry("a", 1, NodeState::Alive));
        let snap = protocol.snapshot();
        assert!(snap.nodes.contains_key(&NodeId::new("a")));
    }

    #[tokio::test]
    async fn run_shuts_down_gracefully() {
        let mut protocol = make_protocol();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let _ = std::mem::replace(&mut protocol.rx, cmd_rx);

        // Send shutdown.
        drop(cmd_tx); // closing the sender causes recv() to return None
        protocol.run().await;
    }

    #[tokio::test]
    async fn handle_command_shutdown_returns_false() {
        let mut protocol = make_protocol();
        assert!(!protocol.handle_command(GossipCommand::Shutdown).await);
    }

    #[tokio::test]
    async fn handle_command_receive_delta_merges() {
        let mut protocol = make_protocol();
        protocol.add_node(make_node_entry("a", 1, NodeState::Alive));

        let delta = GossipDelta { changed: vec![make_node_entry("a", 2, NodeState::Dead)] };
        let result = protocol
            .handle_command(GossipCommand::ReceiveDelta { from: NodeId::new("peer"), delta })
            .await;
        assert!(result);
        // The node should now be dead.
        let alive = protocol.alive_nodes();
        assert!(alive.is_empty());
    }

    #[tokio::test]
    async fn handle_command_push_merges() {
        let mut protocol = make_protocol();

        let delta = GossipDelta { changed: vec![make_node_entry("new-node", 1, NodeState::Alive)] };
        let result =
            protocol.handle_command(GossipCommand::Push { peer: NodeId::new("peer"), delta }).await;
        assert!(result);
        assert!(protocol.snapshot().nodes.contains_key(&NodeId::new("new-node")));
    }
}
