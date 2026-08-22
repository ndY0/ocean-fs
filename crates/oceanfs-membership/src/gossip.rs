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

use oceanfs_core::{
    sub_millisecond_histogram_config, Counter, Histogram, Incarnation, LabelSet, NodeId, NodeState,
};
use oceanfs_network::ConnectionPool;
use rand::seq::IteratorRandom;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, trace, warn};

use crate::membership::state::{GossipDelta, GossipState, NodeEntry};

/// Internal command to the gossip task.
#[derive(Clone)]
pub(crate) enum GossipCommand {
    /// Push a state delta to a specific peer.
    Push { peer: NodeId, delta: GossipDelta },
    /// Receive a delta from a peer and merge it.
    ReceiveDelta { from: NodeId, delta: GossipDelta },
    /// A push response: the peer's pull (delta newer than our vector)
    /// plus its version vector (ADR-0028 D4) — merges and advances the
    /// per-peer watermark.
    Ack {
        peer: NodeId,
        delta: GossipDelta,
        version_vector: std::collections::HashMap<NodeId, std::collections::HashMap<NodeId, u64>>,
    },
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
            Self::Ack { peer, delta, .. } => f
                .debug_struct("Ack")
                .field("peer", peer)
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
    /// Sender clone for spawned push tasks to report acks.
    command_tx: mpsc::Sender<GossipCommand>,
    /// Sender for membership events (state transitions).
    event_tx: broadcast::Sender<GossipCommand>,
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
    /// Random alive peers pushed per round (ADR-0028 D4 bounded fanout).
    fanout_k: u8,
    /// This node's identifier (for excluding self from peer selection).
    node_id: NodeId,
    /// Gossip messages sent counter.
    pub(crate) messages_sent: Counter,
    /// Gossip messages received counter.
    pub(crate) messages_received: Counter,
    /// Gossip messages dropped counter (push failures).
    pub(crate) messages_dropped: Counter,
    /// Gossip round duration histogram (microseconds).
    pub(crate) round_duration_us: Arc<Histogram>,
    /// Peer push (dissemination) duration histogram. ADR-0028 D2: the
    /// push is no longer the SWIM ping proxy — liveness has its own
    /// probe metric (`probe_duration_microseconds`) on the membership
    /// plane. This histogram measures dissemination only.
    pub(crate) push_duration_us: Arc<Histogram>,
    /// Delta sizes pushed per round (ADR-0028 D4) — bounded deltas
    /// after warmup prove the watermarks prune.
    pub(crate) delta_entries_hist: Arc<Histogram>,
    /// Per-peer watermarks: peer → (node → (origin → version)) — the
    /// highest version each peer has acknowledged per attributed entry.
    /// `build_delta_for` only sends entries newer than the watermark.
    pub(crate) watermarks: std::collections::HashMap<
        NodeId,
        std::collections::HashMap<NodeId, std::collections::HashMap<NodeId, u64>>,
    >,
}

impl GossipProtocol {
    /// Creates a new gossip protocol instance.
    pub fn new(
        rx: mpsc::Receiver<GossipCommand>,
        command_tx: mpsc::Sender<GossipCommand>,
        event_tx: broadcast::Sender<GossipCommand>,
        membership_event_tx: broadcast::Sender<crate::membership::MembershipEvent>,
        gossip_interval_ms: u64,
        node_id: NodeId,
        fanout_k: u8,
    ) -> Self {
        Self {
            rx,
            command_tx,
            event_tx,
            state: GossipState::new(),
            incarnations: HashMap::new(),
            membership_event_tx,
            pool: None,
            gossip_interval_ms,
            fanout_k,
            node_id,
            messages_sent: Counter::new(
                "gossip_messages_sent_total".into(),
                "Gossip messages pushed to peers".into(),
                LabelSet::empty(),
            ),
            messages_received: Counter::new(
                "gossip_messages_received_total".into(),
                "Gossip deltas received from peers".into(),
                LabelSet::empty(),
            ),
            messages_dropped: Counter::new(
                "gossip_messages_dropped_total".into(),
                "Gossip messages dropped due to push failures".into(),
                LabelSet::empty(),
            ),
            round_duration_us: Arc::new(Histogram::new(
                "gossip_round_duration_microseconds".into(),
                "Gossip round duration in microseconds".into(),
                &sub_millisecond_histogram_config(),
                LabelSet::empty(),
            )),
            push_duration_us: Arc::new(Histogram::new(
                "gossip_push_duration_microseconds".into(),
                "Gossip push (dissemination) duration in microseconds".into(),
                &sub_millisecond_histogram_config(),
                LabelSet::empty(),
            )),
            delta_entries_hist: Arc::new(Histogram::new(
                "gossip_delta_entries".into(),
                "Membership entries per gossip push delta".into(),
                &sub_millisecond_histogram_config(),
                LabelSet::empty(),
            )),
            watermarks: std::collections::HashMap::new(),
        }
    }

    /// Sets the connection pool for gRPC push calls.
    pub fn set_pool(&mut self, pool: Arc<ConnectionPool>) {
        self.pool = Some(pool);
    }

    /// Registers gossip counters with a metrics registrar.
    pub fn register_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        registrar.register_counter(self.messages_sent.clone());
        registrar.register_counter(self.messages_received.clone());
        registrar.register_counter(self.messages_dropped.clone());
        registrar.register_histogram(Arc::clone(&self.round_duration_us));
        registrar.register_histogram(Arc::clone(&self.push_duration_us));
        registrar.register_histogram(Arc::clone(&self.delta_entries_hist));
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
    /// Selects `k = min(fanout_k, alive-1)` random alive peers and
    /// pushes each a delta computed against that peer's watermark
    /// (ADR-0028 D4) — the historical fanout-all full-state push is
    /// replaced by bounded push-pull rounds.
    async fn on_gossip_tick(&mut self) {
        let start = std::time::Instant::now();

        let alive: Vec<_> = self
            .state
            .nodes
            .iter()
            .filter(|(id, e)| {
                (e.state == NodeState::Alive || e.state == NodeState::Suspect)
                    && *id != &self.node_id
            })
            .map(|(id, _)| id.clone())
            .collect();

        if alive.is_empty() {
            trace!("gossip tick: no alive peers to push to");
            return;
        }

        // Bounded fanout: k random peers per round (ADR-0028 D4). The
        // RNG is scoped so it drops before any await (ThreadRng is not
        // Send).
        let peers: Vec<NodeId> = {
            let mut rng = rand::thread_rng();
            let fanout = self.fanout_k.min(alive.len() as u8) as usize;
            alive
                .iter()
                .choose_multiple(&mut rng, fanout)
                .into_iter()
                .map(|p| (*p).clone())
                .collect()
        };

        for peer in &peers {
            let delta = self.build_delta_for(peer);
            if delta.changed.is_empty() {
                trace!(peer = %peer, "gossip tick: no new entries for peer");
                continue;
            }
            debug!(peer = %peer, changed = delta.changed.len(), "periodic gossip push");
            self.messages_sent.inc();
            self.delta_entries_hist.observe(delta.changed.len() as u64);
            self.handle_command(GossipCommand::Push { peer: peer.clone(), delta }).await;
        }
        self.round_duration_us.observe(start.elapsed().as_micros() as u64);
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
                // would otherwise block the select! loop and prevent
                // the ticker from firing, making failure detection stall.
                if let Some(ref pool) = self.pool {
                    let messages_dropped = self.messages_dropped.clone();
                    if let Some(entry) = self.state.nodes.get(&peer) {
                        let peer_addr = entry.address;
                        let pool = pool.clone();
                        let push_hist = Arc::clone(&self.push_duration_us);
                        let peer_clone = peer.clone();
                        let command_tx = self.command_tx.clone();
                        // Our version vector travels with the push so the
                        // peer can compute the pull (ADR-0028 D4).
                        let version_vector = self.version_vector_proto();

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
                                // ADR-0028 D3: attribution travels with
                                // the entry.
                                version: e.version,
                                origin: e.origin.to_string(),
                                grpc_address: e.grpc_address.to_string(),
                            })
                            .collect();

                        // The push is dissemination only (ADR-0028 D2):
                        // the failure detector's liveness signal is the
                        // real Probe RPC on the membership plane — the
                        // push-as-ping-proxy (DK-007) is removed.
                        tokio::spawn(async move {
                            let push_start = std::time::Instant::now();
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
                                        version_vector,
                                    };

                                    let stream = tokio_stream::iter(vec![msg]);
                                    match client.push(tonic::Request::new(stream)).await {
                                        Ok(response) => {
                                            let ack = response.into_inner();
                                            push_hist
                                                .observe(push_start.elapsed().as_micros() as u64);
                                            if ack.accepted {
                                                debug!(
                                                    peer = %peer_clone,
                                                    updated = ack.updated_entries,
                                                    "gossip push ack received"
                                                );
                                            }
                                            // The ack carries the peer's
                                            // pull + vector (ADR-0028 D4).
                                            let ack_delta = ack.delta.map(|list| GossipDelta {
                                                changed: list
                                                    .entries
                                                    .iter()
                                                    .map(entry_from_proto)
                                                    .collect(),
                                            });
                                            let ack_vector: std::collections::HashMap<_, _> = ack
                                                .version_vector
                                                .iter()
                                                .map(|(node, vv)| {
                                                    (
                                                        NodeId::new(node),
                                                        vv.versions
                                                            .iter()
                                                            .map(|(o, v)| (NodeId::new(o), *v))
                                                            .collect::<std::collections::HashMap<_, _>>(),
                                                    )
                                                })
                                                .collect();
                                            let _ = command_tx.try_send(GossipCommand::Ack {
                                                peer: peer_clone,
                                                delta: ack_delta
                                                    .unwrap_or(GossipDelta { changed: Vec::new() }),
                                                version_vector: ack_vector,
                                            });
                                        }
                                        Err(status) => {
                                            warn!(peer = %peer_clone, error = %status, "gossip push failed");
                                            messages_dropped.inc();
                                            push_hist
                                                .observe(push_start.elapsed().as_micros() as u64);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(peer = %peer_clone, error = %e, "failed to acquire channel for push");
                                    messages_dropped.inc();
                                }
                            }
                        });
                    }
                }
            }
            GossipCommand::Ack { peer, delta, version_vector } => {
                debug!(peer = %peer, changed = delta.changed.len(), "gossip ack: merging peer pull");
                self.messages_received.inc();
                self.merge_delta(&delta);
                // Advance the per-peer watermark: the peer's vector is
                // the highest (node, origin) version it has applied.
                self.watermarks.insert(peer, version_vector);
            }
            GossipCommand::ReceiveDelta { from, delta } => {
                debug!(from = %from, changed = delta.changed.len(), "received gossip delta");
                self.messages_received.inc();
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
    /// Conflict resolution (ADR-0028 D3): the incarnation is the
    /// outer gate — a higher incarnation always wins (rejoin
    /// authority, ADR-0022); a lower one is stale. At equal
    /// incarnation, the authority-class table orders the entry:
    /// the leaver's own Left/Leaving (4) > my detector's facts (3) >
    /// remote detector facts (2) > the target's own announcement (1);
    /// entries about SELF from another origin (0) are rejected. Same
    /// class + same origin is ordered by version; the same class from
    /// a different origin keeps the local entry. Nodes absent from
    /// local state (previously removed via Death) are only re-admitted
    /// at a strictly higher incarnation (F1d).
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

            let local_entry = self.state.nodes.get(&entry.node_id);

            let old_state = local_entry.map(|e| e.state).unwrap_or(NodeState::Alive);
            // Whether the node is new to the local gossip state — captured
            // before the insert so the event condition below can use it.
            let is_new_entry = local_entry.is_none();
            // Snapshot of the previous entry for change detection — a
            // higher-incarnation rejoin may keep the same state while
            // changing the address (ADR-0022), which must still propagate.
            let previous_entry = local_entry.cloned();

            // ADR-0028 D3: at equal incarnation, order by the
            // authority-class table. Higher class wins regardless of
            // version; within the same class and origin, higher version
            // wins; the same class with a different origin keeps the
            // local entry. Also reject re-adding a previously-removed
            // node (present in incarnations but absent from state.nodes)
            // at the same incarnation.
            if entry.incarnation == current_incarnation {
                if local_entry.is_none() && current_incarnation > Incarnation::new(0) {
                    trace!(
                        node_id = %entry.node_id,
                        incarnation = current_incarnation.value(),
                        "ignoring delta: node was removed and re-added at same incarnation"
                    );
                    continue;
                }
                if let Some(local) = local_entry {
                    let self_id = &self.node_id;
                    let incoming_class = crate::membership::authority_class(
                        &entry.node_id,
                        &entry.origin,
                        entry.state,
                        self_id,
                    );
                    let local_class = crate::membership::authority_class(
                        &entry.node_id,
                        &local.origin,
                        local.state,
                        self_id,
                    );
                    if incoming_class < local_class {
                        trace!(
                            node_id = %entry.node_id,
                            incoming = ?entry.state,
                            current = ?local.state,
                            "ignoring delta: incoming authority class below local"
                        );
                        continue;
                    }
                    if incoming_class == local_class {
                        if entry.origin == local.origin {
                            if entry.version <= local.version {
                                trace!(
                                    node_id = %entry.node_id,
                                    incoming = ?entry.state,
                                    current = ?local.state,
                                    "ignoring delta: same-origin entry not newer"
                                );
                                continue;
                            }
                        } else {
                            trace!(
                                node_id = %entry.node_id,
                                incoming = ?entry.state,
                                current = ?local.state,
                                "ignoring delta: equal authority class, different origin"
                            );
                            continue;
                        }
                    }
                }
            }

            // Update local state.
            self.state.nodes.insert(entry.node_id.clone(), entry.clone());
            self.incarnations.insert(entry.node_id.clone(), entry.incarnation);

            debug!(
                node_id = %entry.node_id,
                state = ?entry.state,
                incarnation = entry.incarnation.value(),
                "merge_delta: accepted entry"
            );

            // Emit a membership event when anything meaningful changed:
            // state, address, incarnation — or when the node is new.
            // ADR-0022: a strictly-higher-incarnation rejoin keeps the
            // state (Alive→Alive) but carries a fresh address; without
            // emission on address change, the membership manager never
            // learns the new address and hint delivery keeps dialing the
            // stale one (t21). New nodes must emit too: the gRPC push
            // path routes peer deltas through `merge_delta` (F1d), so a
            // brand-new joiner has no state transition.
            let changed = old_state != entry.state
                || is_new_entry
                || previous_entry.as_ref().is_some_and(|e| {
                    e.address != entry.address || e.incarnation != entry.incarnation
                });
            if changed {
                debug!(
                    node_id = %entry.node_id,
                    old = ?old_state,
                    new = ?entry.state,
                    "merge_delta: emitting membership event"
                );
                let _ = self.membership_event_tx.send(crate::membership::MembershipEvent {
                    node_id: entry.node_id.clone(),
                    old_state,
                    new_state: entry.state,
                    incarnation: entry.incarnation,
                    address: Some(entry.grpc_address),
                    version: entry.version,
                    origin: entry.origin.clone(),
                    membership_address: Some(entry.address),
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

    /// Builds the delta for a specific peer (ADR-0028 D4): only entries
    /// the peer has not acknowledged — per (node, origin), the version
    /// must exceed the peer's watermark, or the (node, origin) key is
    /// unknown to the peer.
    pub(crate) fn build_delta_for(&self, peer: &NodeId) -> GossipDelta {
        let watermark = self.watermarks.get(peer);
        let changed: Vec<_> = self
            .state
            .nodes
            .values()
            .filter(|e| {
                watermark
                    .and_then(|wm| wm.get(&e.node_id))
                    .and_then(|origins| origins.get(&e.origin))
                    .map_or(true, |known| e.version > *known)
            })
            .cloned()
            .collect();
        GossipDelta { changed }
    }

    /// The local version vector as protobuf (node → origin → version).
    pub(crate) fn version_vector_proto(
        &self,
    ) -> std::collections::HashMap<String, oceanfs_network::gossip::VersionVector> {
        let mut vector = std::collections::HashMap::new();
        for entry in self.state.nodes.values() {
            let vv = vector
                .entry(entry.node_id.to_string())
                .or_insert_with(oceanfs_network::gossip::VersionVector::default);
            vv.versions.insert(entry.origin.to_string(), entry.version);
        }
        vector
    }

    /// Returns a snapshot of the current gossip state.
    pub(crate) fn snapshot(&self) -> &GossipState {
        &self.state
    }

    /// Adds a node to the local state (typically on join).
    ///
    /// Enforces the F1d invariant: after a Dead removal, no path may
    /// re-apply an entry for a node id at an incarnation ≤ the recorded
    /// one. A stale re-add of a previously removed node is dropped;
    /// a stale update of a present node keeps the existing entry and
    /// never regresses the incarnation map (T8 monotonicity).
    pub(crate) fn add_node(&mut self, entry: NodeEntry) {
        let recorded =
            self.incarnations.get(&entry.node_id).copied().unwrap_or(Incarnation::new(0));

        if entry.incarnation < recorded {
            trace!(
                node_id = %entry.node_id,
                incoming = entry.incarnation.value(),
                recorded = recorded.value(),
                "add_node: dropping stale entry (incarnation below recorded)"
            );
            return;
        }
        if entry.incarnation == recorded
            && recorded > Incarnation::new(0)
            && !self.state.nodes.contains_key(&entry.node_id)
        {
            trace!(
                node_id = %entry.node_id,
                incarnation = entry.incarnation.value(),
                "add_node: dropping re-add of removed node at equal incarnation"
            );
            return;
        }

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

/// Converts a protobuf membership entry into the internal node entry.
fn entry_from_proto(entry: &oceanfs_core::proto::membership::MembershipEntry) -> NodeEntry {
    let node_id = entry
        .node_id
        .as_ref()
        .map(|nid| NodeId::new(&nid.id))
        .unwrap_or_else(|| NodeId::new("unknown"));
    let state = match entry.state {
        0 => NodeState::Alive,
        1 => NodeState::Suspect,
        2 => NodeState::Dead,
        3 => NodeState::Leaving,
        4 => NodeState::Left,
        _ => NodeState::Alive,
    };
    let address = entry
        .address
        .parse::<std::net::SocketAddr>()
        .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9001)));
    let grpc_address = entry
        .grpc_address
        .parse::<std::net::SocketAddr>()
        .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9001)));
    NodeEntry {
        node_id,
        incarnation: Incarnation::new(entry.incarnation),
        state,
        address,
        grpc_address,
        version: entry.version,
        origin: NodeId::new(&entry.origin),
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
        let (membership_tx, _membership_rx) = tokio::sync::broadcast::channel(16);
        GossipProtocol::new(
            cmd_rx,
            _cmd_tx.clone(),
            event_tx,
            membership_tx,
            1000, // gossip_interval_ms
            NodeId::new("test-node"),
            3, // fanout_k
        )
    }

    fn make_node_entry(id: &str, incarnation: u64, state: NodeState) -> NodeEntry {
        NodeEntry {
            node_id: NodeId::new(id),
            incarnation: Incarnation::new(incarnation),
            state,
            address: "127.0.0.1:9002".parse().unwrap(),
            grpc_address: "127.0.0.1:9001".parse().unwrap(),
            version: 1,
            // Pre-attribution helper default: the origin is the target
            // itself (its own announcement) unless a test overrides it.
            origin: NodeId::new(id),
        }
    }

    /// The fleet churn oscillation fix: a Suspect in a gossip delta at
    /// the SAME incarnation as a local ALIVE must be rejected — the
    /// local Alive was set by the detector's ping-verified recovery
    /// and the sender's Suspect predates it. Without this the deltas
    /// kept re-applying the Suspect (last-writer-wins), the pings kept
    /// recovering it, and the convergence check caught the Suspect
    /// moments.
    #[test]
    fn merge_delta_rejects_stale_suspect_over_ping_verified_alive() {
        let mut protocol = make_protocol();

        // The node was suspected and then RECOVERED locally (the
        // detector's ping-verified Alive at incarnation 9).
        protocol.add_node(make_node_entry("victim", 9, NodeState::Alive));

        // A peer's delta still carries the STALE Suspect at 9.
        let delta = GossipDelta { changed: vec![make_node_entry("victim", 9, NodeState::Suspect)] };
        protocol.merge_delta(&delta);

        let entry = protocol.state.nodes.get(&NodeId::new("victim")).unwrap();
        assert_eq!(
            entry.state,
            NodeState::Alive,
            "a stale equal-incarnation Suspect must not downgrade a ping-verified Alive"
        );

        // A Suspect at a HIGHER incarnation (the node genuinely died
        // again) still applies.
        let delta2 =
            GossipDelta { changed: vec![make_node_entry("victim", 10, NodeState::Suspect)] };
        protocol.merge_delta(&delta2);
        let entry2 = protocol.state.nodes.get(&NodeId::new("victim")).unwrap();
        assert_eq!(entry2.state, NodeState::Suspect);
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

    /// F1d: a node removed as Dead (present in `incarnations`, absent
    /// from `state.nodes`) must NOT be re-admitted at an equal
    /// incarnation by a gossip delta — this is the t24 oscillation
    /// source (stale Alive deltas from peers).
    #[test]
    fn merge_delta_rejects_readmission_of_removed_node_at_equal_incarnation() {
        let mut protocol = make_protocol();

        // Node known at incarnation 5.
        protocol.add_node(make_node_entry("victim", 5, NodeState::Alive));
        // Declared Dead at incarnation 5 via merge.
        protocol.merge_delta(&GossipDelta {
            changed: vec![make_node_entry("victim", 5, NodeState::Dead)],
        });
        // Remove it from the gossip state the way the membership state
        // does on Dead (the node is no longer in state.nodes but its
        // incarnation is recorded).
        protocol.state.nodes.remove(&NodeId::new("victim"));
        assert!(!protocol.state.nodes.contains_key(&NodeId::new("victim")));

        // A peer whose view still lists the node as Alive at inc 5
        // sends a delta — it must be rejected.
        protocol.merge_delta(&GossipDelta {
            changed: vec![make_node_entry("victim", 5, NodeState::Alive)],
        });

        assert!(
            !protocol.state.nodes.contains_key(&NodeId::new("victim")),
            "removed node must not be re-admitted at equal incarnation"
        );
    }

    /// ADR-0022 Decision 2 / F2c: an entry with a strictly higher
    /// incarnation is accepted even for a previously removed node,
    /// and it updates BOTH state and address (the rejoin carries the
    /// fresh address — t21/t43).
    #[test]
    fn merge_delta_accepts_higher_incarnation_with_updated_address() {
        use std::net::SocketAddr;

        let mut protocol = make_protocol();

        // Node known at incarnation 5, old address.
        protocol.add_node(make_node_entry("rejoiner", 5, NodeState::Alive));
        // Removed as Dead at incarnation 5.
        protocol.merge_delta(&GossipDelta {
            changed: vec![make_node_entry("rejoiner", 5, NodeState::Dead)],
        });
        protocol.state.nodes.remove(&NodeId::new("rejoiner"));

        // Self-rejoin: Alive at incarnation 6 with a NEW address.
        let new_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let mut rejoined = make_node_entry("rejoiner", 6, NodeState::Alive);
        rejoined.address = new_addr;
        protocol.merge_delta(&GossipDelta { changed: vec![rejoined] });

        let entry = protocol
            .state
            .nodes
            .get(&NodeId::new("rejoiner"))
            .unwrap_or_else(|| panic!("strictly-higher incarnation must re-admit the node"));
        assert_eq!(entry.state, NodeState::Alive);
        assert_eq!(entry.incarnation, Incarnation::new(6));
        assert_eq!(
            entry.address, new_addr,
            "rejoin must update the address so call sites resolve the fresh one"
        );
    }

    /// F2c: a higher-incarnation merge of a node that is already present
    /// updates its address (address churn without removal).
    #[test]
    fn merge_delta_higher_incarnation_updates_address_of_present_node() {
        use std::net::SocketAddr;

        let mut protocol = make_protocol();

        protocol.add_node(make_node_entry("churner", 2, NodeState::Alive));

        let new_addr: SocketAddr = "127.0.0.1:7777".parse().unwrap();
        let mut updated = make_node_entry("churner", 3, NodeState::Alive);
        updated.address = new_addr;
        protocol.merge_delta(&GossipDelta { changed: vec![updated] });

        let entry = protocol.state.nodes.get(&NodeId::new("churner")).unwrap();
        assert_eq!(entry.incarnation, Incarnation::new(3));
        assert_eq!(entry.address, new_addr);
    }

    /// F1d: `add_node` (the manager→gossip path) must not re-add a
    /// previously removed node at an equal incarnation, and must not
    /// regress the incarnation map on stale entries.
    #[test]
    fn add_node_enforces_readmission_invariant() {
        let mut protocol = make_protocol();

        protocol.add_node(make_node_entry("victim", 5, NodeState::Alive));
        protocol.add_node(make_node_entry("victim", 5, NodeState::Dead));
        protocol.state.nodes.remove(&NodeId::new("victim"));
        // Incarnation 5 recorded, node absent.

        // Stale Alive at equal incarnation via AddNode — rejected.
        protocol.add_node(make_node_entry("victim", 5, NodeState::Alive));
        assert!(
            !protocol.state.nodes.contains_key(&NodeId::new("victim")),
            "add_node must reject equal-incarnation re-admission"
        );

        // Stale entry at lower incarnation — dropped, map not regressed.
        protocol.add_node(make_node_entry("victim", 3, NodeState::Alive));
        assert_eq!(
            protocol.incarnations.get(&NodeId::new("victim")).copied(),
            Some(Incarnation::new(5)),
            "incarnation map must not regress"
        );

        // Strictly higher incarnation — accepted.
        protocol.add_node(make_node_entry("victim", 6, NodeState::Alive));
        assert!(protocol.state.nodes.contains_key(&NodeId::new("victim")));
    }

    /// A brand-new node must emit a membership event from `merge_delta`
    /// even though its state "transition" is Alive→Alive: the gRPC push
    /// path routes peer deltas through `merge_delta`, and the membership
    /// manager only learns new nodes from these events.
    #[test]
    fn merge_delta_emits_event_for_new_node() {
        let mut protocol = make_protocol();
        let mut membership_rx = protocol.membership_event_tx.subscribe();

        let delta = GossipDelta { changed: vec![make_node_entry("fresh", 1, NodeState::Alive)] };
        protocol.merge_delta(&delta);

        assert!(protocol.state.nodes.contains_key(&NodeId::new("fresh")));

        let mut emitted = false;
        while let Ok(event) = membership_rx.try_recv() {
            if event.node_id.as_str() == "fresh" && event.new_state == NodeState::Alive {
                assert_eq!(event.incarnation, Incarnation::new(1));
                emitted = true;
            }
        }
        assert!(emitted, "new node must emit a membership event");
    }

    /// F1d: a stale Alive delta from a peer whose view is behind the
    /// local Suspect must NOT clobber the Suspect (t24 oscillation).
    #[test]
    fn merge_delta_rejects_stale_alive_against_local_suspect() {
        let mut protocol = make_protocol();
        let mut membership_rx = protocol.membership_event_tx.subscribe();

        // Node known Alive at 1.
        protocol.add_node(make_node_entry("victim", 1, NodeState::Alive));
        let _ = membership_rx.try_recv(); // drain any event

        // Local view moves to Suspect (same incarnation).
        protocol.add_node(make_node_entry("victim", 1, NodeState::Suspect));

        // Peer pushes stale Alive at the same incarnation — rejected.
        protocol.merge_delta(&GossipDelta {
            changed: vec![make_node_entry("victim", 1, NodeState::Alive)],
        });

        let entry = protocol.state.nodes.get(&NodeId::new("victim")).unwrap();
        assert_eq!(
            entry.state,
            NodeState::Suspect,
            "stale Alive must not clobber the local Suspect"
        );

        // No Alive event may have been emitted for the victim.
        while let Ok(event) = membership_rx.try_recv() {
            assert_ne!(
                (event.node_id.as_str(), event.new_state),
                ("victim", NodeState::Alive),
                "stale Alive must not emit a membership event"
            );
        }
    }

    #[test]
    fn build_delta_returns_all_known_nodes() {
        let mut protocol = make_protocol();

        protocol.add_node(make_node_entry("a", 1, NodeState::Alive));
        protocol.add_node(make_node_entry("b", 1, NodeState::Alive));

        let delta = protocol.build_delta();
        assert_eq!(delta.changed.len(), 2);
    }

    /// ADR-0028 D4: `build_delta_for` excludes entries the peer's
    /// watermark already covers, per (node, origin).
    #[test]
    fn build_delta_for_excludes_watermarked_entries() {
        let mut protocol = make_protocol();
        let peer = NodeId::new("peer");

        // The applied entry: my detector's Suspect (origin test-node,
        // version 9) — it replaced the target's own earlier
        // announcement (origin victim, version 5).
        let mut suspected = make_node_entry("victim", 3, NodeState::Suspect);
        suspected.origin = NodeId::new("test-node");
        suspected.version = 9;
        protocol.add_node(suspected);

        // No watermark yet → included.
        assert_eq!(protocol.build_delta_for(&peer).changed.len(), 1);

        // The peer acked (victim → victim) at version 5 — the OLD
        // announcement's key. The detector's newer Suspect (victim →
        // test-node) is a DIFFERENT origin key → still included (a new
        // attributed fact must propagate even when the node's entry
        // version regresses relative to another origin's sequence).
        let mut watermark = std::collections::HashMap::new();
        let mut origins = std::collections::HashMap::new();
        origins.insert(NodeId::new("victim"), 5u64);
        watermark.insert(NodeId::new("victim"), origins);
        protocol.watermarks.insert(peer.clone(), watermark);

        let delta = protocol.build_delta_for(&peer);
        assert_eq!(delta.changed.len(), 1);
        assert_eq!(delta.changed[0].origin, NodeId::new("test-node"));

        // The peer acked the actual entry's key (victim → test-node)
        // at version 9 → covered → nothing to push.
        let mut watermark2 = std::collections::HashMap::new();
        let mut origins2 = std::collections::HashMap::new();
        origins2.insert(NodeId::new("test-node"), 9u64);
        watermark2.insert(NodeId::new("victim"), origins2);
        protocol.watermarks.insert(peer.clone(), watermark2);
        assert!(
            protocol.build_delta_for(&peer).changed.is_empty(),
            "the acked attributed entry must not be re-pushed"
        );
    }

    /// ADR-0028 D4 re-sync: a deliberately diverged peer vector (a node
    /// key it never acked) is healed by the next round — the entry the
    /// peer lacks is included in the delta for it until its vector
    /// covers the (node, origin) key. The push-pull round makes an
    /// explicit re-sync pull redundant: the ack's pull (computed from
    /// MY vector) covers the opposite direction every round.
    #[test]
    fn diverged_peer_vector_is_healed_by_the_next_delta() {
        let mut protocol = make_protocol();
        let peer = NodeId::new("peer");

        protocol.add_node(make_node_entry("heal-me", 2, NodeState::Alive));

        // The peer's vector covers OTHER nodes but is missing
        // "heal-me" entirely — a divergence.
        let mut vector = std::collections::HashMap::new();
        let mut origins = std::collections::HashMap::new();
        origins.insert(NodeId::new("peer"), 1u64);
        vector.insert(NodeId::new("peer"), origins);
        protocol.watermarks.insert(peer.clone(), vector);

        let delta = protocol.build_delta_for(&peer);
        assert_eq!(delta.changed.len(), 1, "the missing node must be re-sent");
        assert_eq!(delta.changed[0].node_id.as_str(), "heal-me");
    }

    /// ADR-0028 D4: an ack advances the watermark and the next delta
    /// for that peer excludes the acked entries.
    #[tokio::test]
    async fn ack_advances_watermark_and_prunes_next_delta() {
        let mut protocol = make_protocol();
        let peer = NodeId::new("peer");

        protocol.add_node(make_node_entry("victim", 3, NodeState::Alive));

        // Simulate the ack: the peer's vector covers (victim → victim)
        // at version 1.
        let mut vector = std::collections::HashMap::new();
        let mut origins = std::collections::HashMap::new();
        origins.insert(NodeId::new("victim"), 1u64);
        vector.insert(NodeId::new("victim"), origins);
        protocol
            .handle_command(GossipCommand::Ack {
                peer: peer.clone(),
                delta: GossipDelta { changed: Vec::new() },
                version_vector: vector,
            })
            .await;

        // The entry's version is 1 (make_node_entry default) → covered.
        assert!(
            protocol.build_delta_for(&peer).changed.is_empty(),
            "acked entries must not be re-pushed"
        );
    }

    /// ADR-0028 D4: the round pushes to at most `fanout_k` peers.
    #[tokio::test]
    async fn gossip_tick_respects_fanout_k() {
        let mut protocol = make_protocol();
        // 5 alive peers + fanout_k 3 (make_protocol default).
        for i in 0..5 {
            protocol.add_node(make_node_entry(&format!("peer-{i}"), 1, NodeState::Alive));
        }
        protocol.on_gossip_tick().await;
        assert_eq!(
            protocol.messages_sent.get(),
            3,
            "the round must push to exactly fanout_k peers"
        );
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

    // --- Metrics tests ---

    #[test]
    fn messages_dropped_starts_at_zero() {
        let protocol = make_protocol();
        assert_eq!(protocol.messages_dropped.get(), 0);
    }

    #[test]
    fn messages_dropped_increments() {
        let protocol = make_protocol();
        protocol.messages_dropped.inc();
        assert_eq!(protocol.messages_dropped.get(), 1);
    }

    #[test]
    fn messages_sent_and_received_still_work() {
        let protocol = make_protocol();
        assert_eq!(protocol.messages_sent.get(), 0);
        assert_eq!(protocol.messages_received.get(), 0);
        protocol.messages_sent.inc();
        protocol.messages_received.add(5);
        assert_eq!(protocol.messages_sent.get(), 1);
        assert_eq!(protocol.messages_received.get(), 5);
    }

    #[test]
    fn round_duration_histogram_created() {
        let protocol = make_protocol();
        let hist = &protocol.round_duration_us;
        assert_eq!(hist.count(), 0);
        assert_eq!(hist.sum(), 0);
        // Should not panic when observing.
        hist.observe(42);
        assert_eq!(hist.count(), 1);
    }

    #[test]
    fn register_metrics_includes_dropped_counter() {
        use oceanfs_core::MetricRegistrar;

        struct TestRegistrar {
            names: parking_lot::Mutex<Vec<String>>,
        }
        impl MetricRegistrar for TestRegistrar {
            fn register_counter(&self, counter: oceanfs_core::Counter) {
                self.names.lock().push(counter.name().to_string());
            }
            fn register_gauge(&self, _: oceanfs_core::Gauge) {}
            fn register_histogram(&self, _: std::sync::Arc<oceanfs_core::Histogram>) {}
        }

        let protocol = make_protocol();
        let reg = TestRegistrar { names: parking_lot::Mutex::new(Vec::new()) };

        protocol.register_metrics(&reg);

        let names = reg.names.lock();
        assert!(names.contains(&"gossip_messages_dropped_total".to_string()));
        assert!(names.contains(&"gossip_messages_sent_total".to_string()));
        assert!(names.contains(&"gossip_messages_received_total".to_string()));
    }
}
