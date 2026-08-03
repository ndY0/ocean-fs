//! SWIM failure detector.
//!
//! Implements the SWIM failure detection algorithm:
//! 1. Direct ping: send a ping to a random peer
//! 2. If no ack within timeout: indirect ping via k random peers
//! 3. If still no ack: mark the peer SUSPECT
//! 4. After suspicion timeout: mark DEAD
//!
//! The detector runs as a background task on each gossip interval.

use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use oceanfs_core::{proto::membership::ProbeRequest, Incarnation, NodeId, NodeState};
use rand::seq::IteratorRandom;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

use crate::{grpc::probe_service::ProbeHandler, membership::MembershipEvent};

/// Configuration for the failure detector.
#[derive(Debug, Clone)]
pub(crate) struct DetectorConfig {
    /// Interval between ping rounds in milliseconds.
    pub interval_ms: u64,
    /// Timeout for a direct ping response.
    pub ping_timeout_ms: u64,
    /// Time in SUSPECT state before declaring DEAD.
    pub suspicion_timeout_ms: u64,
    /// Total timeout before declaring DEAD (from initial ping).
    pub failure_timeout_ms: u64,
    /// Number of peers to route indirect pings through.
    pub indirect_ping_count: u8,
}

/// Internal command to the failure detector task.
#[derive(Debug)]
pub(crate) enum DetectorCommand {
    /// A direct ping result.
    PingResponse { target: NodeId, success: bool },
    /// An indirect ping result.
    IndirectPingResult { origin: NodeId, target: NodeId, success: bool },
    /// Update the list of alive nodes for periodic ping selection.
    UpdateAliveNodes { nodes: Vec<(NodeId, NodeState, SocketAddr)> },
    /// Shut down the detector.
    Shutdown,
}

/// The failure detector task.
///
/// Runs a loop: every `interval_ms`, picks a random alive peer,
/// sends a direct ping, and handles timeout/indirect ping logic.
pub(crate) struct FailureDetector {
    /// Receiver for ping results.
    rx: mpsc::Receiver<DetectorCommand>,
    /// Sender for membership events (state transitions).
    event_tx: tokio::sync::broadcast::Sender<MembershipEvent>,
    /// Detector configuration.
    config: DetectorConfig,
    /// Current suspicion timers: node_id → (incarnation, suspect_since).
    suspicion_timers: HashMap<NodeId, (Incarnation, Instant)>,
    /// This node's identifier.
    node_id: NodeId,
    /// List of alive nodes: (node_id, state, address).
    alive_nodes: Vec<(NodeId, NodeState, SocketAddr)>,
    /// Probe handler for in-process self-pings.
    probe_handler: ProbeHandler,
    /// Pending direct pings: target → ping_start_time.
    pending_pings: HashMap<NodeId, Instant>,
    /// Pending indirect pings: target → (origin, ping_start_time).
    pending_indirect: HashMap<NodeId, (NodeId, Instant)>,
}

impl FailureDetector {
    /// Creates a new failure detector and returns a command sender.
    pub fn new(
        config: DetectorConfig,
        event_tx: tokio::sync::broadcast::Sender<MembershipEvent>,
        node_id: NodeId,
        incarnation: Incarnation,
        buffer: usize,
    ) -> (Self, mpsc::Sender<DetectorCommand>) {
        let (tx, rx) = mpsc::channel(buffer);
        (
            Self {
                rx,
                event_tx,
                config,
                suspicion_timers: HashMap::new(),
                node_id: node_id.clone(),
                alive_nodes: Vec::new(),
                probe_handler: ProbeHandler::new(node_id, incarnation),
                pending_pings: HashMap::new(),
                pending_indirect: HashMap::new(),
            },
            tx,
        )
    }

    /// Runs the failure detector loop.
    ///
    /// This should be spawned as a background task. It handles ping
    /// responses, suspicion timeout expiry, and initiates periodic
    /// SWIM pings to random alive peers.
    pub async fn run(&mut self) {
        let mut ticker = tokio::time::interval(Duration::from_millis(self.config.interval_ms));
        // Don't fire immediately — wait for the first interval.
        ticker.tick().await;

        loop {
            tokio::select! {
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            if !self.handle_command(cmd).await {
                                break; // Shutdown.
                            }
                        }
                        None => break, // Channel closed.
                    }
                }
                _ = ticker.tick() => {
                    // Interval tick: initiate ping cycle, check timeouts.
                    self.on_ping_tick();
                    self.check_ping_timeouts();
                    self.check_suspicion_timers();
                }
            }
        }
    }

    /// Called on each SWIM interval tick.
    ///
    /// Selects a random alive peer (excluding self), sends a direct
    /// ping, and registers a pending ping for timeout tracking.
    fn on_ping_tick(&mut self) {
        // Filter alive nodes that are not self and not already pending.
        let target = {
            let alive: Vec<_> = self
                .alive_nodes
                .iter()
                .filter(|(id, state, _)| {
                    *state == NodeState::Alive
                        && *id != self.node_id
                        && !self.pending_pings.contains_key(id)
                })
                .map(|(id, _, _)| id)
                .collect();

            if alive.is_empty() {
                trace!("SWIM tick: no alive peers to ping");
                return;
            }

            let mut rng = rand::thread_rng();
            match alive.iter().choose(&mut rng) {
                Some(p) => (*p).clone(),
                None => return,
            }
        };

        debug!(target = %target, "SWIM: initiating direct ping");

        // Build a probe request and process it in-process for now.
        // For remote targets, a full implementation would send via gRPC.
        let request = ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: target.to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: self.node_id.to_string() }),
            is_indirect: false,
        };

        let response = self.probe_handler.handle_probe(&request);

        if response.ack {
            // Target is self — in-process ack.
            debug!(target = %target, "SWIM: self-ping ack received");
            // Nothing to do — self is always alive.
        } else {
            // Remote target — register pending ping for timeout tracking.
            // In a full implementation, this would send a gRPC Probe request.
            self.pending_pings.insert(target, Instant::now());
        }
    }

    /// Checks pending direct pings for timeout.
    ///
    /// If a direct ping has been pending longer than `ping_timeout_ms`,
    /// initiates indirect pings or marks the target as SUSPECT.
    fn check_ping_timeouts(&mut self) {
        let timeout = Duration::from_millis(self.config.ping_timeout_ms);
        let now = Instant::now();

        let timed_out: Vec<NodeId> = self
            .pending_pings
            .iter()
            .filter(|(_, start)| now.duration_since(**start) >= timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for target in timed_out {
            self.pending_pings.remove(&target);
            debug!(target = %target, "SWIM: direct ping timed out");
            self.initiate_indirect_pings(&target);
        }

        // Check indirect ping timeouts.
        let indirect_timeout = Duration::from_millis(self.config.ping_timeout_ms);
        let indirect_timed_out: Vec<NodeId> = self
            .pending_indirect
            .iter()
            .filter(|(_, (_, start))| now.duration_since(*start) >= indirect_timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for target in indirect_timed_out {
            self.pending_indirect.remove(&target);
            warn!(target = %target, "SWIM: all indirect pings timed out — marking SUSPECT");
            self.mark_suspect(&target);
        }
    }

    /// Initiates indirect pings for a target whose direct ping failed.
    ///
    /// Selects k random alive peers (excluding self and target) as relays.
    /// If no relays are available, marks the target SUSPECT immediately.
    fn initiate_indirect_pings(&mut self, target: &NodeId) {
        let indirect_candidates: Vec<_> = self
            .alive_nodes
            .iter()
            .filter(|(id, state, _)| {
                *state == NodeState::Alive && *id != self.node_id && *id != *target
            })
            .map(|(id, _, _)| id.clone())
            .collect();

        let mut rng = rand::thread_rng();
        let indirect_count = self.config.indirect_ping_count as usize;
        let indirect_targets: Vec<_> = indirect_candidates
            .iter()
            .choose_multiple(&mut rng, indirect_count.min(indirect_candidates.len()))
            .into_iter()
            .cloned()
            .collect();

        if indirect_targets.is_empty() {
            // No indirect peers available — mark suspect immediately.
            self.mark_suspect(target);
        } else {
            for relay in &indirect_targets {
                debug!(target = %target, relay = %relay, "SWIM: initiating indirect ping");
                self.pending_indirect.insert(target.clone(), (relay.clone(), Instant::now()));
            }
        }
    }

    /// Handles a ping result or other command.
    async fn handle_command(&mut self, cmd: DetectorCommand) -> bool {
        match cmd {
            DetectorCommand::PingResponse { target, success } => {
                // Remove from pending pings.
                self.pending_pings.remove(&target);
                if !success {
                    // Only initiate indirect pings if we're not already
                    // waiting on one for this target (avoids resetting the
                    // timeout on every repeated failure).
                    if !self.pending_indirect.contains_key(&target) {
                        debug!(node_id = %target, "direct ping failed, initiating indirect pings");
                        self.initiate_indirect_pings(&target);
                    }
                } else {
                    debug!(node_id = %target, "direct ping succeeded — target is alive");
                    self.suspicion_timers.remove(&target);
                }
            }
            DetectorCommand::IndirectPingResult { origin: _origin, target, success } => {
                // Remove from pending indirect pings.
                self.pending_indirect.remove(&target);
                if !success {
                    // All indirect pings failed — mark suspect.
                    self.mark_suspect(&target);
                } else {
                    debug!(node_id = %target, "indirect ping succeeded — target is alive");
                    self.suspicion_timers.remove(&target);
                }
            }
            DetectorCommand::UpdateAliveNodes { nodes } => {
                self.alive_nodes = nodes;
            }
            DetectorCommand::Shutdown => return false,
        }
        true
    }

    /// Marks a node as SUSPECT and starts the suspicion timer.
    pub(crate) fn mark_suspect(&mut self, node_id: &NodeId) {
        let now = Instant::now();
        let incarnation = Incarnation::new(1); // TODO: track actual incarnation.

        self.suspicion_timers.insert(node_id.clone(), (incarnation, now));

        let _ = self.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Alive,
            new_state: NodeState::Suspect,
        });

        info!(node_id = %node_id, "node marked SUSPECT");
    }

    /// Checks all suspicion timers and transitions expired ones to DEAD.
    pub(crate) fn check_suspicion_timers(&mut self) {
        let now = Instant::now();
        let suspicion_duration = Duration::from_millis(self.config.suspicion_timeout_ms);

        let mut expired = Vec::new();
        for (node_id, (_incarnation, suspect_since)) in &self.suspicion_timers {
            if now.duration_since(*suspect_since) >= suspicion_duration {
                expired.push(node_id.clone());
            }
        }

        for node_id in expired {
            self.suspicion_timers.remove(&node_id);
            let _ = self.event_tx.send(MembershipEvent {
                node_id: node_id.clone(),
                old_state: NodeState::Suspect,
                new_state: NodeState::Dead,
            });
            warn!(node_id = %node_id, "node declared DEAD (suspicion timeout)");
        }
    }

    /// Selects a random alive peer from the given list.
    pub(crate) fn select_random_peer<'a>(
        nodes: &'a [(NodeId, NodeState)],
        exclude: Option<&NodeId>,
    ) -> Option<&'a NodeId> {
        let alive: Vec<_> = nodes
            .iter()
            .filter(|(id, state)| {
                #[allow(clippy::unnecessary_map_or)]
                {
                    *state == NodeState::Alive && exclude.map_or(true, |ex| id != ex)
                }
            })
            .map(|(id, _)| id)
            .collect();

        let mut rng = rand::thread_rng();
        alive.iter().choose(&mut rng).copied()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tokio::sync::{broadcast, mpsc};

    use super::*;

    fn make_detector(
    ) -> (FailureDetector, mpsc::Sender<DetectorCommand>, broadcast::Receiver<MembershipEvent>)
    {
        let (event_tx, event_rx) = broadcast::channel(16);
        let config = DetectorConfig {
            interval_ms: 100,
            ping_timeout_ms: 50,
            suspicion_timeout_ms: 100,
            failure_timeout_ms: 5000,
            indirect_ping_count: 3,
        };
        let node_id = NodeId::new("test-node");
        let incarnation = Incarnation::new(1);
        let (detector, cmd_tx) = FailureDetector::new(config, event_tx, node_id, incarnation, 8);
        (detector, cmd_tx, event_rx)
    }

    #[test]
    fn select_random_peer_returns_alive_node() {
        let nodes = vec![
            (NodeId::new("a"), NodeState::Alive),
            (NodeId::new("b"), NodeState::Dead),
            (NodeId::new("c"), NodeState::Alive),
        ];
        let result = FailureDetector::select_random_peer(&nodes, None);
        assert!(result.is_some());
        let id = result.unwrap();
        assert!(id.as_str() == "a" || id.as_str() == "c");
    }

    #[test]
    fn select_random_peer_excludes_specified_node() {
        let nodes =
            vec![(NodeId::new("a"), NodeState::Alive), (NodeId::new("b"), NodeState::Alive)];
        let result = FailureDetector::select_random_peer(&nodes, Some(&NodeId::new("a")));
        assert_eq!(result.unwrap().as_str(), "b");
    }

    #[test]
    fn select_random_peer_returns_none_when_all_dead() {
        let nodes = vec![(NodeId::new("a"), NodeState::Dead), (NodeId::new("b"), NodeState::Dead)];
        assert!(FailureDetector::select_random_peer(&nodes, None).is_none());
    }

    #[test]
    fn select_random_peer_returns_none_when_all_excluded() {
        let nodes = vec![(NodeId::new("a"), NodeState::Alive)];
        assert!(FailureDetector::select_random_peer(&nodes, Some(&NodeId::new("a"))).is_none());
    }

    #[tokio::test]
    async fn detector_shutdown_stops_gracefully() {
        let (mut detector, cmd_tx, _event_rx) = make_detector();
        // Send shutdown.
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        // Run should exit.
        detector.run().await;
    }

    #[tokio::test]
    async fn indirect_ping_failure_emits_suspect_event() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();

        let target = NodeId::new("target-node");

        // Send indirect ping failure.
        cmd_tx
            .send(DetectorCommand::IndirectPingResult {
                origin: NodeId::new("origin"),
                target: target.clone(),
                success: false,
            })
            .await
            .unwrap();

        // Run one iteration.
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        // Check that a SUSPECT event was emitted.
        let mut found = false;
        while let Ok(event) = event_rx.try_recv() {
            if event.node_id == target && event.new_state == NodeState::Suspect {
                found = true;
                break;
            }
        }
        assert!(found, "expected SUSPECT event for target-node");
    }

    #[tokio::test]
    async fn successful_indirect_ping_does_not_emit_suspect() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();

        let target = NodeId::new("target-node");

        // Send successful indirect ping.
        cmd_tx
            .send(DetectorCommand::IndirectPingResult {
                origin: NodeId::new("origin"),
                target: target.clone(),
                success: true,
            })
            .await
            .unwrap();

        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        // No SUSPECT event should have been emitted.
        while let Ok(event) = event_rx.try_recv() {
            assert_ne!(event.new_state, NodeState::Suspect);
        }
    }

    #[tokio::test]
    async fn suspicion_timer_expiry_emits_dead_event() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();

        let target = NodeId::new("target-node");

        // First, mark the node SUSPECT via indirect ping failure.
        cmd_tx
            .send(DetectorCommand::IndirectPingResult {
                origin: NodeId::new("origin"),
                target: target.clone(),
                success: false,
            })
            .await
            .unwrap();

        // Run one iteration to process the SUSPECT.
        // Then manually insert an expired suspicion timer.
        {
            let past = std::time::Instant::now().checked_sub(Duration::from_millis(200)).unwrap();
            detector.suspicion_timers.insert(target.clone(), (Incarnation::new(1), past));
        }

        // Run the timeout check.
        detector.check_suspicion_timers();

        // Send shutdown.
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        // Should have emitted a DEAD event.
        let mut found = false;
        while let Ok(event) = event_rx.try_recv() {
            if event.node_id == target && event.new_state == NodeState::Dead {
                found = true;
                break;
            }
        }
        assert!(found, "expected DEAD event for target-node after suspicion timeout");
    }
}
