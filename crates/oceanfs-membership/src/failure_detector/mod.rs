//! SWIM failure detector.
//!
//! Implements the SWIM failure detection algorithm:
//! 1. Direct ping: send a ping to a random peer
//! 2. If no ack within timeout: indirect ping via k random peers
//! 3. If still no ack: mark the peer SUSPECT
//! 4. After suspicion timeout: mark DEAD
//!
//! The detector runs as a background task on each gossip interval.

use std::time::Duration;

use oceanfs_core::{Incarnation, NodeId, NodeState};
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::membership::MembershipEvent;

mod ping;
mod suspicion;
mod types;

pub(crate) use types::{DetectorCommand, DetectorConfig, FailureDetector};

impl FailureDetector {
    /// Creates a new failure detector and returns a command sender.
    pub fn new(
        config: DetectorConfig,
        event_tx: tokio::sync::broadcast::Sender<MembershipEvent>,
        node_id: oceanfs_core::NodeId,
        incarnation: Incarnation,
        buffer: usize,
    ) -> (Self, mpsc::Sender<DetectorCommand>) {
        use std::collections::HashMap;

        use crate::grpc::probe_service::ProbeHandler;

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

    /// Looks up the current incarnation for a node from the alive list.
    ///
    /// Returns `None` if the node is not found in the alive nodes list.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let incarnation = detector.incarnation_for(&node_id);
    /// ```
    pub fn incarnation_for(&self, node_id: &NodeId) -> Option<Incarnation> {
        self.alive_nodes
            .iter()
            .find(|(id, _, _, _)| *id == *node_id)
            .map(|(_, _, _, incarnation)| *incarnation)
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
                    ping::on_ping_tick(self);
                    ping::check_ping_timeouts(self);
                    suspicion::check_suspicion_timers(self);
                }
            }
        }
    }

    /// Handles a ping result or other command.
    async fn handle_command(&mut self, cmd: DetectorCommand) -> bool {
        match cmd {
            DetectorCommand::PingResponse { target, success } => {
                self.pending_pings.remove(&target);
                if !success {
                    if !self.pending_indirect.contains_key(&target) {
                        debug!(
                            node_id = %target,
                            "direct ping failed, initiating indirect pings"
                        );
                        ping::initiate_indirect_pings(self, &target);
                    }
                } else {
                    debug!(
                        node_id = %target,
                        "direct ping succeeded — target is alive"
                    );
                    self.recover_suspect(&target);
                }
            }
            DetectorCommand::IndirectPingResult { origin: _origin, target, success } => {
                self.pending_indirect.remove(&target);
                if !success {
                    suspicion::mark_suspect(self, &target);
                } else {
                    debug!(
                        node_id = %target,
                        "indirect ping succeeded — target is alive"
                    );
                    self.recover_suspect(&target);
                }
            }
            DetectorCommand::UpdateAliveNodes { nodes } => {
                self.alive_nodes = nodes;
            }
            DetectorCommand::RemoveNode { node_id } => {
                // F1c: stop probing a node that was declared Dead/Left.
                // Without this the detector keeps failing pings against
                // the removed node and re-fires `node declared DEAD`
                // forever.
                let removed = self.alive_nodes.iter().any(|(id, _, _, _)| *id == node_id);
                self.alive_nodes.retain(|(id, _, _, _)| *id != node_id);
                self.suspicion_timers.remove(&node_id);
                self.pending_pings.remove(&node_id);
                self.pending_indirect.remove(&node_id);
                if removed {
                    debug!(node_id = %node_id, "detector: dropped node from probe set");
                }
            }
            DetectorCommand::UpdateSelfIncarnation { incarnation } => {
                self.probe_handler.set_incarnation(incarnation);
            }
            DetectorCommand::Shutdown => return false,
        }
        true
    }

    /// Recovers a Suspect node after a successful ping (F1b).
    ///
    /// Removing the suspicion timer alone left the node stuck in
    /// Suspect forever (t19): membership and gossip never learned
    /// the recovery. Emitting the Suspect→Alive event lets the
    /// membership manager re-admit the node as Alive.
    fn recover_suspect(&mut self, target: &NodeId) {
        if let Some((incarnation, _since)) = self.suspicion_timers.remove(target) {
            let _ = self.event_tx.send(MembershipEvent {
                node_id: target.clone(),
                old_state: NodeState::Suspect,
                new_state: NodeState::Alive,
                incarnation,
                address: None,
            });
            info!(node_id = %target, "node recovered: SUSPECT → ALIVE after successful ping");
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use oceanfs_core::{Incarnation, NodeId, NodeState};
    use tokio::sync::{broadcast, mpsc};

    use super::*;

    pub(crate) fn make_detector(
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

    #[tokio::test]
    async fn detector_shutdown_stops_gracefully() {
        let (mut detector, cmd_tx, _event_rx) = make_detector();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;
    }

    #[tokio::test]
    async fn indirect_ping_failure_emits_suspect_event() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();
        let target = NodeId::new("target-node");
        // The target must be known-Alive: a node that was never known-Alive
        // cannot be suspected (F1a).
        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Alive,
            "127.0.0.1:9000".parse().unwrap(),
            Incarnation::new(1),
        )];
        cmd_tx
            .send(DetectorCommand::IndirectPingResult {
                origin: NodeId::new("origin"),
                target: target.clone(),
                success: false,
            })
            .await
            .unwrap();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;
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
        while let Ok(event) = event_rx.try_recv() {
            assert_ne!(event.new_state, NodeState::Suspect);
        }
    }

    /// F1a: a ping failure for a node that is NOT in `alive_nodes`
    /// (new joiner whose AddNode is pending, or already removed as
    /// Dead) must not create a suspicion timer or emit a Suspect event.
    #[tokio::test]
    async fn ping_failure_for_unknown_node_does_not_mark_suspect() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();
        let target = NodeId::new("unknown-target");

        cmd_tx
            .send(DetectorCommand::IndirectPingResult {
                origin: NodeId::new("origin"),
                target: target.clone(),
                success: false,
            })
            .await
            .unwrap();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        assert!(
            !detector.suspicion_timers.contains_key(&target),
            "no suspicion timer for a node that was never known-Alive"
        );
        while let Ok(event) = event_rx.try_recv() {
            assert_ne!(
                (event.node_id.clone(), event.new_state),
                (target.clone(), NodeState::Suspect),
                "no Suspect event for an unknown node"
            );
        }
    }

    /// F1b: a successful indirect ping of a node currently in Suspect
    /// must emit a Suspect→Alive recovery event (in addition to
    /// removing the timer) so membership and gossip reflect it.
    #[tokio::test]
    async fn successful_ping_recovers_suspect_to_alive() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();
        let target = NodeId::new("target-node");
        let incarnation = Incarnation::new(5);

        // Node is known-Alive at incarnation 5, then marked Suspect.
        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Alive,
            "127.0.0.1:9000".parse().unwrap(),
            incarnation,
        )];
        cmd_tx
            .send(DetectorCommand::IndirectPingResult {
                origin: NodeId::new("origin"),
                target: target.clone(),
                success: false,
            })
            .await
            .unwrap();
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

        assert!(
            !detector.suspicion_timers.contains_key(&target),
            "suspicion timer must be removed on recovery"
        );
        let mut recovered = false;
        while let Ok(event) = event_rx.try_recv() {
            if event.node_id == target
                && event.old_state == NodeState::Suspect
                && event.new_state == NodeState::Alive
            {
                assert_eq!(event.incarnation, incarnation);
                recovered = true;
            }
        }
        assert!(recovered, "expected SUSPECT → ALIVE recovery event");
    }

    /// F1c: `RemoveNode` drops the node from the probe set, suspicion
    /// timers, and pending ping tracking.
    #[tokio::test]
    async fn remove_node_command_drops_from_probe_set() {
        let (mut detector, cmd_tx, _event_rx) = make_detector();
        let target = NodeId::new("dead-node");

        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Alive,
            "127.0.0.1:9000".parse().unwrap(),
            Incarnation::new(1),
        )];
        detector
            .suspicion_timers
            .insert(target.clone(), (Incarnation::new(1), std::time::Instant::now()));
        detector.pending_pings.insert(target.clone(), std::time::Instant::now());

        cmd_tx.send(DetectorCommand::RemoveNode { node_id: target.clone() }).await.unwrap();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        assert!(detector.alive_nodes.is_empty(), "dead node must leave alive_nodes");
        assert!(!detector.suspicion_timers.contains_key(&target));
        assert!(!detector.pending_pings.contains_key(&target));
    }

    /// F1c (detector side): declaring a node Dead must drop it from the
    /// probe structures immediately, not at the next alive-nodes sync.
    #[test]
    fn suspicion_expiry_drops_node_from_probe_structures() {
        let (mut detector, _cmd_tx, _event_rx) = make_detector();
        let target = NodeId::new("target-node");

        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Alive,
            "127.0.0.1:9000".parse().unwrap(),
            Incarnation::new(3),
        )];
        let past = std::time::Instant::now().checked_sub(Duration::from_millis(200)).unwrap();
        detector.suspicion_timers.insert(target.clone(), (Incarnation::new(3), past));
        detector.pending_pings.insert(target.clone(), std::time::Instant::now());

        suspicion::check_suspicion_timers(&mut detector);

        assert!(detector.alive_nodes.is_empty(), "dead node must leave alive_nodes");
        assert!(!detector.suspicion_timers.contains_key(&target));
        assert!(!detector.pending_pings.contains_key(&target));
    }

    /// F2: `UpdateSelfIncarnation` keeps the probe handler's incarnation
    /// in sync with the announced rejoin value.
    #[tokio::test]
    async fn update_self_incarnation_updates_probe_handler() {
        let (mut detector, cmd_tx, _event_rx) = make_detector();

        cmd_tx
            .send(DetectorCommand::UpdateSelfIncarnation { incarnation: Incarnation::new(9) })
            .await
            .unwrap();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        assert_eq!(detector.probe_handler.incarnation(), Incarnation::new(9));
    }

    #[tokio::test]
    async fn suspicion_timer_expiry_emits_dead_event() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();
        let target = NodeId::new("target-node");
        // Known-Alive node first (F1a requires it for suspicion).
        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Alive,
            "127.0.0.1:9000".parse().unwrap(),
            Incarnation::new(1),
        )];
        cmd_tx
            .send(DetectorCommand::IndirectPingResult {
                origin: NodeId::new("origin"),
                target: target.clone(),
                success: false,
            })
            .await
            .unwrap();
        {
            let past = std::time::Instant::now().checked_sub(Duration::from_millis(200)).unwrap();
            detector.suspicion_timers.insert(target.clone(), (Incarnation::new(1), past));
        }
        suspicion::check_suspicion_timers(&mut detector);
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;
        let mut found = false;
        while let Ok(event) = event_rx.try_recv() {
            if event.node_id == target && event.new_state == NodeState::Dead {
                found = true;
                break;
            }
        }
        assert!(found, "expected DEAD event for target-node after suspicion timeout");
    }

    #[test]
    fn incarnation_for_returns_incarnation_when_node_in_alive_list() {
        let (mut detector, _cmd_tx, _event_rx) = make_detector();
        let target = NodeId::new("target-node");
        let incarnation = Incarnation::new(5);

        // Populate alive_nodes with the target and its incarnation.
        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Alive,
            "127.0.0.1:9000".parse().unwrap(),
            incarnation,
        )];

        assert_eq!(detector.incarnation_for(&target), Some(incarnation));
    }

    #[test]
    fn incarnation_for_returns_none_when_node_not_in_alive_list() {
        let (detector, _cmd_tx, _event_rx) = make_detector();
        let unknown = NodeId::new("unknown-node");
        assert_eq!(detector.incarnation_for(&unknown), None);
    }

    #[test]
    fn incarnation_for_returns_correct_value_with_multiple_nodes() {
        let (mut detector, _cmd_tx, _event_rx) = make_detector();
        let a = NodeId::new("node-a");
        let b = NodeId::new("node-b");
        let inc_a = Incarnation::new(3);
        let inc_b = Incarnation::new(7);

        detector.alive_nodes = vec![
            (a.clone(), NodeState::Alive, "127.0.0.1:9001".parse().unwrap(), inc_a),
            (b.clone(), NodeState::Alive, "127.0.0.1:9002".parse().unwrap(), inc_b),
        ];

        assert_eq!(detector.incarnation_for(&a), Some(inc_a));
        assert_eq!(detector.incarnation_for(&b), Some(inc_b));
    }

    /// End-to-end test: node X in alive_nodes with incarnation 5,
    /// then mark_suspect via indirect ping failure — assert the
    /// suspicion timer carries incarnation 5, not fallback 1.
    #[tokio::test]
    async fn mark_suspect_uses_incarnation_from_alive_nodes() {
        let (mut detector, cmd_tx, _event_rx) = make_detector();
        let target = NodeId::new("target-node");
        let incarnation = Incarnation::new(5);

        // Populate alive_nodes with the target and incarnation 5.
        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Alive,
            "127.0.0.1:9000".parse().unwrap(),
            incarnation,
        )];

        // Trigger an indirect ping failure, which calls mark_suspect.
        cmd_tx
            .send(DetectorCommand::IndirectPingResult {
                origin: NodeId::new("origin"),
                target: target.clone(),
                success: false,
            })
            .await
            .unwrap();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        // Assert the suspicion timer uses incarnation 5 from alive_nodes.
        let timer = detector.suspicion_timers.get(&target);
        assert!(
            timer.is_some(),
            "target should have a suspicion timer after indirect ping failure"
        );
        assert_eq!(
            timer.unwrap().0,
            Incarnation::new(5),
            "suspicion timer incarnation should be 5 (from alive_nodes), not fallback 1"
        );
    }
}
