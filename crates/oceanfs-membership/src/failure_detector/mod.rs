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

use oceanfs_core::Incarnation;
use tokio::sync::mpsc;
use tracing::debug;

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
                    self.suspicion_timers.remove(&target);
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

    #[tokio::test]
    async fn suspicion_timer_expiry_emits_dead_event() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();
        let target = NodeId::new("target-node");
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
}
