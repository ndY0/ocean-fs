//! SWIM failure detector.
//!
//! Implements the SWIM failure detection algorithm (ADR-0028 D2):
//! 1. Direct probe: send a real probe RPC to a random peer over the
//!    membership plane
//! 2. If no ack within `ping_timeout_ms`: indirect probes via k random
//!    relays (each relay forwards to the target and relays the ack)
//! 3. If still no ack: mark the peer SUSPECT
//! 4. After suspicion timeout: mark DEAD
//!
//! The detector runs as a background task on each gossip interval.

use std::time::Duration;

use oceanfs_core::{NodeId, NodeState};
use tracing::{debug, info};

use crate::membership::MembershipEvent;

mod ping;
mod suspicion;
mod types;

pub(crate) use types::{DetectorCommand, DetectorConfig, FailureDetector, ProbeMetrics};

impl FailureDetector {
    /// Runs the failure detector loop.
    ///
    /// This should be spawned as a background task. It handles probe
    /// verdicts, suspicion timeout expiry, and initiates periodic SWIM
    /// probe cycles to random alive peers.
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
                    // Interval tick: initiate a probe cycle, check
                    // suspicion timers.
                    ping::on_ping_tick(self);
                    suspicion::check_suspicion_timers(self);
                }
            }
        }
    }

    /// Handles a probe verdict or other command.
    async fn handle_command(&mut self, cmd: DetectorCommand) -> bool {
        match cmd {
            DetectorCommand::PingResponse { target, success } => {
                self.pending_probes.remove(&target);
                if success {
                    debug!(
                        node_id = %target,
                        "probe cycle succeeded — target is alive"
                    );
                    self.recover_suspect(&target);
                } else {
                    debug!(
                        node_id = %target,
                        "probe cycle failed — marking suspect"
                    );
                    suspicion::mark_suspect(self, &target);
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
                self.pending_probes.remove(&node_id);
                if removed {
                    debug!(node_id = %node_id, "detector: dropped node from probe set");
                }
            }
            DetectorCommand::SetPool { pool } => {
                debug!("detector: membership plane pool set");
                self.pool = Some(pool);
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
    ///
    /// The event must fire even WITHOUT a pending timer: a Suspect
    /// state can arrive via GOSSIP deltas (equal incarnation,
    /// last-writer-wins) and creates no timer here — with the old
    /// guard the successful pings (every interval!) never cleared it
    /// and the node stayed Suspect forever (the fleet churn
    /// convergence failures: node stuck Suspect on the peers through
    /// the settle). Every successful ping is authoritative: the node
    /// is reachable, so any Suspect must clear.
    fn recover_suspect(&mut self, target: &NodeId) {
        let incarnation = self
            .suspicion_timers
            .remove(target)
            .map(|(i, _)| i)
            .or_else(|| self.incarnation_for(target));
        if let Some(incarnation) = incarnation {
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
        let (detector, cmd_tx) = FailureDetector::new(config, event_tx, node_id, 8, None);
        (detector, cmd_tx, event_rx)
    }

    #[tokio::test]
    async fn detector_shutdown_stops_gracefully() {
        let (mut detector, cmd_tx, _event_rx) = make_detector();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;
    }

    #[tokio::test]
    async fn probe_cycle_failure_emits_suspect_event() {
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
            .send(DetectorCommand::PingResponse { target: target.clone(), success: false })
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
    async fn successful_probe_does_not_emit_suspect() {
        let (mut detector, cmd_tx, mut event_rx) = make_detector();
        let target = NodeId::new("target-node");
        cmd_tx
            .send(DetectorCommand::PingResponse { target: target.clone(), success: true })
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
            .send(DetectorCommand::PingResponse { target: target.clone(), success: false })
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
            .send(DetectorCommand::PingResponse { target: target.clone(), success: false })
            .await
            .unwrap();
        cmd_tx
            .send(DetectorCommand::PingResponse { target: target.clone(), success: true })
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
        detector.pending_probes.insert(target.clone(), std::time::Instant::now());

        cmd_tx.send(DetectorCommand::RemoveNode { node_id: target.clone() }).await.unwrap();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        assert!(detector.alive_nodes.is_empty(), "dead node must leave alive_nodes");
        assert!(!detector.suspicion_timers.contains_key(&target));
        assert!(!detector.pending_probes.contains_key(&target));
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
        detector.pending_probes.insert(target.clone(), std::time::Instant::now());

        suspicion::check_suspicion_timers(&mut detector);

        assert!(detector.alive_nodes.is_empty(), "dead node must leave alive_nodes");
        assert!(!detector.suspicion_timers.contains_key(&target));
        assert!(!detector.pending_probes.contains_key(&target));
    }

    /// ADR-0028 D1: the detector accepts the membership plane's pool via
    /// a command when it arrives after `start()`.
    #[tokio::test]
    async fn set_pool_command_wires_the_plane_pool() {
        let (mut detector, cmd_tx, _event_rx) = make_detector();
        assert!(detector.pool.is_none());

        cmd_tx
            .send(DetectorCommand::SetPool {
                pool: std::sync::Arc::new(oceanfs_network::ConnectionPool::new(
                    oceanfs_core::RpcConfig::default(),
                )),
            })
            .await
            .unwrap();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        assert!(detector.pool.is_some(), "SetPool must wire the plane pool");
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
            .send(DetectorCommand::PingResponse { target: target.clone(), success: false })
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

    /// The stale-suspicion cancellation (fleet churn fix): a suspicion
    /// timer started at incarnation 4 must NOT declare DEAD when the
    /// node has since re-announced (rejoined) at incarnation 5 — the
    /// fresh Alive supersedes the pending suspicion.
    #[test]
    fn stale_suspicion_cancelled_when_node_reannounced_at_higher_incarnation() {
        let (mut detector, _cmd_tx, mut event_rx) = make_detector();
        let target = NodeId::new("target-node");
        // The node was killed (suspicion started at inc 4) and has
        // since rejoined at inc 5 — the detector's synced view.
        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Alive,
            "127.0.0.1:9000".parse().unwrap(),
            Incarnation::new(5),
        )];
        // Pending suspicion with the STALE incarnation 4, expired.
        let past = std::time::Instant::now().checked_sub(Duration::from_millis(200)).unwrap();
        detector.suspicion_timers.insert(target.clone(), (Incarnation::new(4), past));

        suspicion::check_suspicion_timers(&mut detector);

        // No DEAD event, no SUSPECT→ALIVE-revert: the timer is gone
        // and the recovery event fired (SUSPECT → ALIVE).
        assert!(
            !detector.suspicion_timers.contains_key(&target),
            "stale suspicion timer must be cancelled"
        );
        let mut saw_dead = false;
        while let Ok(event) = event_rx.try_recv() {
            if event.node_id == target && event.new_state == NodeState::Dead {
                saw_dead = true;
            }
        }
        assert!(!saw_dead, "stale suspicion must not declare DEAD after a rejoin");
    }

    /// The gossip-applied Suspect recovery (fleet churn fix): a
    /// Suspect that arrived via GOSSIP (no detector timer) must still
    /// clear on the next successful ping — with the old timer-only
    /// guard the node stayed Suspect forever through the settle.
    #[test]
    fn successful_ping_recovers_gossip_applied_suspect_without_timer() {
        let (mut detector, _cmd_tx, mut event_rx) = make_detector();
        let target = NodeId::new("target-node");
        // The node is in the detector's synced view at incarnation 7.
        detector.alive_nodes = vec![(
            target.clone(),
            NodeState::Suspect,
            "127.0.0.1:9000".parse().unwrap(),
            Incarnation::new(7),
        )];
        // NO suspicion timer (the Suspect came from a gossip delta).

        detector.recover_suspect(&target);

        let mut saw_recovery = false;
        while let Ok(event) = event_rx.try_recv() {
            if event.node_id == target && event.new_state == NodeState::Alive {
                saw_recovery = true;
            }
        }
        assert!(
            saw_recovery,
            "a successful ping must recover a gossip-applied Suspect even without a timer"
        );
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

        // Trigger a failed probe cycle, which calls mark_suspect.
        cmd_tx
            .send(DetectorCommand::PingResponse { target: target.clone(), success: false })
            .await
            .unwrap();
        cmd_tx.send(DetectorCommand::Shutdown).await.unwrap();
        detector.run().await;

        // Assert the suspicion timer uses incarnation 5 from alive_nodes.
        let timer = detector.suspicion_timers.get(&target);
        assert!(timer.is_some(), "target should have a suspicion timer after a failed probe cycle");
        assert_eq!(
            timer.unwrap().0,
            Incarnation::new(5),
            "suspicion timer incarnation should be 5 (from alive_nodes), not fallback 1"
        );
    }
}
