//! SWIM suspicion mechanism.
//!
//! Manages the suspicion phase of the SWIM protocol: transitioning
//! nodes from ALIVE → SUSPECT on ping failure, maintaining suspicion
//! timers, and escalating expired SUSPECT nodes to DEAD.

use std::time::{Duration, Instant};

use oceanfs_core::{Incarnation, NodeId, NodeState};
use tracing::{info, warn};

use super::FailureDetector;
use crate::membership::MembershipEvent;

/// Marks a node as SUSPECT and starts the suspicion timer.
/// If the node is already SUSPECT, the timer is NOT reset —
/// resetting would prevent the SUSPECT→DEAD transition from
/// ever firing under continuous ping failures.
pub(crate) fn mark_suspect(detector: &mut FailureDetector, node_id: &NodeId) {
    // Don't reset an existing timer — it would restart the countdown.
    if detector.suspicion_timers.contains_key(node_id) {
        return;
    }

    let now = Instant::now();

    // Look up the current incarnation from alive_nodes via the new
    // incarnation_for() accessor. If the node is not found in the
    // alive list, fall back to Incarnation::new(1) with a WARN log.
    let incarnation = detector.incarnation_for(node_id).unwrap_or_else(|| {
        tracing::warn!(
            node_id = %node_id,
            "mark_suspect: node not found in alive_nodes, using default incarnation"
        );
        Incarnation::new(1)
    });

    detector.suspicion_timers.insert(node_id.clone(), (incarnation, now));

    let _ = detector.event_tx.send(MembershipEvent {
        node_id: node_id.clone(),
        old_state: NodeState::Alive,
        new_state: NodeState::Suspect,
    });

    info!(node_id = %node_id, "node marked SUSPECT");
}

/// Checks all suspicion timers and transitions expired ones to DEAD.
pub(crate) fn check_suspicion_timers(detector: &mut FailureDetector) {
    let now = Instant::now();
    let suspicion_duration = Duration::from_millis(detector.config.suspicion_timeout_ms);

    let mut expired = Vec::new();
    for (node_id, (_incarnation, suspect_since)) in &detector.suspicion_timers {
        if now.duration_since(*suspect_since) >= suspicion_duration {
            expired.push(node_id.clone());
        }
    }

    for node_id in expired {
        detector.suspicion_timers.remove(&node_id);
        let _ = detector.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Suspect,
            new_state: NodeState::Dead,
        });
        warn!(node_id = %node_id, "node declared DEAD (suspicion timeout)");
    }
}
