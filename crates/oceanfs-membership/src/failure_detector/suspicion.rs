//! SWIM suspicion mechanism.
//!
//! Manages the suspicion phase of the SWIM protocol: transitioning
//! nodes from ALIVE → SUSPECT on ping failure, maintaining suspicion
//! timers, and escalating expired SUSPECT nodes to DEAD.

use std::time::{Duration, Instant};

use oceanfs_core::{Incarnation, NodeId, NodeState};
use tracing::{info, trace, warn};

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

    // F1a: only a node that was known-Alive can be suspected. A node
    // absent from alive_nodes is either a brand-new joiner whose
    // AddNode has not been applied yet (t5 join-time false Suspect),
    // or a node already removed as Dead — neither may create a
    // suspicion timer, because the timer's incarnation feeds the
    // Suspect→Dead event and a fabricated default would let stale
    // gossip revive the node (t24 oscillation).
    let Some(incarnation) = detector.incarnation_for(node_id) else {
        trace!(
            node_id = %node_id,
            "mark_suspect: node not in alive_nodes — skipping suspicion"
        );
        return;
    };

    detector.suspicion_timers.insert(node_id.clone(), (incarnation, Instant::now()));

    let _ = detector.event_tx.send(MembershipEvent {
        node_id: node_id.clone(),
        old_state: NodeState::Alive,
        new_state: NodeState::Suspect,
        incarnation,
        address: None,
    });

    info!(node_id = %node_id, incarnation = incarnation.value(), "node marked SUSPECT");
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
        let timer = detector.suspicion_timers.get(&node_id).copied();

        // The node may have re-announced (rejoined) at a HIGHER
        // incarnation while the suspicion was pending (the kill +
        // restart window: the announcement lands before the suspicion
        // timer expires). Declaring DEAD with the stale timer
        // incarnation would revert the fresh Alive — the membership
        // manager's effective-incarnation rule then records Dead at
        // the NEW incarnation and F1d rejects every equal-incarnation
        // re-announcement, stranding the rejoined node forever (the
        // fleet churn convergence failure: node-1 stuck Dead(5) after
        // a successful rejoin). The fresh Alive supersedes the stale
        // suspicion — cancel instead.
        if let (Some((timer_inc, _)), Some(current_inc)) =
            (timer, detector.incarnation_for(&node_id))
        {
            if current_inc > timer_inc {
                trace!(
                    node_id = %node_id,
                    timer_inc = timer_inc.value(),
                    current_inc = current_inc.value(),
                    "cancelling stale suspicion: node re-announced at higher incarnation"
                );
                detector.recover_suspect(&node_id);
                continue;
            }
        }

        let timer = detector.suspicion_timers.remove(&node_id);

        // F1c: a Dead node must leave the probe set immediately — the
        // membership manager also sends RemoveNode, but the detector
        // is the authority on its own structures and must not keep
        // probing the removed node between sync ticks.
        detector.alive_nodes.retain(|(id, _, _, _)| id != &node_id);
        detector.pending_probes.remove(&node_id);

        let incarnation =
            timer.map(|(incarnation, _)| incarnation).unwrap_or_else(|| Incarnation::new(1));

        let _ = detector.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Suspect,
            new_state: NodeState::Dead,
            incarnation,
            address: None,
        });
        warn!(
            node_id = %node_id,
            incarnation = incarnation.value(),
            "node declared DEAD (suspicion timeout)"
        );
    }
}
