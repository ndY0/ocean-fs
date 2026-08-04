//! SWIM ping logic — direct and indirect peer probing.
//!
//! Handles the periodic ping cycle: selecting a random alive peer,
//! sending a direct ping, tracking timeouts, and initiating indirect
//! pings through relay peers when direct pings fail.

use std::time::{Duration, Instant};

use oceanfs_core::{proto::membership::ProbeRequest, NodeId, NodeState};
use rand::seq::IteratorRandom;
use tracing::{debug, trace, warn};

use super::FailureDetector;

/// Called on each SWIM interval tick.
///
/// Selects a random alive peer (excluding self), sends a direct
/// ping, and registers a pending ping for timeout tracking.
pub(crate) fn on_ping_tick(detector: &mut FailureDetector) {
    // Filter alive nodes that are not self and not already pending.
    let target = {
        let alive: Vec<_> = detector
            .alive_nodes
            .iter()
            .filter(|(id, state, _)| {
                *state == NodeState::Alive
                    && *id != detector.node_id
                    && !detector.pending_pings.contains_key(id)
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
        origin: Some(oceanfs_core::proto::common::NodeId { id: detector.node_id.to_string() }),
        is_indirect: false,
    };

    let response = detector.probe_handler.handle_probe(&request);

    if response.ack {
        // Target is self — in-process ack.
        debug!(target = %target, "SWIM: self-ping ack received");
        // Nothing to do — self is always alive.
    } else {
        // Remote target — register pending ping for timeout tracking.
        // In a full implementation, this would send a gRPC Probe request.
        detector.pending_pings.insert(target, Instant::now());
    }
}

/// Checks pending direct pings for timeout.
///
/// If a direct ping has been pending longer than `ping_timeout_ms`,
/// initiates indirect pings or marks the target as SUSPECT.
pub(crate) fn check_ping_timeouts(detector: &mut FailureDetector) {
    let timeout = Duration::from_millis(detector.config.ping_timeout_ms);
    let now = Instant::now();

    let timed_out: Vec<NodeId> = detector
        .pending_pings
        .iter()
        .filter(|(_, start)| now.duration_since(**start) >= timeout)
        .map(|(id, _)| id.clone())
        .collect();

    for target in timed_out {
        detector.pending_pings.remove(&target);
        debug!(target = %target, "SWIM: direct ping timed out");
        initiate_indirect_pings(detector, &target);
    }

    // Check indirect ping timeouts.
    let indirect_timeout = Duration::from_millis(detector.config.ping_timeout_ms);
    let indirect_timed_out: Vec<NodeId> = detector
        .pending_indirect
        .iter()
        .filter(|(_, (_, start))| now.duration_since(*start) >= indirect_timeout)
        .map(|(id, _)| id.clone())
        .collect();

    for target in indirect_timed_out {
        detector.pending_indirect.remove(&target);
        warn!(
            target = %target,
            "SWIM: all indirect pings timed out — marking SUSPECT"
        );
        super::suspicion::mark_suspect(detector, &target);
    }
}

/// Initiates indirect pings for a target whose direct ping failed.
///
/// Selects k random alive peers (excluding self and target) as relays.
/// If no relays are available, marks the target SUSPECT immediately.
pub(crate) fn initiate_indirect_pings(detector: &mut FailureDetector, target: &NodeId) {
    let indirect_candidates: Vec<_> = detector
        .alive_nodes
        .iter()
        .filter(|(id, state, _)| {
            *state == NodeState::Alive && *id != detector.node_id && *id != *target
        })
        .map(|(id, _, _)| id.clone())
        .collect();

    let mut rng = rand::thread_rng();
    let indirect_count = detector.config.indirect_ping_count as usize;
    let indirect_targets: Vec<_> = indirect_candidates
        .iter()
        .choose_multiple(&mut rng, indirect_count.min(indirect_candidates.len()))
        .into_iter()
        .cloned()
        .collect();

    if indirect_targets.is_empty() {
        // No indirect peers available — mark suspect immediately.
        super::suspicion::mark_suspect(detector, target);
    } else {
        for relay in &indirect_targets {
            debug!(
                target = %target,
                relay = %relay,
                "SWIM: initiating indirect ping"
            );
            detector.pending_indirect.insert(target.clone(), (relay.clone(), Instant::now()));
        }
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn select_random_peer_returns_alive_node() {
        let nodes = vec![
            (NodeId::new("a"), NodeState::Alive),
            (NodeId::new("b"), NodeState::Dead),
            (NodeId::new("c"), NodeState::Alive),
        ];
        let result = select_random_peer(&nodes, None);
        assert!(result.is_some());
        let id = result.unwrap();
        assert!(id.as_str() == "a" || id.as_str() == "c");
    }

    #[test]
    fn select_random_peer_excludes_specified_node() {
        let nodes =
            vec![(NodeId::new("a"), NodeState::Alive), (NodeId::new("b"), NodeState::Alive)];
        let result = select_random_peer(&nodes, Some(&NodeId::new("a")));
        assert_eq!(result.unwrap().as_str(), "b");
    }

    #[test]
    fn select_random_peer_returns_none_when_all_dead() {
        let nodes = vec![(NodeId::new("a"), NodeState::Dead), (NodeId::new("b"), NodeState::Dead)];
        assert!(select_random_peer(&nodes, None).is_none());
    }

    #[test]
    fn select_random_peer_returns_none_when_all_excluded() {
        let nodes = vec![(NodeId::new("a"), NodeState::Alive)];
        assert!(select_random_peer(&nodes, Some(&NodeId::new("a"))).is_none());
    }
}
