//! SWIM ping logic — real direct and indirect probing (ADR-0028 D2).
//!
//! Each interval the detector picks one random alive peer and spawns a
//! probe cycle task:
//!
//! 1. **Direct probe**: `Probe{origin: self, target, is_indirect: false}`
//!    over the membership plane with a hard `ping_timeout_ms` deadline.
//! 2. On timeout: **indirect probes** through `indirect_ping_count`
//!    relays — `Probe{origin: self, target, is_indirect: true}` to each
//!    relay, which forwards to the target and relays the ack back.
//! 3. **Verdict**: any ack → alive; all probes failed/timed out →
//!    failure, which escalates to SUSPECT.
//!
//! The timeout chain is bound to actual messages. The historical
//! "gossip push as ping proxy" (DK-007) — where the liveness signal was
//! "did the full-state push to the peer succeed" — is removed: probes
//! are tiny, bounded RPCs on the dedicated membership plane, decoupled
//! from dissemination.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use oceanfs_core::{NodeId, NodeState};
use oceanfs_network::{gossip::probe_rpc_client::ProbeRpcClient, ConnectionPool};
use rand::seq::IteratorRandom;
use tracing::{debug, trace, warn};

use super::{DetectorCommand, FailureDetector, ProbeMetrics};

/// Called on each SWIM interval tick.
///
/// Selects a random peer (excluding self and nodes with an in-flight
/// probe), then spawns the probe cycle for it. Suspect nodes are probed
/// too: the suspicion window exists precisely so that a transient
/// failure gets a chance to respond — if Suspect nodes were excluded,
/// the successful-probe recovery path could never fire and every
/// transient failure would escalate to DEAD (t5/t24).
pub(crate) fn on_ping_tick(detector: &mut FailureDetector) {
    // Filter nodes that are Alive or Suspect, not self, and not already
    // being probed.
    let picked = {
        let candidates: Vec<_> = detector
            .alive_nodes
            .iter()
            .filter(|(id, state, _, _)| {
                (*state == NodeState::Alive || *state == NodeState::Suspect)
                    && *id != detector.node_id
                    && !detector.pending_probes.contains_key(id)
            })
            .map(|(id, _, addr, _)| (id.clone(), *addr))
            .collect();

        if candidates.is_empty() {
            trace!("SWIM tick: no alive peers to probe");
            return;
        }

        let mut rng = rand::thread_rng();
        let Some((id, addr)) = candidates.iter().choose(&mut rng) else {
            return;
        };
        (id.clone(), *addr)
    };

    let (target, target_addr) = picked;

    // Select relays for the indirect phase: Alive peers other than self
    // and the target, capped at indirect_ping_count.
    let mut rng = rand::thread_rng();
    let relays: Vec<(NodeId, SocketAddr)> = detector
        .alive_nodes
        .iter()
        .filter(|(id, state, _, _)| {
            *state == NodeState::Alive && *id != detector.node_id && *id != target
        })
        .map(|(id, _, addr, _)| (id.clone(), *addr))
        .choose_multiple(&mut rng, detector.config.indirect_ping_count as usize);

    let Some(pool) = detector.pool.clone() else {
        debug!("SWIM: no membership plane pool — skipping probe of {target}");
        return;
    };

    debug!(target = %target, relays = relays.len(), "SWIM: starting probe cycle");
    detector.pending_probes.insert(target.clone(), Instant::now());

    let detector_tx = detector.command_tx.clone();
    let self_id = detector.node_id.clone();
    let ping_timeout_ms = detector.config.ping_timeout_ms;
    let metrics = detector.metrics.clone();

    tokio::spawn(async move {
        let verdict = run_probe_cycle(
            &pool,
            &self_id,
            &target,
            target_addr,
            &relays,
            ping_timeout_ms,
            &metrics,
        )
        .await;
        if verdict {
            debug!(target = %target, "SWIM: probe cycle succeeded");
        } else {
            warn!(target = %target, "SWIM: probe cycle failed — escalating");
            metrics.failures_total.inc();
        }
        let _ = detector_tx.send(DetectorCommand::PingResponse { target, success: verdict }).await;
    });
}

/// Runs the full probe cycle for a target and returns the verdict.
///
/// Direct probe first (bounded by `ping_timeout_ms`); on failure,
/// concurrent relayed indirect probes (each bounded by
/// `ping_timeout_ms`). Any ack wins.
async fn run_probe_cycle(
    pool: &Arc<ConnectionPool>,
    origin: &NodeId,
    target: &NodeId,
    target_addr: SocketAddr,
    relays: &[(NodeId, SocketAddr)],
    ping_timeout_ms: u64,
    metrics: &ProbeMetrics,
) -> bool {
    let start = Instant::now();
    let direct = probe_direct(pool, target_addr, target, origin, ping_timeout_ms).await;
    let success = if direct.ack {
        true
    } else if relays.is_empty() {
        false
    } else {
        metrics.indirect_total.add(relays.len() as u64);
        indirect_probe(pool, origin, target, relays, ping_timeout_ms).await
    };

    metrics.duration_us.observe(start.elapsed().as_micros() as u64);
    if success {
        debug!(target = %target, elapsed_us = start.elapsed().as_micros(), "SWIM: probe cycle succeeded");
    }
    success
}

/// Sends a direct probe to `addr` and returns the response.
async fn probe_direct(
    pool: &Arc<ConnectionPool>,
    addr: SocketAddr,
    target: &NodeId,
    origin: &NodeId,
    ping_timeout_ms: u64,
) -> oceanfs_core::proto::membership::ProbeResponse {
    let Some(mut client) = make_client(pool, addr, ping_timeout_ms).await else {
        return oceanfs_core::proto::membership::ProbeResponse { ack: false, incarnation: 0 };
    };
    let request = oceanfs_core::proto::membership::ProbeRequest {
        target: Some(oceanfs_core::proto::common::NodeId { id: target.to_string() }),
        origin: Some(oceanfs_core::proto::common::NodeId { id: origin.to_string() }),
        is_indirect: false,
    };
    match tokio::time::timeout(
        Duration::from_millis(ping_timeout_ms),
        client.probe(tonic::Request::new(request)),
    )
    .await
    {
        Ok(Ok(response)) => response.into_inner(),
        Ok(Err(status)) => {
            debug!(target = %target, error = %status, "SWIM: direct probe failed");
            oceanfs_core::proto::membership::ProbeResponse { ack: false, incarnation: 0 }
        }
        Err(_) => {
            debug!(target = %target, "SWIM: direct probe timed out");
            oceanfs_core::proto::membership::ProbeResponse { ack: false, incarnation: 0 }
        }
    }
}

/// Sends concurrent indirect probes through `relays`; any ack wins.
///
/// Each relay receives `Probe{is_indirect: true}` and forwards to the
/// target itself. All relay probes run concurrently (JoinSet), each
/// bounded by `ping_timeout_ms`, so the indirect phase adds at most one
/// `ping_timeout_ms` to the detection bound. The first ack aborts the
/// remaining relays.
async fn indirect_probe(
    pool: &Arc<ConnectionPool>,
    origin: &NodeId,
    target: &NodeId,
    relays: &[(NodeId, SocketAddr)],
    ping_timeout_ms: u64,
) -> bool {
    let mut set = tokio::task::JoinSet::new();
    for (relay_id, relay_addr) in relays {
        let pool = Arc::clone(pool);
        let origin = origin.clone();
        let target = target.clone();
        let relay_id = relay_id.clone();
        let relay_addr = *relay_addr;
        set.spawn(async move {
            probe_relay(&pool, relay_addr, &relay_id, &target, &origin, ping_timeout_ms).await
        });
    }

    let mut any_ack = false;
    while let Some(result) = set.join_next().await {
        if let Ok(response) = result {
            if response.ack {
                any_ack = true;
                trace!(target = %target, "SWIM: indirect probe ack received");
                set.abort_all();
                break;
            }
        }
    }
    any_ack
}

/// Sends an indirect probe to a relay, which forwards to the target.
async fn probe_relay(
    pool: &Arc<ConnectionPool>,
    relay_addr: SocketAddr,
    relay_id: &NodeId,
    target: &NodeId,
    origin: &NodeId,
    ping_timeout_ms: u64,
) -> oceanfs_core::proto::membership::ProbeResponse {
    let Some(mut client) = make_client(pool, relay_addr, ping_timeout_ms).await else {
        return oceanfs_core::proto::membership::ProbeResponse { ack: false, incarnation: 0 };
    };
    let request = oceanfs_core::proto::membership::ProbeRequest {
        target: Some(oceanfs_core::proto::common::NodeId { id: target.to_string() }),
        origin: Some(oceanfs_core::proto::common::NodeId { id: origin.to_string() }),
        is_indirect: true,
    };
    match tokio::time::timeout(
        Duration::from_millis(ping_timeout_ms),
        client.probe(tonic::Request::new(request)),
    )
    .await
    {
        Ok(Ok(response)) => response.into_inner(),
        Ok(Err(status)) => {
            debug!(relay = %relay_id, error = %status, "SWIM: indirect probe via relay failed");
            oceanfs_core::proto::membership::ProbeResponse { ack: false, incarnation: 0 }
        }
        Err(_) => {
            debug!(relay = %relay_id, "SWIM: indirect probe via relay timed out");
            oceanfs_core::proto::membership::ProbeResponse { ack: false, incarnation: 0 }
        }
    }
}

/// Acquires a probe client for the given address over the membership
/// plane pool, bounded by the ping timeout.
async fn make_client(
    pool: &Arc<ConnectionPool>,
    addr: SocketAddr,
    ping_timeout_ms: u64,
) -> Option<ProbeRpcClient<tonic::transport::Channel>> {
    let pooled =
        match tokio::time::timeout(Duration::from_millis(ping_timeout_ms), pool.get_channel(addr))
            .await
        {
            Ok(Ok(pooled)) => pooled,
            Ok(Err(e)) => {
                debug!(peer = %addr, error = %e, "SWIM: channel acquisition failed");
                return None;
            }
            Err(_) => {
                debug!(peer = %addr, "SWIM: channel acquisition timed out");
                return None;
            }
        };
    let channel = pooled.channel().clone();
    drop(pooled);
    Some(ProbeRpcClient::new(channel))
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
