//! SWIM failure detector types.
//!
//! Contains the core data types used by the failure detector:
//! configuration, commands, and the detector struct itself.

use std::{collections::HashMap, net::SocketAddr, time::Instant};

use oceanfs_core::{Incarnation, NodeId, NodeState};
use tokio::sync::mpsc;

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
    UpdateAliveNodes { nodes: Vec<(NodeId, NodeState, SocketAddr, Incarnation)> },
    /// Shut down the detector.
    Shutdown,
}

/// The failure detector task.
///
/// Runs a loop: every `interval_ms`, picks a random alive peer,
/// sends a direct ping, and handles timeout/indirect ping logic.
pub(crate) struct FailureDetector {
    /// Receiver for ping results.
    pub(crate) rx: mpsc::Receiver<DetectorCommand>,
    /// Sender for membership events (state transitions).
    pub(crate) event_tx: tokio::sync::broadcast::Sender<MembershipEvent>,
    /// Detector configuration.
    pub(crate) config: DetectorConfig,
    /// Current suspicion timers: node_id → (incarnation, suspect_since).
    pub(crate) suspicion_timers: HashMap<NodeId, (Incarnation, Instant)>,
    /// This node's identifier.
    pub(crate) node_id: NodeId,
    /// List of alive nodes: (node_id, state, address, incarnation).
    pub(crate) alive_nodes: Vec<(NodeId, NodeState, SocketAddr, Incarnation)>,
    /// Probe handler for in-process self-pings.
    pub(crate) probe_handler: ProbeHandler,
    /// Pending direct pings: target → ping_start_time.
    pub(crate) pending_pings: HashMap<NodeId, Instant>,
    /// Pending indirect pings: target → (origin, ping_start_time).
    pub(crate) pending_indirect: HashMap<NodeId, (NodeId, Instant)>,
}
