//! SWIM failure detector types.
//!
//! Contains the core data types used by the failure detector:
//! configuration, commands, and the detector struct itself.

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Instant};

use oceanfs_core::{Counter, Histogram, Incarnation, LabelSet, NodeId, NodeState};
use tokio::sync::mpsc;

use crate::membership::MembershipEvent;

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
pub(crate) enum DetectorCommand {
    /// The verdict of a complete probe cycle (direct + indirect) for a
    /// target (ADR-0028 D2): `success = true` means any probe in the
    /// cycle was acknowledged; `false` means all direct and relayed
    /// probes failed or timed out.
    PingResponse { target: NodeId, success: bool },
    /// Update the list of alive nodes for periodic ping selection.
    UpdateAliveNodes { nodes: Vec<(NodeId, NodeState, SocketAddr, Incarnation)> },
    /// Drop a node from the probe set — called when a node is declared
    /// Dead/Left so the detector stops probing it (F1c).
    RemoveNode { node_id: NodeId },
    /// Set the membership plane's connection pool (ADR-0028 D1). The
    /// pool may arrive after `start()` (the node wires it via
    /// `Membership::set_pool`).
    SetPool { pool: Arc<oceanfs_network::ConnectionPool> },
    /// Shut down the detector.
    Shutdown,
}

impl std::fmt::Debug for DetectorCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PingResponse { target, success } => f
                .debug_struct("PingResponse")
                .field("target", target)
                .field("success", success)
                .finish(),
            Self::UpdateAliveNodes { nodes } => {
                f.debug_struct("UpdateAliveNodes").field("count", &nodes.len()).finish()
            }
            Self::RemoveNode { node_id } => {
                f.debug_struct("RemoveNode").field("node_id", node_id).finish()
            }
            Self::SetPool { .. } => f.debug_struct("SetPool").finish(),
            Self::Shutdown => write!(f, "Shutdown"),
        }
    }
}

/// Metrics for the SWIM probe cycle (ADR-0028 D2).
#[derive(Clone)]
pub(crate) struct ProbeMetrics {
    /// Probe cycle duration histogram (microseconds).
    pub duration_us: Arc<Histogram>,
    /// Probe cycles that ended in failure (all direct + indirect probes
    /// failed/timed out).
    pub failures_total: Counter,
    /// Indirect (relayed) probes sent.
    pub indirect_total: Counter,
}

/// The failure detector task.
///
/// Runs a loop: every `interval_ms`, picks a random alive peer, spawns a
/// real SWIM probe cycle (direct probe → k relayed indirect probes →
/// verdict, ADR-0028 D2), and handles the verdict plus suspicion timers.
pub(crate) struct FailureDetector {
    /// Receiver for ping results.
    pub(crate) rx: mpsc::Receiver<DetectorCommand>,
    /// Sender clone for spawned probe tasks to report verdicts.
    pub(crate) command_tx: mpsc::Sender<DetectorCommand>,
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
    /// In-flight probe cycles: target → start time. Used to avoid
    /// starting a second probe for the same target while one is running
    /// (the spawned probe task owns the timeout chain).
    pub(crate) pending_probes: HashMap<NodeId, Instant>,
    /// The membership plane's connection pool (ADR-0028 D1), used by
    /// the spawned probe tasks. `None` until the node wires it.
    pub(crate) pool: Option<Arc<oceanfs_network::ConnectionPool>>,
    /// SWIM probe cycle metrics (shared with the spawned tasks).
    pub(crate) metrics: ProbeMetrics,
}

impl FailureDetector {
    /// Creates a new failure detector and returns a command sender.
    pub fn new(
        config: DetectorConfig,
        event_tx: tokio::sync::broadcast::Sender<MembershipEvent>,
        node_id: oceanfs_core::NodeId,
        buffer: usize,
        pool: Option<Arc<oceanfs_network::ConnectionPool>>,
    ) -> (Self, mpsc::Sender<DetectorCommand>) {
        let (tx, rx) = mpsc::channel(buffer);
        (
            Self {
                rx,
                command_tx: tx.clone(),
                event_tx,
                config,
                suspicion_timers: HashMap::new(),
                node_id,
                alive_nodes: Vec::new(),
                pending_probes: HashMap::new(),
                pool,
                metrics: ProbeMetrics {
                    duration_us: Arc::new(Histogram::new(
                        "probe_duration_microseconds".into(),
                        "SWIM probe cycle (direct + indirect) duration in microseconds".into(),
                        &oceanfs_core::sub_millisecond_histogram_config(),
                        LabelSet::empty(),
                    )),
                    failures_total: Counter::new(
                        "probe_failures_total".into(),
                        "SWIM probe cycles that ended in failure".into(),
                        LabelSet::empty(),
                    ),
                    indirect_total: Counter::new(
                        "indirect_probes_total".into(),
                        "SWIM indirect (relayed) probes sent".into(),
                        LabelSet::empty(),
                    ),
                },
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
}
