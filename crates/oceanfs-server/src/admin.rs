//! Admin API — cluster health, segment status, cache stats, and metrics.

/// Response for GET /admin/cluster.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClusterView {
    /// Nodes in the cluster with their states.
    pub nodes: Vec<NodeInfo>,
    /// Total virtual node count.
    pub vnodes: usize,
}

/// Information about a single node in the cluster.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
    /// Node identifier.
    pub id: String,
    /// Current state.
    pub state: String,
}

/// Response for GET /admin/segments.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SegmentReport {
    /// Total segment count.
    pub total: u64,
    /// Sealed segments.
    pub sealed: u64,
    /// Unsealed active segments.
    pub unsealed: u64,
}

/// Admin API handler.
pub struct AdminHandler;

impl AdminHandler {
    /// Creates a new admin handler.
    pub fn new() -> Self {
        Self
    }

    /// Returns the cluster view.
    pub fn cluster_view(&self) -> ClusterView {
        ClusterView { nodes: Vec::new(), vnodes: 0 }
    }

    /// Returns a segment report.
    pub fn segment_report(&self) -> SegmentReport {
        SegmentReport { total: 0, sealed: 0, unsealed: 0 }
    }
}

impl Default for AdminHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn cluster_view_is_empty_by_default() {
        let handler = AdminHandler::new();
        let view = handler.cluster_view();
        assert!(view.nodes.is_empty());
    }

    #[test]
    fn segment_report_is_zero_by_default() {
        let handler = AdminHandler::new();
        let report = handler.segment_report();
        assert_eq!(report.total, 0);
    }
}
