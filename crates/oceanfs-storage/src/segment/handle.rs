//! Segment handle — public reference to an active or sealed segment.
//!
//! `SegmentHandle` is the opaque type returned to callers who append
//! data to a segment. It carries the segment ID and node assignment
//! but hides the internal buffer implementation.

use oceanfs_core::{NodeId, SegmentId};

/// A public handle referencing a segment.
///
/// Returned when a blob is written to an active segment. The handle
/// carries the segment's identity and the set of nodes responsible
/// for its shards (populated after EC distribution in Phase 4).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, SegmentId};
/// use oceanfs_storage::SegmentHandle;
///
/// let handle = SegmentHandle::new(SegmentId::new(), vec![]);
/// assert!(!handle.id().to_string().is_empty());
/// assert!(handle.node_ids().is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct SegmentHandle {
    /// Unique segment identifier.
    id: SegmentId,
    /// Nodes responsible for storing this segment's shards.
    node_ids: Vec<NodeId>,
}

impl SegmentHandle {
    /// Creates a new `SegmentHandle`.
    pub fn new(id: SegmentId, node_ids: Vec<NodeId>) -> Self {
        Self { id, node_ids }
    }

    /// Returns the segment's unique identifier.
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// Returns the set of nodes responsible for this segment's shards.
    ///
    /// Empty until the segment is sealed and EC-distributed (Phase 4).
    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn handle_stores_id() {
        let id = SegmentId::new();
        let handle = SegmentHandle::new(id, vec![]);
        assert_eq!(handle.id(), id);
    }

    #[test]
    fn handle_stores_node_ids() {
        let nodes = vec![NodeId::new("a"), NodeId::new("b")];
        let handle = SegmentHandle::new(SegmentId::new(), nodes.clone());
        assert_eq!(handle.node_ids(), nodes.as_slice());
    }

    #[test]
    fn empty_node_ids_is_valid() {
        let handle = SegmentHandle::new(SegmentId::new(), vec![]);
        assert!(handle.node_ids().is_empty());
    }
}
