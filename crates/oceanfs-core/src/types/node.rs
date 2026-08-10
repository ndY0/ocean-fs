//! Node and cluster operation types.
//!
//! Contains cluster membership types (`NodeState`, `Incarnation`),
//! routing types (`VnodeRange`, `OperationType`), write coordination
//! types (`WriteQuorum`, `WriteResult`, `WriteAck`, `IntendedFor`),
//! and network address types (`PeerAddress`).

use std::fmt;

use crate::types::hash_output::HashOutput;

use super::id::{NodeId, ObjectKey};
use crate::Hlc;

// ---------------------------------------------------------------------------
// Incarnation
// ---------------------------------------------------------------------------

/// An incarnation number for SWIM membership tracking.
///
/// Each time a node rejoins the cluster after being declared dead, its
/// incarnation number is incremented. Higher incarnation numbers take
/// precedence in gossip state merges, resolving split-brain scenarios.
///
/// # Examples
///
/// ```
/// use oceanfs_core::Incarnation;
///
/// let inc = Incarnation::new(1);
/// assert_eq!(inc.value(), 1);
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Incarnation(u64);

impl Incarnation {
    /// Creates a new incarnation number.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the incarnation value.
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Returns the next incarnation number.
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for Incarnation {
    fn default() -> Self {
        Self(1)
    }
}

// ---------------------------------------------------------------------------
// OperationType
// ---------------------------------------------------------------------------

/// The type of operation being routed.
///
/// Used by the request router to make forwarding decisions based
/// on the operation type (read, write, delete, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OperationType {
    /// Read an object.
    Read,
    /// Write an object.
    Write,
    /// Delete an object.
    Delete,
    /// Retrieve object metadata.
    Head,
    /// List objects in a bucket.
    List,
}

// ---------------------------------------------------------------------------
// VnodeRange
// ---------------------------------------------------------------------------

/// A key range affected by a ring topology change.
///
/// When a node is added or removed from the ring, the affected key range
/// identifies which keys need data migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VnodeRange {
    /// Start of the affected key range (inclusive).
    pub start: [u8; 32],
    /// End of the affected key range (exclusive).
    pub end: [u8; 32],
}

// ---------------------------------------------------------------------------
// NodeState
// ---------------------------------------------------------------------------

/// The state of a node in the cluster membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeState {
    /// Node is healthy and participating.
    Alive,
    /// Node is suspected down (unreachable via direct or indirect ping).
    Suspect,
    /// Node is confirmed dead.
    Dead,
    /// Node is gracefully leaving the cluster.
    Leaving,
    /// Node has left the cluster.
    Left,
}

// ---------------------------------------------------------------------------
// WriteResult / WriteAck
// ---------------------------------------------------------------------------

/// Result of a successful write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// The object key that was written.
    pub object_key: ObjectKey,
    /// The chunks referencing the object's data in segments.
    pub chunks: smallvec::SmallVec<[super::metadata::ChunkRef; 4]>,
    /// Total size of the object in bytes.
    pub size: u64,
    /// BLAKE3 hash of the object content.
    pub blake3_hash: Option<HashOutput>,
    /// HLC timestamp assigned to this write for conflict resolution.
    pub hlc: Hlc,
}

/// Acknowledgment from a replica node for a write.
#[derive(Debug, Clone)]
pub struct WriteAck {
    /// The node that acknowledged.
    pub node_id: NodeId,
    /// WAL position on that node.
    pub wal_position: u64,
    /// HLC timestamp of the write.
    pub hlc: Hlc,
}

// ---------------------------------------------------------------------------
// WriteQuorum
// ---------------------------------------------------------------------------

/// Write quorum configuration for a write operation.
///
/// # Examples
///
/// ```
/// use oceanfs_core::WriteQuorum;
///
/// let quorum = WriteQuorum {
///     required: 2,
///     ack_after_wal: true,
///     ec_async: true,
/// };
/// assert_eq!(quorum.required, 2);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct WriteQuorum {
    /// Required number of replica acknowledgments.
    pub required: u8,
    /// Acknowledge to client after WAL quorum (before EC seal).
    pub ack_after_wal: bool,
    /// Trigger EC encoding asynchronously after acknowledgment.
    pub ec_async: bool,
}

impl Default for WriteQuorum {
    fn default() -> Self {
        Self { required: 1, ack_after_wal: true, ec_async: true }
    }
}

// ---------------------------------------------------------------------------
// IntendedFor
// ---------------------------------------------------------------------------

/// Identifies the intended recipient node for a hinted handoff.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{IntendedFor, NodeId};
///
/// let target = IntendedFor(NodeId::new("node-1"));
/// assert_eq!(target.as_str(), "node-1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IntendedFor(pub NodeId);

impl IntendedFor {
    /// Returns the node ID as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for IntendedFor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// PeerAddress
// ---------------------------------------------------------------------------

/// The network address of a peer node.
///
/// Wraps a `std::net::SocketAddr` for type safety and future extensibility.
///
/// # Examples
///
/// ```
/// use std::net::SocketAddr;
/// use oceanfs_core::PeerAddress;
///
/// let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
/// let peer = PeerAddress::new(addr);
/// assert_eq!(peer.to_string(), "127.0.0.1:9001");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerAddress(std::net::SocketAddr);

impl PeerAddress {
    /// Creates a new peer address from a socket address.
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self(addr)
    }

    /// Returns the inner socket address.
    pub fn socket_addr(&self) -> std::net::SocketAddr {
        self.0
    }
}

impl fmt::Display for PeerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<std::net::SocketAddr> for PeerAddress {
    fn from(addr: std::net::SocketAddr) -> Self {
        Self(addr)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- WriteQuorum --

    #[test]
    fn write_quorum_default_values() {
        let q = WriteQuorum::default();
        assert_eq!(q.required, 1);
        assert!(q.ack_after_wal);
        assert!(q.ec_async);
    }

    #[test]
    fn write_quorum_custom_config() {
        let q = WriteQuorum { required: 3, ack_after_wal: false, ec_async: false };
        assert_eq!(q.required, 3);
        assert!(!q.ack_after_wal);
        assert!(!q.ec_async);
    }

    // -- WriteResult / WriteAck --

    #[test]
    fn write_result_construction() {
        let key = ObjectKey::new("test");
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(super::super::metadata::ChunkRef {
            segment_id: super::super::id::SegmentId::new(),
            offset: 0,
            length: 100,
        });
        let result = WriteResult {
            object_key: key.clone(),
            chunks,
            size: 100,
            blake3_hash: None,
            hlc: Hlc::zero(),
        };
        assert_eq!(result.size, 100);
        assert_eq!(result.object_key, key);
        assert_eq!(result.chunks.len(), 1);
    }

    #[test]
    fn write_ack_construction() {
        let ack = WriteAck { node_id: NodeId::new("n1"), wal_position: 42, hlc: Hlc::zero() };
        assert_eq!(ack.node_id.as_str(), "n1");
        assert_eq!(ack.wal_position, 42);
        assert_eq!(ack.hlc, Hlc::zero());
    }

    // -- IntendedFor --

    #[test]
    fn intended_for_from_node_id() {
        let target = IntendedFor(NodeId::new("node-x"));
        assert_eq!(target.as_str(), "node-x");
        assert_eq!(target.to_string(), "node-x");
    }

    #[test]
    fn intended_for_equality() {
        let a = IntendedFor(NodeId::new("a"));
        let b = IntendedFor(NodeId::new("a"));
        let c = IntendedFor(NodeId::new("c"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // -- Incarnation --

    #[test]
    fn incarnation_new_and_value() {
        let inc = Incarnation::new(42);
        assert_eq!(inc.value(), 42);
    }

    #[test]
    fn incarnation_next_increments() {
        let inc = Incarnation::new(1);
        assert_eq!(inc.next().value(), 2);
    }

    #[test]
    fn incarnation_default_is_one() {
        assert_eq!(Incarnation::default().value(), 1);
    }

    // -- NodeState --

    #[test]
    fn node_state_variants_exist() {
        let _states = [
            NodeState::Alive,
            NodeState::Suspect,
            NodeState::Dead,
            NodeState::Leaving,
            NodeState::Left,
        ];
    }

    // -- OperationType --

    #[test]
    fn operation_type_variants_exist() {
        let _ops = [
            OperationType::Read,
            OperationType::Write,
            OperationType::Delete,
            OperationType::Head,
            OperationType::List,
        ];
    }

    // -- VnodeRange --

    #[test]
    fn vnode_range_construction() {
        let range = VnodeRange { start: [0u8; 32], end: [0xFFu8; 32] };
        assert_eq!(range.start, [0u8; 32]);
        assert_eq!(range.end, [0xFFu8; 32]);
    }

    // -- PeerAddress --

    #[test]
    fn peer_address_new_and_socket_addr() {
        let addr: std::net::SocketAddr = "10.0.0.1:9001".parse().unwrap();
        let peer = PeerAddress::new(addr);
        assert_eq!(peer.socket_addr(), addr);
    }

    #[test]
    fn peer_address_display() {
        let addr: std::net::SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer = PeerAddress::new(addr);
        assert_eq!(peer.to_string(), "127.0.0.1:8080");
    }

    #[test]
    fn peer_address_from_socket_addr() {
        let addr: std::net::SocketAddr = "192.168.1.1:9000".parse().unwrap();
        let peer: PeerAddress = addr.into();
        assert_eq!(peer.socket_addr(), addr);
    }
}
