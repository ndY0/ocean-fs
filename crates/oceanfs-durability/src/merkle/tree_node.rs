//! Merkle tree node types for the incremental Merkle tree protocol.
//!
//! Defines the `TreeNode` wire type for gRPC exchange and the
//! `MerkleWalEntry` enum for persistent WAL mutation logging.

use oceanfs_core::SegmentId;

// ---------------------------------------------------------------------------
// TreeNode
// ---------------------------------------------------------------------------

/// A node in the binary Merkle tree.
///
/// Internal nodes have exactly two children (left and right child indices).
/// Leaf nodes have an empty `children` vector.
///
/// # Examples
///
/// ```
/// use oceanfs_durability::merkle::TreeNode;
///
/// let leaf = TreeNode {
///     node_index: 3,
///     hash: [0xAB; 32],
///     children: vec![],
/// };
/// assert!(leaf.is_leaf());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TreeNode {
    /// Position of this node in the binary tree (root = 0).
    pub node_index: u32,
    /// BLAKE3 hash of this node's subtree.
    pub hash: [u8; 32],
    /// For internal nodes: [left_child_index, right_child_index].
    /// For leaf nodes: empty.
    pub children: Vec<u32>,
}

impl TreeNode {
    /// Returns `true` if this node is a leaf (no children).
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    /// Returns the left child index, if any.
    pub fn left_child(&self) -> Option<u32> {
        self.children.first().copied()
    }

    /// Returns the right child index, if any.
    pub fn right_child(&self) -> Option<u32> {
        self.children.get(1).copied()
    }
}

// ---------------------------------------------------------------------------
// MerkleWalEntry
// ---------------------------------------------------------------------------

/// A mutation logged to the Merkle WAL for crash recovery.
///
/// Each entry records a structural change to a Merkle tree: inserting
/// a new node, updating an existing node's hash, or invalidating an
/// entire subtree (used during eviction).
///
/// # Examples
///
/// ```
/// use oceanfs_core::SegmentId;
/// use oceanfs_durability::merkle::MerkleWalEntry;
///
/// let entry = MerkleWalEntry::NodeInsert {
///     segment_id: SegmentId::new(),
///     node_index: 7,
///     hash: [0x42; 32],
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MerkleWalEntry {
    /// A new node was inserted into the tree.
    NodeInsert {
        /// The segment this node belongs to.
        segment_id: SegmentId,
        /// Position of the new node in the binary tree.
        node_index: u32,
        /// BLAKE3 hash of the new node's subtree.
        hash: [u8; 32],
    },
    /// An existing node's hash was updated.
    NodeUpdate {
        /// The segment this node belongs to.
        segment_id: SegmentId,
        /// Position of the updated node.
        node_index: u32,
        /// Hash before the update.
        old_hash: [u8; 32],
        /// Hash after the update.
        new_hash: [u8; 32],
    },
    /// An entire subtree was invalidated (e.g., due to eviction).
    SubtreeInvalidate {
        /// The segment whose subtree was invalidated.
        segment_id: SegmentId,
    },
}
