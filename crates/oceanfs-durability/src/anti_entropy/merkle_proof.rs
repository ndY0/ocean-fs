//! Merkle proof types — `LeafRange` and `MerkleProof`.

use oceanfs_core::HashOutput;

// ---------------------------------------------------------------------------
// LeafRange
// ---------------------------------------------------------------------------

/// A range of leaf indices that have diverged between two Merkle trees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeafRange {
    /// Starting leaf index (inclusive).
    pub start: u64,
    /// Ending leaf index (exclusive).
    pub end: u64,
}

// ---------------------------------------------------------------------------
// MerkleProof
// ---------------------------------------------------------------------------

/// A Merkle proof for a single leaf.
#[derive(Debug, Clone)]
pub struct MerkleProof {
    /// Index of the leaf this proof is for.
    pub leaf_index: u64,
    /// The hash of the leaf being proven.
    pub leaf_hash: HashOutput,
    /// Sibling hashes from leaf to root (inclusive).
    pub siblings: Vec<HashOutput>,
    /// Expected root hash.
    pub root_hash: HashOutput,
}
