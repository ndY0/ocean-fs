//! Merkle tree root hash and associated metadata.

use oceanfs_core::HashOutput;

// ---------------------------------------------------------------------------
// MerkleRoot
// ---------------------------------------------------------------------------

/// The root of a Merkle tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MerkleRoot {
    /// The Merkle root hash.
    pub(crate) hash: HashOutput,
    /// Number of leaf hashes in the tree.
    pub(crate) leaf_count: u64,
    /// Total size of the data used to build this tree (0 if unknown).
    pub(crate) total_size: u64,
}

impl MerkleRoot {
    /// Returns the Merkle root hash.
    pub fn hash(&self) -> HashOutput {
        self.hash
    }

    /// Returns the number of leaves.
    pub fn leaf_count(&self) -> u64 {
        self.leaf_count
    }

    /// Returns the total data size.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }
}
