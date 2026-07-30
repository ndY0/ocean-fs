//! Anti-entropy — Merkle tree exchange for background data integrity.

use oceanfs_core::HashOutput;

/// A Merkle tree over segment data.
///
/// Built at seal time. Leaves are 64 KB BLAKE3 hashes.
/// Used for peer-to-peer integrity verification.
pub struct MerkleTree {
    /// Merkle root hash.
    root: HashOutput,
    /// Number of leaf hashes.
    leaf_count: u64,
}

impl MerkleTree {
    /// Builds a Merkle tree from leaf hashes.
    ///
    /// Returns `None` if the leaf list is empty.
    pub fn build(leaf_hashes: &[HashOutput]) -> Option<Self> {
        if leaf_hashes.is_empty() {
            return None;
        }

        let mut current_level: Vec<HashOutput> = leaf_hashes.to_vec();
        let leaf_count = current_level.len() as u64;

        while current_level.len() > 1 {
            let mut next_level = Vec::with_capacity(current_level.len().div_ceil(2));
            for pair in current_level.chunks(2) {
                let mut hasher = blake3::Hasher::new();
                hasher.update(pair[0].as_bytes());
                if pair.len() > 1 {
                    hasher.update(pair[1].as_bytes());
                } else {
                    hasher.update(pair[0].as_bytes()); // duplicate last if odd
                }
                let hash = hasher.finalize();
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(hash.as_bytes());
                next_level.push(HashOutput::from_bytes(bytes));
            }
            current_level = next_level;
        }

        Some(Self { root: current_level[0], leaf_count })
    }

    /// Returns the Merkle root hash.
    pub fn root(&self) -> HashOutput {
        self.root
    }

    /// Returns the number of leaves.
    pub fn leaf_count(&self) -> u64 {
        self.leaf_count
    }
}

/// Statistics from an anti-entropy cycle.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct AntiEntropyStats {
    /// Number of segments compared.
    pub segments_compared: u64,
    /// Number of segments with mismatched roots.
    pub mismatches_found: u64,
    /// Number of leaf divergences repaired.
    pub leaves_repaired: u64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_hash(b: u8) -> HashOutput {
        let mut bytes = [0u8; 32];
        bytes[0] = b;
        HashOutput::from_bytes(bytes)
    }

    #[test]
    fn empty_tree_returns_none() {
        assert!(MerkleTree::build(&[]).is_none());
    }

    #[test]
    fn single_leaf_tree_has_leaf_as_root() {
        let leaf = make_hash(42);
        let tree = MerkleTree::build(&[leaf]).unwrap();
        assert_eq!(tree.root(), leaf);
        assert_eq!(tree.leaf_count(), 1);
    }

    #[test]
    fn two_leaf_tree_has_correct_root() {
        let a = make_hash(1);
        let b = make_hash(2);
        let tree = MerkleTree::build(&[a, b]).unwrap();
        assert_eq!(tree.leaf_count(), 2);
        // Root should be BLAKE3(a || b)
        let mut hasher = blake3::Hasher::new();
        hasher.update(a.as_bytes());
        hasher.update(b.as_bytes());
        let expected = hasher.finalize();
        let mut exp_bytes = [0u8; 32];
        exp_bytes.copy_from_slice(expected.as_bytes());
        assert_eq!(tree.root(), HashOutput::from_bytes(exp_bytes));
    }

    #[test]
    fn same_leaves_produce_same_root() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3)];
        let t1 = MerkleTree::build(&leaves).unwrap();
        let t2 = MerkleTree::build(&leaves).unwrap();
        assert_eq!(t1.root(), t2.root());
    }

    #[test]
    fn different_leaves_produce_different_root() {
        let t1 = MerkleTree::build(&[make_hash(1)]).unwrap();
        let t2 = MerkleTree::build(&[make_hash(2)]).unwrap();
        assert_ne!(t1.root(), t2.root());
    }
}
