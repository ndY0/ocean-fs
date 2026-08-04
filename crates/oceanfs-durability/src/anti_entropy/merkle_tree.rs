//! Merkle tree over segment data.
//!
//! Built at seal time over 64 KB leaves. Leaves are BLAKE3 hashes of
//! contiguous chunks of data. Used for peer-to-peer integrity verification
//! during anti-entropy exchange. Includes `SegmentDataStore` trait for
//! abstracting data access during tree building and repair.

use std::collections::HashMap;

use oceanfs_core::{HashOutput, SegmentId};
use oceanfs_hash::{Blake3Hasher, Hasher as _};
use oceanfs_storage::Result;

use super::{
    merkle_proof::{LeafRange, MerkleProof},
    merkle_root::MerkleRoot,
};

/// Default leaf size for Merkle tree construction (64 KB).
pub(crate) const DEFAULT_LEAF_SIZE: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// SegmentDataStore — data access trait for anti-entropy repair
// ---------------------------------------------------------------------------

/// Provides access to raw segment data for Merkle tree reconstruction
/// and leaf repair.
///
/// The anti-entropy protocol reads segment data to rebuild Merkle trees
/// for comparison, and writes corrected leaf data during repair.
///
/// In production this is backed by the on-disk segment store; tests use
/// an in-memory implementation.
pub trait SegmentDataStore: Send + Sync {
    /// Reads the full raw data for the given segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment data cannot be read (e.g., segment
    /// not found, I/O error).
    fn read_segment_data(&self, segment_id: &SegmentId) -> Result<Vec<u8>>;

    /// Writes the raw data for the given segment, replacing any existing data.
    ///
    /// Used during anti-entropy leaf repair to overwrite corrupted shards
    /// with corrected data fetched from a peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment data cannot be written.
    fn write_segment_data(&self, segment_id: &SegmentId, data: &[u8]) -> Result<()>;
}

/// An in-memory segment data store for testing anti-entropy.
///
/// Stores segment data in a `HashMap<SegmentId, Vec<u8>>` protected by
/// a `parking_lot::RwLock`. Suitable for unit and integration tests
/// where an on-disk segment store is not needed.
pub struct InMemorySegmentStore {
    data: parking_lot::RwLock<HashMap<SegmentId, Vec<u8>>>,
}

impl InMemorySegmentStore {
    /// Creates a new empty in-memory segment store.
    pub fn new() -> Self {
        Self { data: parking_lot::RwLock::new(HashMap::new()) }
    }
}

impl Default for InMemorySegmentStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentDataStore for InMemorySegmentStore {
    fn read_segment_data(&self, segment_id: &SegmentId) -> Result<Vec<u8>> {
        self.data
            .read()
            .get(segment_id)
            .cloned()
            .ok_or(oceanfs_storage::Error::SegmentNotFound(*segment_id))
    }

    fn write_segment_data(&self, segment_id: &SegmentId, data: &[u8]) -> Result<()> {
        self.data.write().insert(*segment_id, data.to_vec());
        Ok(())
    }
}
// ---------------------------------------------------------------------------
// MerkleTree
// ---------------------------------------------------------------------------

/// A Merkle tree over segment data.
///
/// Built at seal time over 64 KB leaves. Leaves are BLAKE3 hashes of
/// contiguous chunks of data. Used for peer-to-peer integrity verification
/// during anti-entropy exchange.
///
/// # Examples
///
/// ```
/// # use oceanfs_durability::MerkleTree;
/// let data = vec![0u8; 131072]; // 128 KB = 2 leaves
/// let tree = MerkleTree::build(&data, 65536).unwrap();
/// assert_eq!(tree.leaf_count(), 2);
/// let _root = tree.root();
/// ```
pub struct MerkleTree {
    /// Merkle root hash and metadata.
    root: MerkleRoot,
    /// All leaf hashes, stored for descent during diff.
    leaf_hashes: Vec<HashOutput>,
    /// The internal tree nodes (level by level from leaves up, excluding root).
    /// tree[0] = leaf hashes, tree[1] = level 1 parents, etc.
    /// This enables efficient binary descent without recomputation.
    tree_levels: Vec<Vec<HashOutput>>,
}

impl MerkleTree {
    /// Builds a Merkle tree from raw data, splitting into `leaf_size` chunks.
    ///
    /// Each chunk is hashed with BLAKE3 using streaming (never buffers
    /// the full blob unnecessarily, but this constructor assumes the caller
    /// has already assembled the data). For truly streaming construction,
    /// use [`build_from_hashes`](Self::build_from_hashes).
    ///
    /// If `leaf_size` is 0, the default leaf size (64 KB) is used.
    ///
    /// Returns `None` if the data is empty.
    pub fn build(data: &[u8], leaf_size: usize) -> Option<Self> {
        let leaf_size = if leaf_size == 0 { DEFAULT_LEAF_SIZE } else { leaf_size };
        if data.is_empty() {
            return None;
        }

        let data_len = data.len() as u64;
        let leaf_hashes: Vec<HashOutput> = data.chunks(leaf_size).map(Blake3Hasher::hash).collect();

        let leaf_count = leaf_hashes.len() as u64;
        let (tree_levels, root_hash) = Self::build_tree_from_leaves(&leaf_hashes);

        let total_size = data_len;
        let root = MerkleRoot { hash: root_hash, leaf_count, total_size };

        Some(Self { root, leaf_hashes, tree_levels })
    }

    /// Builds a Merkle tree from pre-computed leaf hashes.
    ///
    /// This is useful when leaf hashes have already been computed
    /// (e.g., stored alongside shard data).
    ///
    /// Returns `None` if the leaf list is empty.
    pub fn build_from_hashes(leaf_hashes: &[HashOutput]) -> Option<Self> {
        if leaf_hashes.is_empty() {
            return None;
        }

        let leaf_count = leaf_hashes.len() as u64;
        let leaf_hashes_vec = leaf_hashes.to_vec();
        let (tree_levels, root_hash) = Self::build_tree_from_leaves(&leaf_hashes_vec);

        let total_size = 0; // unknown when building from pre-computed hashes
        let root = MerkleRoot { hash: root_hash, leaf_count, total_size };

        Some(Self { root, leaf_hashes: leaf_hashes_vec, tree_levels })
    }

    /// Core tree-building logic: given leaf hashes, builds all intermediate
    /// levels and returns (levels, root_hash).
    fn build_tree_from_leaves(leaf_hashes: &[HashOutput]) -> (Vec<Vec<HashOutput>>, HashOutput) {
        let mut tree_levels: Vec<Vec<HashOutput>> = Vec::new();
        let mut current_level: Vec<HashOutput> = leaf_hashes.to_vec();

        // Save leaf level (level 0)
        tree_levels.push(current_level.clone());

        while current_level.len() > 1 {
            let mut next_level = Vec::with_capacity(current_level.len().div_ceil(2));
            for pair in current_level.chunks(2) {
                let mut hasher = Blake3Hasher::new();
                hasher.update(pair[0].as_bytes());
                if pair.len() > 1 {
                    hasher.update(pair[1].as_bytes());
                } else {
                    // Duplicate last if odd
                    hasher.update(pair[0].as_bytes());
                }
                next_level.push(hasher.finalize());
            }
            tree_levels.push(next_level.clone());
            current_level = next_level;
        }

        let root_hash = current_level[0];
        (tree_levels, root_hash)
    }

    /// Returns the Merkle root.
    pub fn root(&self) -> MerkleRoot {
        self.root
    }

    /// Returns the number of leaves in the tree.
    pub fn leaf_count(&self) -> u64 {
        self.root.leaf_count
    }

    /// Returns the total size of the data used to build this tree.
    pub fn total_size(&self) -> u64 {
        self.root.total_size
    }

    /// Returns the hash of the leaf at the given index.
    ///
    /// Returns `None` if the index is out of bounds.
    pub fn leaf_hash(&self, index: usize) -> Option<HashOutput> {
        self.leaf_hashes.get(index).copied()
    }

    /// Returns all leaf hashes.
    pub fn leaf_hashes(&self) -> &[HashOutput] {
        &self.leaf_hashes
    }

    /// Compares this Merkle tree with another and returns the set of leaf
    /// indices where the hashes differ.
    ///
    /// Uses rayon parallel iteration for large leaf sets (perf rule 2.1).
    pub fn diff(&self, other: &MerkleTree) -> Vec<LeafRange> {
        let mut divergences = Vec::new();

        // Fast path: roots match
        if self.root.hash == other.root.hash {
            return divergences;
        }

        // Roots differ — compare leaves in parallel for large trees
        let max_leaves = self.leaf_hashes.len().min(other.leaf_hashes.len());

        if max_leaves > 4 {
            // Use rayon parallel iterators for large Merkle tree comparisons
            use rayon::prelude::*;
            let diffs: Vec<LeafRange> = (0..max_leaves)
                .into_par_iter()
                .filter(|&i| self.leaf_hashes[i] != other.leaf_hashes[i])
                .map(|i| LeafRange { start: i as u64, end: i as u64 + 1 })
                .collect();
            return Self::coalesce_ranges(diffs);
        }

        // Sequential for small trees
        let mut i = 0;
        while i < max_leaves {
            if self.leaf_hashes[i] != other.leaf_hashes[i] {
                let start = i as u64;
                i += 1;
                while i < max_leaves && self.leaf_hashes[i] != other.leaf_hashes[i] {
                    i += 1;
                }
                divergences.push(LeafRange { start, end: i as u64 });
            } else {
                i += 1;
            }
        }

        // If self has more leaves than other, mark those as diverged
        for idx in max_leaves..self.leaf_hashes.len() {
            divergences.push(LeafRange { start: idx as u64, end: idx as u64 + 1 });
        }

        divergences
    }

    /// Coalesces adjacent individual leaf ranges into contiguous ranges.
    fn coalesce_ranges(mut ranges: Vec<LeafRange>) -> Vec<LeafRange> {
        if ranges.is_empty() {
            return ranges;
        }
        ranges.sort_by_key(|r| r.start);
        let mut result = Vec::with_capacity(ranges.len());
        let mut current = ranges[0];
        for range in &ranges[1..] {
            if range.start == current.end {
                current.end = range.end;
            } else {
                result.push(current);
                current = *range;
            }
        }
        result.push(current);
        result
    }

    /// Performs binary descent through the tree to find diverged leaves.
    ///
    /// When two Merkle trees have different roots, this method compares
    /// internal nodes level by level to identify exactly which leaf ranges
    /// have diverged. This is more bandwidth-efficient than exchanging all
    /// leaf hashes when the number of diverged leaves is small.
    ///
    /// The descent starts from the top of both trees and avoids descending
    /// into subtrees whose hashes match.
    ///
    /// # Returns
    ///
    /// Vector of [`LeafRange`] indicating which leaf indices differ.
    ///
    /// Used by the anti-entropy gRPC exchange path when a peer returns leaf
    /// hashes that differ from local segment data.
    pub(crate) fn descend_diff(&self, other: &MerkleTree) -> Vec<LeafRange> {
        // Fast path: roots match
        if self.root.hash == other.root.hash {
            return Vec::new();
        }

        let mut divergences = Vec::new();
        let max_level = self.tree_levels.len().saturating_sub(1);

        // Start descent from the top of the tree (just below root)
        if max_level > 0 {
            let top_level_idx = max_level;
            Self::descend_diff_inner(
                &self.tree_levels,
                &other.tree_levels,
                top_level_idx,
                0,
                self.leaf_hashes.len(),
                &mut divergences,
            );
        }

        // If tree only has 1 leaf (no internal nodes), diff at leaf level
        if max_level == 0 && !self.leaf_hashes.is_empty() {
            let max_leaves = self.leaf_hashes.len().min(other.leaf_hashes.len());
            for i in 0..max_leaves {
                if self.leaf_hashes[i] != other.leaf_hashes[i] {
                    divergences.push(LeafRange { start: i as u64, end: i as u64 + 1 });
                }
            }
        }

        Self::coalesce_ranges(divergences)
    }

    /// Recursively descends the tree, finding leaf divergences.
    fn descend_diff_inner(
        self_levels: &[Vec<HashOutput>],
        other_levels: &[Vec<HashOutput>],
        level: usize,
        node_idx: usize,
        leaf_count: usize,
        divergences: &mut Vec<LeafRange>,
    ) {
        // Leaf level: check individual leaves
        if level == 0 {
            if node_idx < self_levels[0].len()
                && node_idx < other_levels[0].len()
                && self_levels[0][node_idx] != other_levels[0][node_idx]
            {
                divergences.push(LeafRange { start: node_idx as u64, end: node_idx as u64 + 1 });
            }
            return;
        }

        // Check if this node's hash matches in both trees
        if node_idx < self_levels[level].len()
            && node_idx < other_levels[level].len()
            && self_levels[level][node_idx] == other_levels[level][node_idx]
        {
            return;
        }

        // Hashes differ — descend to children
        let leaves_per_child_at_level = 1usize << (level - 1);
        let left_child_idx = node_idx * 2;
        let right_child_idx = node_idx * 2 + 1;

        let left_leaf_count = leaves_per_child_at_level.min(leaf_count);
        Self::descend_diff_inner(
            self_levels,
            other_levels,
            level - 1,
            left_child_idx,
            left_leaf_count,
            divergences,
        );

        if leaf_count > left_leaf_count {
            let right_leaf_count = leaf_count - left_leaf_count;
            Self::descend_diff_inner(
                self_levels,
                other_levels,
                level - 1,
                right_child_idx,
                right_leaf_count,
                divergences,
            );
        }
    }

    /// Generates a Merkle proof for the leaf at the given index.
    pub fn generate_proof(&self, leaf_index: usize) -> Option<MerkleProof> {
        if leaf_index >= self.leaf_hashes.len() {
            return None;
        }

        let mut siblings = Vec::new();
        let mut current_idx = leaf_index;

        for level in 0..self.tree_levels.len().saturating_sub(1) {
            let sibling_idx = if current_idx % 2 == 0 { current_idx + 1 } else { current_idx - 1 };

            if sibling_idx < self.tree_levels[level].len() {
                siblings.push(self.tree_levels[level][sibling_idx]);
            }

            current_idx /= 2;
        }

        Some(MerkleProof {
            leaf_index: leaf_index as u64,
            leaf_hash: self.leaf_hashes[leaf_index],
            siblings,
            root_hash: self.root.hash,
        })
    }

    /// Verifies a Merkle proof against this tree's root.
    pub fn verify_proof(&self, proof: &MerkleProof) -> bool {
        if proof.root_hash != self.root.hash {
            return false;
        }

        if proof.leaf_index as usize >= self.leaf_hashes.len() {
            return false;
        }

        let mut current_hash = proof.leaf_hash;
        let mut idx = proof.leaf_index;

        for sibling in &proof.siblings {
            let mut hasher = Blake3Hasher::new();
            if idx % 2 == 0 {
                hasher.update(current_hash.as_bytes());
                hasher.update(sibling.as_bytes());
            } else {
                hasher.update(sibling.as_bytes());
                hasher.update(current_hash.as_bytes());
            }
            current_hash = hasher.finalize();
            idx /= 2;
        }

        current_hash == self.root.hash
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use oceanfs_core::{HashOutput, SegmentId};
    use oceanfs_storage::Error;

    use super::{super::*, DEFAULT_LEAF_SIZE};

    fn make_hash(b: u8) -> HashOutput {
        let mut bytes = [0u8; 32];
        bytes[0] = b;
        HashOutput::from_bytes(bytes)
    }

    // -----------------------------------------------------------------------
    // MerkleTree::build (data-based)
    // -----------------------------------------------------------------------

    #[test]
    fn build_empty_data_returns_none() {
        assert!(MerkleTree::build(&[], DEFAULT_LEAF_SIZE).is_none());
    }

    #[test]
    fn build_with_zero_leaf_size_uses_default() {
        let data = vec![42u8; 100];
        let tree = MerkleTree::build(&data, 0).unwrap();
        assert_eq!(tree.leaf_count(), 1);
        assert!(tree.root().hash() != HashOutput::from_bytes([0u8; 32]));
    }

    #[test]
    fn build_single_chunk_data() {
        let data = vec![42u8; 100];
        let tree = MerkleTree::build(&data, DEFAULT_LEAF_SIZE).unwrap();
        assert_eq!(tree.leaf_count(), 1);
        assert_eq!(tree.total_size(), 100);
        let expected_hash = blake3::hash(&data);
        let mut exp_bytes = [0u8; 32];
        exp_bytes.copy_from_slice(expected_hash.as_bytes());
        assert_eq!(tree.root().hash(), HashOutput::from_bytes(exp_bytes));
    }

    #[test]
    fn build_two_leaf_data() {
        let data = vec![0u8; 65536 * 2]; // 128 KB, 2 leaves
        let tree = MerkleTree::build(&data, 65536).unwrap();
        assert_eq!(tree.leaf_count(), 2);
        assert_eq!(tree.leaf_hashes().len(), 2);
    }

    #[test]
    fn same_data_produce_same_root() {
        let data = vec![1u8; 65536];
        let t1 = MerkleTree::build(&data, 65536).unwrap();
        let t2 = MerkleTree::build(&data, 65536).unwrap();
        assert_eq!(t1.root().hash(), t2.root().hash());
    }

    #[test]
    fn different_data_produce_different_root() {
        let d1 = vec![1u8; 65536];
        let d2 = vec![2u8; 65536];
        let t1 = MerkleTree::build(&d1, 65536).unwrap();
        let t2 = MerkleTree::build(&d2, 65536).unwrap();
        assert_ne!(t1.root().hash(), t2.root().hash());
    }

    #[test]
    fn single_bit_corruption_changes_root() {
        let data = vec![0u8; 65536 * 2];
        let t1 = MerkleTree::build(&data, 65536).unwrap();

        let mut corrupted = data.clone();
        corrupted[131072 - 1] ^= 1; // flip last bit
        let t2 = MerkleTree::build(&corrupted, 65536).unwrap();

        assert_ne!(t1.root().hash(), t2.root().hash());
    }

    // -----------------------------------------------------------------------
    // MerkleTree::build_from_hashes
    // -----------------------------------------------------------------------

    #[test]
    fn build_from_hashes_empty_returns_none() {
        assert!(MerkleTree::build_from_hashes(&[]).is_none());
    }

    #[test]
    fn build_from_hashes_single_leaf() {
        let leaf = make_hash(42);
        let tree = MerkleTree::build_from_hashes(&[leaf]).unwrap();
        assert_eq!(tree.root().hash(), leaf);
        assert_eq!(tree.leaf_count(), 1);
    }

    #[test]
    fn build_from_hashes_two_leaves() {
        let a = make_hash(1);
        let b = make_hash(2);
        let tree = MerkleTree::build_from_hashes(&[a, b]).unwrap();
        assert_eq!(tree.leaf_count(), 2);

        let mut hasher = blake3::Hasher::new();
        hasher.update(a.as_bytes());
        hasher.update(b.as_bytes());
        let expected = hasher.finalize();
        let mut exp_bytes = [0u8; 32];
        exp_bytes.copy_from_slice(expected.as_bytes());
        assert_eq!(tree.root().hash(), HashOutput::from_bytes(exp_bytes));
    }

    #[test]
    fn build_from_hashes_deterministic() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3)];
        let t1 = MerkleTree::build_from_hashes(&leaves).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves).unwrap();
        assert_eq!(t1.root().hash(), t2.root().hash());
    }

    // -----------------------------------------------------------------------
    // MerkleTree::diff
    // -----------------------------------------------------------------------

    #[test]
    fn diff_identical_trees_returns_empty() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let t1 = MerkleTree::build_from_hashes(&leaves).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves).unwrap();
        assert!(t1.diff(&t2).is_empty());
    }

    #[test]
    fn diff_one_leaf_diverged() {
        let leaves1 = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let leaves2 = [make_hash(1), make_hash(99), make_hash(3), make_hash(4)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.diff(&t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 1);
        assert_eq!(diffs[0].end, 2);
    }

    #[test]
    fn diff_multiple_leaves_diverged() {
        let leaves1 = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let leaves2 = [make_hash(99), make_hash(2), make_hash(88), make_hash(4)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.diff(&t2);
        assert_eq!(diffs.len(), 2);
    }

    #[test]
    fn diff_rayon_parallel_for_large_trees() {
        let leaves1: Vec<HashOutput> = (0..64).map(|i| make_hash(i as u8)).collect();
        let mut leaves2 = leaves1.clone();
        leaves2[31] = make_hash(255);
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.diff(&t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 31);
        assert_eq!(diffs[0].end, 32);
    }

    #[test]
    fn diff_different_leaf_count_compares_min() {
        let leaves1 = [make_hash(1), make_hash(2)];
        let leaves2 = [make_hash(1), make_hash(2), make_hash(3)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.diff(&t2);
        assert!(diffs.is_empty()); // first 2 leaves match
    }

    #[test]
    fn diff_extra_leaves_in_self_marked_diverged() {
        let leaves1 = [make_hash(1), make_hash(2), make_hash(99)];
        let leaves2 = [make_hash(1), make_hash(2)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.diff(&t2);
        // The extra leaf at index 2 is marked as diverged
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 2);
    }

    #[test]
    fn leaf_hash_returns_correct_hash() {
        let leaves = [make_hash(10), make_hash(20), make_hash(30)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        assert_eq!(tree.leaf_hash(0), Some(make_hash(10)));
        assert_eq!(tree.leaf_hash(1), Some(make_hash(20)));
        assert_eq!(tree.leaf_hash(2), Some(make_hash(30)));
        assert_eq!(tree.leaf_hash(3), None);
    }

    // -----------------------------------------------------------------------
    // MerkleTree::descend_diff
    // -----------------------------------------------------------------------

    #[test]
    fn descend_diff_identical_trees_returns_empty() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let t1 = MerkleTree::build_from_hashes(&leaves).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves).unwrap();
        let diffs = t1.descend_diff(&t2);
        assert!(diffs.is_empty());
    }

    #[test]
    fn descend_diff_detects_single_diverged_leaf() {
        let leaves1 = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let leaves2 = [make_hash(1), make_hash(99), make_hash(3), make_hash(4)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.descend_diff(&t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 1);
        assert_eq!(diffs[0].end, 2);
    }

    #[test]
    fn descend_diff_with_two_leaves_finds_divergence() {
        let leaves1 = [make_hash(1), make_hash(2)];
        let leaves2 = [make_hash(1), make_hash(99)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.descend_diff(&t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 1);
    }

    #[test]
    fn descend_diff_with_large_tree_finds_all_divergences() {
        let leaves1: Vec<HashOutput> = (0..64).map(|i| make_hash(i as u8)).collect();
        let mut leaves2 = leaves1.clone();
        leaves2[10] = make_hash(200);
        leaves2[50] = make_hash(201);
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.descend_diff(&t2);
        assert_eq!(diffs.len(), 2);
        assert!(diffs.iter().any(|r| r.start == 10));
        assert!(diffs.iter().any(|r| r.start == 50));
    }

    #[test]
    fn descend_diff_with_odd_leaf_count_works() {
        let leaves1 = [make_hash(1), make_hash(2), make_hash(3)];
        let leaves2 = [make_hash(1), make_hash(99), make_hash(3)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.descend_diff(&t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 1);
    }

    #[test]
    fn descend_diff_with_single_leaf_works() {
        let leaves1 = [make_hash(1)];
        let leaves2 = [make_hash(99)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.descend_diff(&t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 0);
    }

    // -----------------------------------------------------------------------
    // MerkleProof
    // -----------------------------------------------------------------------

    #[test]
    fn generate_proof_for_valid_leaf() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        let proof = tree.generate_proof(0).unwrap();
        assert_eq!(proof.leaf_index, 0);
        assert_eq!(proof.root_hash, tree.root().hash());
        assert!(!proof.siblings.is_empty());
    }

    #[test]
    fn generate_proof_out_of_bounds_returns_none() {
        let leaves = [make_hash(1), make_hash(2)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        assert!(tree.generate_proof(2).is_none());
    }

    #[test]
    fn verify_proof_for_valid_leaf() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        let proof = tree.generate_proof(0).unwrap();
        assert!(tree.verify_proof(&proof));
    }

    #[test]
    fn verify_proof_rejects_wrong_root() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        let mut proof = tree.generate_proof(0).unwrap();
        proof.root_hash = make_hash(255);
        assert!(!tree.verify_proof(&proof));
    }

    #[test]
    fn verify_proof_rejects_wrong_leaf() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        let mut proof = tree.generate_proof(0).unwrap();
        proof.leaf_hash = make_hash(255);
        assert!(!tree.verify_proof(&proof));
    }

    #[test]
    fn verify_proof_rejects_mismatched_leaf_index() {
        let leaves = [make_hash(1), make_hash(2)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        let proof = tree.generate_proof(0).unwrap();
        let mut tampered_proof = proof.clone();
        tampered_proof.leaf_index = 1;
        assert!(!tree.verify_proof(&tampered_proof));
    }

    #[test]
    fn verify_proof_rejects_out_of_bounds_leaf() {
        let leaves = [make_hash(1), make_hash(2)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        let mut proof = tree.generate_proof(0).unwrap();
        proof.leaf_index = 99;
        assert!(!tree.verify_proof(&proof));
    }

    // -----------------------------------------------------------------------
    // MerkleRoot
    // -----------------------------------------------------------------------

    #[test]
    fn merkle_root_accessors() {
        let leaves = [make_hash(42)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        let root = tree.root();
        assert_eq!(root.hash(), make_hash(42));
        assert_eq!(root.leaf_count(), 1);
    }

    // -----------------------------------------------------------------------
    // InMemorySegmentStore
    // -----------------------------------------------------------------------

    #[test]
    fn in_memory_segment_store_read_write() {
        let store = InMemorySegmentStore::new();
        let seg_id = SegmentId::new();
        let data = vec![1u8, 2, 3];

        store.write_segment_data(&seg_id, &data).unwrap();
        let read = store.read_segment_data(&seg_id).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn in_memory_segment_store_not_found() {
        let store = InMemorySegmentStore::new();
        let result = store.read_segment_data(&SegmentId::new());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::SegmentNotFound(_)));
    }

    // -----------------------------------------------------------------------
    // MerkleTree with odd leaf count
    // -----------------------------------------------------------------------

    #[test]
    fn odd_leaf_count_tree_builds_correctly() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        assert_eq!(tree.leaf_count(), 3);
        let _root = tree.root();
    }

    #[test]
    fn odd_leaf_count_diff_works() {
        let leaves1 = [make_hash(1), make_hash(2), make_hash(3)];
        let leaves2 = [make_hash(1), make_hash(99), make_hash(3)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.diff(&t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 1);
    }

    #[test]
    fn contiguous_divergence_coalesces_ranges() {
        let leaves1 = [make_hash(1), make_hash(2), make_hash(3), make_hash(4)];
        let leaves2 = [make_hash(1), make_hash(99), make_hash(88), make_hash(4)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.diff(&t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 1);
        assert_eq!(diffs[0].end, 3);
    }

    // -----------------------------------------------------------------------
    // MerkleTree with large leaf count
    // -----------------------------------------------------------------------

    #[test]
    fn large_leaf_tree_builds_correctly() {
        let leaves: Vec<HashOutput> = (0..64).map(|i| make_hash(i as u8)).collect();
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        assert_eq!(tree.leaf_count(), 64);
        let _root = tree.root();
    }

    // -----------------------------------------------------------------------
    // MerkleRoot total_size
    // -----------------------------------------------------------------------

    #[test]
    fn merkle_root_total_size_from_build() {
        let data = vec![42u8; 100];
        let tree = MerkleTree::build(&data, 65536).unwrap();
        assert_eq!(tree.root().total_size(), 100);
    }

    #[test]
    fn merkle_root_total_size_from_hashes_is_zero() {
        let leaves = [make_hash(1), make_hash(2)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        assert_eq!(tree.root().total_size(), 0);
    }

    #[test]
    fn merkle_root_total_size_accessed_via_tree() {
        let data = vec![0u8; 65536];
        let tree = MerkleTree::build(&data, 65536).unwrap();
        assert_eq!(tree.total_size(), 65536);
    }

    // -----------------------------------------------------------------------
    // Leaf repair simulation
    // -----------------------------------------------------------------------

    #[test]
    fn leaf_repair_simulates_replace_corrupt_shard() {
        let original = vec![42u8; 65536 * 4];
        let original_tree = MerkleTree::build(&original, 65536).unwrap();

        let mut corrupted = original.clone();
        corrupted[2 * 65536] ^= 0x01;
        let corrupted_tree = MerkleTree::build(&corrupted, 65536).unwrap();

        assert_ne!(original_tree.root().hash(), corrupted_tree.root().hash());

        let diffs = original_tree.diff(&corrupted_tree);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].start, 2);
        assert_eq!(diffs[0].end, 3);

        let leaf_start = (diffs[0].start as usize) * 65536;
        let leaf_end = (diffs[0].end as usize) * 65536;
        let correct_shard_data = &original[leaf_start..leaf_end.min(original.len())];

        corrupted[leaf_start..leaf_end.min(original.len())].copy_from_slice(correct_shard_data);

        assert_eq!(corrupted, original);

        let repaired_tree = MerkleTree::build(&corrupted, 65536).unwrap();
        assert_eq!(original_tree.root().hash(), repaired_tree.root().hash());
    }

    #[test]
    fn leaf_repair_multiple_diverged_leaves() {
        let original = vec![0u8; 65536 * 8];
        let original_tree = MerkleTree::build(&original, 65536).unwrap();

        let mut corrupted = original.clone();
        corrupted[1 * 65536] ^= 0x01;
        corrupted[5 * 65536 + 100] ^= 0x02;
        let corrupted_tree = MerkleTree::build(&corrupted, 65536).unwrap();

        let diffs = original_tree.diff(&corrupted_tree);
        assert_eq!(diffs.len(), 2);

        for range in &diffs {
            let start = (range.start as usize) * 65536;
            let end = (range.end as usize) * 65536;
            let correct_data = &original[start..end.min(original.len())];
            corrupted[start..end.min(original.len())].copy_from_slice(correct_data);
        }

        assert_eq!(corrupted, original);
        let repaired_tree = MerkleTree::build(&corrupted, 65536).unwrap();
        assert_eq!(original_tree.root().hash(), repaired_tree.root().hash());
    }

    #[test]
    fn leaf_repair_single_leaf_tree() {
        let original = vec![1u8; 10000];
        let original_tree = MerkleTree::build(&original, 65536).unwrap();

        let mut corrupted = original.clone();
        corrupted[5000] ^= 0xff;
        let corrupted_tree = MerkleTree::build(&corrupted, 65536).unwrap();

        assert_ne!(original_tree.root().hash(), corrupted_tree.root().hash());

        let diffs = original_tree.diff(&corrupted_tree);
        assert_eq!(diffs.len(), 1);

        corrupted[0..original.len()].copy_from_slice(&original);
        assert_eq!(corrupted, original);
        let repaired_tree = MerkleTree::build(&corrupted, 65536).unwrap();
        assert_eq!(original_tree.root().hash(), repaired_tree.root().hash());
    }
}
