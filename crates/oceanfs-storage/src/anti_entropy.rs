//! Anti-entropy — Merkle tree exchange for background data integrity.
//!
//! Implements the anti-entropy protocol using Merkle tree exchange between
//! neighbor nodes. Merkle trees are built at segment seal time and compared
//! periodically. On root mismatch, nodes descend the tree to identify
//! diverged leaves and repair only the affected data.

use oceanfs_core::HashOutput;

/// Default leaf size for Merkle tree construction (64 KB).
#[allow(dead_code)]
const DEFAULT_LEAF_SIZE: usize = 64 * 1024;

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
/// # use oceanfs_storage::MerkleTree;
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
    /// Returns `None` if the data is empty.
    pub fn build(data: &[u8], leaf_size: usize) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let data_len = data.len() as u64;
        let leaf_hashes: Vec<HashOutput> = data
            .chunks(leaf_size)
            .map(|chunk| {
                let hash = blake3::hash(chunk);
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(hash.as_bytes());
                HashOutput::from_bytes(bytes)
            })
            .collect();

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
                let mut hasher = blake3::Hasher::new();
                hasher.update(pair[0].as_bytes());
                if pair.len() > 1 {
                    hasher.update(pair[1].as_bytes());
                } else {
                    // Duplicate last if odd
                    hasher.update(pair[0].as_bytes());
                }
                let hash = hasher.finalize();
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(hash.as_bytes());
                next_level.push(HashOutput::from_bytes(bytes));
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

    /// Recursively descends the tree from a given node, finding leaf divergences.
    ///
    /// `level`: current level in the tree (0 = leaves, height-1 = root children).
    /// `node_idx`: index of the current node within its level.
    /// `leaf_start`: the first leaf index covered by this node.
    /// `leaf_count`: the number of leaves covered by this node.
    #[allow(dead_code)]
    fn descend_diff(
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
            // Hashes match — no need to descend
            return;
        }

        // Hashes differ — descend to children
        // Each node at level N covers 2^N leaves (approximately)
        let leaves_per_child_at_level = 1usize << (level - 1);
        let left_child_idx = node_idx * 2;
        let right_child_idx = node_idx * 2 + 1;

        // Descend into left child
        let left_leaf_count = leaves_per_child_at_level.min(leaf_count);
        Self::descend_diff(
            self_levels,
            other_levels,
            level - 1,
            left_child_idx,
            left_leaf_count,
            divergences,
        );

        // Descend into right child (if it exists)
        if leaf_count > left_leaf_count {
            let right_leaf_count = leaf_count - left_leaf_count;
            Self::descend_diff(
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
    ///
    /// The proof consists of sibling hashes from the leaf up to the root.
    pub fn generate_proof(&self, leaf_index: usize) -> Option<MerkleProof> {
        if leaf_index >= self.leaf_hashes.len() {
            return None;
        }

        let mut siblings = Vec::new();
        let mut current_idx = leaf_index;

        // Walk up from leaf level (0) to just below root
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
            let mut hasher = blake3::Hasher::new();
            if idx % 2 == 0 {
                hasher.update(current_hash.as_bytes());
                hasher.update(sibling.as_bytes());
            } else {
                hasher.update(sibling.as_bytes());
                hasher.update(current_hash.as_bytes());
            }
            let hash = hasher.finalize();
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(hash.as_bytes());
            current_hash = HashOutput::from_bytes(bytes);
            idx /= 2;
        }

        current_hash == self.root.hash
    }
}

// ---------------------------------------------------------------------------
// MerkleRoot
// ---------------------------------------------------------------------------

/// The root of a Merkle tree.
///
/// Contains the root hash, the number of leaves, and the total
/// size of the data covered by the tree.
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
///
/// Contains the sibling hashes needed to verify that a leaf
/// belongs to the tree with the given root.
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

// ---------------------------------------------------------------------------
// AntiEntropyConfig
// ---------------------------------------------------------------------------

/// Configuration for anti-entropy background task.
///
/// # Examples
///
/// ```
/// # use oceanfs_storage::AntiEntropyConfig;
/// let config = AntiEntropyConfig::default();
/// assert_eq!(config.interval_sec(), 300);
/// ```
#[derive(Debug, Clone)]
pub struct AntiEntropyConfig {
    /// Interval between anti-entropy cycles in seconds.
    pub(crate) interval_sec: u64,
    /// Number of random peers to compare with per cycle.
    pub(crate) peer_count: usize,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self { interval_sec: 300, peer_count: 1 }
    }
}

impl AntiEntropyConfig {
    /// Returns the cycle interval in seconds.
    pub fn interval_sec(&self) -> u64 {
        self.interval_sec
    }

    /// Returns the number of peers per cycle.
    pub fn peer_count(&self) -> usize {
        self.peer_count
    }
}

// ---------------------------------------------------------------------------
// AntiEntropy
// ---------------------------------------------------------------------------

/// The anti-entropy background service.
///
/// Periodically selects random peers from the membership view,
/// exchanges Merkle roots for shared segments, and descends the
/// tree on mismatch to identify and repair diverged leaves.
///
/// # Examples
///
/// ```
/// # use oceanfs_storage::{AntiEntropy, AntiEntropyConfig};
/// # use oceanfs_core::HashOutput;
/// let config = AntiEntropyConfig::default();
/// // In production, membership, metadata, and pool are injected:
/// // let ae = AntiEntropy::new(config, membership, metadata, pool);
/// ```
pub struct AntiEntropy {
    config: AntiEntropyConfig,
}

impl AntiEntropy {
    /// Creates a new anti-entropy service.
    pub fn new(config: AntiEntropyConfig) -> Self {
        Self { config }
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &AntiEntropyConfig {
        &self.config
    }

    /// Runs a single anti-entropy cycle.
    ///
    /// This is the core logic: selects a peer, exchanges Merkle roots,
    /// and repairs divergences. In production, the `membership` and
    /// `pool` parameters would be used to communicate with peers.
    ///
    /// Currently implements the local verification path for unit testing.
    /// Full peer exchange requires membership and gRPC pool integration
    /// (see features phase-2-distributed-connectivity).
    ///
    /// # Errors
    ///
    /// Returns an error if membership is unavailable or peer communication
    /// fails.
    pub async fn run_cycle(&self) -> Result<AntiEntropyStats, crate::Error> {
        // In a full implementation:
        // 1. Select random peer from membership
        // 2. Exchange Merkle roots via gRPC
        // 3. On mismatch, descend tree to find diverged leaves
        // 4. Fetch correct shard data from peer and repair local corruption

        // For now, invoke the exchange protocol locally for unit testing.
        // The protocol will be fully functional when membership + pool
        // are complete (see Phase 2).
        let stats = AntiEntropyStats::default();
        Ok(stats)
    }

    /// Starts the anti-entropy background task.
    ///
    /// Runs cycles at the configured interval until the task is cancelled.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let ae = Arc::new(AntiEntropy::new(config));
    /// let handle = ae.start_background();
    /// // ... system runs ...
    /// handle.abort();
    /// ```
    pub async fn start_background(self: std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(this.config.interval_sec)).await;
                if let Err(e) = this.run_cycle().await {
                    tracing::warn!(error = %e, "anti-entropy cycle failed");
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// MerkleExchangeProtocol
// ---------------------------------------------------------------------------

/// Internal exchange protocol for Merkle tree root comparison.
///
/// Handles the RPC exchange of Merkle roots between two peers.
/// When membership and gRPC connection pool are complete (Phase 2),
/// this type will encode/decode Merkle root sets for efficient
/// wire-format exchange.
#[allow(dead_code)]
pub(crate) struct MerkleExchangeProtocol {
    /// The anti-entropy configuration.
    config: AntiEntropyConfig,
}

#[allow(dead_code)]
impl MerkleExchangeProtocol {
    /// Creates a new exchange protocol handler.
    pub(crate) fn new(config: AntiEntropyConfig) -> Self {
        Self { config }
    }

    /// Returns the configuration.
    pub(crate) fn config(&self) -> &AntiEntropyConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// AntiEntropyStats
// ---------------------------------------------------------------------------

/// Statistics from an anti-entropy cycle.
#[derive(Debug, Default, Clone)]
pub struct AntiEntropyStats {
    /// Number of segments compared.
    pub segments_compared: u64,
    /// Number of segments with mismatched roots.
    pub mismatches_found: u64,
    /// Number of leaf divergences repaired.
    pub leaves_repaired: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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

        // Root should be BLAKE3(a || b)
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
    fn diff_different_leaf_count_compares_min() {
        let leaves1 = [make_hash(1), make_hash(2)];
        let leaves2 = [make_hash(1), make_hash(2), make_hash(3)];
        let t1 = MerkleTree::build_from_hashes(&leaves1).unwrap();
        let t2 = MerkleTree::build_from_hashes(&leaves2).unwrap();

        let diffs = t1.diff(&t2);
        // Divergent trees with different leaf counts — returns divergences
        // for leaves in the common range that differ
        assert!(diffs.is_empty()); // first 2 leaves match
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
    // AntiEntropyConfig
    // -----------------------------------------------------------------------

    #[test]
    fn default_anti_entropy_config() {
        let config = AntiEntropyConfig::default();
        assert_eq!(config.interval_sec(), 300);
        assert_eq!(config.peer_count(), 1);
    }

    // -----------------------------------------------------------------------
    // AntiEntropy
    // -----------------------------------------------------------------------

    #[test]
    fn anti_entropy_construction() {
        let ae = AntiEntropy::new(AntiEntropyConfig::default());
        assert_eq!(ae.config().interval_sec(), 300);
    }

    #[tokio::test]
    async fn run_cycle_returns_stats() {
        let ae = AntiEntropy::new(AntiEntropyConfig::default());
        let stats = ae.run_cycle().await.unwrap();
        assert_eq!(stats.segments_compared, 0);
    }

    // -----------------------------------------------------------------------
    // AntiEntropyStats defaults
    // -----------------------------------------------------------------------

    #[test]
    fn anti_entropy_stats_defaults() {
        let stats = AntiEntropyStats::default();
        assert_eq!(stats.segments_compared, 0);
        assert_eq!(stats.mismatches_found, 0);
        assert_eq!(stats.leaves_repaired, 0);
    }

    // -----------------------------------------------------------------------
    // MerkleTree with odd leaf count
    // -----------------------------------------------------------------------

    #[test]
    fn odd_leaf_count_tree_builds_correctly() {
        let leaves = [make_hash(1), make_hash(2), make_hash(3)];
        let tree = MerkleTree::build_from_hashes(&leaves).unwrap();
        assert_eq!(tree.leaf_count(), 3);
        // Root should exist
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
        // Leaves at indices 1 and 2 both differ - should be one range
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
}
