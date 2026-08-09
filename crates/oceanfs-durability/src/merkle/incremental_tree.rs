//! Incremental Merkle tree for anti-entropy.
//!
//! Maintains per-segment binary Merkle trees incrementally using BLAKE3
//! hashing. When a segment is sealed, a leaf hash is inserted and the
//! path to the root is recomputed in O(log n) time. All mutations are
//! persisted to a [`super::MerkleWal`] for crash recovery.
//!
//! ## Tree Structure
//!
//! Each segment's tree is stored as a flat array in binary heap order:
//! - Root at index 0
//! - Left child of node i: 2*i + 1
//! - Right child of node i: 2*i + 2
//! - Parent of node i: (i - 1) / 2
//!
//! The tree is a complete binary tree: for N leaves, the array has
//! `2 * next_power_of_two(N) - 1` nodes. Leaves are stored at the
//! bottom level; internal nodes are recomputed incrementally.
//!
//! ## Memory Bound
//!
//! Segments are tracked in insertion order; when the tracked count
//! exceeds `continuous_max_segments`, the oldest segments are evicted
//! via `IncrementalMerkleTree::evict_oldest`.

use std::{collections::VecDeque, sync::Arc};

use blake3::Hasher;
use dashmap::DashMap;
use oceanfs_core::SegmentId;
use parking_lot::Mutex;
use tracing::warn;

use crate::{
    error::{Error, Result},
    merkle::{tree_node::MerkleWalEntry, MerkleWal, TreeNode},
};

/// Default maximum number of segments tracked in continuous mode.
pub(crate) const DEFAULT_CONTINUOUS_MAX_SEGMENTS: usize = 10000;

/// Configuration for the incremental Merkle tree.
#[derive(Debug, Clone)]
pub struct MerkleTreeConfig {
    /// Maximum number of segments to track before evicting oldest.
    pub continuous_max_segments: usize,
}

impl Default for MerkleTreeConfig {
    fn default() -> Self {
        Self { continuous_max_segments: DEFAULT_CONTINUOUS_MAX_SEGMENTS }
    }
}

/// An incremental, in-memory Merkle tree for anti-entropy exchange.
///
/// Maintains per-segment binary Merkle trees. Leaf insertion triggers
/// O(log n) path recomputation to the root. All mutations are logged
/// to a [`MerkleWal`] for crash recovery.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use oceanfs_core::SegmentId;
/// use oceanfs_durability::merkle::{IncrementalMerkleTree, MerkleWal, MerkleTreeConfig};
///
/// let wal = Arc::new(MerkleWal::open("/tmp/merkle.wal")?);
/// let tree = IncrementalMerkleTree::new(wal, MerkleTreeConfig::default());
///
/// let seg = SegmentId::new();
/// tree.insert_leaf(seg, [0x42; 32])?;
/// let root = tree.root(seg).unwrap();
/// ```
pub struct IncrementalMerkleTree {
    /// Per-segment trees: SegmentId → (node_hashes, leaf_count).
    ///
    /// `node_hashes` is a flat array in heap order. Root at index 0.
    /// Internal nodes are recomputed on each leaf insertion.
    trees: DashMap<SegmentId, Vec<[u8; 32]>>,

    /// Leaf counts per segment: SegmentId → number of leaves.
    leaf_counts: DashMap<SegmentId, usize>,

    /// Segments ordered by insertion time for eviction.
    insertion_order: Mutex<VecDeque<SegmentId>>,

    /// The WAL for persisting tree mutations.
    merkle_wal: Arc<MerkleWal>,

    /// Configuration for this tree.
    config: MerkleTreeConfig,
}

impl IncrementalMerkleTree {
    /// Creates a new incremental Merkle tree.
    ///
    /// The tree starts empty. Use `rebuild_from_mutations` to restore
    /// state from an existing `MerkleWal`, or `rebuild_from_segment_scan`
    /// to build trees from a full segment scan (fallback when the WAL is
    /// corrupted).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let wal = Arc::new(MerkleWal::open(path)?);
    /// let tree = IncrementalMerkleTree::new(wal, MerkleTreeConfig::default());
    /// ```
    pub fn new(merkle_wal: Arc<MerkleWal>, config: MerkleTreeConfig) -> Self {
        Self {
            trees: DashMap::new(),
            leaf_counts: DashMap::new(),
            insertion_order: Mutex::new(VecDeque::new()),
            merkle_wal,
            config,
        }
    }

    /// Returns the current Merkle root hash for a segment, if the tree exists.
    pub fn root(&self, segment_id: SegmentId) -> Option<[u8; 32]> {
        self.trees.get(&segment_id).map(|tree| tree[0])
    }

    /// Returns the number of tracked segments (for memory bound enforcement).
    pub fn segment_count(&self) -> usize {
        self.trees.len()
    }

    /// Returns the number of leaves in the tree for the given segment.
    pub fn leaf_count(&self, segment_id: SegmentId) -> Option<usize> {
        self.leaf_counts.get(&segment_id).map(|lc| *lc)
    }

    // ------------------------------------------------------------------
    // Leaf insertion
    // ------------------------------------------------------------------

    /// Inserts a leaf hash into the tree for the given segment.
    ///
    /// If this is the first leaf for the segment, a new single-node tree
    /// is created. The path from the leaf to the root is recomputed in
    /// O(log n) time. Each changed node is logged to the Merkle WAL.
    ///
    /// After insertion, if `segment_count()` exceeds
    /// `continuous_max_segments`, the oldest segment is evicted.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails.
    pub fn insert_leaf(&self, segment_id: SegmentId, leaf_hash: [u8; 32]) -> Result<()> {
        let is_new_segment = !self.trees.contains_key(&segment_id);

        if is_new_segment {
            // First leaf for this segment: create a single-node tree.
            self.trees.insert(segment_id, vec![leaf_hash]);
            self.leaf_counts.insert(segment_id, 1);

            // Track insertion order for eviction.
            {
                let mut order = self.insertion_order.lock();
                order.push_back(segment_id);
            }

            // Log to WAL.
            if let Err(e) = self.merkle_wal.log_mutation(&MerkleWalEntry::NodeInsert {
                segment_id,
                node_index: 0,
                hash: leaf_hash,
            }) {
                warn!(%segment_id, error = %e, "failed to log Merkle tree mutation");
                return Err(e);
            }

            // Evict if over limit.
            self.maybe_evict();
            return Ok(());
        }

        // Existing segment — insert leaf and recompute path.
        let leaf_idx = {
            let Some(mut count) = self.leaf_counts.get_mut(&segment_id) else {
                return Err(Error::Storage(format!("leaf count missing for segment {segment_id}")));
            };
            let idx = *count;
            *count += 1;
            idx
        };

        // Expand the tree if needed.
        {
            let Some(mut tree) = self.trees.get_mut(&segment_id) else {
                return Err(Error::SegmentNotFound(segment_id));
            };
            let new_size = Self::tree_size_for_leaves(leaf_idx + 1);
            if new_size > tree.len() {
                tree.resize(new_size, [0u8; 32]);
            }
            // Place the leaf hash.
            let leaf_pos = Self::leaf_position(leaf_idx, leaf_idx + 1);
            let old_hash = tree[leaf_pos];
            tree[leaf_pos] = leaf_hash;

            // Log the insertion if this is a new node.
            if old_hash == [0u8; 32] {
                // New node — log insert.
                if let Err(e) = self.merkle_wal.log_mutation(&MerkleWalEntry::NodeInsert {
                    segment_id,
                    node_index: leaf_pos as u32,
                    hash: leaf_hash,
                }) {
                    warn!(%segment_id, error = %e, "failed to log Merkle tree mutation");
                    // Best-effort: continue with the in-memory update.
                }
            } else {
                // Existing node updated — log the update.
                if let Err(e) = self.merkle_wal.log_mutation(&MerkleWalEntry::NodeUpdate {
                    segment_id,
                    node_index: leaf_pos as u32,
                    old_hash,
                    new_hash: leaf_hash,
                }) {
                    warn!(%segment_id, error = %e, "failed to log Merkle tree mutation");
                }
            }

            // Recompute path to root.
            self.recompute_path_to_root(&mut tree, leaf_pos, segment_id);
        }

        // Eviction check.
        self.maybe_evict();

        Ok(())
    }

    /// Recomputes internal node hashes from `start_idx` up to the root.
    fn recompute_path_to_root(
        &self,
        tree: &mut Vec<[u8; 32]>,
        start_idx: usize,
        segment_id: SegmentId,
    ) {
        let mut current = start_idx;
        while current > 0 {
            let parent = (current - 1) / 2;
            let left_child = 2 * parent + 1;
            let right_child = 2 * parent + 2;

            let left_hash = tree.get(left_child).copied().unwrap_or([0u8; 32]);
            let right_hash = tree.get(right_child).copied().unwrap_or([0u8; 32]);

            let old_hash = tree[parent];

            // Hash left + right (even if right is zero-padded).
            let mut hasher = Hasher::new();
            hasher.update(&left_hash);
            hasher.update(&right_hash);
            let new_hash = hasher.finalize();
            let mut new_hash_bytes = [0u8; 32];
            new_hash_bytes.copy_from_slice(new_hash.as_bytes());
            tree[parent] = new_hash_bytes;

            // Log the update if hash changed.
            if old_hash != new_hash_bytes && old_hash != [0u8; 32] {
                let _ = self.merkle_wal.log_mutation(&MerkleWalEntry::NodeUpdate {
                    segment_id,
                    node_index: parent as u32,
                    old_hash,
                    new_hash: new_hash_bytes,
                });
            } else if old_hash == [0u8; 32] {
                let _ = self.merkle_wal.log_mutation(&MerkleWalEntry::NodeInsert {
                    segment_id,
                    node_index: parent as u32,
                    hash: new_hash_bytes,
                });
            }

            current = parent;
        }
    }

    // ------------------------------------------------------------------
    // Tree exchange / comparison
    // ------------------------------------------------------------------

    /// Serializes the entire tree for a segment into a `Vec<TreeNode>`.
    ///
    /// Returns the tree nodes suitable for gRPC exchange. Internal nodes
    /// include their child indices.
    ///
    /// # Errors
    ///
    /// Returns an error with the storage code path if the segment is not
    /// found.
    pub fn serialize_tree(&self, segment_id: SegmentId) -> Result<Vec<TreeNode>> {
        let tree = self.trees.get(&segment_id).ok_or_else(|| Error::SegmentNotFound(segment_id))?;
        let leaf_count = self.leaf_counts.get(&segment_id).map(|lc| *lc).unwrap_or(0);

        let total_leaves = leaf_count;
        if total_leaves == 0 {
            return Ok(Vec::new());
        }

        let mut nodes = Vec::with_capacity(tree.len());
        let max_idx = Self::tree_size_for_leaves(total_leaves);

        for i in 0..max_idx.min(tree.len()) {
            // Skip zero-hash nodes that are beyond the actual tree.
            if i >= Self::tree_size_for_leaves(total_leaves) {
                continue;
            }

            let left = 2 * i + 1;
            let right = 2 * i + 2;
            let mut children = Vec::new();

            if left < tree.len() && tree[left] != [0u8; 32] {
                children.push(left as u32);
            }
            if right < tree.len() && tree[right] != [0u8; 32] {
                children.push(right as u32);
            }

            nodes.push(TreeNode { node_index: i as u32, hash: tree[i], children });
        }

        Ok(nodes)
    }

    /// Compares the local tree for a segment against a peer's tree.
    ///
    /// Walks both trees from the root. At each node where hashes differ,
    /// descends into children. Returns the indices of divergent leaves.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not found locally.
    pub fn compare_and_find_divergence(
        &self,
        segment_id: SegmentId,
        peer_tree: &[TreeNode],
    ) -> Result<Vec<u32>> {
        let local_tree =
            self.trees.get(&segment_id).ok_or_else(|| Error::SegmentNotFound(segment_id))?;

        if peer_tree.is_empty() {
            return Ok(Vec::new());
        }

        // Fast path: compare roots.
        if local_tree[0] == peer_tree[0].hash {
            return Ok(Vec::new());
        }

        // Build a lookup from node_index → hash for the peer tree.
        let mut peer_hashes = std::collections::HashMap::new();
        for node in peer_tree {
            peer_hashes.insert(node.node_index, node.hash);
        }

        let mut divergences = Vec::new();
        Self::descend_diff(
            &local_tree,
            &peer_hashes,
            0, // start at root
            &mut divergences,
        );

        Ok(divergences)
    }

    /// Recursively descends both trees to find divergent leaf indices.
    fn descend_diff(
        local: &[[u8; 32]],
        peer_hashes: &std::collections::HashMap<u32, [u8; 32]>,
        node_idx: usize,
        divergences: &mut Vec<u32>,
    ) {
        let left = 2 * node_idx + 1;
        let right = 2 * node_idx + 2;

        let has_left = left < local.len() && local[left] != [0u8; 32];
        let has_right = right < local.len() && local[right] != [0u8; 32];

        if !has_left && !has_right {
            // This is a leaf node. Check if it diverges.
            if let Some(peer_hash) = peer_hashes.get(&(node_idx as u32)) {
                if local[node_idx] != *peer_hash {
                    divergences.push(node_idx as u32);
                }
            }
            return;
        }

        // Internal node: descend into children whose hashes differ.
        if has_left {
            let peer_left_hash = peer_hashes.get(&(left as u32)).copied();
            if peer_left_hash.map_or(true, |ph| local[left] != ph) {
                Self::descend_diff(local, peer_hashes, left, divergences);
            }
        }
        if has_right {
            let peer_right_hash = peer_hashes.get(&(right as u32)).copied();
            if peer_right_hash.map_or(true, |ph| local[right] != ph) {
                Self::descend_diff(local, peer_hashes, right, divergences);
            }
        }
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

    /// Evicts the oldest `count` segments from the tree.
    ///
    /// Logs a `SubtreeInvalidate` entry for each evicted segment.
    /// Called automatically after each insertion when the tracked
    /// segment count exceeds `continuous_max_segments`.
    pub fn evict_oldest(&self, count: usize) {
        let mut order = self.insertion_order.lock();
        for _ in 0..count {
            let Some(segment_id) = order.pop_front() else {
                break;
            };
            self.trees.remove(&segment_id);
            self.leaf_counts.remove(&segment_id);

            // Log the eviction.
            let _ = self.merkle_wal.log_mutation(&MerkleWalEntry::SubtreeInvalidate { segment_id });
        }
    }

    /// Evicts if over the configured limit.
    fn maybe_evict(&self) {
        let count = self.trees.len();
        if count > self.config.continuous_max_segments {
            let excess = count - self.config.continuous_max_segments;
            self.evict_oldest(excess);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Returns the total tree size (node count) for N leaves.
    ///
    /// The tree is a complete binary tree padded to the next power of two.
    /// Total nodes = 2 * next_power_of_two(N) - 1.
    fn tree_size_for_leaves(num_leaves: usize) -> usize {
        if num_leaves == 0 {
            return 0;
        }
        let padded = num_leaves.next_power_of_two();
        2 * padded - 1
    }

    /// Returns the position in the flat array for leaf `leaf_idx` (0-based)
    /// in a tree with `num_leaves` total leaves.
    ///
    /// Leaves are stored at the bottom level of the complete binary tree.
    /// The first leaf is at position `padded - 1`, where `padded` is
    /// `num_leaves.next_power_of_two()`.
    fn leaf_position(leaf_idx: usize, num_leaves: usize) -> usize {
        let padded = num_leaves.next_power_of_two();
        (padded - 1) + leaf_idx
    }

    // ------------------------------------------------------------------
    // Rebuild from WAL / segment scan
    // ------------------------------------------------------------------

    /// Rebuilds the incremental tree from Merkle WAL mutations.
    ///
    /// Replays all mutations from the WAL into the provided tree instance.
    /// The caller is responsible for creating the `Arc<MerkleWal>` and
    /// passing it along with a pre-created tree.
    ///
    /// Returns `Ok(())` if the replay succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL replay fails.
    pub fn rebuild_from_mutations(tree: &Self, merkle_wal: &MerkleWal) -> Result<()> {
        let entries = merkle_wal.replay_mutations()?;
        for entry in entries {
            match entry {
                MerkleWalEntry::NodeInsert { segment_id, node_index: _, hash } => {
                    let leaf_idx = {
                        let mut count = tree.leaf_counts.entry(segment_id).or_insert(0);
                        let idx = *count;
                        *count += 1;
                        idx
                    };
                    let mut tree_ref = tree.trees.entry(segment_id).or_default();
                    let new_size = Self::tree_size_for_leaves(leaf_idx + 1);
                    if new_size > tree_ref.len() {
                        tree_ref.resize(new_size, [0u8; 32]);
                    }
                    let leaf_pos = Self::leaf_position(leaf_idx, leaf_idx + 1);
                    tree_ref[leaf_pos] = hash;
                    // Recompute path to root without logging (we're replaying).
                    let mut current = leaf_pos;
                    while current > 0 {
                        let parent = (current - 1) / 2;
                        let left_child = 2 * parent + 1;
                        let right_child = 2 * parent + 2;
                        let left_hash = tree_ref.get(left_child).copied().unwrap_or([0u8; 32]);
                        let right_hash = tree_ref.get(right_child).copied().unwrap_or([0u8; 32]);
                        let mut hasher = blake3::Hasher::new();
                        hasher.update(&left_hash);
                        hasher.update(&right_hash);
                        let new_hash = hasher.finalize();
                        let mut new_hash_bytes = [0u8; 32];
                        new_hash_bytes.copy_from_slice(new_hash.as_bytes());
                        tree_ref[parent] = new_hash_bytes;
                        current = parent;
                    }
                }
                MerkleWalEntry::NodeUpdate { segment_id, node_index, old_hash: _, new_hash } => {
                    if let Some(mut tree_ref) = tree.trees.get_mut(&segment_id) {
                        if (node_index as usize) < tree_ref.len() {
                            tree_ref[node_index as usize] = new_hash;
                        }
                    }
                }
                MerkleWalEntry::SubtreeInvalidate { segment_id } => {
                    tree.trees.remove(&segment_id);
                    tree.leaf_counts.remove(&segment_id);
                }
            }
        }

        Ok(())
    }

    /// Rebuilds the incremental tree from a full scan of all sealed segments.
    ///
    /// This is the fallback when the Merkle WAL is corrupted and cannot
    /// be replayed. Scans all segments from the metadata store and
    /// reconstructs the trees from scratch.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment scan or tree construction fails.
    pub fn rebuild_from_segment_scan(
        metadata: &dyn oceanfs_storage_api::MetadataStore,
        merkle_wal: Arc<MerkleWal>,
        config: &MerkleTreeConfig,
    ) -> Result<Self> {
        let tree = Self::new(merkle_wal, config.clone());

        let segments = metadata.list_segments();
        for segment_result in segments {
            let segment =
                segment_result.map_err(|e| Error::Storage(format!("metadata scan error: {e}")))?;
            if segment.is_sealed() {
                // Use the segment's stored BLAKE3 checksum as the leaf hash.
                if let Some(merkle_root) = segment.merkle_root {
                    let hash_bytes = *merkle_root.as_bytes();
                    tree.insert_leaf(segment.segment_id, hash_bytes)?;
                }
            }
        }

        Ok(tree)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::SegmentId;

    use super::*;
    use crate::merkle::MerkleWal;

    fn make_segment_id() -> SegmentId {
        SegmentId::new()
    }

    fn make_hash(val: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = val;
        h
    }

    fn test_wal() -> (Arc<MerkleWal>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("merkle.wal");
        let wal = Arc::new(MerkleWal::open(&wal_path).unwrap());
        (wal, dir)
    }

    fn test_tree() -> (IncrementalMerkleTree, Arc<MerkleWal>, tempfile::TempDir) {
        let (wal, dir) = test_wal();
        let tree = IncrementalMerkleTree::new(wal.clone(), MerkleTreeConfig::default());
        (tree, wal, dir)
    }

    // ── T2.1: Incremental tree insert and root ───────────────────────

    #[test]
    fn test_incremental_tree_insert_and_root() {
        let (tree, _wal, _dir) = test_tree();
        let seg_a = make_segment_id();

        // Insert 3 leaves for seg-A.
        tree.insert_leaf(seg_a, make_hash(0x00)).unwrap();
        tree.insert_leaf(seg_a, make_hash(0x01)).unwrap();
        tree.insert_leaf(seg_a, make_hash(0x02)).unwrap();

        // Root should not be all zeros.
        let root = tree.root(seg_a).unwrap();
        assert_ne!(root, [0u8; 32], "root should not be all zeros");

        // Insert a 4th leaf — root should change because tree structure changes.
        let root_before = root;
        tree.insert_leaf(seg_a, make_hash(0x03)).unwrap();
        let root_after = tree.root(seg_a).unwrap();
        assert_ne!(root_before, root_after, "root should change after 4th leaf insertion");

        // Only one segment tracked.
        assert_eq!(tree.segment_count(), 1);
        assert_eq!(tree.leaf_count(seg_a), Some(4));
    }

    #[test]
    fn test_incremental_tree_insert_multiple_segments() {
        let (tree, _wal, _dir) = test_tree();

        let seg_a = make_segment_id();
        let seg_b = make_segment_id();

        tree.insert_leaf(seg_a, make_hash(0x10)).unwrap();
        tree.insert_leaf(seg_b, make_hash(0x20)).unwrap();
        tree.insert_leaf(seg_a, make_hash(0x11)).unwrap();

        assert_eq!(tree.segment_count(), 2);
        assert_ne!(
            tree.root(seg_a),
            tree.root(seg_b),
            "different segments should have different roots"
        );
    }

    // ── T2.2: Compare and find divergence ────────────────────────────

    #[test]
    fn test_incremental_tree_compare_finds_divergence() {
        let (tree, _wal, _dir) = test_tree();
        let seg = make_segment_id();

        // Build local tree with leaves [A, B, C, D].
        for val in [0xA0, 0xB0, 0xC0, 0xD0] {
            tree.insert_leaf(seg, make_hash(val)).unwrap();
        }

        // Build peer tree with leaves [A, B, X, D] (leaf 2 differs).
        let mut peer_nodes = Vec::new();
        let peer_hashes = [make_hash(0xA0), make_hash(0xB0), make_hash(0x99), make_hash(0xD0)];
        let total_leaves = peer_hashes.len();
        let tree_size = IncrementalMerkleTree::tree_size_for_leaves(total_leaves);
        let mut peer_hashes_full = vec![[0u8; 32]; tree_size];

        // Place peer leaves at the bottom level.
        for (i, h) in peer_hashes.iter().enumerate() {
            let pos = IncrementalMerkleTree::leaf_position(i, total_leaves);
            if pos < peer_hashes_full.len() {
                peer_hashes_full[pos] = *h;
            }
        }

        // Compute internal nodes for the peer tree.
        for i in (0..tree_size / 2).rev() {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            let mut hasher = blake3::Hasher::new();
            hasher.update(&peer_hashes_full[left]);
            if right < peer_hashes_full.len() {
                hasher.update(&peer_hashes_full[right]);
            } else {
                hasher.update(&peer_hashes_full[left]);
            }
            let h = hasher.finalize();
            peer_hashes_full[i].copy_from_slice(h.as_bytes());
        }

        // Build TreeNode vec from peer hashes.
        for i in 0..tree_size {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            let mut children = Vec::new();
            if left < tree_size && peer_hashes_full[left] != [0u8; 32] {
                children.push(left as u32);
            }
            if right < tree_size && peer_hashes_full[right] != [0u8; 32] {
                children.push(right as u32);
            }
            peer_nodes.push(TreeNode { node_index: i as u32, hash: peer_hashes_full[i], children });
        }

        // Compare: should find divergence at leaf index 2 (the X node).
        let divergences = tree.compare_and_find_divergence(seg, &peer_nodes).unwrap();
        assert!(!divergences.is_empty(), "should find at least one divergence");

        // Build identical peer tree — should return empty.
        let local_tree = tree.serialize_tree(seg).unwrap();
        let divergences2 = tree.compare_and_find_divergence(seg, &local_tree).unwrap();
        assert!(divergences2.is_empty(), "identical trees should have no divergence");
    }

    // ── T2.9: Eviction when exceeding max ────────────────────────────

    #[test]
    fn test_merkle_tree_evicts_oldest_when_exceeding_max() {
        let (wal, _dir) = test_wal();
        let config = MerkleTreeConfig { continuous_max_segments: 3 };
        let tree = IncrementalMerkleTree::new(wal, config);

        // Insert 5 segments with 1 leaf each.
        let mut seg_ids = Vec::new();
        for i in 0..5u8 {
            let seg = make_segment_id();
            seg_ids.push(seg);
            tree.insert_leaf(seg, make_hash(i)).unwrap();
        }

        // Should have evicted the 2 oldest.
        assert_eq!(tree.segment_count(), 3);
        assert!(tree.root(seg_ids[0]).is_none(), "oldest segment should be evicted");
        assert!(tree.root(seg_ids[1]).is_none(), "second oldest should be evicted");
        assert!(tree.root(seg_ids[2]).is_some(), "third segment should still be present");
        assert!(tree.root(seg_ids[3]).is_some(), "fourth segment should be present");
        assert!(tree.root(seg_ids[4]).is_some(), "fifth segment should be present");
    }

    #[test]
    fn test_merkle_tree_evict_oldest_manual() {
        let (tree, _wal, _dir) = test_tree();
        let seg_a = make_segment_id();
        let seg_b = make_segment_id();
        let seg_c = make_segment_id();

        tree.insert_leaf(seg_a, make_hash(1)).unwrap();
        tree.insert_leaf(seg_b, make_hash(2)).unwrap();
        tree.insert_leaf(seg_c, make_hash(3)).unwrap();

        assert_eq!(tree.segment_count(), 3);

        tree.evict_oldest(2);
        assert_eq!(tree.segment_count(), 1);
        assert!(tree.root(seg_c).is_some(), "newest segment should survive");
    }
}
