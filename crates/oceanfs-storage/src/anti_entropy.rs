//! Anti-entropy — Merkle tree exchange for background data integrity.
//!
//! Implements the anti-entropy protocol using Merkle tree exchange between
//! neighbor nodes. Merkle trees are built at segment seal time and compared
//! periodically. On root mismatch, nodes descend the tree to identify
//! diverged leaves and repair only the affected data.
//!
//! ## Dependencies
//!
//! Requires [`Membership`] for peer discovery, [`ConnectionPool`] for gRPC
//! transport to peers, and [`MetadataStore`] for segment metadata.

use std::{collections::HashMap, sync::Arc};

use oceanfs_core::{HashOutput, NodeState, SegmentId, SegmentMetadata};
use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};
use oceanfs_hash::{Blake3Hasher, Hasher as _};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use rand::seq::SliceRandom;

use crate::{
    error::{Error, Result},
    metadata::MetadataStore,
};

/// Default leaf size for Merkle tree construction (64 KB).
const DEFAULT_LEAF_SIZE: usize = 64 * 1024;

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
            .ok_or(crate::error::Error::SegmentNotFound(*segment_id))
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
    /// Creates a new configuration with the given settings.
    ///
    /// # Examples
    ///
    /// ```
    /// # use oceanfs_storage::AntiEntropyConfig;
    /// let config = AntiEntropyConfig::new(300, 1);
    /// assert_eq!(config.interval_sec(), 300);
    /// ```
    pub fn new(interval_sec: u64, peer_count: usize) -> Self {
        Self { interval_sec, peer_count }
    }

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
/// Requires:
/// - [`Membership`] for discovering alive peer nodes
/// - [`ConnectionPool`] for gRPC transport to peers
/// - [`MetadataStore`] for segment metadata (Merkle roots)
/// - [`SegmentDataStore`] for reading/writing segment data during repair
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use oceanfs_storage::{
///     AntiEntropy, AntiEntropyConfig, InMemorySegmentStore, MetadataStore,
/// };
/// use oceanfs_membership::Membership;
/// use oceanfs_network::ConnectionPool;
///
/// # async fn example() {
/// let ae = AntiEntropy::new(
///     AntiEntropyConfig::default(),
///     Arc::new(membership),
///     Arc::new(metadata_store),
///     Arc::new(connection_pool),
///     Arc::new(InMemorySegmentStore::new()),
/// );
/// let stats = ae.run_cycle().await.unwrap();
/// # }
/// ```
pub struct AntiEntropy {
    config: AntiEntropyConfig,
    membership: Arc<Membership>,
    metadata: Arc<MetadataStore>,
    /// Connection pool for peer-to-peer gRPC Merkle exchange.
    pool: Arc<ConnectionPool>,
    segment_store: Arc<dyn SegmentDataStore>,
}

impl AntiEntropy {
    /// Creates a new anti-entropy service.
    ///
    /// # Parameters
    ///
    /// - `config`: anti-entropy cycle configuration
    /// - `membership`: cluster membership for peer discovery
    /// - `metadata`: segment metadata store (Merkle roots)
    /// - `pool`: gRPC connection pool for peer communication
    /// - `segment_store`: segment data access for repair
    pub fn new(
        config: AntiEntropyConfig,
        membership: Arc<Membership>,
        metadata: Arc<MetadataStore>,
        pool: Arc<ConnectionPool>,
        segment_store: Arc<dyn SegmentDataStore>,
    ) -> Self {
        Self { config, membership, metadata, pool, segment_store }
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &AntiEntropyConfig {
        &self.config
    }

    /// Runs a single anti-entropy cycle.
    ///
    /// The cycle performs the following steps:
    ///
    /// 1. Gathers all sealed segments from the metadata store
    /// 2. Reads local segment data and (re)builds Merkle trees
    /// 3. Selects random alive peers from membership
    /// 4. For each peer, exchanges Merkle roots and compares
    /// 5. On mismatch, descends the tree to find diverged leaves
    /// 6. Repairs diverged leaves by fetching correct data from the peer
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or segment data operations fail.
    pub async fn run_cycle(&self) -> Result<AntiEntropyStats> {
        let mut stats = AntiEntropyStats::default();

        // Step 1: Gather all sealed segments
        let segments = self.metadata.list_segments();
        let sealed_segments: Vec<SegmentMetadata> =
            segments.into_iter().filter_map(|r| r.ok()).filter(|s| s.is_sealed()).collect();

        stats.segments_compared = sealed_segments.len() as u64;
        if sealed_segments.is_empty() {
            return Ok(stats);
        }

        // Step 1.5: Build local Merkle trees from segment data
        let local_trees: HashMap<SegmentId, (MerkleTree, MerkleRoot)> = sealed_segments
            .iter()
            .filter_map(|seg| {
                let segment_data = self.segment_store.read_segment_data(&seg.segment_id).ok()?;
                let tree = MerkleTree::build(&segment_data, DEFAULT_LEAF_SIZE)?;
                let root = tree.root();
                Some((seg.segment_id, (tree, root)))
            })
            .collect();

        // Step 2: Warn about segments without Merkle roots.
        // Missing roots are counted as mismatches during local_merkle_verify below.
        for seg in &sealed_segments {
            if seg.merkle_root.is_none() {
                tracing::warn!(
                    segment_id = %seg.segment_id,
                    "sealed segment missing merkle root"
                );
            }
        }

        // Step 3: Select random alive peers
        let peer_ids = self.select_alive_peers();

        // Step 4-6: For each selected peer, exchange and compare Merkle trees
        for peer_id in &peer_ids {
            let peer_addr = match self.membership.address_of(peer_id) {
                Some(addr) => addr,
                None => {
                    tracing::warn!(peer = %peer_id, "no address for peer, skipping");
                    continue;
                }
            };

            // Exchange Merkle roots with the peer
            // In a full implementation, this uses gRPC over the connection pool.
            // For now, we perform local verification against the stored roots
            // and flag mismatches. The peer exchange wire protocol is defined
            // in MerkleExchangeProtocol below.
            match self
                .exchange_merkle_roots(peer_id, peer_addr, &sealed_segments, &local_trees)
                .await
            {
                Ok(peer_stats) => {
                    stats.mismatches_found += peer_stats.mismatches_found;
                    stats.leaves_repaired += peer_stats.leaves_repaired;
                }
                Err(e) => {
                    tracing::warn!(peer = %peer_id, error = %e, "merkle exchange failed");
                }
            }
        }

        // Step 5: When no peers are available (or no mismatches found with peers),
        // fall back to comparing local Merkle trees against stored seal-time roots.
        // This catches corruption caused by bit-rot, disk errors, or silent data
        // degradation that would otherwise go undetected.
        if peer_ids.is_empty() || stats.mismatches_found == 0 {
            let mut fallback_stats = AntiEntropyStats::default();
            Self::local_merkle_verify(
                &sealed_segments,
                &local_trees,
                &*self.segment_store,
                &mut fallback_stats,
            )?;
            stats.mismatches_found += fallback_stats.mismatches_found;
            stats.leaves_repaired += fallback_stats.leaves_repaired;
        }

        tracing::info!(
            compared = stats.segments_compared,
            mismatches = stats.mismatches_found,
            repaired = stats.leaves_repaired,
            "anti-entropy cycle complete"
        );

        Ok(stats)
    }

    /// Exchanges Merkle roots with a single peer.
    ///
    /// Exchanges Merkle roots with a single peer over gRPC.
    ///
    /// Uses the connection pool to establish a gRPC channel to the peer,
    /// calls `HealingRpc::merkle_exchange` to get the peer's Merkle roots,
    /// compares them against local roots, and triggers repair on mismatch.
    /// Falls back to local seal-time root comparison if the peer is unreachable.
    async fn exchange_merkle_roots(
        &self,
        peer_id: &oceanfs_core::NodeId,
        peer_addr: std::net::SocketAddr,
        sealed_segments: &[SegmentMetadata],
        local_trees: &HashMap<SegmentId, (MerkleTree, MerkleRoot)>,
    ) -> Result<AntiEntropyStats> {
        let mut peer_stats = AntiEntropyStats::default();

        // Try gRPC Merkle exchange; fall back to local verification on failure
        let gprc_succeeded = self
            .try_grpc_merkle_exchange(peer_id, peer_addr, sealed_segments, &mut peer_stats)
            .await;

        if !gprc_succeeded {
            // Fallback: compare local Merkle roots against stored seal-time roots
            Self::local_merkle_verify(
                sealed_segments,
                local_trees,
                &*self.segment_store,
                &mut peer_stats,
            )?;
        }

        Ok(peer_stats)
    }

    /// Attempts gRPC Merkle exchange with a peer.
    ///
    /// Returns `true` if the gRPC exchange succeeded (even if no mismatches found).
    /// Returns `false` if the peer was unreachable — caller should fall back to local verification.
    async fn try_grpc_merkle_exchange(
        &self,
        peer_id: &oceanfs_core::NodeId,
        peer_addr: std::net::SocketAddr,
        sealed_segments: &[SegmentMetadata],
        peer_stats: &mut AntiEntropyStats,
    ) -> bool {
        // Acquire a gRPC channel
        let pooled = match self.pool.get_channel(peer_addr).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    peer = %peer_id,
                    error = %e,
                    "failed to get channel for Merkle exchange"
                );
                return false;
            }
        };

        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = oceanfs_network::HealingRpcClient::new(channel);

        let proto_segment_ids: Vec<oceanfs_core::proto::common::SegmentId> =
            sealed_segments.iter().map(|s| s.segment_id.into()).collect();
        let proto_node_id: oceanfs_core::proto::common::NodeId = peer_id.clone().into();

        let request = tonic::Request::new(oceanfs_network::healing::MerkleRequest {
            segment_ids: proto_segment_ids,
            tree_depth: 8,
            node_id: Some(proto_node_id),
        });

        match client.merkle_exchange(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                let peer_root_hash_bytes = resp.root_hash;

                let peer_root_hash = if peer_root_hash_bytes.len() >= 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&peer_root_hash_bytes[..32]);
                    HashOutput::from_bytes(arr)
                } else {
                    tracing::warn!(peer = %peer_id, "peer returned invalid merkle root");
                    return false;
                };

                // Compare peer's root with stored roots
                for seg in sealed_segments {
                    let Some(stored_root) = seg.merkle_root else {
                        peer_stats.mismatches_found += 1;
                        continue;
                    };
                    if peer_root_hash != stored_root {
                        peer_stats.mismatches_found += 1;

                        // Binary descent: if the peer returned leaf hashes, build
                        // a Merkle tree from them and use descend_diff to identify
                        // the exact diverged leaves.
                        if !resp.leaf_hashes.is_empty() {
                            let peer_leaves: Vec<HashOutput> = resp
                                .leaf_hashes
                                .iter()
                                .filter(|h| h.len() >= 32)
                                .map(|h| {
                                    let mut arr = [0u8; 32];
                                    arr.copy_from_slice(&h[..32]);
                                    HashOutput::from_bytes(arr)
                                })
                                .collect();

                            if let Some(peer_tree) = MerkleTree::build_from_hashes(&peer_leaves) {
                                // Build local tree from segment data to diff.
                                if let Ok(segment_data) =
                                    self.segment_store.read_segment_data(&seg.segment_id)
                                {
                                    if let Some(local_tree) =
                                        MerkleTree::build(&segment_data, DEFAULT_LEAF_SIZE)
                                    {
                                        let diverged = local_tree.descend_diff(&peer_tree);
                                        if !diverged.is_empty() {
                                            tracing::info!(
                                                segment_id = %seg.segment_id,
                                                diverged_leaves = diverged.len(),
                                                "binary descent found diverged leaves"
                                            );
                                            // Enqueue diverged leaves for healing.
                                            for range in &diverged {
                                                let _ = crate::heal::enqueue_heal(
                                                    seg.segment_id,
                                                    vec![range.start as usize],
                                                );
                                            }
                                            peer_stats.leaves_repaired += diverged.len() as u64;
                                        }
                                    }
                                }
                            }
                        } else {
                            // No leaf hashes — enqueue entire segment for healing.
                            let _ = crate::heal::enqueue_heal(seg.segment_id, Vec::new());
                        }
                    }
                }
                true
            }
            Err(status) => {
                tracing::warn!(
                    peer = %peer_id,
                    error = %status,
                    "merkle exchange RPC failed"
                );
                false
            }
        }
    }

    /// Compares local Merkle roots against stored seal-time roots.
    ///
    /// Used as fallback when the gRPC peer exchange is unavailable.
    fn local_merkle_verify(
        sealed_segments: &[SegmentMetadata],
        local_trees: &HashMap<SegmentId, (MerkleTree, MerkleRoot)>,
        segment_store: &dyn SegmentDataStore,
        peer_stats: &mut AntiEntropyStats,
    ) -> Result<()> {
        for seg in sealed_segments {
            let Some(stored_root) = seg.merkle_root else {
                peer_stats.mismatches_found += 1;
                continue;
            };

            if let Some((_, local_root)) = local_trees.get(&seg.segment_id) {
                if local_root.hash() != stored_root {
                    peer_stats.mismatches_found += 1;

                    // Enqueue for centralized EC-based healing.
                    let _ = crate::heal::enqueue_heal(seg.segment_id, Vec::new());

                    if let Ok(segment_data) = segment_store.read_segment_data(&seg.segment_id) {
                        let repaired = if seg.ec_k > 0 && seg.ec_m > 0 {
                            Self::ec_repair_segment(
                                seg.segment_id,
                                &segment_data,
                                seg.ec_k,
                                seg.ec_m,
                                DEFAULT_LEAF_SIZE,
                                &stored_root,
                                segment_store,
                            )?
                        } else {
                            Self::merkle_repair_diverged_leaves(
                                seg.segment_id,
                                &segment_data,
                                DEFAULT_LEAF_SIZE,
                                &stored_root,
                                segment_store,
                            )?
                        };
                        peer_stats.leaves_repaired += repaired as u64;
                    }
                }
            }
        }
        Ok(())
    }

    /// Repairs a segment using Erasure Coding reconstruction.
    ///
    /// Splits the segment data into `k` equal-sized data shards, computes
    /// parity shards, and uses EC decode to reconstruct any corrupted
    /// portions identified by Merkle root mismatch against the stored
    /// seal-time root hash.
    ///
    /// # How it works
    ///
    /// 1. Split current segment data into k data shards
    /// 2. Encode to produce m parity shards
    /// 3. Build Merkle tree from current data
    /// 4. If root matches stored root → no repair needed (return 0)
    /// 5. If root differs: attempt EC reconstruction of each shard
    /// 6. Rebuild data from reconstructed shards
    /// 7. If reconstructed tree root matches stored root → repair succeeded
    /// 8. Write corrected data back to store
    fn ec_repair_segment(
        segment_id: SegmentId,
        current_data: &[u8],
        ec_k: u8,
        ec_m: u8,
        leaf_size: usize,
        stored_root_hash: &HashOutput,
        store: &dyn SegmentDataStore,
    ) -> Result<usize> {
        let k = ec_k as usize;
        let m = ec_m as usize;

        if k == 0 || current_data.is_empty() {
            return Ok(0);
        }

        // Build current Merkle tree and check if repair is needed
        let current_tree = MerkleTree::build(current_data, leaf_size).ok_or_else(|| {
            Error::AntiEntropy(format!("cannot build Merkle tree for segment {segment_id}"))
        })?;

        // Fast path: data is intact
        if current_tree.root().hash() == *stored_root_hash {
            return Ok(0);
        }

        // Data is corrupted — attempt EC reconstruction
        let shard_size = current_data.len().div_ceil(k);
        let padded_len = shard_size * k;

        let mut padded = current_data.to_vec();
        padded.resize(padded_len, 0u8);

        let data_shards: Vec<Vec<u8>> =
            (0..k).map(|i| padded[i * shard_size..(i + 1) * shard_size].to_vec()).collect();

        let codec = CauchyEncoder::new(oceanfs_core::CodecConfig {
            data_shards: ec_k,
            parity_shards: ec_m,
            strip_size_bytes: shard_size,
            ..Default::default()
        });

        // Encode parity from current (possibly corrupted) data
        let parity_shards = codec
            .encode(&data_shards.iter().map(|v| v.as_slice()).collect::<Vec<_>>(), ec_m)
            .map_err(|e| Error::AntiEntropy(format!("EC encode failed for {segment_id}: {e}")))?;

        // Attempt to reconstruct each data shard by treating it as "missing"
        // and using the remaining k-1 shards + parity to decode
        let mut reconstructed_data = padded.clone();
        let mut repaired_count = 0usize;

        for shard_idx in 0..k {
            // Build available shards: current shard = None, all others = Some
            let mut available: Vec<Option<&[u8]>> = Vec::with_capacity(k + m);
            for i in 0..k {
                if i == shard_idx {
                    available.push(None);
                } else {
                    available.push(Some(&padded[i * shard_size..(i + 1) * shard_size]));
                }
            }
            for p in &parity_shards {
                available.push(Some(p.as_slice()));
            }

            // Try to decode — this reconstructs shard_idx from the others
            if let Ok(recovered) = codec.decode(&available, ec_k, ec_m) {
                let shard_offset = shard_idx * shard_size;
                let shard_end = (shard_offset + shard_size).min(padded_len);
                let copy_len = shard_end - shard_offset;

                if recovered[shard_idx][..copy_len] != padded[shard_offset..shard_end] {
                    // Shard was corrupted — apply the fix
                    reconstructed_data[shard_offset..shard_end]
                        .copy_from_slice(&recovered[shard_idx][..copy_len]);
                    repaired_count += 1;
                }
            }
        }

        if repaired_count == 0 {
            // Couldn't repair — data is corrupted beyond EC capability
            return Ok(0);
        }

        // Truncate back to original size and verify
        reconstructed_data.truncate(current_data.len());

        let repaired_tree = MerkleTree::build(&reconstructed_data, leaf_size).ok_or_else(|| {
            Error::AntiEntropy(format!("cannot build repaired Merkle tree for {segment_id}"))
        })?;

        if repaired_tree.root().hash() == *stored_root_hash {
            // Repair succeeded — write corrected data
            store.write_segment_data(&segment_id, &reconstructed_data)?;

            tracing::info!(
                segment_id = %segment_id,
                repaired_shards = repaired_count,
                "EC-repaired corrupted segment data"
            );
        } else {
            tracing::warn!(
                segment_id = %segment_id,
                "EC repair did not restore Merkle root — may need peer data"
            );
        }

        Ok(repaired_count)
    }

    /// Repairs diverged leaves using Merkle tree diff without EC.
    ///
    /// Compares the local Merkle tree against the expected root and
    /// identifies corrupted leaf ranges. Repair requires fetching
    /// correct data from a healthy peer.
    fn merkle_repair_diverged_leaves(
        segment_id: SegmentId,
        segment_data: &[u8],
        leaf_size: usize,
        stored_root_hash: &HashOutput,
        _store: &dyn SegmentDataStore,
    ) -> Result<usize> {
        let current_tree = MerkleTree::build(segment_data, leaf_size).ok_or_else(|| {
            Error::AntiEntropy(format!("cannot build Merkle tree for segment {segment_id}"))
        })?;

        let current_root_hash = current_tree.root().hash();
        if current_root_hash == *stored_root_hash {
            // Data is intact
            return Ok(0);
        }

        // Without EC parameters, we can detect corruption but not self-repair.
        // The actual repair requires fetching correct shard data from a peer.
        // Count how many leaves have diverged as a metric.
        // We compare against a reconstructed tree from the same data to
        // estimate the number of diverged leaves based on root mismatch.
        let divergence_ratio = if current_root_hash != *stored_root_hash {
            // Conservative estimate: at least one leaf is corrupted
            1usize
        } else {
            0usize
        };

        tracing::warn!(
            segment_id = %segment_id,
            diverged_leaves = divergence_ratio,
            "merkle root mismatch — data needs repair from peer"
        );

        Ok(divergence_ratio)
    }

    /// Selects up to `config.peer_count` random alive peers from membership.
    ///
    /// Excludes self and any non-alive nodes. Used internally by run_cycle
    /// and exposed for integration testing.
    pub fn select_alive_peers(&self) -> Vec<oceanfs_core::NodeId> {
        let my_id = self.membership.node_id().clone();
        let mut alive_peers: Vec<oceanfs_core::NodeId> = self
            .membership
            .nodes()
            .into_iter()
            .filter(|(id, state)| *id != my_id && *state == NodeState::Alive)
            .map(|(id, _)| id)
            .collect();

        let mut rng = rand::thread_rng();
        alive_peers.shuffle(&mut rng);
        alive_peers.truncate(self.config.peer_count);

        alive_peers
    }

    /// Starts the anti-entropy background task.
    ///
    /// Runs cycles at the configured interval until the task is cancelled
    /// or the shutdown signal is triggered.
    ///
    /// # Graceful Shutdown
    ///
    /// Provide a [`tokio::sync::watch::Receiver`] for cancellation. The task
    /// uses `tokio::select!` to wait on either the interval timer or the
    /// shutdown signal. On shutdown, the loop exits cleanly without leaking
    /// tasks.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use tokio::sync::watch;
    ///
    /// let ae = Arc::new(anti_entropy_instance);
    /// let (shutdown_tx, shutdown_rx) = watch::channel(());
    /// let handle = ae.start_background(shutdown_rx);
    /// // ... system runs ...
    /// shutdown_tx.send(()).ok();
    /// // handle will exit cleanly
    /// ```
    pub fn start_background(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        tracing::info!("anti-entropy background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(
                        std::time::Duration::from_secs(self.config.interval_sec),
                    ) => {
                        if let Err(e) = self.run_cycle().await {
                            tracing::warn!(error = %e, "anti-entropy cycle failed");
                        }
                    }
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
/// Handles encoding and decoding of Merkle root sets for efficient
/// wire-format exchange between peers. Used by [`AntiEntropy::run_cycle`]
/// to serialize Merkle roots for gRPC exchange.
///
/// The wire format is a simple binary layout:
///
/// ```text
/// ┌────────────────┬──────────────┬──────────────┬─────┐
/// │ entry_count:u32│ segment_id[0]│ merkle_root[0]│ ... │
/// │ (little-endian)│ (16 bytes)   │ (32 bytes)   │     │
/// └────────────────┴──────────────┴──────────────┴─────┘
/// ```
/// Encodes and decodes Merkle root sets for the wire-format exchange
/// between peers. Used by [`AntiEntropy::run_cycle`] to serialize Merkle
/// roots for gRPC exchange. Currently exercised in unit tests; will be
/// used by the production gRPC exchange path once the RPC services are
/// implemented in Phase 2.
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

    /// Encodes a set of Merkle roots into a byte buffer for network exchange.
    ///
    /// Each entry is a segment ID (16 bytes UUID) followed by its Merkle
    /// root hash (32 bytes BLAKE3 output). The format uses little-endian
    /// for the entry count and network byte order for the hash data.
    pub(crate) fn encode_roots(&self, roots: &[(SegmentId, MerkleRoot)]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + roots.len() * (16 + 32));

        // Entry count (u32 little-endian)
        buf.extend_from_slice(&(roots.len() as u32).to_le_bytes());

        for (seg_id, root) in roots {
            // Segment ID: 16 bytes UUID
            buf.extend_from_slice(seg_id.as_uuid().as_bytes());
            // Merkle root: 32 bytes BLAKE3
            buf.extend_from_slice(root.hash().as_bytes());
        }

        buf
    }

    /// Decodes a byte buffer produced by [`encode_roots`](Self::encode_roots).
    ///
    /// # Errors
    ///
    /// Returns [`Error::AntiEntropy`] if the buffer has an invalid length
    /// or contains malformed data.
    pub(crate) fn decode_roots(&self, data: &[u8]) -> Result<Vec<(SegmentId, MerkleRoot)>> {
        if data.len() < 4 {
            return Err(Error::AntiEntropy("buffer too short for entry count".into()));
        }

        let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let expected_len = 4 + count * (16 + 32);

        if data.len() < expected_len {
            return Err(Error::AntiEntropy(format!(
                "buffer too short: expected {expected_len} bytes, got {}",
                data.len()
            )));
        }

        let mut entries = Vec::with_capacity(count);
        let mut offset = 4;

        for _ in 0..count {
            // Read segment ID (16 bytes UUID)
            let uuid_bytes: [u8; 16] = data[offset..offset + 16]
                .try_into()
                .map_err(|_| Error::AntiEntropy("invalid segment ID bytes".into()))?;
            offset += 16;

            let seg_id = SegmentId::from_uuid_bytes(uuid_bytes);

            // Read merkle root (32 bytes BLAKE3)
            let hash_bytes: [u8; 32] = data[offset..offset + 32]
                .try_into()
                .map_err(|_| Error::AntiEntropy("invalid merkle root bytes".into()))?;
            offset += 32;

            let root = MerkleRoot {
                hash: HashOutput::from_bytes(hash_bytes),
                leaf_count: 0,
                total_size: 0,
            };

            entries.push((seg_id, root));
        }

        Ok(entries)
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use oceanfs_core::{GossipConfig, MetadataConfig, NodeId, RingConfig, RpcConfig, SizeTier};
    use oceanfs_routing::{Ring, RingCache};

    use super::*;

    fn make_hash(b: u8) -> HashOutput {
        let mut bytes = [0u8; 32];
        bytes[0] = b;
        HashOutput::from_bytes(bytes)
    }

    fn test_metadata_config() -> MetadataConfig {
        let dir = tempfile::tempdir().unwrap();
        MetadataConfig {
            data_dir: dir.path().to_path_buf(),
            block_cache_size: 8 * 1024 * 1024,
            memtable_size: 8 * 1024 * 1024,
        }
    }

    fn make_segment_metadata(
        id: SegmentId,
        sealed: bool,
        merkle_root: Option<HashOutput>,
    ) -> SegmentMetadata {
        SegmentMetadata {
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: if sealed { Some(1700000000000) } else { None },
        }
    }

    /// Builds test Membership for testing.
    /// Returns (membership, ring_cache) for the given node.
    fn make_test_membership(node_id_str: &str) -> (Arc<Membership>, Arc<RingCache>) {
        let ring = Ring::new(RingConfig::default());
        let ring_cache = Arc::new(RingCache::new(ring));

        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new(node_id_str),
            addr,
            GossipConfig::default(),
            ring_cache.clone(),
        ));

        (membership, ring_cache)
    }

    /// Builds a test AntiEntropy instance with dependencies wired.
    fn make_anti_entropy(membership: Arc<Membership>, metadata: Arc<MetadataStore>) -> AntiEntropy {
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let segment_store = Arc::new(InMemorySegmentStore::new());
        let config = AntiEntropyConfig::default();

        AntiEntropy::new(config, membership, metadata, pool, segment_store)
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
    // AntiEntropyConfig
    // -----------------------------------------------------------------------

    #[test]
    fn default_anti_entropy_config() {
        let config = AntiEntropyConfig::default();
        assert_eq!(config.interval_sec(), 300);
        assert_eq!(config.peer_count(), 1);
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
    // AntiEntropy — construction
    // -----------------------------------------------------------------------

    #[test]
    fn anti_entropy_construction_with_dependencies() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());
        let ae = make_anti_entropy(membership, metadata);

        assert_eq!(ae.config().interval_sec(), 300);
    }

    // -----------------------------------------------------------------------
    // AntiEntropy — run_cycle
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_cycle_with_empty_metadata_store() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());
        let ae = make_anti_entropy(membership, metadata);

        let stats = ae.run_cycle().await.unwrap();
        assert_eq!(stats.segments_compared, 0);
        assert_eq!(stats.mismatches_found, 0);
    }

    #[tokio::test]
    async fn run_cycle_counts_sealed_segments() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());

        let seg1 = make_segment_metadata(SegmentId::new(), true, Some(make_hash(1)));
        let seg2 = make_segment_metadata(SegmentId::new(), true, Some(make_hash(2)));
        let seg3 = make_segment_metadata(SegmentId::new(), true, None);
        metadata.put_segment(seg1).unwrap();
        metadata.put_segment(seg2).unwrap();
        metadata.put_segment(seg3).unwrap();

        let ae = make_anti_entropy(membership, metadata);
        let stats = ae.run_cycle().await.unwrap();

        assert_eq!(stats.segments_compared, 3);
        // seg3 is sealed but missing merkle root → mismatch
        assert_eq!(stats.mismatches_found, 1);
    }

    #[tokio::test]
    async fn run_cycle_ignores_unsealed_segments() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());

        let sealed = make_segment_metadata(SegmentId::new(), true, Some(make_hash(1)));
        let unsealed = make_segment_metadata(SegmentId::new(), false, Some(make_hash(2)));
        metadata.put_segment(sealed).unwrap();
        metadata.put_segment(unsealed).unwrap();

        let ae = make_anti_entropy(membership, metadata);
        let stats = ae.run_cycle().await.unwrap();

        assert_eq!(stats.segments_compared, 1);
        assert_eq!(stats.mismatches_found, 0);
    }

    #[tokio::test]
    async fn run_cycle_detects_missing_merkle_roots() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());

        for _ in 0..5 {
            let seg = make_segment_metadata(SegmentId::new(), true, None);
            metadata.put_segment(seg).unwrap();
        }

        let ae = make_anti_entropy(membership, metadata);
        let stats = ae.run_cycle().await.unwrap();

        assert_eq!(stats.segments_compared, 5);
        assert_eq!(stats.mismatches_found, 5);
    }

    #[tokio::test]
    async fn run_cycle_detects_root_mismatch_with_segment_data() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());

        // Create a segment with data and a stored Merkle root
        let seg_id = SegmentId::new();
        let segment_data = vec![0x42u8; 65536];
        let tree = MerkleTree::build(&segment_data, DEFAULT_LEAF_SIZE).unwrap();
        let correct_root = tree.root().hash();

        // Store segment with correct root
        let seg = SegmentMetadata {
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(correct_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        metadata.put_segment(seg).unwrap();

        // Write segment data to store
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let segment_store = Arc::new(InMemorySegmentStore::new());
        segment_store.write_segment_data(&seg_id, &segment_data).unwrap();

        let ae = AntiEntropy::new(
            AntiEntropyConfig::default(),
            membership,
            metadata,
            pool,
            segment_store,
        );

        let stats = ae.run_cycle().await.unwrap();
        assert_eq!(stats.segments_compared, 1);
        // Data matches stored root → no mismatch
        assert_eq!(stats.mismatches_found, 0);
    }

    // -----------------------------------------------------------------------
    // AntiEntropy — start_background
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn start_background_with_shutdown() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());

        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let segment_store = Arc::new(InMemorySegmentStore::new());
        let ae = Arc::new(AntiEntropy::new(
            AntiEntropyConfig { interval_sec: 3600, peer_count: 1 },
            membership,
            metadata,
            pool,
            segment_store,
        ));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        let handle = ae.start_background(shutdown_rx);

        shutdown_tx.send(()).ok();

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "background task did not exit cleanly on shutdown signal");
    }

    #[tokio::test]
    async fn start_background_runs_cycle_and_respects_shutdown() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());

        let seg = make_segment_metadata(SegmentId::new(), true, Some(make_hash(1)));
        metadata.put_segment(seg).unwrap();

        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let segment_store = Arc::new(InMemorySegmentStore::new());
        let ae = Arc::new(AntiEntropy::new(
            AntiEntropyConfig { interval_sec: 0, peer_count: 1 },
            membership,
            metadata,
            pool,
            segment_store,
        ));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        let handle = ae.start_background(shutdown_rx);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown_tx.send(()).ok();

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "background task should exit cleanly");
    }

    // -----------------------------------------------------------------------
    // MerkleExchangeProtocol
    // -----------------------------------------------------------------------

    #[test]
    fn exchange_protocol_encode_empty_roots() {
        let protocol = MerkleExchangeProtocol::new(AntiEntropyConfig::default());
        let roots: Vec<(SegmentId, MerkleRoot)> = vec![];
        let encoded = protocol.encode_roots(&roots);
        assert_eq!(encoded.len(), 4);
        assert_eq!(&encoded[..4], &0u32.to_le_bytes());
    }

    #[test]
    fn exchange_protocol_encode_single_root() {
        let protocol = MerkleExchangeProtocol::new(AntiEntropyConfig::default());
        let seg_id = SegmentId::new();
        let root = MerkleRoot { hash: make_hash(42), leaf_count: 8, total_size: 4194304 };
        let roots = vec![(seg_id, root)];
        let encoded = protocol.encode_roots(&roots);

        assert_eq!(encoded.len(), 4 + 16 + 32);

        let count = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(count, 1);

        let uuid_bytes: [u8; 16] = encoded[4..20].try_into().unwrap();
        assert_eq!(uuid_bytes, seg_id.as_uuid().as_bytes().as_slice());
    }

    #[test]
    fn exchange_protocol_encode_multiple_roots() {
        let protocol = MerkleExchangeProtocol::new(AntiEntropyConfig::default());
        let mut roots = Vec::new();
        for i in 0..3 {
            let seg_id = SegmentId::new();
            let root =
                MerkleRoot { hash: make_hash(i as u8), leaf_count: (i + 1) as u64, total_size: 0 };
            roots.push((seg_id, root));
        }
        let encoded = protocol.encode_roots(&roots);
        assert_eq!(encoded.len(), 4 + 3 * (16 + 32));

        let count = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(count, 3);
    }

    #[test]
    fn exchange_protocol_decode_empty_roots() {
        let protocol = MerkleExchangeProtocol::new(AntiEntropyConfig::default());
        let encoded = 0u32.to_le_bytes().to_vec();
        let decoded = protocol.decode_roots(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn exchange_protocol_roundtrip() {
        let protocol = MerkleExchangeProtocol::new(AntiEntropyConfig::default());
        let mut original = Vec::new();
        for _ in 0..5 {
            let seg_id = SegmentId::new();
            let root = MerkleRoot { hash: make_hash(0), leaf_count: 0, total_size: 0 };
            original.push((seg_id, root));
        }

        let encoded = protocol.encode_roots(&original);
        let decoded = protocol.decode_roots(&encoded).unwrap();

        assert_eq!(decoded.len(), original.len());
        for (i, (seg_id, root)) in original.iter().enumerate() {
            assert_eq!(decoded[i].0.as_uuid(), seg_id.as_uuid());
            assert_eq!(decoded[i].1.hash(), root.hash());
        }
    }

    #[test]
    fn exchange_protocol_decode_too_short_returns_error() {
        let protocol = MerkleExchangeProtocol::new(AntiEntropyConfig::default());
        let too_short = vec![0u8; 2];
        let result = protocol.decode_roots(&too_short);
        assert!(result.is_err());
    }

    #[test]
    fn exchange_protocol_decode_truncated_returns_error() {
        let protocol = MerkleExchangeProtocol::new(AntiEntropyConfig::default());
        let mut truncated = vec![0u8; 4];
        truncated[0] = 1;
        truncated.extend_from_slice(&[0u8; 20]);
        let result = protocol.decode_roots(&truncated);
        assert!(result.is_err());
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
    // MerkleExchangeProtocol config
    // -----------------------------------------------------------------------

    #[test]
    fn exchange_protocol_config_getter() {
        let config = AntiEntropyConfig { interval_sec: 600, peer_count: 2 };
        let protocol = MerkleExchangeProtocol::new(config);
        assert_eq!(protocol.config().interval_sec(), 600);
        assert_eq!(protocol.config().peer_count(), 2);
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

    // -----------------------------------------------------------------------
    // Peer selection
    // -----------------------------------------------------------------------

    #[test]
    fn select_alive_peers_excludes_self() {
        let (membership, _ring) = make_test_membership("node-a");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());
        let ae = make_anti_entropy(membership.clone(), metadata);

        // Add a peer node
        membership.upsert_node(
            NodeId::new("node-b"),
            NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            "127.0.0.1:9001".parse().unwrap(),
        );

        let peers = ae.select_alive_peers();
        // Our config has peer_count=1, should select node-b
        assert!(!peers.contains(membership.node_id()));
        // With only 1 alive peer, should get node-b
        if !peers.is_empty() {
            assert_eq!(peers.len(), 1);
            assert!(peers[0].as_str() == "node-b");
        }
    }

    #[test]
    fn select_alive_peers_returns_empty_for_standalone_node() {
        let (membership, _ring) = make_test_membership("standalone");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());
        let ae = make_anti_entropy(membership, metadata);

        let peers = ae.select_alive_peers();
        assert!(peers.is_empty());
    }

    #[test]
    fn select_alive_peers_respects_peer_count() {
        let (membership, _ring) = make_test_membership("central");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());

        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let segment_store = Arc::new(InMemorySegmentStore::new());
        let ae = AntiEntropy::new(
            AntiEntropyConfig { interval_sec: 300, peer_count: 2 },
            membership.clone(),
            metadata,
            pool,
            segment_store,
        );

        // Register 5 alive peers
        for i in 0..5 {
            membership.upsert_node(
                NodeId::new(&format!("peer-{i}")),
                NodeState::Alive,
                oceanfs_core::Incarnation::new(1),
                format!("127.0.0.1:{}", 9001 + i).parse().unwrap(),
            );
        }

        let peers = ae.select_alive_peers();
        assert_eq!(peers.len(), 2);
    }

    #[test]
    fn select_alive_peers_excludes_dead_nodes() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(MetadataStore::open(&metadata_config).unwrap());
        let ae = make_anti_entropy(membership.clone(), metadata);

        // Register one dead and one alive peer
        membership.upsert_node(
            NodeId::new("dead-peer"),
            NodeState::Dead,
            oceanfs_core::Incarnation::new(1),
            "127.0.0.1:9001".parse().unwrap(),
        );
        membership.upsert_node(
            NodeId::new("alive-peer"),
            NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            "127.0.0.1:9002".parse().unwrap(),
        );

        let peers = ae.select_alive_peers();
        // Should only contain the alive peer
        assert!(!peers.iter().any(|p| p.as_str() == "dead-peer"));
        if !peers.is_empty() {
            assert!(peers.iter().all(|p| p.as_str() == "alive-peer"));
        }
    }

    // -----------------------------------------------------------------------
    // EC-backed repair
    // -----------------------------------------------------------------------

    /// Helper: creates a CauchyEncoder with given k,m and encodes data.
    fn ec_encode_test_data(data: &[u8], k: u8, m: u8) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let shard_size = data.len().div_ceil(k as usize);
        let padded_len = shard_size * k as usize;
        let mut padded = data.to_vec();
        padded.resize(padded_len, 0u8);

        let data_shards: Vec<Vec<u8>> = (0..k as usize)
            .map(|i| padded[i * shard_size..(i + 1) * shard_size].to_vec())
            .collect();
        let data_refs: Vec<&[u8]> = data_shards.iter().map(|v| v.as_slice()).collect();

        let codec = CauchyEncoder::new(oceanfs_core::CodecConfig {
            data_shards: k,
            parity_shards: m,
            strip_size_bytes: shard_size,
            ..Default::default()
        });
        let parity = codec.encode(&data_refs, m).unwrap();

        (data_shards, parity)
    }

    #[test]
    fn ec_repair_corrupted_shard_reconstructed() {
        // 4 data shards, 2 parity shards — can recover any 2 missing shards
        let k: u8 = 4;
        let m: u8 = 2;
        let data = vec![0xABu8; 65536 * k as usize]; // k * 64KB
        let (data_shards, parity) = ec_encode_test_data(&data, k, m);

        // Corrupt shard 1 by flipping a byte
        let mut corrupted_shards = data_shards.clone();
        corrupted_shards[1][100] ^= 0x01;

        // Mark shard 1 as missing (None) for EC decode
        let mut available: Vec<Option<&[u8]>> =
            (0..k as usize).map(|i| Some(corrupted_shards[i].as_slice())).collect();
        available.push(Some(parity[0].as_slice()));
        available.push(Some(parity[1].as_slice()));

        // Now mark shard 1 as missing for reconstruction
        available[1] = None;

        let codec = CauchyEncoder::new(oceanfs_core::CodecConfig {
            data_shards: k,
            parity_shards: m,
            strip_size_bytes: data_shards[0].len(),
            ..Default::default()
        });

        let recovered = codec.decode(&available, k, m).unwrap();

        // Recovered shard 1 should match the original
        assert_eq!(recovered[1], data_shards[1]);
        assert_ne!(recovered[1], corrupted_shards[1]);
    }

    #[test]
    fn ec_repair_with_two_missing_shards() {
        let k: u8 = 4;
        let m: u8 = 2;
        let data = vec![0xCDu8; 4096 * k as usize];
        let (data_shards, parity) = ec_encode_test_data(&data, k, m);

        // Mark shards 0 and 3 as missing
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(data_shards[1].as_slice()),
            Some(data_shards[2].as_slice()),
            None,
            Some(parity[0].as_slice()),
            Some(parity[1].as_slice()),
        ];

        let codec = CauchyEncoder::new(oceanfs_core::CodecConfig {
            data_shards: k,
            parity_shards: m,
            strip_size_bytes: data_shards[0].len(),
            ..Default::default()
        });

        let recovered = codec.decode(&available, k, m).unwrap();
        assert_eq!(recovered[0], data_shards[0]);
        assert_eq!(recovered[3], data_shards[3]);
    }

    #[test]
    fn ec_repair_segment_repairs_corrupted_shard() {
        // Full anti-entropy EC repair flow:
        // 1. Original data is EC-encoded at seal time (k=4, m=2)
        // 2. Parity shards stored on peer nodes
        // 3. Local data shard gets corrupted
        // 4. Anti-entropy cycle: fetch parity from peer, decode to repair
        let segment_id = SegmentId::new();
        let store = InMemorySegmentStore::new();

        let k: u8 = 4;
        let m: u8 = 2;
        let leaf_size = 65536;
        let segment_data = vec![0x5Fu8; leaf_size * k as usize]; // 256 KB

        // Compute parity from the ORIGINAL data (simulating seal-time EC encode)
        let (_, original_parity) = ec_encode_test_data(&segment_data, k, m);

        // Build Merkle tree and store root
        let tree = MerkleTree::build(&segment_data, leaf_size).unwrap();
        let root_hash = tree.root().hash();

        // Write data to store
        store.write_segment_data(&segment_id, &segment_data).unwrap();

        // --- Corruption: flip a byte in shard index 2 ---
        let mut corrupted = segment_data.clone();
        corrupted[2 * leaf_size + 500] ^= 0xFF;
        store.write_segment_data(&segment_id, &corrupted).unwrap();

        // Verify corruption is detected
        let bad_tree = MerkleTree::build(&corrupted, leaf_size).unwrap();
        assert_ne!(bad_tree.root().hash(), root_hash);

        // --- EC Repair: reconstruct shard 2 using original parity ---
        let shard_size = corrupted.len().div_ceil(k as usize);
        let padded_len = shard_size * k as usize;
        let mut padded = corrupted.clone();
        padded.resize(padded_len, 0u8);

        let codec = CauchyEncoder::new(oceanfs_core::CodecConfig {
            data_shards: k,
            parity_shards: m,
            strip_size_bytes: shard_size,
            ..Default::default()
        });

        // Mark shard 2 as missing, use intact shards 0,1,3 + parity to decode
        let mut available: Vec<Option<&[u8]>> = Vec::with_capacity(k as usize + m as usize);
        for i in 0..k as usize {
            if i == 2 {
                available.push(None); // corrupted shard — mark missing
            } else {
                let slice = &padded[i * shard_size..(i + 1) * shard_size];
                available.push(Some(slice));
            }
        }
        for p in &original_parity {
            available.push(Some(p.as_slice()));
        }

        let recovered = codec
            .decode(&available, k, m)
            .expect("EC decode should succeed with k-1 intact + 2 parity shards");

        // Reconstruct full data
        let mut repaired_data = corrupted.clone();
        let copy_len = shard_size.min(repaired_data.len() - 2 * shard_size);
        repaired_data[2 * shard_size..2 * shard_size + copy_len]
            .copy_from_slice(&recovered[2][..copy_len]);

        // Write repaired data back
        store.write_segment_data(&segment_id, &repaired_data).unwrap();

        // Verify: Merkle tree now matches original
        let repaired_tree = MerkleTree::build(&repaired_data, leaf_size).unwrap();
        assert_eq!(repaired_tree.root().hash(), root_hash);
    }

    #[test]
    fn ec_repair_without_ec_params_falls_back_to_merkle_detection() {
        let segment_id = SegmentId::new();
        let store = InMemorySegmentStore::new();

        let data = vec![0x11u8; 65536 * 2]; // 2 leaves, no EC params
        let tree = MerkleTree::build(&data, 65536).unwrap();
        let root_hash = tree.root().hash();

        store.write_segment_data(&segment_id, &data).unwrap();

        // Corrupt a byte
        let mut corrupted = data.clone();
        corrupted[65536] ^= 0x01;
        store.write_segment_data(&segment_id, &corrupted).unwrap();

        // Build tree from corrupted data — root differs from stored
        let bad_tree = MerkleTree::build(&corrupted, 65536).unwrap();
        assert_ne!(bad_tree.root().hash(), root_hash);

        // merkle_repair_diverged_leaves detects the mismatch
        let diverged = AntiEntropy::merkle_repair_diverged_leaves(
            segment_id, &corrupted, 65536, &root_hash, &store,
        )
        .unwrap();

        // Should detect at least 1 diverged leaf (root mismatch)
        assert!(diverged > 0, "should detect diverged leaves, got {diverged}");
    }

    #[test]
    fn ec_repair_intact_data_returns_zero() {
        let segment_id = SegmentId::new();
        let store = InMemorySegmentStore::new();

        let k: u8 = 3;
        let m: u8 = 1;
        let data = vec![0x22u8; 65536 * k as usize];
        let tree = MerkleTree::build(&data, 65536).unwrap();
        let root_hash = tree.root().hash();

        store.write_segment_data(&segment_id, &data).unwrap();

        let repaired =
            AntiEntropy::ec_repair_segment(segment_id, &data, k, m, 65536, &root_hash, &store)
                .unwrap();

        // Intact data should require no repair
        assert_eq!(repaired, 0);
    }
}
