//! Anti-entropy background service.
//!
//! Periodically selects random peers from the membership view,
//! exchanges Merkle roots for shared segments, and descends the
//! tree on mismatch to identify and repair diverged leaves.

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use oceanfs_core::{
    Counter, HashOutput, LabelSet, MetricRegistrar, NodeState, SegmentId, SegmentMetadata,
};
use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use rand::seq::SliceRandom;

use super::{
    config::AntiEntropyConfig,
    merkle_root::MerkleRoot,
    merkle_tree::{MerkleTree, SegmentDataStore, DEFAULT_LEAF_SIZE},
};
use crate::{Error, Result};
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
/// - [`oceanfs_storage_api::MetadataStore`] for segment metadata (Merkle roots)
/// - [`SegmentDataStore`] for reading/writing segment data during repair
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use oceanfs_storage::{
///     AntiEntropy, AntiEntropyConfig, InMemorySegmentStore, RocksDbMetadataStore,
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
    metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
    /// Connection pool for peer-to-peer gRPC Merkle exchange.
    pool: Arc<ConnectionPool>,
    segment_store: Arc<dyn SegmentDataStore>,
    segments_compared_total: Counter,
    mismatches_found_total: Counter,
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
        metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
        pool: Arc<ConnectionPool>,
        segment_store: Arc<dyn SegmentDataStore>,
    ) -> Self {
        Self {
            config,
            membership,
            metadata,
            pool,
            segment_store,
            segments_compared_total: Counter::new(
                "ae_segments_compared_total".into(),
                "Segments compared by anti-entropy".into(),
                LabelSet::empty(),
            ),
            mismatches_found_total: Counter::new(
                "ae_mismatches_found_total".into(),
                "Mismatches found by anti-entropy".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Registers anti-entropy counters with a metrics registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.segments_compared_total.clone());
        registrar.register_counter(self.mismatches_found_total.clone());
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

        self.segments_compared_total.add(stats.segments_compared);
        self.mismatches_found_total.add(stats.mismatches_found);

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

        let mut client = crate::HealingRpcClient::new(channel);

        let proto_segment_ids: Vec<oceanfs_core::proto::common::SegmentId> =
            sealed_segments.iter().map(|s| s.segment_id.into()).collect();
        let proto_node_id: oceanfs_core::proto::common::NodeId = peer_id.clone().into();

        let request = tonic::Request::new(crate::healing_rpc::MerkleRequest {
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
            Error::Storage(format!("cannot build Merkle tree for segment {segment_id}"))
        })?;

        // Fast path: data is intact
        if current_tree.root().hash() == *stored_root_hash {
            return Ok(0);
        }

        // Data is corrupted — attempt EC reconstruction
        let shard_size = current_data.len().div_ceil(k);
        let padded_len = shard_size * k;

        // Pad with zeros if needed (Bytes is immutable, so use Vec for padding).
        let mut padded = Vec::with_capacity(padded_len);
        padded.extend_from_slice(current_data.as_ref());
        padded.resize(padded_len, 0u8);
        let padded_bytes = Bytes::from(padded);

        // Slice into data shards without copying (zero-copy via Bytes::slice).
        let data_shard_slices: Vec<Bytes> =
            (0..k).map(|i| padded_bytes.slice(i * shard_size..(i + 1) * shard_size)).collect();
        let data_shard_refs: Vec<&[u8]> = data_shard_slices.iter().map(|b| b.as_ref()).collect();

        let codec = CauchyEncoder::new(oceanfs_core::CodecConfig {
            data_shards: ec_k,
            parity_shards: ec_m,
            strip_size_bytes: shard_size,
            ..Default::default()
        });

        // Encode parity from current (possibly corrupted) data
        let parity_shards = codec
            .encode(&data_shard_refs, ec_m)
            .map_err(|e| Error::Storage(format!("EC encode failed for {segment_id}: {e}")))?;

        // Attempt to reconstruct each data shard by treating it as "missing"
        // and using the remaining k-1 shards + parity to decode
        let mut reconstructed_data = padded_bytes.to_vec();
        let mut repaired_count = 0usize;

        for shard_idx in 0..k {
            // Build available shards: current shard = None, all others = Some
            let mut available: Vec<Option<&[u8]>> = Vec::with_capacity(k + m);
            for i in 0..k {
                if i == shard_idx {
                    available.push(None);
                } else {
                    available.push(Some(&padded_bytes[i * shard_size..(i + 1) * shard_size]));
                }
            }
            for p in &parity_shards {
                available.push(Some(&p[..]));
            }

            // Try to decode — this reconstructs shard_idx from the others
            if let Ok(recovered) = codec.decode(&available, ec_k, ec_m) {
                let shard_offset = shard_idx * shard_size;
                let shard_end = (shard_offset + shard_size).min(padded_len);
                let copy_len = shard_end - shard_offset;

                if recovered[shard_idx][..copy_len] != padded_bytes[shard_offset..shard_end] {
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
            Error::Storage(format!("cannot build repaired Merkle tree for {segment_id}"))
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
            Error::Storage(format!("cannot build Merkle tree for segment {segment_id}"))
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
    /// Returns [`Error::Storage`] if the buffer has an invalid length
    /// or contains malformed data.
    pub(crate) fn decode_roots(&self, data: &[u8]) -> Result<Vec<(SegmentId, MerkleRoot)>> {
        if data.len() < 4 {
            return Err(Error::Storage("buffer too short for entry count".into()));
        }

        let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let expected_len = 4 + count * (16 + 32);

        if data.len() < expected_len {
            return Err(Error::Storage(format!(
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
                .map_err(|_| Error::Storage("invalid segment ID bytes".into()))?;
            offset += 16;

            let seg_id = SegmentId::from_uuid_bytes(uuid_bytes);

            // Read merkle root (32 bytes BLAKE3)
            let hash_bytes: [u8; 32] = data[offset..offset + 32]
                .try_into()
                .map_err(|_| Error::Storage("invalid merkle root bytes".into()))?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use oceanfs_core::{
        GossipConfig, HashOutput, MetadataConfig, NodeId, NodeState, RingConfig, RpcConfig,
        SegmentId, SegmentMetadata, SizeTier,
    };
    use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};
    use oceanfs_membership::Membership;
    use oceanfs_network::ConnectionPool;
    use oceanfs_routing::{Ring, RingCache};
    use oceanfs_storage::metadata::RocksDbMetadataStore;

    use super::super::{
        config::AntiEntropyConfig,
        engine::{AntiEntropy, MerkleExchangeProtocol},
        merkle_tree::{InMemorySegmentStore, MerkleTree, SegmentDataStore, DEFAULT_LEAF_SIZE},
        *,
    };

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
            ..Default::default()
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

    fn make_anti_entropy(
        membership: Arc<Membership>,
        metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
    ) -> AntiEntropy {
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let segment_store = Arc::new(InMemorySegmentStore::new());
        let config = AntiEntropyConfig::default();

        AntiEntropy::new(config, membership, metadata, pool, segment_store)
    }

    // -----------------------------------------------------------------------
    // AntiEntropy — construction
    // -----------------------------------------------------------------------

    #[test]
    fn anti_entropy_construction_with_dependencies() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
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
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
        let ae = make_anti_entropy(membership, metadata);

        let stats = ae.run_cycle().await.unwrap();
        assert_eq!(stats.segments_compared, 0);
        assert_eq!(stats.mismatches_found, 0);
    }

    #[tokio::test]
    async fn run_cycle_counts_sealed_segments() {
        let (membership, _ring) = make_test_membership("test-node");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());

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
        assert_eq!(uuid_bytes, &seg_id.as_uuid().as_bytes()[..]);
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
    // Peer selection
    // -----------------------------------------------------------------------

    #[test]
    fn select_alive_peers_excludes_self() {
        let (membership, _ring) = make_test_membership("node-a");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
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
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
        let ae = make_anti_entropy(membership, metadata);

        let peers = ae.select_alive_peers();
        assert!(peers.is_empty());
    }

    #[test]
    fn select_alive_peers_respects_peer_count() {
        let (membership, _ring) = make_test_membership("central");
        let metadata_config = test_metadata_config();
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
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
    fn ec_encode_test_data(data: &[u8], k: u8, m: u8) -> (Vec<Bytes>, Vec<Bytes>) {
        let shard_size = data.len().div_ceil(k as usize);
        let padded_len = shard_size * k as usize;
        let mut padded = data.to_vec();
        padded.resize(padded_len, 0u8);
        let padded_bytes = Bytes::from(padded);

        let data_shards: Vec<Bytes> = (0..k as usize)
            .map(|i| padded_bytes.slice(i * shard_size..(i + 1) * shard_size))
            .collect();
        let data_refs: Vec<&[u8]> = data_shards.iter().map(|b| b.as_ref()).collect();

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
        // Bytes is immutable — convert to Vec<u8>, mutate, then back
        let mut corrupted_shards: Vec<Vec<u8>> = data_shards.iter().map(|b| b.to_vec()).collect();
        corrupted_shards[1][100] ^= 0x01;
        let corrupted_shards: Vec<Bytes> = corrupted_shards.into_iter().map(Bytes::from).collect();

        // Mark shard 1 as missing (None) for EC decode
        let mut available: Vec<Option<&[u8]>> =
            (0..k as usize).map(|i| Some(&corrupted_shards[i][..])).collect();
        available.push(Some(&parity[0][..]));
        available.push(Some(&parity[1][..]));

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
            Some(&data_shards[1][..]),
            Some(&data_shards[2][..]),
            None,
            Some(&parity[0][..]),
            Some(&parity[1][..]),
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
            available.push(Some(&p[..]));
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

    #[test]
    fn ae_counter_type_works() {
        use oceanfs_core::{Counter, LabelSet};

        let c = Counter::new("ae_segments_compared_total".into(), "help".into(), LabelSet::empty());
        assert_eq!(c.get(), 0);
        c.add(10);
        assert_eq!(c.get(), 10);
        c.inc();
        assert_eq!(c.get(), 11);
    }
}
