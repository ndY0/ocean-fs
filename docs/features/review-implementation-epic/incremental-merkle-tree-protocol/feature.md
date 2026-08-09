---
feature: "Incremental Merkle Tree Protocol"
epic: "review-implementation-epic"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: Hinted Handoff Durability
    reason: MerkleWal reuses the WalWriter pattern established by HintWal;
      both are WalWriter impls, and building MerkleWal second avoids duplicating
      the WAL framing/CRC/replay infrastructure
  - epic: gap-closure-addendum
    reason: Item 6 (trait-object conversion for durability components) must be
      complete so AntiEntropy can consume Arc<dyn MetadataStore> rather than
      concrete RocksDbMetadataStore
adr:
  - 0015-anti-entropy-merkle-protocol
  - 0009-storage-crate-split
perf:
  - "1.3 pre-size collections"
created: 2026-08-09
updated: 2026-08-09
---

# Incremental Merkle Tree Protocol

## Summary

The current anti-entropy implementation rebuilds Merkle trees from scratch for
every cycle, reconstructs peer trees locally instead of receiving them over
the wire, runs on the full keyspace unconditionally, and inconsistently uses
EC optimisations (review findings #15–#18, #27). This feature implements the
design from ADR-0015: an incremental Merkle tree maintained in
`oceanfs-durability`, updated on each segment seal event via a notifier
channel; a `MerkleWal` (third `WalWriter` implementation) for crash recovery;
two anti-entropy modes (continuous root-only exchange and 5% sampling); pre-built
tree sending over gRPC; and unified EC repair through the heal pool. The tree
is per-segment with BLAKE3 hashing, and memory is bounded by
`continuous_max_segments`.

## Scope

### In Scope
- `IncrementalMerkleTree` in `oceanfs-durability` — per-segment binary Merkle tree with BLAKE3 hashing, O(log n) leaf insertion
- `MerkleWal` in `oceanfs-durability` — implements `WalWriter` trait, persists tree mutations as append-only log
- Segment seal notifier: `oceanfs-storage` sends sealed segment IDs via `tokio::sync::mpsc` channel wired by `oceanfs-node`
- Continuous AE mode: root exchange on every N segment writes or gossip interval; descent on mismatch
- Sampling AE mode: random 5% segment subset per cycle; descent on mismatch
- `MerkleExchange` gRPC protocol extension: pre-built tree serialized in `MerkleResponse`
- EC path unification: all AE-detected divergence routes to heal pool; no local Cauchy matrix usage
- Configuration in `NodeConfig`: `anti_entropy` section with `continuous_enabled`, `continuous_max_segments`, `sampling_enabled`, `sampling_interval_sec`, `sampling_fraction`

### Out of Scope (for this feature)
- Full scrub redesign (scrub remains full-scan, spec §7.5)
- Intra-segment Merkle tree (per-blob hashes within a segment — covered by BLAKE3 per-blob checksums)
- Adaptive sampling rate (static 5% fraction, configurable)
- Merkle proof verification for individual blob reads (separate trust-but-verify feature)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New modules: `merkle/incremental_tree.rs`, `merkle/merkle_wal.rs`, `merkle/tree_node.rs`; modify `anti_entropy/engine.rs` to use incremental tree and two modes |
| `oceanfs-storage` | In `segment/sealer.rs`, after sealing a segment, send `SegmentId` through a `tokio::sync::mpsc::Sender<SegmentId>` notifier channel |
| `oceanfs-core` | New config section `AntiEntropyConfig` fields: `continuous_enabled`, `continuous_max_segments`, `sampling_enabled`, `sampling_interval_sec`, `sampling_fraction` |
| `oceanfs-node` | Wire notifier channel: create `mpsc::channel`, pass sender to `SegmentSealer`, pass receiver to `IncrementalMerkleTree` |
| `oceanfs-routing` | No changes |
| `oceanfs-server` | No changes |
| `proto/merkle.proto` | Update `MerkleRequest`/`MerkleResponse` with `include_full_tree` field and `repeated TreeNode internal_nodes` |

## Interface (Public API)

- `pub struct IncrementalMerkleTree`
  - `pub fn new(config: MerkleTreeConfig) -> Self`
  - `pub fn insert_leaf(&self, segment_id: SegmentId, leaf_hash: [u8; 32]) -> Result<()>` — insert a new leaf, recompute path to root, log mutation to MerkleWal
  - `pub fn root(&self, segment_id: SegmentId) -> Option<[u8; 32]>` — get current root for a segment
  - `pub fn serialize_tree(&self, segment_id: SegmentId) -> Result<Vec<TreeNode>>` — serialize entire tree for gRPC exchange
  - `pub fn compare_and_find_divergence(&self, segment_id: SegmentId, peer_tree: &[TreeNode]) -> Result<Vec<u32>>` — compare peer tree against local, return list of divergent leaf indices
  - `pub fn segment_count(&self) -> usize` — number of tracked segments (for memory bound enforcement)
  - `pub fn evict_oldest(&self, count: usize)` — remove oldest segments when exceeding `continuous_max_segments`

- `pub struct TreeNode`
  - `pub node_index: u32`
  - `pub hash: [u8; 32]`
  - `pub children: Vec<u32>`

- `pub struct MerkleWal`
  - Implements `oceanfs_storage_api::WalWriter`
  - `pub fn open(path: &Path) -> Result<Self>`
  - `pub fn log_mutation(&self, entry: MerkleWalEntry) -> Result<u64>`
  - `pub fn replay_mutations(&self) -> Result<Vec<MerkleWalEntry>>`

- `pub enum MerkleWalEntry`
  - `NodeInsert { segment_id: SegmentId, node_index: u32, hash: [u8; 32] }`
  - `NodeUpdate { segment_id: SegmentId, node_index: u32, old_hash: [u8; 32], new_hash: [u8; 32] }`
  - `SubtreeInvalidate { segment_id: SegmentId }`

- `pub struct AntiEntropyConfig`
  - `pub continuous_enabled: bool`
  - `pub continuous_max_segments: usize`
  - `pub sampling_enabled: bool`
  - `pub sampling_interval_sec: u64`
  - `pub sampling_fraction: f64` — in (0.0, 1.0]; default 0.05

## Data Flow

```
Segment sealed (oceanfs-storage)
  ↓
SegmentSealer::seal_and_persist()
  ↓ (after segment metadata written to RocksDB)
tokio::sync::mpsc::Sender<SegmentId>::send(segment_id)
  ↓
IncrementalMerkleTree::on_segment_sealed(segment_id)
  ├→ compute leaf hash = BLAKE3(segment_data)
  ├→ insert_leaf(segment_id, leaf_hash)
  │   ├→ recompute path to root (O(log n) node updates)
  │   └→ MerkleWal::log_mutation(NodeInsert { ... })  [persist]
  └→ if segment_count() > continuous_max_segments:
      └→ evict_oldest(1)

--- Continuous AE (every N writes or gossip_interval_ms) ---

Node A → Node B: MerkleRequest { segment_ids, include_full_tree: false }
Node B → Node A: MerkleResponse { merkle_root }
  ↓ (roots match → done)
  ↓ (roots differ)
Node A → Node B: MerkleRequest { segment_ids, include_full_tree: true }
Node B → Node A: MerkleResponse { merkle_root, internal_nodes }
  ↓
IncrementalMerkleTree::compare_and_find_divergence(segment_id, peer_nodes)
  ↓ returns Vec<u32> of divergent leaf indices
  ↓
Route each divergent segment to heal pool
  ↓
HealPool::enqueue(segment_id) → AccelDispatcher → EC decode → repair shards

--- Sampling AE (every sampling_interval_sec, 5% of segments) ---

Select random 5% subset of tracked segments
  → for each: exchange MerkleRequest with peer (include_full_tree: false)
  → on root mismatch: same descent+heal as continuous mode

--- Crash Recovery ---

Node restart:
  ↓
MerkleWal::open(path)
  ↓
MerkleWal::replay_mutations() → Vec<MerkleWalEntry>
  ↓
IncrementalMerkleTree::rebuild_from_mutations(entries)
  ↓ (if replay fails or WAL corrupted)
WARN "MerkleWal replay failed; rebuilding from segment scan"
  ↓
Full scan of segments RocksDB CF → rebuild all trees → write fresh MerkleWal
```

## Definition of Done

- [ ] **D2.1** In `crates/oceanfs-durability/src/merkle/tree_node.rs`, define:
  ```rust
  /// A node in the binary Merkle tree.
  #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
  pub struct TreeNode {
      pub node_index: u32,
      pub hash: [u8; 32],
      pub children: Vec<u32>,  // [left_child_index, right_child_index] for internal nodes; empty for leaves
  }
  ```

- [ ] **D2.2** In `crates/oceanfs-durability/src/merkle/incremental_tree.rs`, implement `struct IncrementalMerkleTree`:
  ```rust
  pub struct IncrementalMerkleTree {
      /// Per-segment tree: SegmentId → (Vec<TreeNode>, leaf_count)
      trees: DashMap<SegmentId, (Vec<TreeNode>, usize)>,
      /// Segments ordered by insertion time for eviction.
      insertion_order: Mutex<VecDeque<SegmentId>>,
      merkle_wal: Arc<MerkleWal>,
      config: MerkleTreeConfig,
      hash_hasher: Blake3Hasher,
  }
  ```
  Methods:
  - `pub fn new(merkle_wal: Arc<MerkleWal>, config: MerkleTreeConfig) -> Self`
  - `pub fn insert_leaf(&self, segment_id: SegmentId, leaf_hash: [u8; 32]) -> Result<()>` — place leaf at `trees[segment_id].len()`, recompute internal nodes from `node_index` up to root (parent = (index-1)/2), log each changed node via `merkle_wal.log_mutation(NodeUpdate{...})`. If this is the first leaf for `segment_id`, also log `NodeInsert`.
  - `pub fn root(&self, segment_id: SegmentId) -> Option<[u8; 32]>` — returns `trees[segment_id].0[0].hash` if tree exists.
  - `pub fn serialize_tree(&self, segment_id: SegmentId) -> Result<Vec<TreeNode>>` — returns clone of `trees[segment_id].0`.
  - `pub fn compare_and_find_divergence(&self, segment_id: SegmentId, peer_tree: &[TreeNode]) -> Result<Vec<u32>>` — walk both trees from root. At each node where hashes differ, recurse into children. Return leaf indices (nodes with `children.is_empty()`) where hashes differ.
  - `pub fn segment_count(&self) -> usize` — returns `trees.len()`.
  - `pub fn evict_oldest(&self, count: usize)` — pop `count` SegmentIds from front of `insertion_order`, remove from `trees`, log `SubtreeInvalidate` for each.

- [ ] **D2.3** In `crates/oceanfs-durability/src/merkle/merkle_wal.rs`, implement `struct MerkleWal`:
  ```rust
  pub struct MerkleWal {
      file: Arc<Mutex<std::fs::File>>,
      path: PathBuf,
  }
  ```
  - `pub fn open(path: &Path) -> Result<Self>` — opens file with `create(true).append(true).read(true)`.
  - `pub fn log_mutation(&self, entry: MerkleWalEntry) -> Result<u64>` — protobuf-encode `MerkleWalEntry`, write as length-delimited + CRC32 frame (same framing as HintWal), fsync, return byte offset.
  - `pub fn replay_mutations(&self) -> Result<Vec<MerkleWalEntry>>` — read entire file, decode each frame, verify CRC32, return entries.
  - Implement `oceanfs_storage_api::WalWriter` for `MerkleWal`:
    ```rust
    impl WalWriter for MerkleWal {
        fn write(&self, data: &[u8]) -> Result<u64> { /* length-delimited + CRC32 frame */ }
        fn sync(&self) -> Result<()> { self.file.lock().unwrap().sync_all().map_err(...) }
        fn truncate(&self, position: u64) -> Result<()> { ... }
        fn replay(&self) -> Result<Vec<(u64, Vec<u8>)>> { ... }
    }
    ```

- [ ] **D2.4** In `crates/oceanfs-durability/src/merkle/mod.rs`, implement function:
  ```rust
  /// Rebuild incremental trees from a full segment scan (fallback when MerkleWal is corrupted).
  pub fn rebuild_from_segment_scan(
      metadata: &dyn MetadataStore,
      merkle_wal: &MerkleWal,
      config: &MerkleTreeConfig,
  ) -> Result<IncrementalMerkleTree> {
      let tree = IncrementalMerkleTree::new(Arc::new(merkle_wal.clone()), config.clone());
      let segments = metadata.list_all_segments()?;
      for segment in segments {
          let leaf_hash = blake3::hash(&segment.data);  // or fetch segment data
          tree.insert_leaf(segment.id, leaf_hash.into())?;
      }
      Ok(tree)
  }
  ```

- [ ] **D2.5** In `crates/oceanfs-storage/src/segment/sealer.rs`, add a field to `SegmentSealer`:
  ```rust
  pub struct SegmentSealer {
      // ... existing fields ...
      /// Notifier channel for sealed segments. Durability crate observes this.
      on_sealed: Option<tokio::sync::mpsc::UnboundedSender<SegmentId>>,
  }
  ```
  After the seal completes and segment metadata is persisted (after line ~350, where `seal_result` is returned), call:
  ```rust
  if let Some(tx) = &self.on_sealed {
      let _ = tx.send(sealed_segment_id);
  }
  ```
  Add constructor parameter `on_sealed: Option<tokio::sync::mpsc::UnboundedSender<SegmentId>>`.

- [ ] **D2.6** In `crates/oceanfs-durability/src/anti_entropy/engine.rs`, refactor `AntiEntropy`:
  - Add fields:
    ```rust
    pub struct AntiEntropy {
        // ... existing fields ...
        merkle_tree: Arc<IncrementalMerkleTree>,
        config: AntiEntropyConfig,
        write_counter: AtomicU64,  // tracks segment writes for continuous AE trigger
    }
    ```
  - Implement `ContinuousAeRunner` — runs in a loop, triggered every `gossip_interval_ms` OR after `write_counter` increments by N (configurable, default N=1). Exchanges roots with peers for recently-written segments. On mismatch: requests full tree, compares, routes divergent segments to heal pool.
  - Implement `SamplingAeRunner` — runs every `sampling_interval_sec`. Selects random `sampling_fraction` of tracked segments, exchanges roots, descends on mismatch.

- [ ] **D2.7** In `crates/oceanfs-durability/src/anti_entropy/engine.rs`, replace any local Cauchy matrix usage with heal pool enqueue:
  ```rust
  // OLD (review finding #18):
  // let decoded = cauchy_decode_locally(missing_shard, available_shards);
  //
  // NEW:
  self.heal_pool.enqueue(HealRequest {
      segment_id: divergent_segment,
      missing_shard_indices: divergent_indices,
      priority: HealPriority::AntiEntropy,
  });
  ```

- [ ] **D2.8** In `crates/oceanfs-core/src/config/node.rs`, add to `NodeConfig`:
  ```rust
  /// Anti-entropy configuration.
  #[serde(default)]
  pub anti_entropy: AntiEntropyConfig,
  ```
  Define `AntiEntropyConfig`:
  ```rust
  #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
  pub struct AntiEntropyConfig {
      #[serde(default = "default_true")]
      pub continuous_enabled: bool,
      #[serde(default = "default_continuous_max_segments")]
      pub continuous_max_segments: usize,
      #[serde(default = "default_true")]
      pub sampling_enabled: bool,
      #[serde(default = "default_ae_sampling_interval_sec")]
      pub sampling_interval_sec: u64,
      #[serde(default = "default_ae_sampling_fraction")]
      pub sampling_fraction: f64,
  }
  ```
  With defaults: `continuous_max_segments = 10000`, `sampling_interval_sec = 300`, `sampling_fraction = 0.05`.

- [ ] **D2.9** Update `proto/merkle.proto`:
  ```protobuf
  message MerkleRequest {
    repeated bytes segment_ids = 1;
    bool include_full_tree = 2;
  }

  message MerkleResponse {
    bytes merkle_root = 1;
    repeated TreeNode internal_nodes = 2;
    bool full_tree_included = 3;
  }

  message TreeNode {
    uint32 node_index = 1;
    bytes hash = 2;           // [u8; 32]
    repeated uint32 children = 3;
  }
  ```
  Regenerate Rust stubs via `prost-build`.

- [ ] **D2.10** In `crates/oceanfs-node/src/node.rs`, wire the notifier channel:
  ```rust
  let (segment_sealed_tx, segment_sealed_rx) = tokio::sync::mpsc::unbounded_channel();
  // Pass segment_sealed_tx to SegmentSealer constructor
  // Pass segment_sealed_rx to IncrementalMerkleTree constructor or a background task
  ```
  Spawn a background task:
  ```rust
  let merkle_tree = Arc::clone(&incremental_merkle_tree);
  tokio::spawn(async move {
      while let Some(segment_id) = segment_sealed_rx.recv().await {
          // Compute leaf hash from segment data (or fetch from storage)
          if let Err(e) = merkle_tree.insert_leaf(segment_id, leaf_hash) {
              tracing::error!(%segment_id, error = %e, "Failed to insert leaf into Merkle tree");
          }
      }
  });
  ```

- [ ] **D2.11** In `crates/oceanfs-node/src/node.rs`, construct `MerkleWal` and `IncrementalMerkleTree` at startup:
  ```rust
  let merkle_wal_path = config.data_dir.join("merkle.wal");
  let merkle_wal = Arc::new(MerkleWal::open(&merkle_wal_path)?);
  let merkle_tree = match IncrementalMerkleTree::rebuild_from_mutations(&merkle_wal) {
      Ok(tree) => Arc::new(tree),
      Err(e) => {
          tracing::warn!(error = %e, "MerkleWal replay failed; rebuilding from segment scan");
          Arc::new(IncrementalMerkleTree::rebuild_from_segment_scan(
              metadata_store.as_ref(),
              &merkle_wal,
              &MerkleTreeConfig::default(),
          )?)
      }
  };
  ```

## Tests Required

- [ ] **T2.1** `test_incremental_tree_insert_and_root` — In `crates/oceanfs-durability/src/merkle/incremental_tree.rs` test module:
  - Create tree with temp MerkleWal.
  - Insert 3 leaves for segment `seg-A`: `hash([0x00; 32])`, `hash([0x01; 32])`, `hash([0x02; 32])`.
  - Assert `root(seg-A)` is `Some(...)` and not all zeros.
  - Insert a 4th leaf. Assert root changed (because tree structure changed).
  - Assert `segment_count() == 1`.

- [ ] **T2.2** `test_incremental_tree_compare_finds_divergence` — In same module:
  - Build local tree with leaves: `[A, B, C, D]`.
  - Build peer tree with leaves: `[A, B, X, D]` (leaf 2 differs).
  - Call `compare_and_find_divergence(segment_id, peer_tree)`.
  - Assert returns `vec![2]` (leaf index 2 is divergent).
  - Build peer tree identical. Assert returns empty `vec![]`.

- [ ] **T2.3** `test_merkle_wal_mutation_log_replay` — In `crates/oceanfs-durability/src/merkle/merkle_wal.rs` test module:
  - Open temp MerkleWal.
  - Log 5 `NodeInsert` mutations with distinct `segment_id`, `node_index`, `hash`.
  - Log 3 `NodeUpdate` mutations.
  - Close, reopen, call `replay_mutations()`.
  - Assert 8 entries returned.
  - Assert each `NodeInsert` has correct fields.
  - Assert each `NodeUpdate` has correct `old_hash` and `new_hash`.

- [ ] **T2.4** `test_merkle_wal_corruption_falls_back_to_segment_scan` — In `crates/oceanfs-durability/tests/merkle_recovery.rs`:
  - Log 5 mutations, corrupt the CRC32 of entry 3.
  - Call `replay_mutations()`. Assert returns error.
  - Call `rebuild_from_segment_scan()`. Assert tree is reconstructed with correct root.
  - Verify `WARN` log message contains "MerkleWal replay failed".

- [ ] **T2.5** `test_continuous_ae_exchanges_roots_on_segment_write` — In `crates/oceanfs-durability/tests/anti_entropy_integration.rs`:
  - Create 2-node cluster with continuous AE enabled.
  - Write 5 segments on node_a.
  - Wait for AE cycle (max 2× gossip_interval_ms).
  - Assert each segment's Merkle root was exchanged (check metrics or internal counters).

- [ ] **T2.6** `test_sampling_ae_exchanges_subset` — In same test:
  - Enable sampling mode with `sampling_fraction = 0.2`.
  - Create 100 segments.
  - Run one sampling AE cycle.
  - Assert between 10 and 30 segments had their roots exchanged (20±statistical noise). Use a counter metric.

- [ ] **T2.7** `test_ae_divergence_triggers_heal_pool_enqueue` — In same test:
  - Create identical data on node_a and node_b, then corrupt one segment shard on node_b.
  - Run continuous AE.
  - Assert the divergent segment is enqueued in heal pool (check heal pool queue depth increments).

- [ ] **T2.8** `test_ae_no_local_cauchy_matrix_usage` — In `crates/oceanfs-durability/src/anti_entropy/engine.rs`, search for any `cauchy_` or `Cauchy` function calls. Assert zero matches in the AE code path (the only Cauchy usage should be inside the heal pool, not in AE).

- [ ] **T2.9** `test_merkle_tree_evicts_oldest_when_exceeding_max` — In incremental_tree.rs test module:
  - Set `continuous_max_segments = 3`.
  - Insert 5 segments.
  - Assert `segment_count() == 3`.
  - Assert the first 2 segment IDs are no longer in the tree.

- [ ] **T2.10** `test_grpc_merkle_exchange_full_tree` — In a gRPC integration test:
  - Node A has a Merkle tree for `seg-1`.
  - Node B sends `MerkleRequest { segment_ids: [seg-1], include_full_tree: true }`.
  - Node A responds with `MerkleResponse { full_tree_included: true, internal_nodes: [...] }`.
  - Assert `internal_nodes` is non-empty and contains a valid binary tree structure (node 0 is root, children indices point within bounds).

## ADR References

- [ADR-0015](../../adr/0015-anti-entropy-merkle-protocol.md) — Full design: incremental trees, MerkleWal, continuous/sampling modes, gRPC protocol, EC path unification
- [ADR-0009](../../adr/0009-storage-crate-split.md) — `WalWriter` trait in `oceanfs-storage-api`; MerkleWal is the third implementation
- [ADR-0005](../../adr/0005-trait-in-consuming-crate.md) — Notifier channel is a cross-crate dependency resolved by `oceanfs-node` at startup
