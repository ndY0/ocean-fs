---
feature: "Anti-Entropy & Merkle Tree Exchange"
epic: "phase-7-durability"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: swim-gossip-membership
    reason: Anti-entropy uses gossip channels for Merkle root exchange
  - feature: connection-pool-grpc
    reason: Merkle tree descent uses gRPC for shard comparison
  - feature: ec-codec-trait-cauchy-rs
    reason: Diverged shards are reconstructed via EC decode
adr: []
perf:
  - "5.1: BLAKE3 with runtime SIMD detection"
  - "5.2: Streaming hash — never buffer the full blob"
  - "2.1: Rayon parallel iterators for large Merkle tree comparisons"
created: 2026-07-30
updated: 2026-07-30
---

# Anti-Entropy & Merkle Tree Exchange

## Summary

Implement the anti-entropy protocol in `oceanfs-storage` using Merkle tree
exchange between neighbor nodes. Every `anti_entropy_interval_sec` (default
300s), pairs of nodes compare Merkle roots for shared segments. On mismatch,
they descend the tree to identify diverged 64 KB leaf hashes and repair only the
affected data. This provides continuous background data integrity verification
without full segment re-reads.

## Scope

### In Scope
- `MerkleTree`: build Merkle tree over segment data at 64 KB leaf granularity
- Merkle root stored in `SegmentMetadata.merkle_root` (populated at seal time)
- `AntiEntropy`: periodic task selecting random peer, exchanging Merkle roots
- Merkle root exchange protocol: `MerkleExchange` gRPC (request → response with root set)
- Tree descent: on root mismatch, binary search down tree to find diverged leaves
- Leaf repair: fetch correct shard from peer, verify, replace local corrupt shard
- Integration: Merkle tree built during segment seal (Phase 1); anti-entropy runs in background
- Configurable: `anti_entropy_interval_sec`, anti-entropy peer selection strategy
- Unit tests for Merkle tree construction, root comparison, leaf divergence detection

### Out of Scope
- Full distributed scrubbing (separate feature — partitions work across all nodes)
- Cross-segment Merkle aggregation (per-segment trees only)
- Merkle tree persistence on disk (compute on-demand from shard hashes)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `MerkleRoot`, `MerkleProof`, `AntiEntropyConfig` |
| `oceanfs-storage` | New modules: `merkle/tree.rs`, `merkle/exchange.rs`, `anti_entropy.rs` |

## Interface (Public API)

- `pub struct MerkleTree` — `pub fn build(data: &[u8], leaf_size: usize) -> Self`, `pub fn root(&self) -> MerkleRoot`, `pub fn diff(&self, other: &MerkleTree) -> Vec<LeafRange>`, `pub fn leaf_hash(&self, index: usize) -> &HashOutput`
- `pub struct MerkleRoot` — `hash: HashOutput`, `leaf_count: u64`, `total_size: u64`
- `pub struct AntiEntropy` — `pub fn new(config: AntiEntropyConfig, membership: Arc<Membership>, metadata: Arc<MetadataStore>, pool: Arc<ConnectionPool>) -> Self`, `pub async fn run_cycle(&self) -> Result<AntiEntropyStats>`, `pub async fn start_background(self: Arc<Self>) -> JoinHandle<()>`
- `pub struct AntiEntropyConfig` — `interval_sec: u64` (default 300), `peer_count: usize` (default 1)
- `pub(crate) struct MerkleExchangeProtocol` — internal: RPC exchange logic

## Data Flow

```
Merkle tree construction (at segment seal):
  Segment data (4 MB):
    → split into 64 KB leaves: [leaf_0, leaf_1, ..., leaf_63]
      → BLAKE3::hash(each leaf) → leaf_hashes
        → build binary tree: parent = BLAKE3::hash(left_child || right_child)
          → MerkleRoot = tree.root()
            → store in SegmentMetadata.merkle_root

Anti-entropy cycle (every 300s):
  1. Select random peer from membership
  2. Exchange Merkle roots for all shared segments:
       local_roots = {seg_id: merkle_root} for segments both nodes claim to hold
       → send MerkleRequest to peer
         ← receive MerkleResponse with peer's roots
  3. Compare roots:
       for each segment where local_root != peer_root:
         ├─ Binary descent: exchange child hashes at each level
         │    └─ Identify diverged leaf indices
         ├─ For each diverged leaf:
         │    ├─ Peer has correct data → fetch shard containing that leaf
         │    ├─ Verify shard BLAKE3 matches peer's leaf hash
         │    └─ Replace local corrupt shard with peer's correct shard
         └─ Record repair in metrics
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: Merkle tree root deterministic for same data, single-bit corruption → different root, tree diff identifies exact leaf index, exchange protocol (mock peer), descent finds divergence at correct depth, repair replaces corrupt shard, empty segment → valid tree (single leaf)
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-storage`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `MerkleTree` and `AntiEntropy` documented
- [ ] **ADR:** N/A (spec §7.4 covers anti-entropy)
- [ ] **Perf:** Rule 5.1 (BLAKE3 SIMD for hashing), 5.2 (streaming hash for segment data), 2.1 (rayon for tree comparison on large segment sets)
- [ ] **Integration:** `tests/anti_entropy.rs`: 2 nodes, write same segment, corrupt one shard on node A, run anti-entropy cycle, verify node A detects corruption and repairs from node B
- [ ] **Manual:** Example in `MerkleTree` docs compiles and runs
