---
feature: "Durability Background Tasks & Verification"
epic: "final-integration"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: final-integration
    feature: final-integration-composition-root
    reason: Needs Node struct with background task spawning infrastructure
  - epic: final-integration
    feature: final-integration-grpc-services
    reason: Needs gRPC MerkleExchange and HintedHandoff for cross-node durability ops
  - epic: phase-7-durability
    reason: GC, anti-entropy, scrub, and orphan reaper components exist but are stubbed or dead-code
adr:
  - 0001-segment-packing
perf:
  - "2.6: Bounded channels for inter-task communication"
  - "2.7: Tokio semaphore for concurrency limits"
  - "8.5: Bounded semaphore for task concurrency"
  - "5.1: BLAKE3 with runtime SIMD detection"
created: 2026-08-01
updated: 2026-08-01
---

# Durability Background Tasks & Verification

## Summary

Wire the durability subsystem (garbage collection, anti-entropy Merkle exchange,
distributed scrubbing, and orphan reaping) into the node's background task loop
and complete their actual implementations. Currently these components are either
`#[allow(dead_code)]`, return no-op results (empty stats, `healthy: true`), or
perform zero actual work. This feature makes them real: GC compacts under-live
segments, anti-entropy exchanges Merkle trees over gRPC, scrub actually verifies
segment data against BLAKE3 and Merkle roots, and the orphan reaper reclaims
unreferenced data. Additionally, wire the admin endpoints to return real
operational data from `Membership`, `RingCache`, GC stats, cache stats, and
acceleration status.

## Scope

### In Scope

1. **Garbage Collector — full wiring and compaction:**
   - Remove `#[allow(dead_code)]` on `GarbageCollector`
   - Spawn GC background task in `Node::start()` with config from
     `NodeConfig.gc`:
     - Interval: `gc_interval_sec` (default 3600)
     - Tombstone TTL: `gc_tombstone_ttl_sec` (default 259200 = 3 days)
     - Compact threshold: `gc_compact_threshold` (default 0.5)
   - GC cycle implementation:
     a. Scan `deletions` column family for tombstones older than TTL
     b. For each expired tombstone, mark the associated segment chunks as free
        in the segment's `blob_index`
     c. Scan `segments` column family for segments with liveness ratio <
        `gc_compact_threshold`
     d. For each candidate segment:
        - Read all live blobs from the segment (via the blob index)
        - Re-pack them into a new segment using the tiered sizing rules from
          spec §3.2 (inline → inline, small → small segment, standard →
          standard segment)
        - Write the new segment, EC-encode, and distribute shards
        - Update object metadata in RocksDB to point to the new segment's
          chunk refs
        - Mark the old segment for deletion
     e. For compacted old segments: delete shard files from disk and remove
        segment metadata from RocksDB
     f. Emit GC metrics: `gc_segments_scanned`, `gc_segments_compacted`,
        `gc_bytes_reclaimed`, `gc_duration_sec`
   - Concurrent GC semantics: the write path must not be blocked by GC. GC
     operates on sealed segments only (never on active segments). Acquring
     segment metadata is fast (RocksDB read); the expensive part (EC
     re-encoding) runs on a bounded semaphore.

2. **Anti-Entropy — actual Merkle tree exchange:**
   - Replace no-op cycle in `anti_entropy.rs:513` that returns empty stats
   - Anti-entropy cycle (every `anti_entropy_interval_sec`, default 300s):
     a. Determine partner nodes: for each vnode the local node owns, find the
        peer that owns the adjacent vnode on the ring (the "neighbor" for
        anti-entropy)
     b. For each partner, partition the shared segment ID space (segments
        whose vnode falls in the overlapping range)
     c. Compute local Merkle roots for each segment in the partition (depth
        configurable, default 3 = 8 leaves per segment)
     d. Call `HealingRpcClient::MerkleExchange` gRPC to send local roots and
        receive the partner's roots
     e. Compare: for each segment where roots differ:
        - Request deeper leaf hashes from the partner (or send own leaf hashes)
        - Descend the tree to identify exactly which shards diverge
        - Enqueue diverged shards for repair (the healing path in spec §6.5
          reconstructs from k surviving shards)
     f. Emit anti-entropy metrics: `ae_cycles_total`, `ae_segments_compared`,
        `ae_divergences_found`, `ae_repairs_enqueued`
   - Merkle tree computation is incremental: after first full scan, only
     recompute roots for segments modified since the last cycle (tracked by
     a `last_ae_hlc` watermark).

3. **Scrub Coordinator — real verification:**
   - Replace "always healthy" placeholder in `scrub.rs:148`
   - Distributed scrub cycle (every `scrub_interval_sec`, default 604800 = 7
     days):
     a. Elect a scrub coordinator: node with the lowest `NodeId` in the
        membership list (deterministic, no election protocol needed)
     b. Coordinator partitions the segment ID space across all healthy nodes
        (including itself) — equal-sized ranges based on NodeId hash
     c. Coordinator sends each node its partition via gRPC (or the node
        determines its own partition from the consistent hash ring)
     d. Each node scrubs its assigned partition:
        - For each segment: read all local shard files from disk
        - Verify each shard against its stored BLAKE3 hash
        - Verify the segment's Merkle root: recompute from shard data
        - Verify EC consistency: any k shards can reconstruct the full segment
        - On mismatch: log ERROR, enqueue segment for healing
        - Track: `scrub_segments_checked`, `scrub_bytes_verified`,
          `scrub_errors_found`
     e. Nodes report scrub results to the coordinator
     f. Coordinator aggregates results and emits a cluster-wide scrub report
     g. Discrepant segments are placed on the heal queue (healing is a
        separate background task, spec §6.5)
   - Scrub I/O is throttled via `heal_throttle_bytes_sec` to avoid saturating
     disk I/O during production hours
   - Scrub uses a bounded semaphore to limit concurrent segment reads

4. **Orphan Reaper — full wiring:**
   - Remove `#[allow(dead_code)]` on `OrphanReaper`
   - Reaper cycle (runs after each GC cycle):
     a. Scan `segments` column family for all segment IDs
     b. For each segment, check if any object in `objects` column family
        references it (scan for segment ID in chunk refs)
     c. If no object references the segment AND the segment was sealed more
        than `gc_tombstone_ttl_sec` ago: delete segment shards from disk,
        remove segment metadata from RocksDB
     d. Emit: `orphan_segments_found`, `orphan_segments_reaped`

5. **Wire admin endpoints with real data:**
   - `GET /admin/cluster`: Return real `Membership` state (node states,
     incarnations, last seen timestamps) and `RingCache` topology (vnodes →
     node mapping, ring version)
   - `GET /admin/segments`: Return real segment health from the last scrub
     cycle: total segments, healthy count, diverged count, last scrub
     timestamp
   - `GET /admin/caches`: Return real cache stats: `ObjectCache` hit/miss
     rates, `MetadataCache` hit/miss rates, `NegativeCache` hit/miss rates,
     cache sizes in bytes
   - `GET /admin/metrics`: Prometheus metrics endpoint already exists; verify
     it exposes all durability metrics (GC, AE, scrub, reaper)
   - `POST /admin/scrub`: Trigger an immediate full scrub cycle (bypassing
     the interval timer)
   - `GET /admin/acceleration`: Return real `AccelDispatcher` status: active
     tier, available backends, fallback count (per ADR-0006 §9.8.3)

6. **Heal queue integration:**
   - When scrub or anti-entropy detects a diverged segment, enqueue it on the
     `HealQueue` (a bounded `tokio::sync::mpsc` channel)
   - The heal worker (already spawned as a background task in the composition
     root) dequeues entries and:
     - Reads k surviving shards from healthy nodes
     - EC-decodes the full segment (using the acceleration tier)
     - Reconstructs the missing/corrupt shard
     - Places the reconstructed shard on the designated node
     - Updates segment metadata with new storage locations
   - Heal concurrency bounded by `heal_parallel_segments` (default 16) via a
     semaphore (perf §2.7)

### Out of Scope

- GC compaction producing new chunk refs that cross segment size tiers with
  adaptive k selection (initial implementation uses same k,m as the original
  segment)
- WAN replication of anti-entropy (single-region only per spec §16)
- Scrub coordinator fault tolerance (coordinator election handled by
  deterministic lowest-NodeId; if the coordinator fails, the next scheduled
  cycle will elect a new one)
- Heal bandwidth scheduling / QoS (throttle is a flat bytes/sec; no
  work-conserving weighted-fair-queue)
- Proactive rebalancing on node addition/removal (data migrates via standard
  consistent hashing rebalance; no explicit rebalance scheduler here)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | MODIFIED: `src/gc.rs` — remove `#[allow(dead_code)]`, implement full compaction cycle with segment re-pack and EC re-encode |
| `oceanfs-storage` | MODIFIED: `src/anti_entropy.rs` — replace no-op `anti_entropy.rs:513` with real Merkle tree exchange via gRPC |
| `oceanfs-storage` | MODIFIED: `src/scrub.rs` — replace `scrub.rs:148` "always healthy" with real BLAKE3 + Merkle verification |
| `oceanfs-storage` | MODIFIED: `src/orphan_reaper.rs` — remove `#[allow(dead_code)]`, implement full scan-and-reap cycle |
| `oceanfs-storage` | NEW: `src/heal_queue.rs` — bounded channel for heal requests; heal worker consuming from the queue |
| `oceanfs-storage` | NEW: `src/merkle.rs` — Merkle tree construction over segment shards, incremental recomputation |
| `oceanfs-server` | MODIFIED: `src/admin.rs` — replace all empty-data responses with real Membership, RingCache, GC, cache, acceleration data |
| `oceanfs-node` | MODIFIED: `src/node.rs` — spawn GC, AE, scrub, reaper, heal worker as background tasks; wire admin handler with real data sources |

## Interface (Public API)

- `pub struct GarbageCollector` — updated:
  - `pub fn new(metadata: Arc<MetadataStore>, segment_store: Arc<dyn SegmentStore>, encoder: Arc<dyn Encoder>, config: GcConfig) -> Self`
  - `pub async fn run_cycle(&self) -> Result<GcStats>`
- `pub struct GcStats` — `segments_scanned: u64`, `segments_compacted: u64`,
  `bytes_reclaimed: u64`, `tombstones_expired: u64`, `duration: Duration`
- `pub struct AntiEntropyWorker` — updated:
  - `pub fn new(membership: Arc<Membership>, ring: Arc<RingCache>, segment_store: Arc<dyn SegmentStore>, pool: Arc<ConnectionPool>, config: AntiEntropyConfig) -> Self`
  - `pub async fn run_cycle(&self) -> Result<AntiEntropyStats>`
- `pub struct AntiEntropyStats` — `cycles: u64`, `segments_compared: u64`,
  `divergences_found: u64`, `repairs_enqueued: u64`
- `pub struct ScrubCoordinator` — updated:
  - `pub fn new(node_id: NodeId, membership: Arc<Membership>, ring: Arc<RingCache>, segment_store: Arc<dyn SegmentStore>, config: ScrubConfig) -> Self`
  - `pub async fn run_cycle(&self) -> Result<ScrubReport>`
- `pub struct ScrubReport` — `total_segments: u64`, `healthy: u64`,
  `diverged: u64`, `errors: Vec<ScrubError>`, `bytes_verified: u64`,
  `duration: Duration`
- `pub struct OrphanReaper` — updated:
  - `pub fn new(metadata: Arc<MetadataStore>) -> Self`
  - `pub async fn run_cycle(&self) -> Result<OrphanStats>`
- `pub struct HealQueue` — bounded mpsc channel wrapper:
  - `pub fn new(capacity: usize) -> (HealSender, HealReceiver)`
  - `pub async fn enqueue(&self, segment_id: SegmentId, shard_index: ShardIndex, reason: HealReason) -> Result<()>`
- `pub enum HealReason` — `ScrubDiverged`, `AntiEntropyDiverged`,
  `NodeFailure(NodeId)`, `ReadRepairDetected`

## Data Flow

```
Background Task Lifecycle (per cycle):

Garbage Collector:
  1. wake on gc_interval_sec timer (or cancellation token)
  2. scan deletions CF: find tombstones > gc_tombstone_ttl_sec old
  3. for each expired tombstone → mark chunk free in segment blob_index
  4. scan segments CF: find segments with liveness < gc_compact_threshold
  5. for each candidate segment (bounded semaphore):
     a. read live blobs via segment index
     b. segment_sealer::repack(live_blobs, tier_config) → new segment
     c. EC encode new segment (AccelDispatcher)
     d. distribute shards (gRPC AppendSegment to storage nodes)
     e. update object metadata → point to new chunk refs
     f. mark old segment → delete shards, remove metadata
  6. emit GcStats, log summary, sleep until next interval

Anti-Entropy Worker:
  1. wake on anti_entropy_interval_sec timer
  2. for each neighbor node (adjacent vnode owner):
     a. compute segment partition shared with neighbor
     b. compute Merkle roots for segments in partition (incremental:
        only segments modified since last AE)
     c. MerkleExchange gRPC → send roots, receive neighbor's roots
     d. compare: for each mismatched root → descend tree → identify
        diverged shards
     e. enqueue diverged segments on HealQueue
  3. emit AntiEntropyStats, log summary, sleep until next interval

Scrub Coordinator:
  1. wake on scrub_interval_sec timer (or POST /admin/scrub)
  2. if self is coordinator (lowest NodeId):
     a. partition segment IDs across all healthy nodes
     b. distribute partitions (or nodes compute own from consistent hash)
  3. for each segment in assigned partition (bounded semaphore):
     a. read all local shard files from disk
     b. verify each shard: BLAKE3(shard_data) == stored hash
     c. verify Merkle root: recompute from shard hashes
     d. verify EC consistency: decode k of k+m shards → compare
     e. on mismatch → enqueue on HealQueue, log ERROR
  4. if coordinator: collect reports from all nodes, aggregate, emit
     ScrubReport
  5. log summary, sleep until next interval

Orphan Reaper:
  1. wake after each GC cycle (or on timer)
  2. scan segments CF: get all segment IDs
  3. for each segment: check objects CF for any reference
  4. if no reference AND seal_age > gc_tombstone_ttl_sec:
     delete shard files from disk, remove segment metadata
  5. emit OrphanStats, log summary

Heal Worker:
  1. loop: recv from HealQueue channel
  2. acquire heal semaphore permit (heal_parallel_segments)
  3. for the enqueued segment:
     a. locate k healthy nodes with shards (from ring + membership)
     b. read k surviving shards via FetchShard gRPC
     c. EC decode → reconstruct full segment (AccelDispatcher)
     d. place reconstructed shard on target node via AppendSegment gRPC
     e. update segment metadata → new storage_locations[]
  4. release semaphore permit
  5. loop
```

## Key Decisions

### DK-001: GC Compaction Strategy

**Decision:** GC re-packs live blobs from under-live segments into new segments
following the tiered sizing rules from spec §3.2. A 4 MB segment with 40%
liveness becomes: inline blobs (≤4 KB) stored inline in metadata, small blobs
(4-256 KB) packed into a 64 KB small segment, and remaining blobs packed into a
new standard segment.

**Rationale:** Without re-tiering during compaction, small fragments accumulate
in standard segments, wasting space and increasing read amplification. Re-tiering
during compaction is the only opportunity to "fix" the segment layout — the write
path must make fast decisions (appending to the active segment pool) and cannot
retroactively optimize.

### DK-002: Incremental Merkle Tree Computation

**Decision:** Use an HLC watermark per segment (`last_modified_hlc`) to track
whether a segment's Merkle root needs recomputation. On the first AE cycle, all
segments are scanned (full Merkle tree construction). On subsequent cycles, only
segments with `last_modified_hlc > last_ae_hlc` are recomputed.

**Rationale:** Recomputing Merkle trees for millions of segments every 5 minutes
wastes CPU and I/O. With incremental computation, the cost of the AE cycle is
proportional to the write rate, not the total data size. A 1 PB cluster with 100
MB/s of writes touches ~25 segments per cycle (at 4 MB each) — negligible
overhead.

### DK-003: Scrub Coordinator Election

**Decision:** The node with the lowest `NodeId` in the healthy membership list
is the scrub coordinator. No distributed election protocol.

**Rationale:** Scrub runs every 7 days. The coordinator is only needed for
partitioning and report aggregation — not for the scrub work itself. If the
coordinator fails mid-cycle, nodes continue scrubbing their partitions
independently (no coordination needed after partition assignment). The next
cycle elects a new coordinator. Paxos/Raft election is overkill for a
best-effort weekly maintenance task.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds; no `#[allow(dead_code)]`
  on GC, reaper, or heal queue
<!-- REVIEW (Iteration 3): ✅ Build passes. But `#[allow(dead_code)]` persists: gc.rs (2 occurrences at lines 182,190 on internal helpers), anti_entropy.rs (4 occurrences at lines 11,234,561,567), scrub.rs (5 occurrences at lines 94,112,126,132,238), and 24+ more across oceanfs-storage (segment/buffer, segment/header, segment/sealer, etc.). HealQueue does not exist (no heal_queue.rs file) ❌. Dead code is MORE widespread than iteration 2 report suggested — ALL segment submodules have dead_code annotations -->
- [ ] **Tests:** Unit tests per component:
  - `GarbageCollector`: empty CF → no-op, tombstone within TTL → not expired,
    tombstone beyond TTL → expired, segment below compact threshold →
    compacted, segment above threshold → skipped, after compaction old segment
    deleted, stats accurate
  - `AntiEntropyWorker`: identical Merkle roots → no divergence, different
    roots → tree descent finds diverged shard, incremental mode skips
    unmodified segments, enqueued repairs land on HealQueue
  - `ScrubCoordinator`: healthy segment → `healthy: true`, corrupt shard →
    error logged + enqueued, Merkle mismatch → error, EC consistency check
    passes with any k shards
  - `OrphanReaper`: segment with references → not reaped, segment without
    references + beyond TTL → reaped, segment without references but within
    TTL → not reaped
  - `AdminHandler`: `GET /admin/cluster` → real membership data,
    `GET /admin/segments` → real scrub report, `GET /admin/caches` → real
    cache stats
<!-- REVIEW (Iteration 3): GC tests exist (gc.rs) ✅. AE tests exist for MerkleTree ✅. Scrub tests exist ✅. OrphanReaper has real logic but no unit tests. AdminHandler: cluster_view NOW returns real membership data via nodes_full() (admin.rs:364-378) ✅ — NEW in iteration 3. But segment_report returns all zeros (admin.rs:392-394) ❌, cache_stats returns all zeros (admin.rs:400-405) ❌, trigger_scrub is no-op (admin.rs:410-413) ❌. metrics_endpoint returns real data ✅ -->
- [ ] **Tests:** Integration tests:
  - `oceanfs-node/tests/gc_compaction.rs`: write 100 blobs, delete 60, wait
    for GC cycle, verify under-live segment compacted, live blobs still
    readable
  - `oceanfs-node/tests/anti_entropy.rs`: 2-node cluster, corrupt one shard
    on node 2, run AE cycle, verify divergence detected and repair enqueued
  - `oceanfs-node/tests/scrub_cycle.rs`: trigger scrub via admin endpoint,
    verify scrub report shows healthy segments
  - `oceanfs-node/tests/orphan_reaper.rs`: write blob, delete blob, force GC,
    wait TTL, verify segment shards removed from disk
<!-- REVIEW: `oceanfs-node/tests/` directory does not exist. No integration test files anywhere -->
  GC, AE, scrub, reaper modules all exercised
<!-- REVIEW: Coverage not verified — protoc not available -->
<!-- REVIEW (Iteration 3): ✅ clippy passes. BUT `#[allow(dead_code)]` remains on 11+ items in durability modules: scrub.rs (5), anti_entropy.rs (4), gc.rs (2). oceanfs-network/lib.rs:30 has `#![allow(dead_code)]` at crate level. The clippy check passes because dead_code is a compiler lint (not clippy), and the `#[allow]` annotations suppress it -->
- [ ] **Docs:** Every `pub` item documented; module docs explain durability
  lifecycle (GC cycle, AE exchange, scrub partition, reaper cycle)
<!-- REVIEW: Module-level docs exist for gc.rs, anti_entropy.rs, scrub.rs. But individual pub methods on GarbageCollector, AntiEntropy, ScrubCoordinator, OrphanReaper have docs that describe the full implementation when the implementation is partial. AE run_cycle docs describe gRPC exchange but implementation is no-op -->
- [ ] **ADR:** ADR-0001 (segment packing): GC compaction re-packs using tiered
  sizing rules correctly
<!-- REVIEW: GC uses TierRouter (gc.rs:327) and tier_target_size (gc.rs:24-32) for repacking. Config has compact_threshold=0.5 and tombstone_ttl_sec=259200 matching ADR-0001. But compaction is incomplete — SegmentCompactor and repack logic exist but are flagged as `#[allow(dead_code)]` on parts, suggesting the full re-pack + EC-re-encode path is not fully wired -->
- [ ] **Perf:** Rule 2.6 (HealQueue is a bounded channel — capacity
  configurable via `heal_queue_capacity`), Rule 2.7 (heal semaphore limits
  concurrent heal ops to `heal_parallel_segments`; scrub semaphore limits
  concurrent segment verification), Rule 8.5 (semaphore acquired before each
  GC compaction, AE exchange, and heal operation), Rule 5.1 (BLAKE3 crypto
  hash for Merkle leaves and scrub verification)
<!-- REVIEW: 2.6: No HealQueue exists (no heal_queue.rs). 2.7: Scrub uses semaphore (scrub.rs:308-319) ✅. GC uses semaphore (gc.rs:325-344, 339-343) ✅. 8.5: Semaphore acquired in GC ✅ and Scrub ✅. 5.1: MerkleTree uses BLAKE3 (anti_entropy.rs) ✅ -->
- [ ] **Integration:** Full durability cycle in a 3-node cluster: write data,
  delete some blobs, run GC → verify compaction, trigger scrub → verify report,
  corrupt a shard manually, run AE → verify detection, verify heal worker
  reconstructs the shard
<!-- REVIEW: Integration tests not possible: no integration test directory, no gRPC services, AE is no-op -->
  stats; `curl -X POST http://localhost:9000/admin/scrub` triggers scrub cycle
  (visible in logs)
<!-- REVIEW: All admin endpoints return hardcoded data. cluster_view returns empty nodes/zero vnodes (admin.rs:326-327). segment_report returns all zeros (admin.rs:336-337). cache_stats returns all zeros (admin.rs:344-348). trigger_scrub acknowledges but does not trigger actual scrub (admin.rs:354-358). Only metrics_endpoint returns real data -->
