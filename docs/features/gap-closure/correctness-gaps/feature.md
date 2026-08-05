---
feature: "Correctness Gaps — WAL Recovery, Read Repair, EC Decode, Hinted Handoff, Graceful Leave"
epic: "correctness-gaps"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: write-path-unification
    reason: Read repair, EC decode, and hinted handoff need real segment data to operate on
  - epic: metrics-infrastructure
    reason: Correctness fixes need observability for verification
adr:
  - 0001-segment-packing
  - 0006-hardware-acceleration-tier-model
perf:
  - "8.1 FuturesUnordered for parallel shard fetches"
  - "8.2 tokio::select! with timeout branches"
created: 2026-08-05
updated: 2026-08-05
---

# Correctness Gaps — WAL Recovery, Read Repair, EC Decode, Hinted Handoff, Graceful Leave

## Summary

Seven critical/high-severity functional correctness bugs exist across the storage,
server, and distributed subsystems. WAL crash recovery is unwired (node startup
never calls `WalReader::open()`/`replay()`). Read repair compares the same HLC
against itself and never pushes corrected data to stale replicas. EC decode is
dead code — reads that need parity shard reconstruction will fail. Hinted handoff
buffers writes during node failure but never delivers them when the node returns.
Graceful leave is a 100ms stub with no WAL handoff or shard streaming. Multi-replica
HLC comparison for concurrent writes is not implemented. This feature fixes every
one of these correctness gaps, targeting the four failing e2e tests (T21, T43, T45,
plus crash recovery) and closing deviations D6 and D7.

## Scope

### In Scope

#### 4.1 — WAL Crash Recovery (C4-storage, D6 deviation)
- Call `WalReader::open()` and `replay()` during `Node::start()`, before the HTTP server binds
- Replay unsealed segment data from WAL into active segments
- Rebuild in-memory state (HLC, segment pool state) from replayed entries
- Wire replay result handling: if replay fails, node startup fails with clear error

#### 4.2 — Read Repair (C1-server, M9-server)
- Implement multi-replica HLC gathering in `ReadCoordinator::get_object()`
- Fetch metadata from N replicas (not just local), extract HLC timestamps
- Compare HLCs via `ConflictResolver`, identify stale replicas
- Push corrected data to stale replicas via gRPC `CacheInvalidate` or a new `RepairPush` RPC
- Remove the same-HLC degenerate call: `schedule_repair(meta.hlc, meta.hlc, ...)`

#### 4.3 — EC Decode Integration (C2-server)
- Wire `decode_ec_shards()` into the shard-level fetch path in `read/fetch.rs`
- Implement per-shard gRPC `FetchShard` calls for parity shard retrieval
- When k data shards are unavailable, fetch m parity shards and decode
- Remove `#[allow(dead_code)]` from `decode_ec_shards()`

#### 4.4 — Hinted Handoff Delivery (C5-storage, H5-server, T21)
- Wire `HintedHandoff::handoff()` into `WriteCoordinator` replication path
- When a successor is unreachable during quorum write, call `handoff()` with `{intended_for}`
- On node rejoin detection (ALIVE event from membership), drain the handoff buffer via gRPC
- Deliver buffered hints to returned node via `HealingRpcClient`
- Add integration test: T21 — write during node failure, restart node, verify data delivered

#### 4.5 — Graceful Leave (H2-distributed)
- Implement `leave()`: replace the 100ms sleep with real WAL handoff and shard streaming
- Seal active WAL segments, push them to next ring successor
- Stream owned segment shards to successors
- Announce LEAVING → drain in-flight requests → announce LEFT
- Write e2e tests T40/T41 exercising real graceful leave with data integrity validation

#### 4.6 — Multi-Replica HLC Comparison (H4-server, T45)
- In `ReadCoordinator::get_object()`, when `read_quorum > 1`, fetch metadata from N replicas
- Compare HLC timestamps from all replicas via `ConflictResolver`
- Serve the winning version to the client
- Asynchronously push corrected data to stale nodes (overlaps with 4.2 read repair)

#### 4.7 — Port Preservation for Cluster Restart (H3-distributed, H6-server, T43)
- In `Cluster::restart()` (e2e harness), write assigned ports to a port-file in the temp data dir
- On restart, read ports from the file before re-binding
- This enables T43 (crash recovery + rejoin) to pass

#### Additional Fixes
- H1-server: Wire `ReadTuningConfig` (`parallel_fetch`, `use_fastest_k`, `stripe_parallelism`) — apply semaphore for `stripe_parallelism`, implement serial fetch when `parallel_fetch = false`
- H2-storage: Implement `sync_all()` in the `WalSyncGroup` flusher task — true group commit
- H3-storage: Implement distributed shard fetch in `HealWorker::execute_heal()` — use `ConnectionPool` + `HealingRpcClient::fetch_shard()` instead of local-only `SegmentDataStore`
- H4-storage: Implement peer-to-peer `MerkleExchange` gRPC in anti-entropy — exchange Merkle roots with neighbor nodes, descend tree on mismatch
- H5-storage: Wire distributed scrub partition assignment — thread `Membership` and `ConnectionPool` into `ScrubCoordinator`
- H7-server: Implement `POST /{bucket}?policy` endpoint for setting/updating bucket policy
- M4-server: Replace `Vec<u8>` with `BytesMut` in `FetchShard` response accumulation
- M5-server: Fix `forward_write()` returning `Hlc::zero()` — return the HLC from the forwarding node's clock
- M8-server: Implement adjacent-key discovery for GET-triggered prefetch — per-bucket key ordering with range-scan
- L3-server: Fix `/admin/cluster` vnodes hardcoded constant — read actual `vnodes_per_node * node_count` from ring
- L4-server: Remove `DEFAULT_READ_TIMEOUT_MS` dead code

### Out of Scope

- EC re-encode during segment compaction (DEV-002, tracked separately)
- Full multi-node cluster integration tests beyond existing e2e suite
- Virtual-host-style S3 paths (M11-server, documented as path-style only)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | Wire `WalReader::open()`/`replay()` in `start()`. Wire `ReadTuningConfig` semaphore. |
| `oceanfs-server` | Fix `ReadCoordinator::get_object()` for multi-replica HLC fetch. Wire `decode_ec_shards()` in `read/fetch.rs`. Fix `read_repair` to push corrected data. Wire hinted handoff in `WriteCoordinator`. Implement `POST /{bucket}?policy`. Fix `forward_write()` HLC. Replace `Vec<u8>` with `BytesMut` in fetch. |
| `oceanfs-storage` | Implement `sync_all()` in `WalSyncGroup`. Add `WalReader` replay to node startup. Wire WAL truncation. |
| `oceanfs-durability` | Implement distributed shard fetch in `HealWorker`. Implement peer-to-peer `MerkleExchange`. Wire `ScrubCoordinator` with `Membership` + `ConnectionPool`. Wire hinted handoff delivery. |
| `oceanfs-membership` | Implement `leave()` with WAL handoff + shard streaming. |
| `oceanfs-cache` | Adjacent-key discovery for prefetch. |
| `e2e/` | Port preservation in `Cluster::restart()`. New T40/T41 graceful leave tests. Fix T21, T43, T45. |

## Interface (Public API)

- `pub async fn replay_wal(reader: &mut WalReader, segment_pool: &SegmentPool) -> Result<ReplaySummary>` — called at node startup
- `pub async fn fetch_and_compare_metadata(key: &Key, replicas: &[NodeId]) -> Result<Vec<(NodeId, ObjectMetadata)>>` — multi-replica metadata fetch
- `pub trait MerkleExchange: Send + Sync` — trait for peer-to-peer Merkle exchange (in `oceanfs-durability`)
- `pub async fn put_bucket_policy(bucket: &str, policy: BucketPolicy) -> Result<()>` — new S3 handler
- `pub async fn handoff(intended_for: NodeId, data: Bytes) -> Result<()>` — hinted handoff delivery method (already exists, now called from write path)

## Data Flow

### WAL Crash Recovery
```
Node::start():
  1. Open RocksDB
  2. Open WalReader → replay()
     ├── Read WAL entries since last truncation
     ├── Rebuild unsealed segments → SegmentPool
     ├── Rebuild HLC from max observed timestamp
     └── Return ReplaySummary { segments_recovered, bytes_replayed }
  3. Bind HTTP server
  4. Spawn background tasks
```

### Read Repair (After Fix)
```
GET /{bucket}/{key} (read_quorum = 3):
  1. Fetch metadata from N=3 replicas in parallel
  2. Extract HLC timestamps: [t1, t2, t3]
  3. ConflictResolver::resolve(t1, t2, t3) → winning HLC
  4. Serve winning version to client
  5. For each stale replica: async gRPC push corrected data
```

### Hinted Handoff (After Fix)
```
WriteCoordinator::put(key, data):
  for each successor in replica_set:
    try gRPC AppendSegment(successor, data)
    if gRPC fails:
      HintedHandoff::handoff(intended_for: successor, data)
        → buffer in RocksDB

Membership event: node X returns (ALIVE):
  HintedHandoff::deliver_pending(node_x)
    → drain buffer for node_x
    → gRPC HealingRpcClient::hinted_handoff(data) to node_x
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in all affected crates
- [ ] **Tests:** All existing tests pass. New tests:
  - WAL replay integration test: write to WAL, SIGKILL, restart, verify data readable (fixes D6)
  - Read repair test: write to node A version 1, write to node B version 2, read with quorum=3 → serve v2, push v2 to A
  - EC decode test: write EC(4,2) segment, drop 1 data shard, read using parity → succeeds
  - Hinted handoff delivery test: write during node failure, restart node, verify data readable (fixes T21)
  - Graceful leave test: write data, SIGTERM node, verify successors have data (fixes T40/T41)
  - Multi-replica HLC test: concurrent writes to same key from 2 nodes, read returns consistent winner (fixes T45)
  - Port preservation test: restart node, same ports, can rejoin cluster (fixes T43)
- [ ] **Tests:** `cargo test -p e2e` — all 43 tests pass (currently 39/43, target 43/43)
- [ ] **Docs:** Every new `pub` item has doc comments; `#![deny(missing_docs)]` passes
- [ ] **ADR:** ADR-0006 fallback chain remains working after EC decode integration
- [ ] **Perf:** Perf §8.1 — parallel shard fetch uses `FuturesUnordered`. Perf §8.2 — WAL replay uses `tokio::select!` with timeout
- [ ] **Integration:** End-to-end crash recovery scenario: write → kill -9 → restart → read returns correct data
- [ ] **Deviation closure:** D6 (WAL crash recovery) marked resolved
