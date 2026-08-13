# Gap Closure — Master Index

**Date:** 2026-08-05  
**Context:** Six domain-specific audits (storage durability, distributed systems,
EC/accel/hash, server/cache, integration/config, metrics implementation) conducted
on 2026-08-05 identified **140 findings** (19 critical, 47 high, 41 medium, 33 low)
across 14 crates. This gap-closure plan organizes every finding into 6 epics with
dependency-ordered execution.

**Unified Gap Plan:** [`docs/brainstorm/implementation-gap-plan.md`](../brainstorm/implementation-gap-plan.md)

---

## Epic Summary

| # | Epic | Priority | Findings Covered | Blocks | Blocked By |
|---|---|---|---|---|---|
| 1 | [config-system-fix](#epic-1-config-system-fix) | **critical** | 13 (1C, 5H, 7M) | Epics 3, 4 | — |
| 2 | [metrics-infrastructure](#epic-2-metrics-infrastructure) | **critical** | 45 (10C, 23H, 8M, 4L) | Epics 3, 4 | — (parallel with Epic 1) |
| 3 | [write-path-unification](#epic-3-write-path-unification) | **critical** | 17 (3C, 6H, 3M, 5L) | Epics 4, 5 | Epics 1, 2 |
| 4 | [correctness-gaps](#epic-4-correctness-gaps) | **critical** | 25 (4C, 12H, 6M, 3L) | — | Epics 2, 3 |
| 5 | [background-task-cleanup](#epic-5-background-task-cleanup) | **high** | 17 (0C, 3H, 7M, 7L) | — | Epic 3 |
| 6 | [codebase-hygiene](#epic-6-codebase-hygiene) | **medium** | 28 (1C, 1H, 11M, 15L) | — | — (anytime) |

---

## Dependency Graph

```
Epic 1 (config-system-fix) ─────┐
                                ├──▶ Epic 3 (write-path-unification) ──▶ Epic 4 (correctness-gaps)
Epic 2 (metrics-infrastructure)─┘         │
                                          └──▶ Epic 5 (background-task-cleanup)

Epic 6 (codebase-hygiene) — independent, can start anytime
```

**Execution order:** Sprint A (Epic 1 + 2 in parallel) → Sprint B (Epic 3) → Sprint C (Epic 4) → Sprint D (Epic 5). Epic 6 can run as background work throughout.

---

## Epic 1: config-system-fix

**Feature:** [`config-system-fix/feature.md`](config-system-fix/feature.md)

Fixes the critical TOML merge bug that silently drops 16+ config fields (root cause of e2e deviations D2, D3, D4, D8). Adds missing serde derives, `BucketPolicy` struct, env var support, and missing config fields (`vnodes_per_node`, `replication_factor`, etc.).

### Audit Findings Covered

| ID | Severity | Source | Description |
|---|---|---|---|
| C1-integration | Critical | Integration audit | `merge_config()` only copies 6 fields; 16+ silently dropped |
| M1-integration | Medium | Integration audit | `SegmentSizeConfig` not serde-deserializable |
| M2-integration | Medium | Integration audit | No `BucketPolicy` struct or replication fields in `NodeConfig` |
| M3-integration | Medium | Integration audit | No env var support for intervals/toggles |
| M5-integration | Medium | Integration audit | `merge_config` uses brittle sentinel-value checks |
| M7-integration | Medium | Integration audit | `GossipConfig` and other config types lack `serde::Deserialize` |
| M1-distributed | Medium | Distributed audit | `vnodes_per_node` not configurable from `NodeConfig` |
| L4-distributed | Low | Distributed audit | Missing `vnodes_per_node`, `replication_factor`, `pool_size_per_peer`, `keepalive_sec` in `NodeConfig` |
| L5-integration | Low | Integration audit | Duplicate `GossipConfig` definitions across two modules |
| D2 deviation | — | e2e smoke tests | GC interval appears hardcoded (caused by C1-integration) |
| D3 deviation | — | e2e smoke tests | Orphan reaper depends on GC (caused by C1-integration) |
| D4 deviation | — | e2e smoke tests | AE interval appears hardcoded (caused by C1-integration) |
| D8 deviation | — | e2e smoke tests | 2MB body size limit (caused by C1-integration) |

---

## Epic 2: metrics-infrastructure

**Feature:** [`metrics-infrastructure/feature.md`](metrics-infrastructure/feature.md)

Adds `Gauge` type, label support, per-bucket `AtomicU64` histograms to the
`MetricsRegistry`. Wires all 25 existing internal counters (cache, heal, accel,
buffer pool, segment pool) into `/admin/metrics`. Adds process metrics (memory,
FDs), RocksDB properties, gossip counters, WAL counters, and 6 timing histograms.
Unblocks Phase 2-5 load testing.

### Audit Findings Covered

**All 45 findings from `docs/audits/2026-08-05-metrics-implementation-gaps.md`:**
C1–C10 (10 critical), H1–H23 (23 high), M1–M8 (8 medium), L1–L4 (4 low).

**Plus related findings from other audits:**

| ID | Severity | Source | Description |
|---|---|---|---|
| H2-server | High | Server audit | `MetricsRegistry` uses `RwLock<HashMap>` — should be `DashMap` |
| H3-server | High | Server audit | No Gauge support, no label support |
| M2-server | Medium | Server audit | Zero production metrics registered |
| M6-server | Medium | Server audit | `registrar()` acquires write lock on common path |
| M7-server | Medium | Server audit | `gather()` read lock blocks registration hot path |
| H4-integration | High | Integration audit | MetricsRegistry constructed empty — no subsystem feeds it |

---

## Epic 3: write-path-unification

**Feature:** [`write-path-unification/feature.md`](write-path-unification/feature.md)

Wires the segment pipeline (TierRouter → SegmentPool → ActiveSegment → SegmentSealer → EC encode → RocksDB metadata) into the S3 PUT handler. Replaces/coexists with `BlobStore` so that `put_segment()` is called, the `segments` CF is populated, and GC/scrub/anti-entropy/heal operate on real segment data.

### Audit Findings Covered

| ID | Severity | Source | Description |
|---|---|---|---|
| C1-storage | Critical | Storage audit | Segment pipeline (10 files) entirely dead code |
| C2-storage | Critical | Storage audit | `BufferPool` / `SegmentSealer` constructed as `_unused` |
| C3-storage | Critical | Storage audit | `put_segment()` never called — segments CF empty |
| H1-storage | High | Storage audit | `SegmentPool` entirely `#[allow(dead_code)]` |
| H7-storage | High | Storage audit | `route_write` dead code, uses single `ActiveSegment` |
| H8-storage | High | Storage audit | WAL truncation never called in production |
| M5-storage | Medium | Storage audit | `BlobStore` vs `SegmentStore` architectural ambiguity |
| L1-storage | Low | Storage audit | `PoolSlotState` / `PoolSlot` type-level `#[allow(dead_code)]` |
| L2-storage | Low | Storage audit | `ChunkListBuilder` methods `#[allow(dead_code)]` |
| L5-storage | Low | Storage audit | `route_write` wildcard arm silently ignores unknown tier |
| L7-storage | Low | Storage audit | `BufferPool` effectively dead code in production |
| H3-integration | High | Integration audit | BufferPool/SegmentSealer constructed but never wired |
| D1 deviation | — | e2e smoke tests | Segment metadata not created in write path |

---

## Epic 4: correctness-gaps

**Feature:** [`correctness-gaps/feature.md`](correctness-gaps/feature.md)

**Feature:** [`hlc-causality-closure/feature.md`](hlc-causality-closure/feature.md)

Fixes seven functional correctness bugs: WAL crash recovery (fixes D6), read
repair with multi-replica HLC comparison + corrective push, EC decode integration
into read path, hinted handoff delivery wiring (fixes T21), graceful leave with
WAL handoff + shard streaming, multi-replica HLC comparison for concurrent writes
(fixes T45), and port preservation in Cluster harness (fixes T43). Also wires
`ReadTuningConfig`, implements group commit fsync, distributed shard fetch,
peer-to-peer Merkle exchange, distributed scrub, and the bucket policy endpoint.

The `hlc-causality-closure` feature provides the causality substrate the
multi-replica comparisons depend on: a live wall clock, the HLC
receive-merge rule wired into every remote-HLC reception site, and HLC
propagation through replication, tombstones, and hinted handoff.

**Feature:** [`read-path-integrity-under-load/feature.md`](read-path-integrity-under-load/feature.md)

Discovered 2026-08-13 by the fidelity-fixed load test: 176/417 objects
unreadable after a 30 s run. Two confirmed multi-tier write-path defects
(blob-relative chunk offsets, missing blob-index registration).

### Audit Findings Covered

| ID | Severity | Source | Description |
|---|---|---|---|
| C4-storage | Critical | Storage audit | WAL crash recovery not wired (fixes D6) |
| C1-server | Critical | Server audit | Read repair compares same HLC, no corrective push |
| C2-server | Critical | Server audit | `decode_ec_shards()` dead code |
| C5-storage | Critical | Storage audit | Hinted handoff not integrated into write path |
| H1-server | High | Server audit | `ReadTuningConfig` fields only logged, never applied |
| H2-storage | High | Storage audit | WAL group commit uses no-op fsync |
| H3-storage | High | Storage audit | Heal worker local-only; no distributed shard fetch |
| H4-storage | High | Storage audit | Anti-entropy peer-to-peer Merkle exchange stubbed |
| H5-storage | High | Storage audit | Scrub coordinator no distributed partition assignment |
| H2-distributed | High | Distributed audit | Graceful leave is 100ms stub |
| H3-distributed | High | Distributed audit | T43: port reassignment on restart |
| H4-server | High | Server audit | T45: no multi-replica HLC comparison for concurrent writes |
| H5-server | High | Server audit | T21: hinted handoff delivery not wired |
| H6-server | High | Server audit | T43: Cluster::restart() assigns new ports |
| H7-server | High | Server audit | POST /{bucket}?policy not implemented |
| M4-server | Medium | Server audit | `FetchShard` uses `Vec<u8>` instead of `BytesMut` |
| M5-server | Medium | Server audit | `forward_write()` returns `Hlc::zero()` |
| M8-server | Medium | Server audit | Prefetch adjacent-key discovery not implemented |
| M9-server | Medium | Server audit | `perform_read_repair()` never pushes corrected data |
| L3-server | Low | Server audit | `/admin/cluster` vnodes hardcoded to 256 |
| L4-server | Low | Server audit | `DEFAULT_READ_TIMEOUT_MS` dead code |
| D6 deviation | — | e2e smoke tests | WAL crash recovery returns 500 |

---

## Epic 5: background-task-cleanup

**Feature:** [`background-task-cleanup/feature.md`](background-task-cleanup/feature.md)

Removes dormant gossip and failure-detector background tasks (redundant with
`Membership::start()`). Wires or removes the prefetch keep-alive loop. Implements
SWIM remote probes (or documents the proxy approach), connection pool health
checking, incarnation tracking, and graceful shutdown for gRPC + RocksDB + WAL.
Writes the missing ADR-0002.

### Audit Findings Covered

| ID | Severity | Source | Description |
|---|---|---|---|
| H1-integration | High | Integration audit | Gossip background task is `std::future::pending` |
| H2-integration | High | Integration audit | Failure detector task is 1-second sleep |
| H5-integration | High | Integration audit | Prefetch task is 60-second sleep keep-alive |
| H1-distributed | High | Distributed audit | SWIM remote probes never sent via gRPC |
| M2-distributed | Medium | Distributed audit | ConnectionPool health_check() is no-op |
| M3-distributed | Medium | Distributed audit | Incarnation hardcoded to Incarnation::new(1) |
| M4-distributed | Medium | Distributed audit | ADR-0002 (SWIM vs Raft) missing |
| M5-distributed | Medium | Distributed audit | T40/T41 graceful leave tests are placeholders |
| M6-distributed | Medium | Distributed audit | Stale comment referencing removed try_forward |
| L3-integration | Low | Integration audit | gRPC server spawn has no graceful shutdown |
| L4-integration | Low | Integration audit | Shutdown doesn't close MetadataStore or flush WalWriter |
| L5-distributed | Low | Distributed audit | ProbeHandler always called, misleading trace logs |
| L6-distributed | Low | Distributed audit | ProbeHandler pub visibility should be pub(crate) |
| L7-distributed | Low | Distributed audit | No successful connection pool integration test |
| L6-integration | Low | Integration audit | Auth key loading validated at request time, not startup |
| M10-server | Medium | Server audit | Placeholder comments remain in node.rs |

---

## Epic 6: codebase-hygiene

**Feature:** [`codebase-hygiene/feature.md`](codebase-hygiene/feature.md)

Comprehensive dead code removal, type consolidation, and structural cleanup across
all 14 crates. Removes the dead `IsalEncoder` stub from `oceanfs-ec`, crate-level
`#[allow(dead_code)]` from membership and network, the unused `bytes` dependency
from hash, the `RpcClient` marker trait, and async wrappers. Consolidates
`MetadataStore`/`MetadataOps` trait fragmentation, GF(2^8) tables, and duplicate
`GossipConfig`. Completes GPU cooldown, `NvcompBufferPool`, nvCOMP codec support,
`split-node-rs`, and manual CLI→clap migration.

### Audit Findings Covered

| ID | Severity | Source | Description |
|---|---|---|---|
| C1-accel | Critical | EC/Accel audit | Dead `IsalEncoder` stub in `oceanfs-ec` |
| H1-accel | High | EC/Accel audit | GPU cooldown/recovery incomplete |
| H6-storage | High | Storage audit | MetadataStore/MetadataOps trait fragmentation |
| M1-accel | Medium | EC/Accel audit | `NvcompBufferPool` not implemented |
| M2-accel | Medium | EC/Accel audit | nvCOMP Snappy/zstd FFI not implemented |
| M3-accel | Medium | EC/Accel audit | GF(2^8) tables duplicated across 3 modules |
| M1-server | Medium | Server audit | `/admin/segments` encoding hardcoded to 0 |
| M3-server | Medium | Server audit | Duplicate `invalidate_cache_on_replicas()` call |
| M11-server | Medium | Server audit | Catch-all key routing path-style only (doc gap) |
| M4-storage | Medium | Storage audit | `StubDecoder` used in tests; no real decoder failure tests |
| M6-integration | Medium | Integration audit | `node.rs` 1015 lines — needs split refactoring |
| M4-integration | Medium | Integration audit | Manual CLI parsing — no clap |
| M4-metrics-audit | Medium | Metrics audit | No `docs/metrics.md` catalog |
| L1-accel | Low | EC/Accel audit | nvCOMP `num_chunks` hardcoded to 1 |
| L2-accel | Low | EC/Accel audit | CUDA probe returns true unconditionally |
| L3-accel | Low | EC/Accel audit | `ArmSveLevel` missing `#[non_exhaustive]` |
| L4-accel | Low | EC/Accel audit | Unused `bytes` dependency in `oceanfs-hash` |
| L1-distributed | Low | Distributed audit | Crate-level `#![allow(dead_code)]` in membership |
| L2-integration | Low | Integration audit | Crate-level `#![allow(dead_code)]` in network |
| L2-distributed | Low | Distributed audit | TLS placeholder ungated |
| L3-distributed | Low | Distributed audit | `RpcClient` marker trait with zero implementors |
| L1-server | Low | Server audit | No CORS middleware |
| L2-server | Low | Server audit | Prefetch silently disabled without warning |
| L5-server | Low | Server audit | Negative cache inverted semantics undocumented |
| L3-storage | Low | Storage audit | Async wrappers duplicate sync methods |
| L4-storage | Low | Storage audit | MetadataStore trait consolidation |
| M1-storage | Medium | Storage audit | `encode_deletion_key` dead code |
| M2-storage | Medium | Storage audit | `MerkleExchangeProtocol` undocumented `#[allow(dead_code)]` |
| M3-storage | Medium | Storage audit | `throttle_bytes_sec` and `partition_segments` undocumented |
| L6-storage | Low | Storage audit | EC re-encode during compaction deferred (document as ADR) |

---

## Audit Reports Source

| Audit | File | Crates | Findings |
|---|---|---|---|
| Storage & Durability | [`2026-08-05-storage-durability-completeness.md`](../audits/2026-08-05-storage-durability-completeness.md) | `oceanfs-storage`, `oceanfs-storage-api`, `oceanfs-durability` | 5C, 8H, 6M, 7L |
| Distributed Systems | [`2026-08-05-distributed-systems-layer.md`](../audits/2026-08-05-distributed-systems-layer.md) | `oceanfs-routing`, `oceanfs-membership`, `oceanfs-network` | 0C, 3H, 6M, 7L |
| EC, Accel & Hash | [`2026-08-05-ec-accel-hash-subsystem-audit.md`](../audits/2026-08-05-ec-accel-hash-subsystem-audit.md) | `oceanfs-ec`, `oceanfs-accel`, `oceanfs-hash` | 1C, 1H, 3M, 4L |
| Server & Cache | [`2025-08-05-server-cache-implementation-audit.md`](../audits/2025-08-05-server-cache-implementation-audit.md) | `oceanfs-server`, `oceanfs-cache` | 2C, 7H, 11M, 5L |
| Integration & Config | [`2026-08-05-integration-config-composition-audit.md`](../audits/2026-08-05-integration-config-composition-audit.md) | `oceanfs-node`, `oceanfs`, `oceanfs-core` | 1C, 5H, 7M, 6L |
| Metrics Gaps | [`2026-08-05-metrics-implementation-gaps.md`](../audits/2026-08-05-metrics-implementation-gaps.md) | All crates | 10C, 23H, 8M, 4L |
| **Total** | | | **19C, 47H, 41M, 33L** |

---

## E2E Test Deviation Resolution

| Deviation | Status | Resolved By |
|---|---|---|
| D1 (segment metadata not created) | → Resolved | Epic 3 — write-path-unification |
| D2 (GC interval hardcoded) | → Resolved | Epic 1 — config-system-fix |
| D3 (orphan reaper depends on GC) | → Resolved | Epic 1 — config-system-fix |
| D4 (AE interval hardcoded) | → Resolved | Epic 1 — config-system-fix |
| D6 (WAL crash recovery) | → Resolved | Epic 4 — correctness-gaps §4.1 |
| D7 (prefetch L2 entry_count) | → Resolved | Epic 5 — background-task-cleanup §H5-integration |
| D8 (2MB body size limit) | → Resolved | Epic 1 — config-system-fix |

---

## Failing E2E Test Resolution

| Test | Status | Resolved By |
|---|---|---|
| T21 (hinted handoff delivery) | → Pass | refactoring epic — membership-stability-fixes (ADR-0022 incarnation bump + address-merge rule) |
| T43 (crash recovery rejoin) | → Pass | refactoring epic — membership-stability-fixes (ADR-0022 persisted incarnation + fallback seeds) |
| T45 (concurrent writes same key) | → Pass | Epic 4 — correctness-gaps §4.6 |
| T24/T26 (SWIM intermittent) | → Pass | refactoring epic — membership-stability-fixes (F1 SWIM state-machine fixes; earlier Epic 5 attempt was incomplete) |
