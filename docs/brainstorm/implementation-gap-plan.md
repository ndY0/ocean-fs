# Implementation Gap Plan — Unified Synthesis

**Author:** Brainstorm Agent (Architect)
**Date:** 2026-08-05
**Context:** Synthesis of five domain-specific audits conducted 2026-08-05 across
all 14 workspace crates. This document prioritizes gaps by dependency chain and
produces an actionable closing plan.
**Source Reports:**
- `docs/audits/2026-08-05-storage-durability-completeness.md`
- `docs/audits/2026-08-05-distributed-systems-layer.md`
- `docs/audits/2026-08-05-ec-accel-hash-subsystem-audit.md`
- `docs/audits/2025-08-05-server-cache-implementation-audit.md`
- `docs/audits/2026-08-05-integration-config-composition-audit.md`

---

## 1. Overall Assessment

| Layer | Crate(s) | Grade | Key Verdict |
|---|---|---|---|
| EC & Acceleration | `oceanfs-ec`, `oceanfs-accel`, `oceanfs-hash` | **Solid** (1 critical, 1 high) | Cauchy RS, all 3 tiers (CPU/ISA-L/CUDA), BLAKE3 all functional. One dead stub (`IsalEncoder` in ec crate). GPU cooldown incomplete. |
| Distributed Systems | `oceanfs-routing`, `oceanfs-membership`, `oceanfs-network` | **Functional** (0 critical, 3 high) | PR1-PR6 all verified fixed. Ring, gossip, failure detection work. SWIM gRPC pings never sent. Graceful leave is a stub. |
| Server, API & Cache | `oceanfs-server`, `oceanfs-cache` | **Production-ready single-node** (2 critical, 7 high) | S3 API, cache cascade, write/read coordination all functional. Read repair broken (same-HLC compare). EC decode dead code. Hinted handoff delivery not wired. |
| Storage & Durability | `oceanfs-storage`, `oceanfs-durability` | **Split architecture** (5 critical, 8 high) | Two parallel write paths: segment pipeline (dead code, 10 files) vs BlobStore flat files (operational). GC/scrub/AE/heal functional but operate on empty segments CF. WAL crash recovery not wired. |
| Integration & Config | `oceanfs-node`, `oceanfs`, `oceanfs-core` | **Partially broken** (1 critical, 5 high) | Config TOML silently drops 16+ fields — root cause of smoke test deviations. Background tasks are dormant placeholders (gossip = `pending`, FD = sleep). BufferPool/SegmentSealer constructed but unused. MetricsRegistry empty. |

**Total across 5 audits:** 9 critical, 24 high, 33 medium, 28 low.

---

## 2. The Dependency Chain — What Blocks What

Some gaps cascade. Fixing them in the right order matters.

```
FIX FIRST: Config TOML merge bug (C1-integration)
    │  Unblocks: all e2e smoke tests with shortened intervals
    │  Unblocks: Phase 2 load tests (configurable GC/AE/scrub intervals)
    │
    ├─► FIX SECOND: MetricsRegistry (gauge + labels + wiring)
    │       Unblocks: Phase 1-4 load test assertions
    │
    ├─► FIX THIRD: Write path unification
    │       Unblocks: GC on real segments, scrub on real data, AE on real data
    │
    ├─► FIX FOURTH: Correctness gaps
    │       Read repair, EC decode integration, hinted handoff delivery,
    │       WAL crash recovery, graceful leave
    │
    └─► FIX FIFTH: Background task wiring
            Gossip/FD tasks dormant → remove or wire
            Prefetch not triggered
            BufferPool/SegmentSealer unused
```

---

## 3. Priority 1 — Config TOML Merge Bug (Critical, ~30 min fix)

**Finding C1-integration:** `merge_config()` in `crates/oceanfs/src/config.rs:100-119` only copies 6 fields (node_id, data_dir, listen_addr, grpc_listen_addr, seed_nodes, log_level) from the TOML file. All maintenance intervals, timing configs, feature toggles, and body size limits are parsed from TOML by serde but **silently discarded**.

**Impact:** This is the root cause of e2e smoke test deviations D2, D3, D4 (claimed "hardcoded intervals"). The fields exist in `NodeConfig` and `node.rs` reads them correctly — they just never get populated from the file.

**Fix:** Replace the sentinel-value merge logic with `*target = source.clone()` and re-apply CLI overrides. Or add the missing 16+ field merges. Also add env var support for key intervals.

**Verification:** After fix, `cargo test -p e2e -- garbage_collection` with a shortened-interval config should exercise real GC cycling.

---

## 4. Priority 2 — Metrics Infrastructure (Critical, 2-3 sessions)

Confirmed by all 5 auditors: **0 production metrics at `/admin/metrics`.** The `MetricsRegistry` works but is never populated. Subsystems track 25 internal counters that are never exposed.

**Required registry fixes (Phase A from metrics doc):**
1. Add `Gauge` type (C9-metrics-audit)
2. Add label support to Counter/Gauge/Histogram (C10-metrics-audit)
3. Convert Histogram to per-bucket `AtomicU64` (M6-metrics-audit, M7-metrics-audit)
4. Store `Arc<MetricsRegistry>` on `Node` struct (M3-metrics-audit)

**Required wiring (Phase B from metrics doc):**
- Cache hits/misses per tier (data exists — 7 AtomicU64 fields)
- HealStats (data exists — 4 AtomicU64 fields)
- AccelMetrics + fallback counters (data exists — 9 AtomicU64 fields)
- Process memory/FDs (new: read `/proc/self`)

**Verification:** `curl localhost:9000/admin/metrics` returns non-empty Prometheus text with 18+ metrics.

---

## 5. Priority 3 — Write Path Unification (Critical, 3-4 sessions)

**Finding C1-storage:** The segment pipeline (`oceanfs-storage/src/segment/` — 10 files, 20+ `#[allow(dead_code)]` annotations) is entirely dead code. The production S3 handler uses `BlobStore` flat files + `InMemorySegmentReader`, which never creates `SegmentMetadata` entries. This means `put_segment()` is never called, the `segments` CF is empty, and GC/scrub/anti-entropy/heal operate on zero segments.

There are two paths forward:

**Option A: Wire the segment pipeline into the S3 handler.** Replace `BlobStore` with `SegmentPool` → `ActiveSegment` → `SegmentSealer`. This is the spec-intended architecture. Effort: high (touches the critical write path).

**Option B: Remove the dead segment code.** Keep the `BlobStore` write path and simplify. GC/scrub/AE/heal work on segment-like abstractions over `BlobStore`. Effort: medium (delete 10 files, adapt durability crates).

**Recommendation:** Option A. The segment pipeline is spec'd, has extensive tests, and the tiered sizing/pipeline-parallelism/EC-batching architecture depends on it. `BlobStore` was a temporary shortcut.

**Verification:** After fix, `PUT /bucket/key` creates `SegmentMetadata` entries. `GET /admin/segments` returns non-zero counts. GC compacts real segments. Scrub verifies real data.

---

## 6. Priority 4 — Correctness Gaps (Critical, 3-5 sessions)

These are functional bugs that cause data loss, incorrect reads, or cluster test failures.

| # | Gap | Audit Source | Impact | Effort |
|---|---|---|---|---|
| 4.1 | **WAL crash recovery not wired.** `WalReader::open()`/`replay()` never called at startup. Causes e2e deviation D6 (GET after crash → 500). | C4-storage | Data loss on crash | Medium |
| 4.2 | **Read repair does nothing.** `schedule_repair(meta.hlc, meta.hlc, ...)` — same HLC compared. No multi-replica fetch. | C1-server | Stale reads after node writes | Medium |
| 4.3 | **EC decode is dead code.** `decode_ec_shards()` exists (`#[allow(dead_code)]`) but never called. Reads needing parity shards fail. | C2-server | Cannot tolerate failures during reads | High |
| 4.4 | **Hinted handoff delivery not wired.** Writes buffered during failure but never pushed when node returns. Causes e2e T21 failure. | C5-storage, H5-server | Data loss on temporary failures | Medium |
| 4.5 | **Graceful leave is a stub.** `leave()` is a 100ms sleep. No WAL handoff, no shard streaming. | H2-distributed | Data loss on planned decommission | High |
| 4.6 | **T45: concurrent writes to same key.** No multi-replica HLC comparison in ReadCoordinator. | H4-server | Split-brain on concurrent writes | Medium |
| 4.7 | **T43: crash recovery + rejoin fails.** `Cluster::restart()` assigns new ports. | H3-distributed, H6-server | Harness limitation | Low (harness fix) |

---

## 7. Priority 5 — Background Task Wiring & Dead Code Cleanup (Medium, 2-3 sessions)

| # | Gap | Audit Source | Impact | Action |
|---|---|---|---|---|
| 5.1 | **Gossip background task is `std::future::pending`.** | H1-integration | Dormant forever | Remove it — `Membership::start()` already spawns the real gossip protocol. The node-level task is redundant. |
| 5.2 | **Failure detector background task is 1-second sleep.** | H2-integration | Dead weight | Remove it — `Membership::start()` already spawns the real failure detector. |
| 5.3 | **Prefetch background task is a 60-second sleep loop.** | H5-integration | Prefetch never triggers background cycles | Wire `PrefetchEngine` internal worker or remove the node-level task. |
| 5.4 | **BufferPool and SegmentSealer constructed as `_unused`.** | H3-integration, C2-storage | Wasted resources | Wire them into the write path as part of Priority 3. |
| 5.5 | **Dead stub `IsalEncoder` in `oceanfs-ec`.** | C1-accel | Misleads consumers | Remove `oceanfs-ec/src/isal.rs`. Real ISA-L backend is in `oceanfs-accel`. |
| 5.6 | **Crate-level `#![allow(dead_code)]`** in `oceanfs-membership` and `oceanfs-network`. | L1-distributed, L2-integration | Hides dead code | Remove and use targeted `#[allow(dead_code)]` on specific items. |

---

## 8. Accepted Deviations Revisited

The e2e smoke test deviations in `broad-smoke-tests/feature.md` were accepted based on outdated information. The audits reveal:

| Deviation | Original Claim | Audit Finding | Action |
|---|---|---|---|
| D1 | Segment metadata not created in write path | **Confirmed.** C3-storage: `put_segment()` never called, segments CF empty. | Fix via Priority 3 |
| D2 | GC interval hardcoded at 3600s | **Outdated.** `NodeConfig` HAS `gc_interval_sec` and `tombstone_ttl_sec`. The TOML merge bug (C1-integration) prevents them from being set from config files. | Fix via Priority 1 |
| D3 | Orphan reaper depends on GC | Same as D2 — config merge bug, not hardcoded. | Fix via Priority 1 |
| D4 | AE interval hardcoded at 300s | Same as D2 — config merge bug. `NodeConfig` HAS `ae_interval_sec`. | Fix via Priority 1 |
| D6 | WAL crash recovery returns 500 | **Confirmed.** C4-storage: `WalReader` never called at startup. | Fix via Priority 4.1 |
| D7 | Prefetch L2 entry_count increase deferred | **Confirmed.** H5-integration: prefetch background task is a 60s sleep. Separate from LIST-triggered prefetch. | Fix via Priority 5.3 |
| D8 | 2MB body size limit | **Confirmed.** M1-integration: `max_body_size` not in `NodeConfig` — also hits the TOML merge bug. | Fix via Priority 1 (add field + merge) |

**Net new finding:** 5 of 9 accepted deviations (D2, D3, D4, and partially D8) are caused by the single config merge bug (C1-integration), not by missing config fields. Fixing Priority 1 resolves them immediately.

---

## 9. Summary by Impact

### What's Actually Working Well

- **S3 API single-node:** PUT/GET/HEAD/DELETE + bucket CRUD, inline storage, all 4 blob size tiers
- **Cache cascade:** L1/L2/L3 fully wired with DashMap-based LRU/TTL, cache invalidation fan-out
- **Write coordination:** Quorum writes, HLC timestamps, replica fanout, forwarding
- **Read coordination:** Metadata lookup, inline serving, multi-chunk assembly, BLAKE3 streaming
- **All 5 gRPC services:** Segment, Gossip, Healing, Cache, Scrub — implemented, registered, functional
- **DHT Ring:** Consistent hashing, ArcSwap-based RingCache, rebalance on node join/leave
- **SWIM failure detection:** State machine works via gossip-push-as-ping-proxy (DK-007)
- **Gossip protocol:** Push-pull with delta propagation, verified working (PR1-PR5 fixed)
- **Cauchy RS + 3 acceleration tiers:** All functional with proptest round-trips passing
- **AWS SigV4 auth:** 410 lines, 11 tests, config-driven
- **39/43 cluster E2E tests pass (91%)**
- **Crate DAG clean:** No circular dependencies. `oceanfs-core` purity check passes.

### What Blocks Load Testing

| Blocker | Blocks Phase | Fix Priority |
|---|---|---|
| Config TOML merge bug | 2, 3, 4 | 1 |
| Zero production metrics | 2, 3, 4, 5 | 2 |
| WAL crash recovery not wired | 2 (post-crash verification) | 4.1 |
| EC decode dead code | 4 (degraded mode reads from parity) | 4.3 |
| Hinted handoff delivery not wired | 3, 4 (churn verification) | 4.4 |
| Write path doesn't create segment metadata | 3 (segment assertions) | 3 |
| Gossip/FD background tasks dormant | 3 (gossip doesn't run at node level) | 5.1/5.2 |

### Suggested Sprint Sequence

| Sprint | Focus | Deliverables |
|---|---|---|
| **Sprint A** (1-2 days) | Config fix + registry basics | Fixed `merge_config`, Gauge type, empty registry wired to Node |
| **Sprint B** (2-3 days) | Wire existing metrics | 25 internal counters → `/admin/metrics`, process metrics, RocksDB metrics. **Phase 1 load tests become runnable.** |
| **Sprint C** (3-4 days) | Write path unification | Segment pipeline wired to S3 handler. Segment metadata created on write. GC/scrub/AE operate on real segments. **Phase 2 load tests become meaningful.** |
| **Sprint D** (3-5 days) | Correctness fixes | WAL recovery, read repair, EC decode, hinted handoff delivery, graceful leave. **Phase 3-4 load tests become runnable.** |
| **Sprint E** (1-2 days) | Dead code cleanup | Remove dormant background tasks, dead IsalEncoder stub, crate-level dead_code allows. |
