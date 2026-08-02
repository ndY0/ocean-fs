---
audit_date: 2026-08-02
scope: targeted
target_crates: oceanfs-core, oceanfs-server, oceanfs-storage
severity_counts:
  critical: 0
  high: 3
  medium: 5
  low: 2
---

# Audit Report: Phase 4 Implementation Status Verification

## Summary

This audit cross-references the **declared statuses** of five Phase 4 features
against their **actual implementation** in the codebase. One feature (HLC
Versioning) is genuinely complete. The four `in_progress` features have
scaffolding and partial logic in place, but each has at least one major gap
that would prevent the feature from functioning in a real multi-node cluster.
Status labels are all **accurate** — no feature needs promotion to `done`, and
no feature has regressed from its declared status.

---

## Findings

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `crates/oceanfs-server/src/write/replication.rs:60-82` | `replicate_to_single` returns a **simulated ack** (`WriteAck { wal_position: 0, ... }`) without performing a real gRPC call. The write coordinator counts these simulated acks toward quorum, so writes "succeed" without actual replication to remote nodes. | Implement real gRPC `AppendSegment` calls via `ConnectionPool`. This is the single largest gap in Phase 4 — without it, quorum writes are not truly distributed. |
| H2 | `crates/oceanfs-server/src/read_coordinator.rs:335-342` | `fetch_chunks` uses a **sequential fetch loop**, not `FuturesUnordered` parallel fan-out as specified. There is no fastest-k cancelation logic. Reads will block on the slowest replica. | Replace sequential `for` loop in `fetch_chunks` with `FuturesUnordered` fan-out and `tokio::select!` for fastest-k. This is the core performance promise of the read coordinator. |
| H3 | `crates/oceanfs-server/src/hinted_handoff.rs:209-213` | `deliver_single` is a **no-op stub** — always returns `Ok(())` without pushing any data to the remote node. Hints accumulate in memory but are never actually delivered. | Implement real gRPC hint delivery using `ConnectionPool`. Without this, hinted handoff silently loses data on node return. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `crates/oceanfs-server/src/write_coordinator.rs:117-139` | Non-local writes return `Error::ForwardFailed` with message "gRPC forwarding not yet implemented — use local-only mode". The coordinator cannot forward writes to remote nodes at all. | Implement gRPC forwarding path. This is related to H1 but distinct: H1 is about replicating to successors after local write; M1 is about forwarding when the local node is not in the replica set. |
| M2 | `crates/oceanfs-server/src/hinted_handoff.rs:65` | Hint storage uses **unbounded in-memory `RwLock<HashMap>`** with no capacity limit, no RocksDB column family, and no durability. A node restart loses all pending hints. | Implement RocksDB `hints` column family or bounded ring buffer with WAL-backed persistence. Empty hints on restart is a data-loss risk. |
| M3 | `crates/oceanfs-storage/src/segment/pool.rs:262-265` | `enqueue_encoding` uses `try_send` on the encode channel, meaning **backpressure is not enforced** — writes continue even when the encoding queue is full. The test comment in the feature doc confirms: "try_send makes it non-blocking." | Change to `send().await` with a bounded timeout, or return backpressure error to the caller. The spec requires writes to block when the queue is full. |
| M4 | `crates/oceanfs-server/src/read_coordinator.rs:339` | Read timeout is **hardcoded as `30_000`** (30s) rather than using `OperationTimeouts::default().metadata_read_ms` or a configurable per-bucket setting. | Use `OperationTimeouts` from `oceanfs-core` for consistency with the write path. |
| M5 | `crates/oceanfs-server/src/read_coordinator.rs:87` | `ConflictResolver` is held and referenced but **never actually called** during reads. The read coordinator does not compare replicas or resolve conflicts. Read repair is completely absent from the implementation. | Implement `read::repair.rs` module as specified in the feature doc. Call `ConflictResolver::resolve()` when R > 1 and replicas return differing HLCs. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | All Phase 4 feature files under `docs/features/phase-4-distributed-read-write/` | Feature docs have **no epic-level `README.md`** summarizing phase status, cross-feature dependencies, or integration test matrix. Each feature file is self-contained but phase-level progress tracking requires reading all 5 files. | Create `docs/features/phase-4-distributed-read-write/README.md` with a status table, integration test matrix, and dependency graph. Follow the pattern recommended in the Phase 2 audit (`docs/audits/2026-08-02-phase-2-status-verification.md`). |
| L2 | `crates/oceanfs-server/src/read_coordinator.rs:163-174` | `ring` and `node_id` fields on `ReadCoordinator` are marked `#[allow(dead_code)]` — they are held but the code that should use them (distributed shard fetch, read repair targeting) is not yet implemented. | Remove `#[allow(dead_code)]` as each field becomes actively used. These annotations are accurate signals that the distributed read path is incomplete. |

---

## Per-Feature Detailed Assessment

### HLC Versioning & Conflict Resolution — Status: `done` ✅ ACCURATE

| DoD Item | Status | Evidence |
|---|---|---|
| `Hlc` type (wall_time + logical, 96-bit) | ✅ Done | `crates/oceanfs-core/src/hlc.rs:26-31` |
| `HlcClock` with `AtomicU64` + `#[repr(align(64))]` | ✅ Done | `crates/oceanfs-core/src/hlc.rs:72-104` |
| `ConflictResolver` trait | ✅ Done | `crates/oceanfs-core/src/conflict.rs:69-76` |
| `LwwResolver` | ✅ Done | `crates/oceanfs-core/src/conflict.rs:100-110` |
| `Resolution` enum | ✅ Done | `crates/oceanfs-core/src/conflict.rs:27-34` |
| Integration: write coordinator stamps writes | ✅ Done | `write_coordinator.rs:142` — `let hlc = self.hlc_clock.now()` |
| HLC monotonicity tests | ✅ Done | 17+ unit tests in `hlc.rs` tests module |
| LWW conflict tests | ✅ Done | 6 tests in `conflict.rs` tests module |
| Integration test `tests/hlc_ordering.rs` | ✅ Done | 5 tests pass |
| Cache-line alignment verified | ✅ Done | `hlc_clock_has_64_byte_alignment` test passes |
| Coverage ≥ 80% | ❌ Not met | `hlc.rs` 80.4%, but `oceanfs-core` overall at 34.21% (bottleneck is `types.rs`) |

**Assessment:** Genuinely complete. The coverage gap is in `oceanfs-core` overall, not in the HLC module itself. The only caveat: HLC is integrated into the write coordinator but not yet used for read-side conflict resolution (see M5).

---

### Write Coordinator & Quorum — Status: `in_progress` ✅ ACCURATE

| Capability | Status | Notes |
|---|---|---|
| Route key via ring | ✅ | `write_coordinator.rs:109` |
| Local append + HLC stamp | ✅ | Lines 141-159 |
| Quorum counting | ✅ | Lines 162-197 — acks counted, quorum verified |
| `tokio::select!` timeout | ❌ | `replicate_write` captures `write_timeout_ms` but discards it (line 41: `let _write_timeout = ...`) |
| Remote replication via gRPC | ❌ | `replicate_to_single` returns simulated `WriteAck { wal_position: 0 }` — **H1** |
| Forward to remote node (non-local) | ❌ | Returns `Error::ForwardFailed` — **M1** |
| `ack_after_wal` / `write_ec_async` flags | 🔶 | Fields exist on `WriteRequest` but are never read in the `put()` method |
| BLAKE3 hashing | ✅ | Line 148 |
| `FuturesUnordered` fan-out | ❌ | `replicate_write` uses sequential `for` loop |

**Assessment:** The local-only write path is functional (quorum=1 with local node in the replica set). The distributed path (gRPC replication, timeouts, forwarding) is scaffolded but non-functional. ~45% complete overall.

---

### Read Coordinator & Parallel Fetch — Status: `in_progress` ✅ ACCURATE

| Capability | Status | Notes |
|---|---|---|
| Metadata-first read | ✅ | `get_object` → `lookup_metadata` |
| Inline data served directly | ✅ | Lines 237-238 |
| Chunk assembly via `MultiChunkAssembler` | ✅ | Lines 345-354 |
| BLAKE3 verification | ✅ | `assemble_chunks` and `get_object` |
| `fetch_chunks` module | ✅ | `crates/oceanfs-server/src/read/fetch.rs` exists |
| `FuturesUnordered` parallel fetch | ❌ | Sequential loop — **H2** |
| Fastest-k cancelation | ❌ | Not implemented — **H2** |
| Read repair (R>1) | ❌ | `conflict_resolver` never called — **M5** |
| EC decode integration | ❌ | `ParallelDecoder` not referenced in read path |
| Segment reader integration | 🔶 | Optional field, defaults to `None` — most reads return empty data |
| `OperationTimeouts` integration | ❌ | Hardcoded 30s — **M4** |

**Assessment:** The read skeleton (types, metadata path, inline reads, classify, chunk assembly) is in place. The distributed aspects (parallel shard fetch, fastest-k, read repair, EC decode) are not implemented. ~35% complete overall.

---

### Pipeline Parallelism & Active Segment Pool — Status: `in_progress` ✅ ACCURATE

| Capability | Status | Notes |
|---|---|---|
| `SegmentPool` struct | ✅ | `pool.rs:92-111` |
| `PoolSlot` + `PoolSlotState` enum (all 4 states) | ✅ | `pool.rs:33-48` |
| `append()` method | ✅ | Routes to next available slot |
| `try_activate_slot()` (idle→appending) | ✅ | `pool.rs:272-285` |
| `enqueue_encoding()` (via mpsc) | ✅ | `pool.rs:252-270` |
| `Semaphore` for in-flight encode bounds | ✅ | Tested: `encode_semaphore_has_correct_permits` |
| `SegmentShard` with hash routing | ✅ | `shard.rs:34-44` |
| Pool rotation (fill→seal→encode cycle) | ❌ | Tests use tiny data on 4MB-target segments; `is_full()` never triggers |
| Backpressure on full encode queue | ❌ | `try_send` non-blocking — **M3** |
| Shard→Pool integration | ❌ | Shard routes to `ActiveSegment` directly, not through `SegmentPool` |
| `BufferPool` integration | ✅ | `try_activate_slot` creates from `BufferPool` |

**Assessment:** The pool structure (slots, states, channels, semaphore) is well-designed and matches the spec. The missing pieces are the actual rotation behavior (fill-then-seal) and proper backpressure. ~50% complete overall.

---

### Hinted Handoff — Status: `in_progress` ✅ ACCURATE

| Capability | Status | Notes |
|---|---|---|
| `HintedHandoff` struct | ✅ | `hinted_handoff.rs:60-69` |
| `handoff()` (store hint) | ✅ | Lines 97-114 |
| `deliver_pending()` (batch delivery) | ✅ | Lines 132-181 |
| `pending_count()` / `total_pending_count()` | ✅ | Lines 187-193 |
| gRPC service stub | ✅ | `healing_service.rs:30` |
| Real delivery via gRPC | ❌ | `deliver_single` is no-op — **H3** |
| RocksDB-backed durable storage | ❌ | In-memory `RwLock<HashMap>` — **M2** |
| Bounded capacity / backpressure | ❌ | Unbounded HashMap — **M2** |
| Retry on failed delivery | ❌ | Errors logged but not retried |
| Duplicate hint prevention | ❌ | Not implemented |
| Integration with write coordinator | ❌ | `put()` never calls `HintedHandoff::handoff()` |
| Timeout enforcement | ❌ | `hint_delivery_ms` captured but discarded |

**Assessment:** The in-memory hint buffer works for the basic create/deliver/clear lifecycle. The gaps are all on the durability and real-delivery side. ~30% complete overall.

---

## Phase 4 Progress Summary

| Feature | Status | Real Completeness | Critical Gap |
|---|---|---|---|
| HLC Versioning | `done` | 90% | Coverage threshold not met |
| Write Coordinator | `in_progress` | 45% | gRPC replication is simulated (H1) |
| Read Coordinator | `in_progress` | 35% | No parallel fetch (H2), no read repair (M5) |
| Pipeline Parallelism | `in_progress` | 50% | Pool rotation not tested (M3) |
| Hinted Handoff | `in_progress` | 30% | Delivery is no-op stub (H3), no durability (M2) |
| **Phase 4 aggregate** | — | **~50%** | All 4 in-progress features share the same root cause: real gRPC operations are stubbed |

## Dependency Graph

The critical path to completing Phase 4 runs through a single bottleneck:

```
gRPC replication (H1)  ←── enables ──→  Write Coordinator quorum
       │                                         │
       │                                    Hinted Handoff delivery (H3)
       │
       └── enables ──→  Read Coordinator parallel fetch (H2)
                               │
                          Read repair (M5)
```

Once `ConnectionPool`-backed gRPC calls work for writes, the same infrastructure
can be used for read shard fetch and hint delivery.

---

## Recommendations

1. **Immediate — implement gRPC write replication.** Unblocking `replicate_to_single` (H1)
   is the single highest-impact action. It directly enables write quorum (the
   coordinator already has quorum-counting logic), and the same gRPC pattern
   can be reused for reads (H2) and hints (H3).

2. **Second — parallel read fetch.** Once gRPC writes work, convert
   `read/fetch.rs` from sequential to `FuturesUnordered` fan-out with
   fastest-k (H2). This is a well-scoped change within a single module.

3. **Third — hint delivery and durability.** Swap the in-memory `HashMap` for
   a RocksDB column family (M2) and implement real `deliver_single` via gRPC
   (H3). Connect `HintedHandoff` to the write coordinator's failure path.

4. **Fourth — pool rotation and backpressure.** Fix `try_send` → `send().await`
   (M3) and add integration tests that trigger the full fill→seal→encode cycle.

5. **Ongoing — create a Phase 4 epic README** (L1) with status table, integration
   test matrix, and dependency tracking for cross-feature integration work.
