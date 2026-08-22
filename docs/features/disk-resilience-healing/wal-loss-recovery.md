---
feature: "WAL-Pool Loss Recovery (Catch-up from Replicas)"
epic: "disk-resilience-healing"
status: proposed
priority: high
owner: ""
dependencies: ["loss-announcement", "re-replication-worker"]
adr: [0029]
perf: [1.3, 7.1]
created: 2026-08-22
updated: 2026-08-22
---

# WAL-Pool Loss Recovery (Catch-up from Replicas)

## Summary

ADR-0029 §D7's first durability-critical path: when the **wal** pool dies,
the node rejects new writes (write_degraded, g2) but keeps serving reads.
When the pool is replaced/remounted, the node must NOT trust the old WAL
contents (empty/replaced device): it starts a fresh data WAL + event WAL,
then **catches up from replicas** — re-fetching every segment it should
hold but lost with the WAL. The objects CF (metadata pool, intact) is the
enumeration source: objects reference segments via chunk refs; any segment
referenced locally but missing on disk must be re-fetched. Writes resume
only after catch-up completes + the fresh WAL is verified.

## Scope

### In Scope

- `oceanfs-node` recovery flow (runs at startup when the wal pool was
  replaced, and on live remount via the g2 recovery path):
  - **Fresh-WAL boot (pinned)**: if the wal pool's root is empty or its
    WAL files fail CRC validation (the torn-write discipline from
    `wal/reader.rs` replay, node.rs:1159-1181 recovery), the node opens a
    fresh data WAL (`WalWriter::open` on the empty dir, node.rs:558-564)
    and a fresh event WAL (node.rs:696-707) — the old files are NOT
    replayed or trusted.
  - **Registry rebuild (pinned)**: the event-WAL (and with it the
    lifecycle registry) died with the wal pool. Rebuild the registry by
    scanning each data pool root for `{segment_id}.dat` files:
    `SegmentMetadata{ segment_id, pool_id: <the root's pool>, sealed_at:
    file_mtime, ... }` — the pool root IS the pool_id (Phase A f5); a
    segment file's header (segment_flush.rs) provides the metadata
    essentials. This MUST run before GC/orphan-reaper start (otherwise
    the reaper deletes rebuilt segments as orphans).
  - **Missing-segment enumeration (pinned)**: iterate the objects CF
    (metadata pool, intact — `RocksDbMetadataStore`, store.rs:201+) and
    collect every `ChunkRef` segment_id; a segment is MISSING iff no
    `{segment_id}.dat` exists in ANY data pool root (checked after the
    registry rebuild). Unsealed segments that never reached a `.dat` (the
    WAL held them) are missing by construction — their data must come
    from replicas.
  - **Catch-up execution**: feed the missing set into the ReRepWorker
    (g5) — fetch from holders (`storage_locations − self`, the fetch
    primitive heal/worker.rs:431-515), write through the pool-aware store
    (f5), update `storage_locations` (g5).
  - **Write resume gate**: the node's `write_degraded` flag clears only
    when (a) the missing-segment set is empty AND (b) the fresh WAL has
    completed a verification write (one write+fsync+read-back through
    `WalWriter`). Until then the S3 write path 503s (g6 enforces).
  - **Hint WAL note**: hint debt lives on the hints pool (Phase A f4) —
    if THAT pool is also lost, hints are delivery intent only; the
    reconciliation loop (g4) rebuilds any debt (ADR-0029 §D7).
- Metrics: `oceanfs_wal_recovery_missing_segments` (gauge),
  `oceanfs_wal_recovery_caught_up_total`, `oceanfs_wal_recovery_seconds`.
- Tests:
  - unit: missing-segment enumeration (objects CF refs vs local .dat
    files, intersection correct; segments present locally excluded);
  - unit: write-resume gate (flag clears only after empty set + WAL
    verification write);
  - integration (local 3-node): kill the wal pool on A → writes 503,
    reads OK → replace with a fresh empty root → node catches up (all
    referenced segments present), writes resume, cluster data intact.

### Out of Scope

- Metadata-pool loss recovery (g8) — different store, different flow.
- Announcement/reconciliation/healing machinery (g3-g5) — reused, not built.
- Drain/rebalance (Phase C).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | Recovery flow: fresh-WAL boot, missing-segment enumeration, write-resume gate |
| `oceanfs-durability` | (reuses g5's ReRepWorker + fetch primitive) |

## Interface (Public API)

- `pub fn enumerate_missing_segments(objects: &MetadataStore,
  registry: &LifecycleRegistry, store: &SegmentStore) -> Vec<SegmentId>`
  — the enumeration (node-side helper).
- `pub struct WalRecoveryOutcome { missing: Vec<SegmentId>, caught_up: usize,
  verified: bool }`.

## Data Flow

```
wal pool Dead (g2) ──▶ write_degraded = true ──▶ S3 writes 503 (g6)
replacement/remount ──▶ fresh data WAL + event WAL (no trust of old files)
   └─ enumerate_missing_segments(objects CF × registry × .dat)
        └─ RepairRequest set ──▶ ReRepWorker (g5) fetch+write+locations
   └─ empty set ∧ WAL verify write ──▶ write_degraded = false ──▶ writes resume
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-node`
- [ ] **Tests:** all listed green (enumeration, resume gate, 3-node
      integration)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D7 (WAL loss recoverable by design: fresh WAL +
      catch-up from replicas, writes resume when caught up + verified)
      satisfied
- [ ] **Perf:** 1.3 (enumeration pre-sized vec), 7.1 (enumeration is a
      one-time recovery scan — no hot-path change)
- [ ] **Integration:** the epic's wal-pool-kill DoD — writes rejected
      during outage, resumed after catch-up, no data loss (verified by
      reading every written key post-recovery)

## Deviations (accepted)

- **Catch-up is objects-CF-driven, not WAL-driven.** The brainstorm
  suggested "catch-up for accepted-but-uncheckpointed data" from the WAL
  itself; the WAL is gone by definition. The objects CF (on the intact
  metadata pool) is the only surviving record of what the node holds —
  the code-grounding correction from the Phase-B read (WalEntry carries
  no object key, wal/entry.rs:52-79).
