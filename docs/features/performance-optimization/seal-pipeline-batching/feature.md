---
feature: "Seal Pipeline Batching — Segment Fsync Group Commit & Metadata Write Batching"
epic: "performance-optimization"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: gap-closure/pool-backpressure-and-buffer-recycling
    reason: Its concurrent (semaphore-bounded, spawned-task) seal worker is the baseline this feature batches over; its recycle path (try_into_mut + release_buffer) must keep working after the write/flush split
adr:
  - 0021-seal-window-data-set
perf:
  - "3.4 Group commit for WAL fsync"
  - "3.1 Sequential-only WAL writes"
  - "2.7 Tokio semaphore for concurrency limits"
created: 2026-08-13
updated: 2026-08-13
---

# Seal Pipeline Batching — Segment Fsync Group Commit & Metadata Write Batching

## Summary

Two batching changes to the per-seal critical path
(`crates/oceanfs-storage/src/segment/sealer.rs` `seal_from_data`, called
from the coordinator's concurrent seal worker): (A) a segment-fsync group
commit that batches `sync_data()` across segment files within a short
window (≤ 5 ms), mirroring the WAL's existing group commit
(`fsync_batch_timeout_ms`, wal/sync.rs); (B) metadata write batching that
accumulates per-seal RocksDB writes (`put_segment`, plus the seal worker's
other per-seal writes) into one RocksDB `WriteBatch` flushed per drain
cycle (or per N seals / T ms) instead of one write per segment. Today each
segment seal costs 1 fsync + 1 RocksDB write + 1 spawned task; under the
seal bursts measured in gap-closure (~16 fills/s), this serializes on the
disk barrier and RocksDB's internal locks.

## Evidence/Motivation

Per-seal cost today (verified):

- `seal_from_data` (sealer.rs:142–248): bincode-serialize the index
  (`SegmentIndex::new` + `to_bytes`, sealer.rs:155, 168) → BLAKE3 the data
  (sealer.rs:158) → build file bytes (sealer.rs:176–179) → `write_atomic`
  (sealer.rs:217) which does `file.sync_data()` (io/atomic_write.rs:115) =
  **one fsync per segment** → one `RocksDbMetadataStore::put_segment`
  (sealer.rs:242–244, a synchronous RocksDB write) → the seal worker then
  does `remove_seal_buffer` + `try_into_mut` recycle (coordinator.rs:672–706).
- Each segment = 1 fsync + 1 RocksDB write + 1 spawned task
  (coordinator.rs:657).
- The WAL already solves the analogous fsync problem: group commit with
  `fsync_batch_timeout_ms` (wal/writer.rs:162 registers with group commit;
  wal/sync.rs flusher batches waiters; default 5 ms in tests, 10 ms in
  wal/writer.rs:429). Perf rule 3.4 documents the rationale: each fsync is
  a 1–10 ms disk barrier.
- The metadata store already has a batch primitive:
  `RocksDbMetadataStore::batch_write(Vec<BatchOp>)`
  (metadata/store.rs:781+) applies a `rocksdb::WriteBatch` in one write.
- WAL interaction: WAL entries for sealed segments are cleaned up at file
  rotation time (sealer.rs:246 comment), not per seal — the batching
  change must not disturb this ordering (see Open Question 3).

## Design & Scope

### Design A — segment fsync group commit

1. New flush module in oceanfs-storage (under `io/` or `wal/`): a
   segment-flush coordinator.
2. The sealer's write path splits into write (async, no `sync_data`) +
   flush (batched): after writing the segment file (and before
   `put_segment`), the seal task registers (file handle, completion
   signal) with the coordinator.
3. A dedicated flusher task collects registrations within a short window
   (e.g. ≤ 5 ms) and performs one `sync_data` per registered file (files
   still synced individually — the win is amortizing the barrier/queue
   cost across the burst), then wakes all waiters in the window.
4. Instrumentation: a test-visible fsync counter in the flush module
   (cfg(test) seam or metrics counter).

### Design B — metadata write batching

1. Accumulate `put_segment` (and the seal worker's other per-seal RocksDB
   writes) into one `WriteBatch` flushed per drain cycle (or per N seals /
   T ms) — reusing/extending `batch_write` (store.rs:781).
2. **Crash-window semantics:** metadata must be durable by the time
   `remove_seal_buffer` runs. Today `remove_seal_buffer` happens right
   after `seal_from_data` returns Ok (coordinator.rs:676–684); with
   batched metadata, decide whether removal waits for the batch flush.
   ADR-0021 scoping — removal only on success — must be kept: never remove
   before the segment is durably on disk AND its metadata persisted; a
   crash before the flush re-seals from WAL replay.
3. The sealing-data read window (ADR-0021) is unaffected: reads hit the
   in-memory `sealing_data` set until removal.

### Out of Scope

- No change to the WAL group commit itself (already batched).
- No change to seal-queue `try_send` semantics (drop-on-full stays).
- No EC/streaming changes.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/sealer.rs`: write/flush split, batched metadata call path. New flush module (e.g. `io/segment_flush.rs` or `wal/`-adjacent). `metadata/store.rs`: batch API extension if needed (flush trigger / `batch_write` variant). Tests incl. fsync-count instrumentation. |
| `oceanfs-server` | `write/coordinator.rs` seal worker: register/batch coordination — completion awaits, and removal waits on the batch flush per the ADR-0021 invariant. |
| `oceanfs-node` | Verify only (composition root unchanged). |

## Definition of Done

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` on touched crates,
      `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean
- [ ] **Tests:** `cargo test -p oceanfs-storage -- --test-threads=1` and
      `cargo test -p oceanfs-server --lib -- --test-threads=1` green
      (PIPELINE.md §4.6 RocksDB SIGABRT caveat)
- [ ] **Tests:** unit test — N concurrent seals produce
      ≤ ceil(N / window) fsyncs (via the flush module's test counter)
- [ ] **Tests:** metadata batch test — N seals produce ≤ N / batch
      RocksDB writes
- [ ] **Tests:** `wal_truncation_after_seal` and `segment_roundtrip`
      integration suites still pass (oceanfs-storage/tests/)
- [ ] **Integration:** seed-42 30 s + 3 random seeds load tests PASS with
      `manifest_integrity` 0 mismatches and the puts_5xx log-gate clean
      (no `no appending segment available in pool`)
- [ ] **Perf:** 120 s run — RSS stable
- [ ] **Bench:** a criterion bench of the seal path (throughput
      before/after) scaffolded under `benches/` (workspace benches/ holds
      storage_benchmark.rs, wal_sync_benchmark.rs et al.) with
      instructions in the bench file for a later baseline run

## Open Questions

1. Flush window size — 5 ms default (mirroring WAL `fsync_batch_timeout_ms`)?
2. Does `remove_seal_buffer` block on the metadata batch flush (durability
   ordering), or does removal proceed and rely on WAL replay if the batch
   is lost?
3. WAL truncation timing: WAL entries are cleaned at file rotation —
   verify no ordering hazard between the batched metadata flush, removal,
   and WAL rotation.
