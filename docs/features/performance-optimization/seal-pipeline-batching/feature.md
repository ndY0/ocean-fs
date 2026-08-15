---
feature: "Seal Pipeline Batching — Segment Fsync Group Commit & Metadata Write Batching"
epic: "performance-optimization"
status: done
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
updated: 2026-08-15
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

### Design A — segment fsync group commit (implemented)

1. New flush module in oceanfs-storage (`io/segment_flush.rs`): a
   segment-flush coordinator (`SegmentFlushGroup`), mirroring the WAL's
   `WalSyncGroup` but per-file: registrations carry the open temp file
   handle, the final name, the finalize op (`O_TMPFILE` link vs
   rename), and the segment metadata.
2. The sealer's write path splits into write (temp file, no fsync, on
   the blocking pool) + flush (batched): `seal_from_data` writes the
   temp file via `spawn_blocking`, then registers with the coordinator
   and awaits the completion signal.
3. A dedicated flusher collects registrations within a configurable
   window (`fsync_batch_timeout_ms`, userland-configurable via
   `NodeConfig`, default 10 ms) or until `fsync_max_waiters` (default
   8) are pending, then on the blocking pool: kicks write-back for all
   files (`sync_file_range(WRITE)`), issues one `fdatasync` per file
   (files still synced individually — the win is amortizing the
   barrier/queue cost across the burst and moving the fsync off the
   runtime worker threads), finalizes each file (link/rename — the
   file is never visible before its sync, preserving the O_TMPFILE
   atomicity contract), and persists all metadata in ONE RocksDB
   `WriteBatch`. Then it wakes all waiters.
4. Instrumentation: `FlushStats` counters (`fsyncs_total`,
   `batches_total`, `metadata_batches_total`) registered via
   `SegmentSealer::register_metrics`, plus a cfg(test) thread-pin seam
   (`LAST_FLUSH_THREAD`) and a cfg(test) sync-failure seam
   (`FAIL_SYNC`).

### Design B — metadata write batching (implemented)

1. `put_segment` for every seal in a flush cycle is accumulated into
   one RocksDB `WriteBatch` (`batch_write`, store.rs:780) flushed per
   drain cycle — reusing the existing `batch_write` API unchanged.
2. **Crash-window semantics (resolved):** removal waits for the batch
   flush, INSIDE `seal_from_data` — `seal_from_data` returns `Ok` only
   after the segment file is synced+finalized AND its metadata is in
   the batch flush. ADR-0021's letter is preserved: the seal worker
   removes the sealing-data entry only after `seal_from_data` returns
   `Ok`, and the recycle path (`try_into_mut` + `release_buffer`) is
   untouched. A crash before the flush loses the metadata entry — the
   same end-state as the pre-existing ack-before-seal window, and WAL
   replay re-seals from the WAL entries (unchanged recovery path).
3. The sealing-data read window (ADR-0021) is unaffected: reads hit
   the in-memory `sealing_data` set until removal.

### Out of Scope

- No change to the WAL group commit itself (already batched).
- No change to seal-queue enqueue semantics: the production path is
  the deadline-bounded async enqueue (`finish_seal_handoff_async`,
  never drops); the sync `try_send` path remains for tests.
- No EC/streaming changes.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New `io/segment_flush.rs` (flush coordinator + stats + seams); `segment/sealer.rs` write/flush split, `spawn_blocking` temp write, Direct-arm fsync (F5 — the O_DIRECT arm previously never synced: `File::flush()` is a no-op) and zero-copy direct write (`write_segment_temp`: in-place padding — no double copy, ONE aligned buffer for O_DIRECT, `SegmentFileParts` header/data/parity/index written directly from source slices; `write_all(&data)` zero-copy on the buffered path), `SealConfig` gains `fsync_batch_timeout_ms`/`fsync_max_waiters` + `Default`; `io/atomic_write.rs` split into `create_temp`/`finalize_temp` primitives; `wal/sync.rs` exposes `sync_file_range_write` kick. |
| `oceanfs-core` | `NodeConfig` gains `seal_fsync_batch_timeout_ms` (10) + `seal_fsync_max_waiters` (8) — userland-configurable (TOML), not build time. |
| `oceanfs-server` | Originally verify-only; two review-driven changes landed: `MerkleTree::build` moved to `spawn_blocking` in the coordinator seal worker (`write/coordinator.rs`, graceful fallback) and gRPC `DeleteObject` routed through the `AsyncMetadataOps` adapter (`grpc/segment_service.rs`) so it never blocks a runtime worker. Coordinator contract otherwise unchanged. |
| `oceanfs-node` | Composition root passes the config knobs into `SealConfig` (defaults only). |
| `benches/` | New `seal_pipeline_benchmark.rs` (criterion, before/after throughput instructions). |

## Definition of Done

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` on touched crates,
      `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean
- [x] **Tests:** `cargo test -p oceanfs-storage -- --test-threads=1` and
      `cargo test -p oceanfs-server --lib -- --test-threads=1` green
      (PIPELINE.md §4.6 RocksDB SIGABRT caveat)
- [x] **Tests:** all N syncs issued by the flusher task (thread-pin via
      `LAST_FLUSH_THREAD`, mirroring `LAST_ENCODE_THREAD`), waiters
      woken in ≤ ceil(N / max_waiters) waves (`batches_total`
      counter), per-seal wait bounded by window + one barrier —
      `concurrent_seals_group_commit_fsyncs_and_batch_metadata`
      (sealer.rs) and `group_commit_batches_concurrent_registrations`
      (segment_flush.rs). N fsync syscalls is the floor (one per file);
      the count is not batchable, the barrier/queue cost is.
- [x] **Tests:** metadata batch test — N seals produce ≤ ceil(N /
      batch_size) RocksDB writes (`metadata_batches_total` ≤ 2 for 16
      seals with max_waiters=8)
- [x] **Tests:** `wal_truncation_after_seal` and `segment_roundtrip`
      integration suites still pass (oceanfs-storage/tests/), plus
      `streaming_ec_encode` and `disk_segment_reader`
- [x] **Integration:** seed-42 30 s + 3 random seeds load tests PASS with
      `manifest_integrity` 0 mismatches and the puts_5xx log-gate clean
      (no `no appending segment available in pool`)
<!-- REVIEW: independently re-run by reviewer: LOAD_TEST_SEED=42/7/1234/987654 at 30 s each + seed-42 at 120 s, all PASS (e2e/tests/load_concurrency.rs gates: manifest_integrity 0, zero_4xx_puts, logs_clean; node logs captured by the harness contained none of the four gate patterns). -->
- [x] **Perf:** 120 s run — RSS stable, demonstrated by quantitative
      before/after comparison (seed-42 120 s, 2 s sampling):
      baseline (pre-feature HEAD binary via `OCEANFS_BIN`) min=459160
      p50=957132 p90=1214964 max=1294056 avg=909411 kB vs final build
      min=204620 p50=861744 p90=1433508 max=1556972 avg=933075 kB —
      avg +2.6%, p50 −10%, sawtooth with repeated drops, no monotonic
      growth; the remaining max delta is RocksDB metadata growth
      (VmData near-identical between builds; see ADR-0023 §2)
<!-- REVIEW: FAIL (iter 1, HIGH) — reviewer sampled node RSS every 10 s during a 120 s seed-42 load run: 0.98→1.18→1.12→1.12→1.59→1.49→1.38→1.71→1.99→1.97→1.82→1.60 GB; swings ~2× and no before-baseline was recorded. CLOSED: the quantitative before/after comparison above (2 s sampling, OCEANFS_BIN baseline vs final) satisfies the pass condition — bounded band with no monotonic growth; remaining max delta attributable to RocksDB metadata growth (VmData near-identical). No leaks (RSS falls back at run end); swing partly RocksDB block-cache/memtable growth (mlock is MCL_CURRENT-only). -->
- [x] **Bench:** a criterion bench of the seal path (throughput
      before/after) scaffolded under `benches/`
      (`benches/seal_pipeline_benchmark.rs`, registered in
      `benches/Cargo.toml`) with instructions in the bench file for a
      later baseline run

## Review & Accepted Deviations

Review history: iteration 1 returned FAIL — one HIGH gap (Perf/RSS DoD item
not evidenced) plus MEDIUM/LOW gaps; every gap was addressed and verified by
the implementer. Iteration 2 was accepted by the user directly (the reviewer
agent stalled); the user accepted on their own confidence after confirming
all iteration-1 gaps were fixed.

- **DoD fsync-count test revision (user-approved, already reflected in
  Design A).** N fsync syscalls is the floor (one per file) — the count
  itself is not batchable; the testable claims are the flusher-thread pin
  (`LAST_FLUSH_THREAD`), ≤ ceil(N / max_waiters) wake waves
  (`batches_total` counter), and ≤ ceil(N / 8) metadata batches
  (`metadata_batches_total` counter). The "one fsync per segment" figure in
  Evidence/Motivation is the per-seal cost being amortized, not a batchable
  quantity.
- **Iteration-1 gaps fixed:**
  - (a) Direct-mode double copy eliminated — in-place padding plus
    zero-copy file write via `SegmentFileParts` (header/data/parity/index
    written directly from source slices; `write_all(&data)` zero-copy on
    the buffered path; ONE aligned buffer for O_DIRECT) — sealer.rs
    `write_segment_temp`.
  - (b) `MerkleTree::build` moved off the runtime thread to
    `spawn_blocking` in the coordinator seal worker (write/coordinator.rs)
    with graceful fallback.
  - (c) Temp-file cleanup on failed seals (sealer error path + flush
    coordinator sync-failure path + `FAIL_SYNC` test seam).
  - (d) gRPC `DeleteObject` routed through the `AsyncMetadataOps` adapter
    (grpc/segment_service.rs) so it never blocks a runtime worker.
- **mlock fix — accepted cross-cutting bugfix (see the
  advanced-io-optimizations note; ADR-0023 §4 records the incident).**
  `mlockall(MCL_CURRENT|MCL_FUTURE)` → `mlockall(MCL_CURRENT)` with a
  `getrlimit` pre-check in metadata/store.rs: `MCL_FUTURE` made every
  subsequent allocation count against `RLIMIT_MEMLOCK`, and once the
  ceiling was crossed all allocations failed with `EAGAIN`, aborting the
  whole node via `handle_alloc_error`. Regression test:
  `crates/oceanfs-storage/tests/mlock_no_future_cap.rs`.
- **Perf/RSS evidence (closes the iteration-1 HIGH gap).** Recorded in the
  Perf DoD item above: baseline (pre-feature HEAD via `OCEANFS_BIN`,
  seed-42 120 s, 2 s sampling) vs final build — avg +2.6%, p50 −10%,
  sawtooth with repeated drops, no monotonic growth; remaining max delta is
  RocksDB metadata growth (VmData near-identical between builds; ADR-0023
  §2 attributes the growth to the RocksDB dependency, not the seal path).
- **Iteration 2 acceptance:** the reviewer agent stalled; the user accepted
  the iteration on their own confidence after the implementer addressed
  every iteration-1 gap (verified fixed before acceptance).

## Open Questions

1. **Resolved.** Flush window default = 10 ms (userland-configurable,
   not build time): `NodeConfig.seal_fsync_batch_timeout_ms` + early
   flush at `seal_fsync_max_waiters` (default 8, matching
   `max_inflight_encodes`). At the measured ~16 fills/s a 5 ms window
   rarely batches; the win concentrates in bursts, and the config knob
   lets ops tune it per workload.
2. **Resolved.** `remove_seal_buffer` does NOT proceed independently —
   removal (and the recycle) waits for the batch flush inside
   `seal_from_data` (ADR-0021-literal; see Design B §2).
3. **Resolved.** No ordering hazard with WAL rotation: the metadata
   flush completes before `seal_from_data` returns, WAL rotation is
   independent and later, and sealed segments' WAL entries are cleaned
   at rotation as before (untouched).
