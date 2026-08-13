---
feature: "Read-Path Integrity Under Load — Multi-Tier Chunk Corruption"
epic: "gap-closure"
status: done
priority: critical
owner: ""
dependencies:
  - epic: refactoring/load-test-harness-fidelity
    reason: The fidelity fixes are what exposed this defect (the test previously passed because 45% of writes were 413-rejected and multi-tier was never stored)
adr:
  - 0001-segment-packing
  - 0004-tiered-segment-sizing
perf:
  - "8.1 FuturesUnordered for parallel shard fetches"
  - "2.7 Tokio semaphore for concurrency limits"
  - "8.5 Bounded semaphore for task concurrency"
created: 2026-08-13
updated: 2026-08-13
---

# Read-Path Integrity Under Load — Multi-Tier Chunk Corruption

## Summary

With the load-test-harness-fidelity fixes landed (16 MiB body limit,
multi-thread harness, fast blob generation), `load_concurrency` now
exercises the system at ~10× the previous volume — and **fails with a
real data-integrity defect**. Post-run manifest verification shows
**176 of 417 written objects unreadable (HTTP 500)**. Two server-side
failure signatures, both rooted in the multi-tier write path:

1. **`BadDigest` — hash verification failed** (113 occurrences in a
   20 s run): chunk reads return wrong bytes.
2. **`cannot fetch chunk — no segment reader and gRPC not available`**
   (36 occurrences): the segment referenced by a chunk ref is
   unreachable from the read path.

This is exactly the bug class Phase 1 exists to catch (load-test-campaign.md
§2: "the cheapest test that catches the most dangerous bugs — data
corruption under concurrent access"), and it was invisible until the
fidelity fixes landed: the old test stored ~20 objects, of which ~zero
were multi-tier (every >2 MiB PUT was 413-rejected), so the corrupting
path never ran.

## Evidence

**30 s run, seed 42 (post-fidelity):**

- 970 ops, 455 PUTs (453 × 200, 2 × 5xx), `puts_multi = 90` successful
  multi-tier writes, `errors_total = 0`, `puts_4xx = 0`.
- Manifest: 417 written, 241 verified, **176 mismatches** — every
  mismatch is `"HTTP 500 Internal Server Error"` (plus 3 unreachable).
- Worker GETs during load: 21 × 200 — read path mostly works *during*
  the run; the failures concentrate in the post-run verification phase.

**20 s node log (captured):** 113 `BadDigest`, 36 `cannot fetch chunk`,
192 `segment sealed successfully`, 0 `seal queue full`.

## Confirmed Defect 1 — Multi-Tier Chunk Refs Store Blob-Relative Offsets

**File:** `crates/oceanfs-server/src/write/coordinator.rs`, `put()`,
`SizeTier::Multi` arm (line ~295).

```rust
for (chunk_offset, chunk_data) in &split_chunks {
    let (seg_id, seg_offset, length) = self.segment_pool_standard.append(chunk_data)?;
    self.write_wal_entry(seg_id, seg_offset, ...).await?;   // ← uses seg_offset ✓
    chunks.push(ChunkRef { segment_id: seg_id, offset: *chunk_offset, length }); // ✗
}
```

`chunk_offset` is the splitter's offset **within the original blob**
(0, 4 MiB, 8 MiB, …). `seg_offset` is where `append()` actually placed
the chunk **within its segment** — the value every reader uses to slice
the segment. The metadata therefore points reads at blob-relative
positions inside segments that were appended at segment-relative
positions:

- Chunk 0 happens to work only when it lands at segment offset 0.
- Every later chunk, and any chunk landing mid-segment, reads the wrong
  byte range → `BadDigest` (when the range happens to fall inside the
  segment) or fetch failure (when it falls outside).

The Small/Standard arms are correct (`offset` from `append()`).

**Fix:** `chunks.push(ChunkRef { segment_id: seg_id, offset: seg_offset, length });`

## Confirmed Defect 2 — Multi-Tier Chunks Never Register a Blob Index Entry

**File:** same arm. The Small and Standard arms call
`self.record_blob_entry(segment_id, offset, length, blake3_hash)` after
each append (lines ~256, ~270) so the seal worker can build the segment
blob index. The Multi arm **never calls `record_blob_entry`**.

Consequence (per `SegmentPool::append` fill path + seal worker in
`write/coordinator.rs:start_seal_worker`): when a segment filled by
multi-tier chunks is dequeued for sealing, `entries` is empty → the
seal worker logs "no index entries for sealed segment; skipping seal"
and `continue`s — the segment **never reaches disk** and its bytes
linger in the pool's `sealing_data` map. The chunk refs in object
metadata point at a segment that the disk reader will never have and
the pool eventually stops holding (slot re-activation), which matches
the `cannot fetch chunk` signature. Even when sealing does proceed
(entries from other blobs), the index misses the multi-tier chunks.

**Fix:** in the Multi arm, call `self.record_blob_entry(seg_id, seg_offset, length, blake3_hash)` after each chunk append, exactly like the
Small/Standard arms.

## Correctness Verification Plan (before marking this closed)

Both fixes are one-liners, but the failure modes are subtle. The
implementer must prove the read path end to end, not just that the test
turns green:

1. **Unit round-trip:** multi-tier PUT of a deterministic payload
   (e.g. 9.5 MiB, chunk sizes forced to cross ≥3 segments) → GET →
   BLAKE3 matches. Run in `oceanfs-server` tests against an in-memory
   pool with a tiny `default_target_size` so every chunk lands at a
   non-zero segment offset.
2. **Sealed-segment read:** same round-trip, but force a seal before
   the GET (fill the segment to trigger rotation, then read) — asserts
   the blob index now contains the chunk (Defect 2 fix).
3. **Load test green:** `LOAD_TEST_SEED=42 LOAD_TEST_DURATION_SECS=30
   cargo test -p e2e --test load_concurrency` → `manifest_integrity`
   passes (0 mismatches), and a 3× repeat with random seeds is flake-free.
4. **Log hygiene:** node log shows zero `BadDigest` and zero
   `cannot fetch chunk` during the run.
5. **Sustained sanity (Phase 2 precursor):** 120 s run — RSS stable
   (guards against the `sealing_data` leak that Defect 2 masked:
   skipped seals never remove their entry).

## Scope Notes

- **In scope:** the two multi-tier defects above and the verification
  plan.
- **Out of scope:** re-architecting the seal path (e.g. making the
  seal queue blocking instead of `try_send`-drop, or making the seal
  worker skip-without-leak). If evidence shows the `sealing_data` leak
  is unbounded under sustained multi-tier load, open a follow-up
  feature rather than expanding this one.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | `write/coordinator.rs`: Defect 1 + 2 fixes (Multi arm — chunk ref `offset: seg_offset`; `record_blob_entry` per chunk), seal-worker concurrency (semaphore-bounded concurrent seals instead of serial `try_send`-drop, `append_with_hook` wiring), round-trip + concurrency tests in `#[cfg(test)]`. `s3_handler/handlers.rs` (+ `s3_handler/mod.rs` test fixture): L3 negative-cache invalidation on PUT. |
| `oceanfs-storage` | `segment/pool.rs`: `append_with_hook` (records the blob-index entry under the segment lock before the fill-triggered seal enqueue) + SegmentFull next-slot retry. `io/segment_reader.rs`: Direct-mode read loop (fills the buffer, errors on short reads) + >2 MiB regression test. `segment/streaming.rs`: out-of-order parity completion fix. |
| `oceanfs-cache` | `l3_negative.rs`: `NegativeCache::invalidate` (bounded overlay, conservative filter reset on overflow). |
| `oceanfs-cache`, `oceanfs-node` | Test files: `tests/cache_behavior.rs` in both crates updated for the new L3 invalidation behavior. |

## Definition of Done

- [x] **Code:** `cargo build --all-targets` workspace-wide
- [x] **Code:** `cargo clippy --lib -- -D warnings` clean on `oceanfs-server`
      (independently also clean on `oceanfs-storage` and `oceanfs-cache`)
- [x] **Tests:** `cargo test -p oceanfs-server --lib` green incl. the
      two new multi-tier round-trip tests (active + sealed) —
      reviewer ran 207/207; both round-trip tests independently
      mutation-verified (re-introducing Defect 1 blob-relative offsets
      or removing the Defect 2 `record_blob_entry` makes them fail)
- [x] **Tests:** `LOAD_TEST_SEED=42 LOAD_TEST_DURATION_SECS=30 cargo test
      -p e2e --test load_concurrency` → pass with `manifest_integrity`
      0 mismatches (reviewer run: 652 written / 652 verified / 0
      mismatches, `puts_multi = 141`, `puts_5xx = 0`)
- [x] **Tests:** 3× repeat with random seeds — zero flakes (reviewer
      runs: seed 42 × 30 s, seed 1337 × 120 s, previously-flaky seed
      5829453283693345810 × 30 s — all PASS with 0 mismatches)
- [x] **Observability:** node log clean of `BadDigest` / `cannot fetch
      chunk` during a load run (reviewer captured debug-level node log
      for a fresh 120 s run: 0 BadDigest / 0 cannot fetch chunk /
      0 skipping seal / 0 seal queue full / 0 segment seal failed /
      0 Direct read short; 1931 segments sealed successfully)
- [x] **Perf:** 120 s run — RSS stable (no `sealing_data` leak)
      (reviewer sampled RSS every 5 s: range ~190–960 MB, spiky with
      load, no monotonic growth; no `sealing_data` leak)
<!-- REVIEW: INFO (out-of-scope per Open Question 1, follow-up recommended): the reviewer's 120 s run showed 6 × PUT 500 `InternalError` "multi tier append: invalid config: no appending segment available in pool" within 11 ms during a write burst (e2e node log 19:54:08.848–859, all 4 standard-pool slots transiently Sealing; `active_pool_size = 4` default at crates/oceanfs-core/src/types/config.rs:245, error return at crates/oceanfs-storage/src/segment/pool.rs:410). The implementer's deviation (e) claims the SegmentFull retry fixed the "2 × 5xx" open question — only partially true: the retry narrows the race but the PUT can still fail when every slot is Sealing. Feature doc Open Question 1 explicitly defers this; recommend a follow-up feature (bounded blocking wait for slot activation or burst-sized pool) rather than expanding this one. -->
- [x] **Integration:** load report shows `puts_multi > 0` **and** all
      multi-tier objects verified readable (reviewer runs:
      `puts_multi` = 141 / 486 / 118 with 0 manifest mismatches)

> The reviewer independently verified all 8 DoD items above (marked `[x]`
> with evidence in-place); those annotations are preserved unchanged.

## Deviations & Scope Expansions (accepted)

Recorded at implementation completion (reviewer PASS, 1st iteration).
All items below were accepted.

### a) Scope expansion — four additional defects fixed beyond the two documented

The DoD's 0-mismatch/flake-free gates could not be met by fixing Defects
1–2 alone. Four further defects were root-caused and fixed in the same
change set:

1. **Direct-mode segment reads truncated at 2 MiB.** `tokio::fs::File`
   caps one read syscall at 2 MiB, and the Direct arm ignored the
   returned read count — silently zero-padding every >2 MiB chunk and
   producing `BadDigest` on multi-tier GETs. Fixed in
   `crates/oceanfs-storage/src/io/segment_reader.rs`: the read now loops
   until the buffer is full and errors on short reads (`Direct read
   short`).
2. **Seal-worker entries race.** On the multi-threaded runtime the seal
   worker drains the blob-index `entries` map before the PUT thread
   records its entry — the original "record before the WAL await"
   assumption only holds on a single-threaded runtime. Fixed via
   `SegmentPool::append_with_hook`, which records the blob-index entry
   under the segment lock *before* the fill-triggered seal enqueue.
3. **Seal-queue overflow under write bursts.** `try_send` returning
   `Full` dropped the sealing-data → the segment never reached disk →
   `cannot fetch chunk`. Fixed by running seals concurrently (spawned
   tasks), bounded by the existing seal semaphore (perf §2.7/8.5),
   instead of serially.
4. **L3 negative-cache stale 404s.** The Bloom filter of deleted keys was
   never cleared on PUT, so delete-then-put keys kept answering
   "definitely absent". Fixed with `NegativeCache::invalidate` (bounded
   overlay, conservative filter reset on overflow), invoked by the PUT
   handler.

### b) SegmentFull PUT race (Open Question 1, "2 × 5xx") — partially addressed

The pool now retries the next available slot when the round-robin
target's segment filled concurrently. Residual: ~0.3% of PUTs in a 120 s
burst run still fail with `no appending segment available in pool` (all
4 standard-pool slots transiently `Sealing` during 4 MiB churn) — the
failure class moved but the rate is unchanged. Per Open Question 1 this
is explicitly deferred; recommend a follow-up feature (bounded blocking
wait for slot activation, or a burst-sized pool).

### c) Verification-plan test-design deviation

"Tiny `default_target_size` so every chunk lands at a non-zero segment
offset" is not achievable through the coordinator as written: the
splitter chunk size equals the segment capacity, so full chunks always
land at offset 0 in a fresh segment. Non-zero offsets are instead
produced deterministically with a single-slot standard pool pre-filled
by a Standard-tier blob: the first multi-tier chunk lands at offset
2048 — the in-bounds silent-corruption case. Both new round-trip tests
were mutation-tested (re-introducing each original defect makes them
fail).

### d) Pre-existing unrelated failure (not caused by this feature)

`cargo test -p oceanfs-cache --doc` fails in
`crates/oceanfs-cache/src/prefetch.rs` (unresolved
`oceanfs_core::MetadataStore` import). That file was last changed in a
prior commit and is untouched by this feature.

### e) Frontmatter ADR citation does not exist (pre-existing repo issue)

The frontmatter cites `0004-tiered-segment-sizing`, but no such ADR
exists in `docs/adr/` — tier sizing is defined in ADR-0001
(segment-packing). The citation is left as-is to preserve document
history; no feature work is affected.

## Open Questions

1. Why did `puts_5xx = 2` occur during the load phase? If they are
   `QuorumNotMet`/`Internal` transient errors at high concurrency,
   record their S3 codes in the report and assess separately — not part
   of this feature.
2. The `sealing_data` map growth under skipped seals: bounded per
   `active_pool_size` but leaked per skipped seal. Confirm the bound
   analysis after Defect 2 is fixed; a follow-up may be needed.
