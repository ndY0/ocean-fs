---
feature: "Read-Path Integrity Under Load — Multi-Tier Chunk Corruption"
epic: "gap-closure"
status: proposed
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
| `oceanfs-server` | `write/coordinator.rs`: Multi arm — chunk ref `offset: seg_offset`; `record_blob_entry` per chunk. New round-trip tests in `#[cfg(test)]`. |

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` workspace-wide
- [ ] **Code:** `cargo clippy --lib -- -D warnings` clean on `oceanfs-server`
- [ ] **Tests:** `cargo test -p oceanfs-server --lib` green incl. the
      two new multi-tier round-trip tests (active + sealed)
- [ ] **Tests:** `LOAD_TEST_SEED=42 LOAD_TEST_DURATION_SECS=30 cargo test
      -p e2e --test load_concurrency` → pass with `manifest_integrity`
      0 mismatches
- [ ] **Tests:** 3× repeat with random seeds — zero flakes
- [ ] **Observability:** node log clean of `BadDigest` / `cannot fetch
      chunk` during a load run
- [ ] **Perf:** 120 s run — RSS stable (no `sealing_data` leak)
- [ ] **Integration:** load report shows `puts_multi > 0` **and** all
      multi-tier objects verified readable

## Open Questions

1. Why did `puts_5xx = 2` occur during the load phase? If they are
   `QuorumNotMet`/`Internal` transient errors at high concurrency,
   record their S3 codes in the report and assess separately — not part
   of this feature.
2. The `sealing_data` map growth under skipped seals: bounded per
   `active_pool_size` but leaked per skipped seal. Confirm the bound
   analysis after Defect 2 is fixed; a follow-up may be needed.
