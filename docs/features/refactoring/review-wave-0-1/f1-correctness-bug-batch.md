---
feature: "f1: Correctness Bug Batch"
epic: "refactoring/review-wave-0-1"
status: in_progress
priority: critical
owner: ""
dependencies:
  - feature: f0-close-stale-comments
    epic: refactoring/review-wave-0-1
    reason: Land on a marker-free tree so the bug fixes are reviewable in isolation
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f1: Correctness Bug Batch

## Summary

Six small, independent, real defects found during the 2026-09 review
triage. None needs a design discussion; each is a correctness hardening
with a regression test. They are bundled so the wave-2 refactors do not
carry them.

## Bugs

### B1 — Fixed 76-byte header slice in leave handler (review #35)
**Location:** `crates/oceanfs-node/src/node.rs` `NodeLeaveHandler::read_segment_data` (~:206-217).
**Bug:** `data[76..]` unconditionally skips a v1 76-byte header, but the
sealer writes v2 **92-byte** headers (`SegmentHeader::with_parity`,
`oceanfs-storage/src/segment/sealer.rs`). The graceful-leave push would
truncate/corrupt v2 segment payloads.
**Fix — DISPOSITION (DECISION 2026-09-04): DEFERRED, closed by c1.**
No in-place fix in this epic. `NodeLeaveHandler` is deleted by
composition-root c1 (review #34) in the next session, and that deletion is
the authoritative close for B1 — the buggy reader (and its `[review]`
marker) is removed with the handler. The dependency is recorded in both
docs (here + `composition-root-decomposition/c1-split-storage-builder.md`).

### B2 — Silent default network address (review #64)
**Location:** `node.rs:1501-1506` — `.with_self_grpc_addr(parse().unwrap_or_else(127.0.0.1:9001))`.
**Bug:** an unparseable `grpc_listen_addr` silently falls back to
`127.0.0.1:9001`, producing an unusable hint-fetch self address.
**Fix:** fail startup with an explicit error when a required network
address is missing/invalid (guideline: missing essential config halts
startup).

### B3 — Hard-coded shard geometry in fetch_shard (review #101 residue)
**Location:** `crates/oceanfs-server/src/grpc/segment_service.rs` (~:472)
`let total_shards = 6; // default k=4, m=2`.
**Bug:** ignores per-segment `ec_k/ec_m`; any non-default EC config is
served/verified with the wrong geometry.
**Fix:** read `ec_k/ec_m` from the lifecycle registry / segment metadata
(like `push_sealed_segment` does) instead of the constant.

### B4 — Missing HLC silently zeroed (review #102)
**Location:** `segment_service.rs:627-633` `put_object_metadata`.
**Bug:** a request with `hlc = None` is written with `Hlc::zero()`, and the
tombstone logic treats zero as a tolerated legacy case.
**Fix:** reject requests missing HLC (hard error), not silently degrade.
Remove the zero-HLC tolerance.

### B5 — Default/degenerate segment metadata accepted on push (review #103)
**Location:** `segment_service.rs:746`, `push_sealed_segment` (~:761-929).
**Bug:** initializes `segment_id = SegmentId::default()`, `tier = Standard`,
`ec_k = 1`, `ec_m = 0`, and validates merkle/emptiness but never validates
that a real (non-default) `segment_id`/EC params arrived; a malformed push
persists under defaults.
**Fix:** reject pushes whose segment metadata is default/degenerate
(missing segment_id, invalid EC params) with an explicit error.

### B6 — Hard-coded 2-node ring gate as quorum proxy (reviews #66/#69)
**Location:** `node.rs:1565-1588` ready gate + `node.rs:2356-2381`
background rejoin both wait for `ring_nodes >= 2`.
**Bug:** "2" is a stand-in for w=2 semantics and does not derive from
config; with RF > 2 the gate opens before true quorum is reachable.
**Fix:** derive the required node count from the configured minimum quorum
(w = replication-factor-derived), falling back to a documented minimum for
single-node dev deployments; keep `cluster_ready_timeout_sec` as the bound.

## Scope

### In Scope
- B2, B3, B4, B5, B6 as described, with regression tests.
- B1's disposition is authoritative here: **deferred to composition-root
  c1's `NodeLeaveHandler` deletion** (DECISION 2026-09-04, next session).
  No in-place fix; recorded in this doc and in c1's doc.

### Out of Scope
- ACK semantics (review #105) — dropped by decision 2026-09-04.
- Any architectural change.

## Implementation decisions (2026-09-04)

- **B1** — deferred to c1's leave-handler deletion (see above); no
  regression test in this epic.
- **B2** — fail `Node::start` with an explicit error on an unparseable
  `grpc_listen_addr` (missing essential config halts startup).
- **B3** — `fetch_shard` resolves `ec_k`/`ec_m` from the segment's
  lifecycle-registry entry. A segment with **no** registry entry (or a
  service without a lifecycle) is **rejected** with an explicit error —
  no fallback geometry, no silent legacy slicing.
- **B4** — hard rejection of **missing and all-zero HLC** applies to both
  `put_object_metadata` AND `append_segment` (no-legacy-mode policy);
  the zero-HLC tombstone/LWW tolerance is removed.
- **B5** — validation of the pushed metadata (real `segment_id`,
  recognized tier, non-degenerate EC params) happens BEFORE the first
  data write; reserve-after-write ordering is otherwise preserved.
- **B6** — new `NodeConfig.cluster_min_quorum_nodes` field (default 2),
  used by the cluster-readiness gate and the background rejoin loop;
  `cluster_ready_timeout_sec` remains the time bound. Single-node
  deployments (no seeds) still skip the gate entirely.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | B1 (deferred to c1), B2, B6 |
| `oceanfs-server` | B3, B4, B5 |

## Definition of Done

- [x] Each bug has a regression test that fails before / passes after
      (B1 excepted — deferred to c1's leave-handler deletion, disposition
      recorded below).
- [x] `cargo build --all-targets` passes.
- [x] `cargo test -p oceanfs-node --lib -- --test-threads=1` green
      (PIPELINE.md §4.6).
- [x] `cargo test -p oceanfs-server --lib -- --test-threads=1` green.
- [x] B1 disposition recorded (DECISION 2026-09-04: **superseded by c1's
      `NodeLeaveHandler` deletion** — recorded here and in c1's doc).

## Deviations

- **B1 (fixed 76-byte header slice in `NodeLeaveHandler`, review #35) is
  NOT fixed in this epic.** DECISION 2026-09-04: deferred; closed by
  composition-root c1's `NodeLeaveHandler` deletion (review #34) in the
  next session. Disposition recorded in f1,
  `composition-root-decomposition/c1-split-storage-builder.md`,
  composition-root README, roadmap, and orchestration. No regression test
  for B1 (the code is deleted, not fixed).
- **Pre-existing failures outside this epic's control** (verified
  identical on clean HEAD `83dd5ce`): 4 server integration tests
  (`replicated_hlc` ×2, `write_quorum` ×1, `grpc_services`
  `swim_death_detection` ×1) and 2 `oceanfs-server` rustdoc errors
  (`admin.rs` `RING_PROBE_HASHES`, `write/coordinator.rs`
  `HintObjectApplier`).
