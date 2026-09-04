---
feature: "f1: Correctness Bug Batch"
epic: "refactoring/review-wave-0-1"
status: proposed
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
**Fix:** route the leave handler's segment read through the shared store /
header-aware read, or parse the header version and slice by
`header_size(version)`. **Ownership: this epic owns B1.** If
composition-root c1 deletes `NodeLeaveHandler` (review #34) before this
lands, B1 is closed by that deletion — record the dependency in both docs.
Do NOT defer B1 to c1 as an unfixed live bug while the handler still exists.

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
- B1, B2, B3, B4, B5, B6 as described, with regression tests.
- B1's disposition is authoritative here (closed by this epic's fix, or by
  c1's leave-handler deletion if c1 lands first — record which).

### Out of Scope
- ACK semantics (review #105) — dropped by decision 2026-09-04.
- Any architectural change.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | B1 (or defer to c1), B2, B6 |
| `oceanfs-server` | B3, B4, B5 |

## Definition of Done

- [ ] Each bug has a regression test that fails before / passes after.
- [ ] `cargo build --all-targets` passes.
- [ ] `cargo test -p oceanfs-node --lib -- --test-threads=1` green
      (PIPELINE.md §4.6).
- [ ] `cargo test -p oceanfs-server --lib -- --test-threads=1` green.
- [ ] B1 disposition recorded (fixed here or superseded by c1).
