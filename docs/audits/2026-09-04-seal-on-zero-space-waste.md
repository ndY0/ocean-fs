---
audit_date: 2026-09-04
scope: write-path segment lifecycle
target_crates: [oceanfs-storage, oceanfs-server, oceanfs-node]
severity_counts:
  critical: 0
  high: 0
  medium: 2
  low: 1
---

# Audit Report: Seal-on-Zero Space Waste Under Sparse Load (Review #89)

## Summary

Review comment `[strategy][critcial]` at
`crates/oceanfs-storage/src/segment/lifecycle.rs:1131` asks whether the
current sealing strategy wastes disk space under low or sparse load, and
whether the data WAL + event WAL now make a **seal-on-full** strategy safe.

**Verdict: the behavioral premise is TRUE; the switch is feasible but has a
read-path consequence that must be decided before implementation.** This
audit documents the current mechanics, the waste, the proposed alternative,
and the open questions. It is intentionally **no code change** — it exists
to seed a future design session (roadmap wave 5).

## Current behavior (verified in code)

- **Seal trigger is seal-on-zero (last-writer-exit).** The lifecycle
  coordinator documents a "deterministic seal-on-zero trigger": the last
  `writer_leave` notes the segment, and the pending-seal drain freezes and
  enqueues it — "no timer, no heuristic"
  (`lifecycle.rs:1154-1159`).
- The write coordinator leaves every joined segment at request completion
  and immediately drains:
  `writer_leave → note_pending_seal → drain_pending_seals().await`
  (`write/coordinator.rs:846-861`).
- `drain_pending_seals` freezes even a barely-filled segment via
  `pool.freeze_partial_for_seal(id)` with **no minimum-size gate**
  (`lifecycle.rs:2351-2402`).
- Small-tier target size is `small_target_size = 65536` (64 KiB,
  `core/src/types/config.rs:78`), so under sequential writes each request
  is typically its own last writer → one ~64 KiB (or smaller) segment per
  request, each carrying its own header + per-segment overhead + a full EC
  decision at seal time.
- `request_seal` durability is guaranteed by the event WAL + data WAL
  (ADR-0024/0025), so the reviewer's premise that seal-on-full would not
  risk data loss is consistent with the code — sealing is a *durability
  acceleration* decision, not the durability mechanism itself.

## Impact quantification (order of magnitude)

| Scenario | Behavior today | Waste |
|---|---|---|
| Sustained concurrent load | Many writers share segments; seal-on-zero ≈ full segments | Negligible |
| Sequential / sparse PUTs | Each request seals its own partial segment | Header overhead + index entry + seal fsync per tiny segment; segment count grows ~linearly with request count, not with bytes |
| Small-object workload (e.g. 1–16 KiB objects) | Many segments far below the 64 KiB / 256 KiB stripe sizes | Low packing efficiency; more `.dat` files; more registry entries; more seal events; larger event WAL |
| No workload (idle) | Nothing seals | None (seal-on-zero never fires without a writer) |

The second-order costs matter as much as raw space: more segments → larger
lifecycle registry (ADR-0025 memory bound is O(live segments)), more entries
per event-WAL checkpoint, more AE/scrub/GC enumeration work, and more
seal-time EC calls on sub-stripe data.

## Why it was designed this way

Seal-on-zero makes the **read-after-write path simple**: an object written
and immediately read is found in the active pool buffer / in-flight segment
(ADR-0020/0021 machinery). Delaying sealing (seal-on-full) means a written
object sits in an open segment that must still be readable — which is why
the read path currently probes active pool slots before falling back to
`.dat`. Seal-on-full therefore interacts directly with the read path, not
just the write path.

## Options (for the future design session)

| Option | Mechanism | Pros | Cons |
|---|---|---|---|
| **A. Keep seal-on-zero (status quo)** | Current | Simple; read-after-write trivially served from active slots | Space/segment-count waste under sparse load |
| **B. Seal-on-size-threshold** | Add a minimum fill gate to `drain_pending_seals`; only freeze segments above e.g. 50% of target OR older than a bound | Bounded waste; keeps segments open under sparse load | An open-segment timeout reintroduces a timer (the anti-timer principle); idle-partial segments linger |
| **C. Seal-on-full + read-from-open-segments** | Seal only when a segment buffer fills; read path serves unsealed data from the open segment (already supported by pool-slot reads) | Maximizes packing; simplest mental model | Open segments can stay open a long time under sparse load; event-WAL position grows while a segment stays open; crash window between data-WAL entries and seal is larger (though ADR-0025 covers it) |
| **D. Hybrid: size gate + defer-only-under-idle** | Adaptive: seal on zero when the next-segment allocation would otherwise fail or when a segment is large enough; otherwise hold with a Notify-based drain | Best of both | More machinery; needs the adaptive-strategy infrastructure the review also calls for (node.rs:8 header remarks) |

## Recommendation

Do **not** change the strategy during the current disk-aware/recovery work.
Document this audit as the seed for a dedicated design session in roadmap
wave 5. When that session happens, the decision hinges on **Option B vs C**
and specifically on: (1) whether an idle-time bound is acceptable despite
the anti-timer principle, and (2) whether the read path's pool-slot probe is
sufficient to serve long-open segments without a separate in-memory index
of unsealed-but-readable objects.

## References

- Review comment: `crates/oceanfs-storage/src/segment/lifecycle.rs:1131`
- ADR-0020 (read from active segments), ADR-0021 (seal window data set),
  ADR-0024 (segment event log), ADR-0025 (lifecycle state machine)
- `write/coordinator.rs:846-861` (leave → drain), `lifecycle.rs:2351-2402`
  (drain_pending_seals)
