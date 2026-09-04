---
feature: "2026-09 Review Triage — Program Roadmap"
epic: "refactoring"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: Composition-root decomposition (wave 2 item 1) is the first structural gate; most later refactors land inside its module builders
  - epic: refactoring/store-unification
  - epic: refactoring/durability-scheduler
  - epic: refactoring/manifest-aware-repair
  - epic: refactoring/legacy-mode-removal
  - epic: refactoring/bounded-metadata-scans
  - epic: refactoring/review-wave-0-1
  - epic: refactoring/review-wave-4
  - epic: refactoring/review-wave-5
adr:
  - 0031-remove-single-datadir-legacy-mode
  - 0032-unify-segment-data-access
  - 0033-manifest-aware-peer-selection
  - 0034-bounded-metadata-accounting
  - 0017-durability-task-abstraction
created: 2026-09-04
updated: 2026-09-04
---

# 2026-09 Review Triage — Program Roadmap

> **This is the coordination document for the review triage program.** If
> you are implementing any feature under this program, read this first:
> it tells you where your work sits in the whole, what must exist before
> you start, and what must not regress while you work. Artifacts produced
> by the 2026-09-04 triage session are listed in §1; waves are in §3.

## Summary

The 2026-08-25/09-03 whole-project review (merged as `e005895`, "merge:
whole-project review comments") left **112 in-code `[review]` blocks** in
production code across 36 files. The triage session (2026-09-04) read each
comment against the actual code — not the spec — and classified it:

| Verdict | Count | Meaning |
|---|---|---|
| Valid / actionable | ~50 | Claim holds against today's code; real work |
| Partially valid | ~45 | Observation true but overstated, or a *design discussion* rather than a defect |
| Stale / wrong / already resolved | ~17 | Factually wrong in today's code, or closed by recent commits |

The ~95 non-stale comments compress into **8 design themes** (see §2),
which are scheduled across **6 waves** (§3).

## 1. Artifacts from the 2026-09-04 session

| Artifact | Status | Purpose |
|---|---|---|
| `adr/0031-remove-single-datadir-legacy-mode.md` | **Accepted** | Legacy single-`data_dir` mode removed; pools mandatory at boot. Theme 1 + legacy theme |
| `adr/0032-unify-segment-data-access.md` | **Accepted** | One segment data-access trait/impl/instance; lifecycle-coordinated writes |
| `adr/0033-manifest-aware-peer-selection.md` | **Accepted** | AE/scrub select peers by `storage_locations` + manifest, not random alive nodes |
| `adr/0017-durability-task-abstraction.md` | **Accepted** | DurabilityTask + DurabilityScheduler (decided 08-09; now formally accepted) |
| `adr/0034-bounded-metadata-accounting.md` | **Accepted** | Accounting-based scan elimination (supersede-capture, seal-time membership, remap object keys). Theme 4 |
| `features/refactoring/composition-root-decomposition/README.md` | proposed | Epic: split the ~2,230-line `Node::start()` into module builders. Wave 2 ① |
| `features/refactoring/store-unification/` | proposed | Wave 2 ② — one store trait/impl/instance (ADR-0032) |
| `features/refactoring/durability-scheduler/` | proposed | Wave 2 ③ — DurabilityTask + Scheduler (ADR-0017) |
| `features/refactoring/manifest-aware-repair/` | proposed | Wave 2 ④ — AE/scrub holder-aware selection (ADR-0033) |
| `features/refactoring/legacy-mode-removal/` | proposed | Wave 2 ⑤ — pools mandatory (ADR-0031) |
| `features/refactoring/bounded-metadata-scans/` | proposed | Wave 2 ⑥ — scan elimination (ADR-0034) |
| `features/refactoring/review-wave-0-1/` | **implemented (2026-09-04)** | Wave 0+1 combined: stale-comment closure + bug batch (f0/f1 landed; B1 deferred to composition-root c1) |
| `features/refactoring/review-wave-4/` | proposed | Mechanical hygiene: config plumbing, dead-code purge, folders, docs/graphs |
| `features/refactoring/review-wave-5/` | proposed | Deferred design ADRs (D1–D6) |
| `features/refactoring/review-2026-09-orchestration.md` | proposed | Implementer navigation map: global order, gates, status board |
| `audits/2026-09-04-seal-on-zero-space-waste.md` | complete | Review #89 audit; seeds a future seal-strategy ADR (wave 5) |
| This document | proposed | Program coordination |

## 2. The eight themes

| Theme | Title | Exemplar anchors | Wave |
|---|---|---|---|
| 1 | Segment data-access: one store, one writer | `node.rs:1233,1269,1285,1450`; `gc/garbage_collector.rs:29,548,613`; `healing_service.rs:1327`; `heal/worker.rs:97`; `anti_entropy/merkle_tree.rs:22` | 2 |
| 2 | Background orchestration: global concurrency/scheduling (ADR-0017, decided but unbuilt) | `healing_service.rs:1030`; `node.rs:369`; `garbage_collector.rs:160,192`; `health.rs:83`; `node.rs:1932,2620`; `write/coordinator.rs:382` | 2 |
| 3 | Config not plumbed from userland | `node.rs:615,625,715,732,737,777,784,865,887,989,1073,1211,1260,1373,1382,3141`; `pool/health.rs:647` | 4 |
| 4 | Full-space scans & unbounded in-memory growth at scale | `healing_service.rs:671`; `gc/orphan_reaper.rs:297`; `reconcile.rs:148`; `anti_entropy/engine.rs:184,199`; `hinted_handoff/hint_delivery.rs:360` | 2 (⑥) / 5 |
| 5 | Replication/manifest awareness for AE + scrub + heal | `anti_entropy/engine.rs:226`; `scrub.rs:601` | 2 |
| 6 | Streaming read path (large-object memory) | `read/coordinator.rs:1341`; `read/assembly.rs:92`; `read/fetch.rs:18` | 5 |
| 7 | Seal signaling & space efficiency | `segment/lifecycle.rs:1131,1802,2330`; `segment/pool.rs:594,887` | 5 (audit: #89) |
| 8 | Correctness / hardening (independent bugs) | `node.rs:202` (fixed 76-byte header); `node.rs:1501` (default addr); `segment_service.rs:627,746`; `read/coordinator.rs:1539` | 1 |

## 3. Waves

### Wave 0 — Ground truth (close stale comments)

> **UPDATE 2026-09-04:** Waves 0 and 1 are treated together as the epic
> `refactoring/review-wave-0-1` (`f0-close-stale-comments`,
> `f1-correctness-bug-batch`). Stale comments are **deleted**, not
> annotated.
>
> **STATUS (implemented 2026-09-04):** all rows in the wave-0 table that
> are marked stale/wrong/resolved were deleted by f0 (see the authoritative
> per-comment list there). The `route_write.rs:15,51` and
> `event_checkpoint.rs:453` rows are NOT deletions — they stay as wave-4
> cleanup markers (dead code / live-compat notes).

The following comments are **stale / wrong / already resolved** in today's
code. They are **deleted** (not annotated) — see
`refactoring/review-wave-0-1/f0-close-stale-comments.md` for the
authoritative per-comment list:

| Anchor | Why stale |
|---|---|
| `durability/repair.rs:447` | EC shape (tier/ec_k/ec_m) now carried in `RequestReReplicationRequest` proto + filled by dispatcher (`node/repair.rs:493-501`) |
| `durability/anti_entropy/engine.rs:632` | `local_merkle_verify` is the active no-peer fallback, not dead |
| `durability/anti_entropy/merkle_tree.rs:22` | `SegmentDataStore` is used by heal/repair/GC/AE — premise "never written directly" is false; only the *placement* critique survives |
| `server/grpc/segment_service.rs:300` | Remap alias is a shared `Arc<SegmentRemapAlias>` updated live by the healing service; not stale |
| `server/grpc/segment_service.rs:467` | `fetch_shard` serves real data; residual: hard-coded `total_shards = 6` (see wave 1) |
| `server/write/coordinator.rs:472` | `replicate_write` + `forward_write` exist |
| `server/read/assembly.rs:76` | "64 MB buffer" is actually a 64 KiB capacity hint |
| `node.rs:1069` | The `repair_sink = repair_dispatcher.clone()` is needed (3 owners) |
| `segment/event_wal.rs:1579` | Test pins the live `pool_id=0` wire format — keep, do not "discard" |
| `segment/lifecycle.rs:425` | Hash-uniformity question, no defect |
| `segment/route_write.rs:15,51` | Dead `InlineWriter`/`route_write` → delete (also wave-4 cleanup) |
| `segment/event_checkpoint.rs:453` | Partial: v2 decode is live-compat until ADR-0031 lands; then removable |

### Wave 1 — Bug batch (independent, no design)

> **UPDATE 2026-09-04:** folded into `refactoring/review-wave-0-1`/`f1`.
>
> **STATUS (implemented 2026-09-04):** B2 (#64) fixed in f1 with a
> regression test; B3/B4/B5/B6 fixed in f1 with regression tests. B1
> (#35, `node.rs:202`) is **deferred** to composition-root c1's
> `NodeLeaveHandler` deletion (see the row above).
>
> **Follow-ups from wave 0/1 (same bug classes at locations f1 did not
> list — track in wave 2/4, do not lose):**
> - `durability/healing_service.rs` `fetch_shard` (~:1198) still hard-codes
>   `total_shards = 6` (same class as f1 B3) and the healing hint paths
>   still zero-fill missing HLCs (~:735,823,881,969,1263; same class as
>   B4). Fix under store-unification / hinted-handoff work (ADR-0032 /
>   ADR-0031).
> - `server/grpc/segment_service.rs` `delete_object` handler zero-fills a
>   missing HLC (~:404-407; same class as B4).
> - membership crate defaults unparseable addresses to `127.0.0.1:9001`
>   (`gossip.rs:721,725`, `gossip_service.rs:119,124`,
>   `manager.rs:539,543,1244,1247`; same class as B2) — config theme,
>   wave 4.

| Anchor | Bug |
|---|---|---|
| `node.rs:202` (`NodeLeaveHandler::read_segment_data`) | Unconditional `data[76..]` slice; sealer writes 92-byte v2 headers. Real corruption on leave handoff. **Owned by wave-0/1 f1 B1 — DEFERRED (DECISION 2026-09-04):** no in-place fix; closed by composition-root c1's `NodeLeaveHandler` deletion (review #34). Disposition recorded in f1 + c1 docs |
| `node.rs:1501` | Silent `127.0.0.1:9001` fallback on unparseable gRPC addr → must halt |
| `grpc/segment_service.rs:467` residual | `total_shards = 6` hard-coded; use per-segment `ec_k/ec_m` |
| `grpc/segment_service.rs:627` | Missing HLC silently zeroed → reject |
| `grpc/segment_service.rs:746` | Default/degenerate `SegmentId`/tier accepted on `push_sealed_segment` → reject |
| `node.rs:1565,2356` | `ring_nodes >= 2` hard-coded as quorum proxy; derive from config minimums |

### Wave 2 — Structure gate (before continuing g7/g8)

1. **① Composition-root decomposition** — `refactoring/composition-root-decomposition/` (split `Node::start` into module builders; no `tokio::spawn` in `start`).
2. **② Theme 1 store unification** — `refactoring/store-unification/` (ADR-0032): one segment data-access abstraction routed through the lifecycle coordinator + one optimized read path. Deletes the 8-store sprawl and the divergent readers.
3. **③ ADR-0017 scheduler** — `refactoring/durability-scheduler/`: implement `DurabilityTask` + `DurabilityScheduler` (global semaphore, keyspace fraction, unified metrics); retire per-task interval loops.
4. **④ Theme 5 manifest-aware AE/scrub** — `refactoring/manifest-aware-repair/` (ADR-0033): AE peer selection and scrub partitions keyed off `storage_locations`/manifest (reuse `ManifestRepairTargetSelector`).
5. **⑤ Legacy removal** — `refactoring/legacy-mode-removal/` (ADR-0031): implement the cleanup.
6. **⑥ Bounded metadata scans** — `refactoring/bounded-metadata-scans/` (ADR-0034): supersede-capture on overwrite, accounting-based GC/orphan liveness, seal-time membership list, remap-carrying object keys. Resolves Theme 4's O(all-objects) scans; lands before scheduler f3 sharding and g7.

### Wave 3 — Resume disk-aware / recovery epics

Land g7 (`wal-loss-recovery`) and g8 (`metadata-loss-recovery`) on the
single-store, scheduler-bounded substrate. Also: the replicated-lifecycle
state ADR from review #30 (`segment_replicator.rs:353`).

### Wave 4 — Mechanical / config plumbing

> **UPDATE 2026-09-04:** detailed in `refactoring/review-wave-4/`
> (`f1-config-plumbing`, `f2-dead-code-test-purge`,
> `f3-durability-folder-hygiene`, `f4-docs-and-interaction-graphs`).

- Theme 3 config plumbing (thread `NodeConfig` values instead of
  `XxxConfig::default()`).
- Dead-code purge: `route_write.rs`, `verify_blake3` (`read/coordinator.rs:1539`),
  `shard_small`/`shard_standard` (`write/coordinator.rs:119`),
  `ALL_COLUMN_FAMILIES`/`encode_deletion_key` (`metadata/cf.rs`),
  test-only items → `#[cfg(test)]` (`reconcile.rs:241`, `engine.rs:663,807,951`,
  `garbage_collector.rs:599`, `segment/pool.rs:700,762`).
- Durability crate folder hygiene (scrub/reconcile get folders).
- Architecture documentation + interaction-graphs pass (see §5).

### Wave 5 — Deferred design ADRs

> **UPDATE 2026-09-04:** detailed in `refactoring/review-wave-5/` (D1–D6).
> The former D7 (bounded metadata scans) is **resolved** — it became wave 2
> ⑥, ADR-0034 (Accepted), epic `refactoring/bounded-metadata-scans/`. Do not
> re-open it as a wave-5 backlog item.

| Topic | Source | Artifact |
|---|---|---|
| Seal-on-full / seal strategy | review #89 (`lifecycle.rs:1131`) | audit written; future ADR from `audits/2026-09-04-seal-on-zero-space-waste.md` |
| ACK semantics | review #105 (`write/coordinator.rs:15`) | **Dropped** — stakeholder decision 2026-09-04: keep the durability contract as-is |
| Graceful-leave redesign | review #34 (`node.rs:163`) | ADR when g7/g8 land |
| Streaming read path | Theme 6 | feature when large-object SLO lands |
| Adaptive full-scan strategies | `node.rs:8` header remarks | feature once scheduler (wave 2 ③) exists |
| Membership-state resilience | `membership_state.rs:59` | small feature (corrupt file → regenerate via gossip seeds) |
| Generic reactor / event bus | review #32, #107 | **Rejected** — stakeholder decision 2026-09-04; targeted Notify only |

## 4. Sequencing rules

- Wave 0/1 (combined epic `review-wave-0-1`) is independent of everything;
  land first. It fixes #64 (f1 B2). #35 (f1 B1) is **deferred** to
  composition-root c1's `NodeLeaveHandler` deletion (DECISION 2026-09-04)
  — the disposition is recorded in f1 and c1; later epics must NOT
  re-claim #64 or re-open B1 as a live unfixed bug.
- Wave 2 ordering within the gate:
  - **① c1** (composition root storage builder) lands first — single
    wiring point for everything below.
  - **⑤ legacy f1/f2 → ② store-unification f2**: legacy f2's delegacy of
    `DiskSegmentStore`/`DiskSegmentShardStore` must land BEFORE
    store-unification f2 deletes those impls (they edit the same files;
    do not parallelize these two features). ⑤ f3 (format break) is
    independent of ② and can run in parallel.
  - **⑥ bounded-metadata-scans** depends on ② + ⑤ (reads the unified
    store; bumps the checkpoint/event-WAL formats after ⑤ f3's break).
  - **③ durability-scheduler** depends on ② + ⑥ (f3 keyspace sharding
    must not land until the O(n) scans in ⑥ are gone).
  - **④ manifest-aware-repair** depends on ②; if it changes
    `AntiEntropy::new`, that lands BEFORE ③ f4 wiring (or uses a
    `with_peer_selector` injection so the scheduler adaptor is stable).
- Composition-root c1 ends with **two** shared store instances (one
  `DiskSegmentStore` + one `DiskSegmentShardStore`); the **one** unified
  store is store-unification f3's end state (review #57/#59/#60 close
  there, not in c1).
- Wave 3 **must not start** before Wave 2 ② and ③ are done: g7/g8 add more
  `.dat` writers (Theme 1) and more background loops (Theme 2) to the exact
  surfaces being fixed.
- Wave 4 config plumbing can interleave with Wave 3 (different files, low
  risk).

## 5. Documentation & interaction graphs

The review author noted (2026-09-04) that the reactor idea partly arose from
a lack of *documentation of subsystem interactions* and *architecture
graphs*. A dedicated pass — module-interaction diagrams (Mermaid) for the
write path, read path, durability background tasks, and the healing
epic — is scheduled as part of Wave 4. Until then, this roadmap and the
composition-root epic's module map serve as the interaction reference.

## References

- Review merge: `e005895` ("merge: whole-project review comments")
- ADRs: 0017, 0031, 0032, 0033, 0034
- Epic dirs under `features/refactoring/`: `composition-root-decomposition/`,
  `store-unification/`, `durability-scheduler/`, `manifest-aware-repair/`,
  `legacy-mode-removal/`, `bounded-metadata-scans/`, `review-wave-0-1/`,
  `review-wave-4/`, `review-wave-5/`
- Orchestration: `features/refactoring/review-2026-09-orchestration.md`
- `audits/2026-09-04-seal-on-zero-space-waste.md`
- In-flight: `features/disk-resilience-healing/` (g7/g8 proposed),
  `features/phase-7-durability/` (in-progress)
