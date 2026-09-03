---
feature: "Re-Replication Worker"
epic: "disk-resilience-healing"
status: done
priority: high
owner: ""
dependencies: ["reconciliation"]
adr: [0029, 0030]
perf: [1.3, 2.7, 8.1, 8.5]
created: 2026-08-22
updated: 2026-09-03
---

# Re-Replication Worker

## Summary

The repair executor of ADR-0029 §D4/D5/D6. Re-replication is
**target-pull** (ADR-0030): the holder that detects under-replication
(g3 announcement / g4 reconciliation) acts only as a **dispatcher** —
it selects a target node (capacity-aware via peer manifests, f7) and
sends the target a dedicated **`RequestReReplication`** RPC. The
target's `ReRepWorker` fetches the missing segment from a live holder
(via `HealingRpcClient::fetch_shard` in full-segment mode), writes it
through the pool-aware `SegmentDataStore` (Phase A f5 — the target's
own store picks the pool via `PlacementPolicy`, f3), registers it in
its lifecycle, and stamps `storage_locations`. Concurrency is bounded
by a semaphore (HealWorker pattern, heal/worker.rs:8-12).

## Scope

### In Scope

- `oceanfs-durability::repair`:
  - `struct ReRepWorker` — the target-side executor, mirrors
    HealWorker's shape:
    - bounded queue (mpsc, perf 2.6) + `tokio::sync::Semaphore`
      (perf 2.7/8.5, `max_concurrent_repairs`, default 16);
    - `process(repair_request)`: fetch the full segment data from a
      live holder (iterate the request's `holders − self` via
      `HealingRpcClient::fetch_shard` in **full-segment mode** —
      shard_index 0 + length 0 → the whole data section) → write via
      `SegmentDataStore::write_segment_data` → register (reserve +
      seal) → update `storage_locations`.
    - **placement**: the write goes through the target's own pool-aware
      store, so `PlacementPolicy` (f3) picks the pool on the node whose
      pool it actually is.
  - **target-node selection (pinned, injected)**: a trait
    `RepairTargetSelector` implemented in `oceanfs-node`:
    `fn pick_repair_target(&self, source: &SegmentId, holders: &[NodeId]) -> Option<NodeId>`
    — filters candidates by manifest health (excludes nodes with
    `write_degraded` / no Healthy data pool, f7 cache) and prefers the
    node with the most free data-pool capacity (manifests'
    `capacity_free_bytes`). The durability crate never touches manifests
    directly (ring_cache is a dev-dependency there today — same
    boundary, heal/worker.rs:86-88).
  - `RepairReason::{Announcement, Reconciliation}` rides the request
    (metrics `oceanfs_repair_queue_depth{priority}`).
- `oceanfs-durability::healing_service`:
  - new `RequestReReplication` RPC (proto healing.proto) — routing
    intent only (segment id + live holders + reason), no data;
  - `fetch_shard` gains a full-segment mode (shard_index 0 + length 0 →
    whole data section) — the data mover for the target's fetch.
- `oceanfs-durability::ReconciliationLoop` / g3 handler: unchanged
  public contract (`RepairSink::enqueue`); the node-side sink
  implementation becomes the dispatcher.
- `oceanfs-storage` lifecycle: `request_refresh_metadata` extended to
  carry the new location set (durable event-WAL write, ADR-0025) — the
  post-repair `storage_locations` stamp.
- Node wiring: build the worker with the injected `RepairTargetSelector`
  over the manifest cache (f7) + migration pool/membership (ADR-0030
  Decision 4 keeps the wiring plane-agnostic); wire g3's announcement
  handler and g4's reconciliation into the same `RepairSink` whose
  impl dispatches via the new RPC.
- Metrics: `oceanfs_ranges_re_replicated_total`,
  `oceanfs_repair_queue_depth{priority}`, `oceanfs_repair_failures_total`.
- Tests:
  - unit: worker processes a request end-to-end with an in-memory data
    store (fetch from a mock holder, write, locations updated);
  - unit: selector — excludes write_degraded / no-Healthy-pool
    candidates; prefers most-free-capacity; falls back to any live
    holder when all filtered out;
  - unit: concurrency bound (semaphore limits in-flight ops);
  - unit: `MetadataRefreshEvent` with locations round-trips
    byte-exact (WAL format regression);
  - integration (local 3-node, RF=2): kill a data pool on A → B's
    dispatcher sends `RequestReReplication` to C → C pulls the segment,
    writes, registers, stamps locations; `storage_locations` converges
    to include C; the cluster serves reads of the affected keys without
    data loss. Both paths tested (announcement enabled + announcements
    disabled → reconciliation alone drives it).

### Out of Scope

- Announcement (g3) and reconciliation (g4) — the worker only executes.
- Migration-plane topology (ADR-0030 Decision 4) — a forward-looking
  consequence, not implemented here; the worker receives pool +
  membership injected so a later swap is a wiring change.
- WAL/metadata-loss recovery (g7/g8) — those use the worker's fetch
  primitive but have their own recovery flows.
- EC shard repair (existing HealWorker/scrub path — unchanged).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New `repair` module (worker, selector trait, RepairReason); `RequestReReplication` RPC; `fetch_shard` full-segment mode; `MetadataRefreshEvent` locations payload |
| `oceanfs-storage` | Lifecycle `request_refresh_metadata` + event-WAL payload extension (locations) |
| `oceanfs-node` | Selector impl over manifest cache; `RepairSink` dispatcher impl; worker wiring |

## Interface (Public API)

- `pub struct ReRepWorker` — `new(config, data_store, lifecycle, pool, membership, timeouts)`, `run(shutdown)`.
- `pub trait RepairTargetSelector: Send + Sync` — `pick_repair_target(...)`.
- `pub enum RepairReason { Announcement, Reconciliation }`.
- `pub struct ReRepRequest { origin: NodeId, segment_id: SegmentId, holders: Vec<NodeId>, reason: RepairReason, retry_count: u32, merkle_root: Option<HashOutput>, tier: SizeTier, ec_k: u8, ec_m: u8 }` — the `RepairSink` input (g3/g4 unchanged contract; the seal-time shape rides the request so the worker registers the pulled copy with the source's real tier/EC geometry).
- `request_refresh_metadata(id, merkle_root, storage_locations: Option<SmallVec<[NodeId; 16]>>)`.
- `RequestReReplicationRequest` (proto) carries `tier`/`ec_k`/`ec_m` (wire encoding matches the segment-push protocol: tier = SizeTier as u8, 0=Inline 1=Small 2=Standard 3=Multi).

## Data Flow

```
g3 announcement / g4 reconciliation ──▶ RepairSink::enqueue (holder side)
   └─ dispatcher (node): filter live holders → selector.pick_repair_target
   └─ RequestReReplication RPC ──▶ target's bounded queue
        └─ ReRepWorker.process (target side):
             fetch_shard full-segment from a live holder (holders − self)
             └─ write via SegmentDataStore (PlacementPolicy picks the pool)
             └─ reserve + seal in lifecycle
             └─ request_refresh_metadata(storage_locations += target)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-durability`,
      `oceanfs-storage`, `oceanfs-node`
<!-- REVIEW (iteration 2): verified — `cargo build --workspace --all-targets` clean;
`cargo clippy --workspace --lib -- -D warnings` clean; `cargo fmt -- --check` clean.
The previously-flagged unused `oceanfs_routing::Ring` import is REMOVED (fix 9);
the only remaining warning is the pre-existing dead fn
`test_hint_wal_implements_wal_writer_trait` in
crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs:848 (test-only, not a
feature-completeness gate). -->
<!-- REVIEW (iteration 3): re-verified after the iteration-3 edits — `cargo fmt -- --check`,
`cargo build --workspace --all-targets`, `cargo clippy --workspace --lib -- -D warnings`,
and `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` (forced rebuild of the four changed
files) all clean. OPEN (owner decision): the implementer's own `[review][implementation]
[critical]` marker at crates/oceanfs-durability/src/repair.rs:452-457 stands — the
re-replicated copy is registered with HARDCODED shape (`SizeTier::Standard`, ec_k=1,
ec_m=0) instead of the source segment's seal-time shape; the implementer asks for shape
propagation via the request. Reads of re-replicated copies are byte-correct (merkle-
verified; integration GETs pass), but the durable lifecycle entry misstates the tier/ec
shape until an AE/scrub exchange corrects it. Also OPEN (low): `ReRepConfig.fetch_timeout_ms`
(crates/oceanfs-durability/src/repair.rs:68) is a DEAD public field — the fetch deadline
comes from `OperationTimeouts::shard_fetch_ms` (default 30 s); wire the knob or remove it.
Whole-project `[review][...]` marker comments (merge e005895/e2b451b) remain embedded in
production sources awaiting architecture-owner adjudication. -->
<!-- REVIEW (iteration 3b): the owner-selected Option A (seal-time shape rides
`RequestReReplication`) is IMPLEMENTED and independently re-verified — `cargo fmt -- --check`,
`cargo build --workspace --all-targets`, `cargo clippy --workspace --lib -- -D warnings`,
and `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` (forced rebuild via `touch`) all clean.
The `[review][implementation][critical]` marker block is REMOVED from
crates/oceanfs-durability/src/repair.rs:440-452 and replaced with an explanation; the worker's
`request_reserve(segment_id, tier, ec_k, ec_m)` (repair.rs:454-455) now reads the shape from
`request.tier/ec_k/ec_m` — NO hardcoded `Standard/1/0` in production. Encode hop verified:
the dispatcher (node repair.rs:488-508) maps tier to the segment-push wire u8 (0=Inline,
1=Small, 2=Standard, 3=Multi, `_ => 2`) and sends `ec_k/ec_m as u32`; decode hop verified:
`request_re_replication` (healing_service.rs:1642-1655) maps 0..3 and degrades unknown tiers
to Standard (identical to the push receiver, server segment_service.rs:787-795) and clamps
`ec_k/ec_m` to u8 — round-trip is bijective for all four tiers. Enqueuers (g3
healing_service.rs:1417-1435, g4 reconcile.rs:710-727) populate the shape from their own
registry entry (fallback Standard/1/0 only when the entry is absent — unreachable on the
holds-guarded g3 path). Parked/swept requests preserve shape (the whole `ReRepRequest` is
stored in the dispatcher's DashMap and re-cloned on sweep); the worker retry re-enqueue uses
`ReRepRequest { retry_count + 1, ..request }` (repair.rs:333-336). Wire layout matches the
generated prost struct (`uint32 tier = 5; ec_k = 6; ec_m = 7`). Dead knob removed:
`ReRepConfig.fetch_timeout_ms` is GONE — no reads/writes remain anywhere; the fetch deadline
is `OperationTimeouts::shard_fetch_ms` (repair.rs:536). `rep_config_defaults_are_sane`
updated. LOW (test-code hygiene, NOT a feature gate per guidelines/coding.md §9.2.1):
`cargo clippy --all-targets -p oceanfs-node -- -D warnings` flags `clippy::never_loop` at
crates/oceanfs-node/tests/re_replication.rs:235 (the `'batches: loop` wrapper never actually
loops — the inner `for batch in 0..4` either `break 'batches` or panics; remove the label or
make it loop). -->
- [x] **Tests:** all listed green (worker e2e, selector rules,
      concurrency, WAL format round-trip, 3-node RF=2 integration both
      paths)
<!-- REVIEW (iteration 2): worker e2e now EXISTS and is genuine —
`worker_pulls_writes_registers_and_stamps_end_to_end`
(crates/oceanfs-durability/src/repair.rs:845-931) spins up a real healing gRPC
service as the holder (seeded data + computed merkle root), registers it in
membership, drives `execute_repair`, and asserts byte-identical data AND
`storage_locations` includes the acquiring node — green (265 durability lib
tests pass). Selector (5), concurrency, WAL round-trip incl. extended refresh
(6), fetch_shard full/single modes, and the RPC handler tests all green with
--test-threads=1. BLOCKER: the 3-node RF=2 integration test
(crates/oceanfs-node/tests/re_replication.rs) is FLAKY — it does NOT reliably
exercise the target-pull flow. In 4 of 6 announcement runs and 2 of 2
reconciliation runs, ZERO dispatch/worker logs appear and the test still
passes: when the segment-id's ring replica set excludes the owner A,
`segment_replica_set − self` = {B, C}, so A pushes the sealed segment to BOTH
B and C (3 copies with RF=2) and there is nothing to repair. The
convergence/read assertions then hold trivially. Only when the replica set
includes A (1 of 6 runs: re-replication dispatched → request accepted →
worker pulled 32768 B from a holder → succeeded) is the pull flow genuinely
exercised. Also the convergence assertions (re_replication.rs:312-321) only
check each node lists ITSELF, so the holder-side `converge_holder_registry`
append is never directly asserted. Fix: make the setup deterministic (e.g.
assert the node that was NOT an initial holder gains the copy, or force a
replica set that includes the owner). -->
<!-- REVIEW (iteration 3): ALL unit-suite counts independently re-verified
(--test-threads=1): storage 424 passed, durability 265 passed, node 65 passed
(2 ignored). The three new tests run and pass individually:
`worker_rejects_merkle_mismatch` (repair.rs:917 — drives a REAL fetch against a
live healing gRPC service holding the real bytes with a WRONG merkle root in the
request; asserts execute_repair Err + target store empty + lifecycle empty — the
old already-held short-circuit is now its own test
`worker_skips_fetch_when_segment_already_held` at repair.rs:1012; the duplicate
`execute_repair_already_held_is_noop` is deleted), `data_dead_semantics_match_
reconciler_snapshot` (node repair.rs:793), `metadata_refresh_empty_payload_is_
rejected_not_panicked_on` (event_wal.rs:1709). RESIDUAL (low):
`ReRepConfig.fetch_timeout_ms` is dead config (see Code note); the integration
test's batch-precondition can still FAIL LOUDLY (never pass vacuously) when
every sealed segment's ring set lands on {B,C} for 4 consecutive batches —
probability ~(1/3)^(4·segments_per_batch), observed ≈0 with ≥6 segments/batch. -->
<!-- REVIEW (iteration 3b): all counts independently re-verified (--test-threads=1):
storage 424 passed, durability 265 passed, node 65 passed (2 ignored), durability
doctests 24 passed. Re-ran the feature-relevant node integration binaries —
re_replication 2/2 (~41 s; 12 dispatches → 12 accepted → 12 succeeded in the observed
run, non-vacuous), loss_announcement 1/1, reconciliation 1/1, segment_replication 3/3.
Shape tests verified: worker e2e `worker_pulls_writes_registers_and_stamps_end_to_end`
(repair.rs:862-909) asserts the pulled copy is registered with the REQUEST's shape
(Small, ec_k=4, ec_m=2); the RPC round-trip test (healing_service.rs:2497-2521) asserts
tier/ec_k/ec_m ride the wire into the enqueued request; unknown-tier degrade is code-
verified (healing_service.rs:1652) but not directly asserted by a test. LOW test-
coverage gaps (code inspected and correct; not blocked on): (1) no test asserts the g3/g4
enqueuer actually copies the registry entry's shape into the request (seed an EC k=4/m=2
Small-tier entry → assert the enqueued `ReRepRequest` carries it); (2) no test asserts the
dispatcher's tier wire-encode mapping (a pure function of `request.tier`); (3) the
integration test does not assert the acquiring node's REGISTERED tier/ec after repair (its
file/locations/read assertions would pass even under a Standard/1/0 misregistration). -->
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
<!-- REVIEW: verified — RUSTDOCFLAGS="-D warnings" cargo doc --no-deps for storage, durability,
membership, node is clean; doctests 24/90/36/7 pass. -->
<!-- REVIEW (iteration 3): re-verified with a forced rebuild of the four changed files
(RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p oceanfs-storage -p oceanfs-durability
-p oceanfs-node after `touch`) — clean. -->
- [x] **ADR:** ADR-0030 (target-pull + dedicated RPC, durable locations
      via refresh) satisfied; ADR-0029 §D5 (routing hint — selector
      consults manifests), §D6 (repair pacing via bounded concurrency +
      priority) satisfied
<!-- REVIEW (iteration 2): all four ADR-0030 Decisions + §D5/§D6 now satisfied.
Decision 3's holder-side handoff is IMPLEMENTED: `converge_holder_registry`
(crates/oceanfs-node/src/repair.rs:530-559) appends the acquiring target to the
holder's OWN `storage_locations` via `request_refresh_metadata` after an
accepted dispatch (repair.rs:497), and `live_copy_count` (reconcile.rs:136)
counts from `storage_locations`, so the g4 reconciler stops re-dispatching.
§D6 priority observability is IMPLEMENTED: two `oceanfs_repair_queue_depth`
gauges with {priority="announcement"|"reconciliation"} labels (repair.rs:182-
191) registered via the name+labels keyed registry (admin.rs:198). CAVEAT:
the holder-side convergence is not directly asserted by any unit or
integration test (the integration test only checks each node lists itself),
so re-dispatch-stoppage is verified only indirectly via logs. -->
<!-- REVIEW (iteration 3): the iteration-2 CAVEAT is CLOSED — the rewritten
integration test now asserts per-segment `storage_locations` on BOTH nodes
cover {node-b, node-c} for every at-risk segment (re_replication.rs:369-393),
directly asserting holder-side convergence (Decision 3) AND the worker's stamp.
The dispatcher live-holder filter now excludes data-dead holders
(`is_data_dead`, node repair.rs:327-333) with semantics identical to
reconcile.rs `membership_snapshot` (manifest has data pools AND all dead);
unknown/no-manifest nodes stay eligible (no stranding). One duplicate
dispatch round per under-replication is observed in runs (the data-dead
origin's own g4 loop re-dispatches after the holder's repair); the worker's
already-held idempotency absorbs it and each dispatcher converges its own
registry, so it terminates — no loop. Note: the fetch-side holder set is only
as fresh as the dispatcher's gossip view; a just-data-dead node can remain in
the request for ~1 gossip round and even serve bytes in tests (pool death is
simulated via health state, files remain readable) — the D5 stale-cache error
path covers production. -->
<!-- REVIEW (iteration 3b): ADR-0030 constraints re-checked against the shape
propagation — Decision 1/2 are UPHOLD: the dedicated RPC still carries routing intent only
(the shape fields are registry metadata, not data), the acquiring worker registers the copy
with the dispatcher's real seal-time shape, so the target's lifecycle entry is accurate from
the first moment (no dependence on a later AE/scrub correction — the marker's Option A).
Decision 3 (holder-side convergence via refresh) unchanged and still verified by the
integration test. The wire encoding matches ADR-0030's referenced segment-push protocol
(explicitly documented in proto/oceanfs/healing.proto:136-146). -->
- [x] **Perf:** 2.7/8.5 (semaphore-bounded concurrency), 8.1
      (FuturesUnordered for parallel holder fetch attempts), 1.3
      (pre-sized fetch buffers), 2.6 (bounded queue backpressure)
<!-- REVIEW (iteration 2): 2.7/8.5 semaphore + 2.6 bounded mpsc verified. 8.1 is a documented
DEVIATION: crates/oceanfs-durability/src/repair.rs:537 uses tokio JoinSet (abort_all
first-success-wins), not FuturesUnordered — justified (futures is not a durability dep;
healing_service.rs uses the same JoinSet pattern). The module doc at repair.rs:23-29 NOW
documents the JoinSet + abort_all deviation (stale claim fixed). 1.3 verified: the fetch buffer
is pre-sized to the chunk size — `BytesMut::with_capacity(64 * 1024)` (repair.rs:578), matching
the server's 65536-byte stream chunks (healing_service.rs:1190). -->
<!-- REVIEW (iteration 3): iteration-3 changes add no perf-rule violations — the fetch
attempt-deadline (repair.rs:570-620) races the initial RPC AND every stream.message() against
ONE `timeout_at` budget (OperationTimeouts::shard_fetch_ms, default 30 s — same field the heal
worker uses at heal/worker.rs:493, perf 4.5): a stalled holder cannot hang the attempt, and a
legitimate transfer only fails if the WHOLE stream exceeds the 30 s budget, in which case the
failure is safe (no partial write; merkle verification second line) and retried (×3) then
re-detected by g4. Parallel fetch attempts remain capped (MAX_PARALLEL_FETCHES=16 semaphore,
perf 8.5). -->
<!-- REVIEW (iteration 3b): the shape-propagation change adds no perf-rule violations — the
encode/decode matches are constant-time; the request gains 3×u32 on a metadata-only RPC
(negligible vs the 64 KiB streamed fetch). -->
- [x] **Integration:** the epic's "re-replication restores RF" DoD — a
      killed data pool's segments return to RF via announcement AND via
      reconciliation alone (both paths tested)
<!-- REVIEW (iteration 2): the g3 loss_announcement (1) and g4 reconciliation (1)
tests are deterministic and green (RF=3 → no eligible target → requests park →
pending_repairs grows) — they verify the DETECTION→ENQUEUE side only, as the
feature doc's out-of-scope states. The re_replication integration test
(crates/oceanfs-node/tests/re_replication.rs, 2 tests) PASSES but is FLAKY:
in 6/8 runs it passed WITHOUT any dispatch/worker-pull log, because the random
segment-id ring replica set excluded the owner A, so the seal-time replicator
had already placed copies on both surviving nodes (RF=2 exceeded) and there
was nothing to repair. The DoD's "both paths tested" is therefore not
RELIABLY verified — the test must force an under-replicated state (only one
surviving holder) to genuinely exercise dispatcher → RequestReReplication →
worker pull → stamp. One observed exercising run confirmed the full flow:
"re-replication dispatched to acquiring node" (Announcement, holders=2) →
"request accepted; worker will pull" → "fetched segment from holder
bytes=32768" → "re-replication succeeded", plus a Reconciliation-path
duplicate dispatch exercising worker idempotency. -->
<!-- REVIEW (iteration 3): BLOCKER CLOSED — the rewritten test is not vacuous.
(crates/oceanfs-node/tests/re_replication.rs) — PUT batches loop until the
at-risk precondition genuinely holds: symmetric difference of B's and C's
pre-kill .dat sets is non-empty (only segments whose ring replica set
contained A are at risk after A's pool death — ring sets {A,B}/{A,C} pre-kill
push to exactly one of B/C). An all-{B,C} outcome FAILS LOUDLY
(re_replication.rs:328-331), it cannot pass silently. Quiescence waits on all
three replicators drained + B/C file sets stable across 3×300 ms polls, then
snapshots. Post-kill assertions are PER-SEGMENT: every at-risk .dat on both B
and C (file convergence), every at-risk segment's storage_locations on BOTH
nodes covering {node-b, node-c} (registry convergence incl. the holder-side
append), and every PUT key GETs byte-identical through A. Post-kill, the only
mechanism that can place a new .dat on the non-holder is dispatcher → RPC →
worker pull, so a passing run proves the flow ran. INDEPENDENTLY VERIFIED (3
full runs + suite run): 2/2 tests green (~40 s each); dispatch logs observed
in every run — 4 Announcement dispatches + real worker fetches (65536 B full
segments) in the g3 test, 4 Reconciliation-only dispatches in the g4 test;
0 parked/permanently-failed/convergence-failed warnings. All 27 node
integration binaries green (loss_announcement, reconciliation,
segment_replication regressions included). Residual (low): one idempotent
duplicate round per segment (the data-dead origin's own g4 re-dispatches
once before its converge lands — absorbed by worker idempotency, terminates);
the batch precondition may panic (~(1/3)^(4k), k segments/batch) instead of
passing vacuously — loud failure, re-run note in the message. -->


## Deviations (accepted)

- **Target-pull over holder-push** (ADR-0030 Decision 1): the worker
  executes on the acquiring node, not the holder. The holder's
  `RepairSink` impl becomes a dispatcher.
- **`storage_locations` update rides `request_refresh_metadata`** rather
  than a new lifecycle event. The coordinator's refresh path already
  exists and is durable; extending its payload avoids a new event type
  while keeping the event-WAL the single durable writer.
- **Shape propagation rides `RequestReReplication`** (the
  `[review][implementation][critical]` marker at repair.rs, now
  RESOLVED): the dispatcher/enqueuer reads the source segment's
  seal-time tier/ec_k/ec_m from its own registry entry and sends them
  alongside the merkle root; the acquiring worker registers the pulled
  copy with that real shape (no hardcoded Standard/1/0 defaults, no
  dependence on a later AE/scrub correction).
- **Migration-plane isolation deferred** (ADR-0030 Decision 4): worker
  receives pool + membership injected so a later topology change is a
  wiring change only.
