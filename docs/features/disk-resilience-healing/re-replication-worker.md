---
feature: "Re-Replication Worker"
epic: "disk-resilience-healing"
status: in_progress
priority: high
owner: ""
dependencies: ["reconciliation"]
adr: [0029, 0030]
perf: [1.3, 2.7, 8.1, 8.5]
created: 2026-08-22
updated: 2026-08-23
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
- `pub struct ReRepRequest { origin: NodeId, segment_id: SegmentId, holders: Vec<NodeId>, reason: RepairReason, retry_count: u32, merkle_root: Option<HashOutput> }` — the `RepairSink` input (g3/g4 unchanged contract).
- `request_refresh_metadata(id, merkle_root, storage_locations: Option<SmallVec<[NodeId; 16]>>)`.

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
- [ ] **Tests:** all listed green (worker e2e, selector rules,
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
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
<!-- REVIEW: verified — RUSTDOCFLAGS="-D warnings" cargo doc --no-deps for storage, durability,
membership, node is clean; doctests 24/90/36/7 pass. -->
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
- [ ] **Integration:** the epic's "re-replication restores RF" DoD — a
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

## Deviations (accepted)

- **Target-pull over holder-push** (ADR-0030 Decision 1): the worker
  executes on the acquiring node, not the holder. The holder's
  `RepairSink` impl becomes a dispatcher.
- **`storage_locations` update rides `request_refresh_metadata`** rather
  than a new lifecycle event. The coordinator's refresh path already
  exists and is durable; extending its payload avoids a new event type
  while keeping the event-WAL the single durable writer.
- **Migration-plane isolation deferred** (ADR-0030 Decision 4): worker
  receives pool + membership injected so a later topology change is a
  wiring change only.
