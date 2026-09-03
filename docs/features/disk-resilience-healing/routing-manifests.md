---
feature: "Routing on Manifests (Read/Write Path)"
epic: "disk-resilience-healing"
status: done
priority: high
owner: ""
dependencies: ["failure-state-machine"]
adr: [0029]
perf: [2.4, 7.1]
created: 2026-08-22
updated: 2026-09-03
---

# Routing on Manifests (Read/Write Path)

## Summary

Make Phase A's cached routing state (f7) *live*: the write path
(`WriteCoordinator`, oceanfs-server/src/write/coordinator.rs) and read path
(`ReadCoordinator`, node.rs:1235-1253) consult the per-peer `ManifestCache`
(f7) to (a) avoid `write_degraded` nodes and nodes with no Healthy data
pool when selecting replica targets, and (b) fail over to the next replica
on I/O error regardless of the cache (the cache is a hint, not a
dependency — ADR-0029 §D5). This feature activates the filters that f7
built as observationally-neutral stubs.

## Scope

### In Scope

- `oceanfs-server` write path (`WriteCoordinator`, observed at
  coordinator.rs:1228-1346):
  - `with_manifest_cache(cache: Arc<ManifestCache>)` — injected by the
    node (f7's cache lives in `oceanfs-node`; the server is wired from the
    node, node.rs:1089+).
  - **replica target selection (pinned)**: `forward_write` and the
    replication target loop (coordinator.rs:1274-1346) iterate the ring
    replica set — skip candidates whose manifest reports
    `write_degraded` OR zero Healthy data pools; if the primary target is
    skipped, fall through to the next ring successor (same failover
    principle as the read path).
  - The local write path must also respect the LOCAL node's `write_degraded`
    (wal pool dead → this node cannot journal): reject with 503 before WAL
    append (g2 sets the flag; this feature enforces it at the HTTP/S3
    boundary).
  - Hint target preference: hinted-handoff debt is per-failed-target by
    construction (a hint exists because node B failed the write); the
    SENDER does not re-target hints — the receiving node's local placement
    picks the pool (Phase A f5). The manifest influences the WRITE path
    only (avoid selecting B as a target in the first place when B's
    manifest shows no Healthy data pool); a hint already enqueued for B
    is delivered when B recovers (the delivery sweep, node.rs:1740-1817,
    is unchanged).
- `oceanfs-node` read path (`ReadCoordinator`, node.rs:1235-1253):
  - the f7 node-granular filter becomes live: candidates with zero Healthy
    data pools are skipped; the fetch-strategy fallback (LocalFirst →
    remote) already exists and is preserved — on I/O error, move to the
    next replica (existing behavior, now informed by manifests).
- Metrics: `oceanfs_routing_manifest_skips_total{path}` (read/write/hint),
  `oceanfs_routing_failover_total` (f7, now live).
- Tests:
  - unit (server): a `write_degraded` candidate is skipped; a no-Healthy-
    data-pool candidate is skipped; fall-through lands on the next ring
    successor; local write_degraded → 503; metadata-Dead read/write → 503;
  - unit (hint): sender skips a no-Healthy-data-pool target;
    `manifest_skips_total{path}` counts per-path exclusions;
  - integration (`routing_manifests.rs`): mark node B `write_degraded` via
    the g2 state machine (wal pool fault-injected) → writes through A keep
    succeeding and reads of seeded keys keep serving; metadata-Dead node
    rejects local reads AND writes with 503.

### Out of Scope

- Status detection/state machine (g2) — this feature only *consumes* the
  flags.
- Announcement/reconciliation/healing (g3-g5).
- Capacity-aware *placement* within a node (Phase A f3/f5, already there).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | Write path manifest filters (f7); local write_degraded + metadata-Dead 503 gates; `Error::ServiceUnavailable` |
| `oceanfs-node` | Read path filter live (f7); same-`PoolRegistry` injection into both coordinators; `Node::node_unavailable()` reads the registry; `can_accept_writes`; `manifest_skips_total{path}` counters |
| `oceanfs-storage` | `PoolRegistry::node_serves_requests()` / `accepts_writes()` — the shared availability derivation |

## Interface (Public API)

- `WriteCoordinator::with_routing_hint(hint: Arc<dyn RoutingHint>)` (f7)
  + `with_pool_registry(registry)` — the local availability gate.
- `ReadCoordinator::with_routing_hint(hint: Arc<dyn RoutingHint>)` (f7)
  + `with_pool_registry(registry)` — the local availability gate.
- `pub fn can_accept_writes(manifest: &NodeManifest) -> bool` — the shared
  filter (not write_degraded AND ≥1 Healthy data pool).
- `PoolRegistry::node_serves_requests() -> bool` and
  `PoolRegistry::accepts_writes() -> bool` — the shared LOCAL availability
  derivation both coordinators' gates consult (metadata pool Dead → the
  node serves nothing; wal `write_degraded` → no new writes).
- `Error::ServiceUnavailable(&'static str)` → `503 ServiceUnavailable`.

## Data Flow

```
PUT ──▶ WriteCoordinator: ring replica set
   └─ can_accept_writes(candidate.manifest)? no ──▶ next successor (f7 cache)
   └─ local write_degraded? ──▶ 503 before WAL append
GET ──▶ ReadCoordinator: fetch strategy candidates
   └─ zero Healthy data pools? ──▶ next candidate
   └─ I/O error ──▶ failover (cache is hint, error is truth)
hint enqueue ──▶ can_accept_writes(target.manifest)? no ──▶ next replica
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-server`,
      `oceanfs-node` (+ storage — the shared availability derivation)
      <!-- REVIEW: independently re-ran `cargo build --workspace --all-targets` → clean (2026-09-03); the only warning is a pre-existing unused test fn in oceanfs-durability (hint_wal.rs:848), outside this feature. -->
- [x] **Tests:** all listed green (write skips, 503, hint filter, read
      filter, integration)
      <!-- REVIEW: verified runs (--test-threads=1): storage lib 426 pass (incl. availability_derives_from_metadata_and_wal_pools, availability_defaults_open_without_role_pools), server lib 235 pass (incl. local_availability_gate_rejects_when_metadata_dead_or_wal_write_degraded, read_gate_rejects_when_metadata_pool_dead, service_unavailable_maps_to_503), node lib 66 pass + 2 ignored (incl. manifest_skips_count_per_path), ALL 29 node integration binaries pass incl. routing_manifests 2/2. NOTE: `cargo test -p oceanfs-server --tests` is not green on this machine — grpc_services::swim_death_detection_within_timeout and 2×replicated_hlc fail identically at HEAD (pre-existing, unrelated to this feature). -->
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
      <!-- REVIEW: rustdoc clean for oceanfs-storage + oceanfs-node (RUSTDOCFLAGS=-D warnings). oceanfs-server rustdoc still fails on 2 PRE-EXISTING links (RING_PROBE_HASHES admin.rs:325, HintObjectApplier coordinator.rs:1938) — verified present at HEAD via stash; unrelated to this change. -->
- [x] **ADR:** ADR-0029 §D5 (cached routing = hint, failover on error) +
      §D3 role consequences (wal Dead → write rejection) satisfied
      <!-- REVIEW: §D5 hint-not-dependency verified (fetch fallthrough on error, forward falls back to first alive when all candidates excluded, coordinator.rs:557-569/891-898); §D3 verified: metadata Dead → 503 gate at both coordinators' entry, wal write_degraded → 503 gate on the LOCAL write branch only (write/coordinator.rs:583-587), hints Dead → enqueue rejection (hints_pool_accepts). -->
- [x] **Perf:** 2.4 (manifest cache is ArcSwap — lock-free reads on the
      hot path), 7.1 (no locks added to the write/read paths; filters are
      manifest-field reads)
      <!-- REVIEW: routing_cache.rs:61 uses ArcSwap (2.4); gates read atomics + a short parking_lot RwLock clone of one Arc per request — no lock held across I/O (7.1). -->
- [x] **Integration:** the epic's "Degraded pool routes reads/writes
      around it" DoD — with a pool Degraded (not Dead), reads/writes avoid
      it with NO re-replication storm (g4 enqueues nothing for Degraded)
      <!-- REVIEW: routing_manifests.rs drives Dead (wal/metadata), not Degraded; the Degraded-pool exclusion is covered indirectly by the healthy_data_pools predicate tests (routing_cache.rs node lib) + g4/g5 Degraded tests (failure_state_machine.rs, loss_announcement.rs, re_replication.rs). No dedicated Degraded-data-pool routing integration scenario in this feature. -->

## Deviations (accepted)

- **`can_accept_writes` is node-granular, not pool-granular.** The
  manifest carries per-pool status but the write path selects *nodes*;
  a node with ≥1 Healthy data pool remains a valid target even if one of
  its pools is Degraded (its local placement picks the healthy pool).
  Pool-granular write routing is a Phase C refinement.
- **Local availability is derived from the shared `PoolRegistry`, not a
  mirrored flag.** g2's health monitor writes pool status/`write_degraded`
  into the registry (the single source of truth); both coordinators gate
  on `PoolRegistry::node_serves_requests()` / `accepts_writes()`, and
  `Node::node_unavailable()` reads the registry. The previous
  `Arc<AtomicBool>` mirror in node.rs is removed — no duplicated state.
- **`oceanfs_routing_manifest_skips_total{path}` is counted in the
  `ManifestCache::RoutingHint` impl** (incremented when the filter
  excludes a candidate), keeping the skip metric next to the decision.
- **The write-degraded routing integration test asserts cluster-level
  behavior** (writes keep succeeding + reads keep serving while B is
  `write_degraded`) rather than B's data-dir file count — B's data pool
  may still receive sealed-segment pushes from the seal-time backbone
  (a different feature), which is out of this feature's scope. The
  deterministic per-candidate exclusion is asserted by the coordinator
  unit test `write_target_selection_skips_excluded_peers` (an excluding
  hint causes the replica fan-out to skip those peers while a local
  write still succeeds) plus the routing-hint predicate tests.
  <!-- REVIEW: wording nit — RESOLVED (implementer, post-review): a
  server-coordinator unit test `write_target_selection_skips_excluded_peers`
  (write/coordinator.rs) now injects an excluding RoutingHint and asserts the
  replica fan-out skips the excluded peers while a local write still succeeds;
  the deviation text above was updated to reference it. The routing-hint
  PREDICATE level remains covered by ManifestCache unit tests + the node
  integration routing_cache.rs. -->
