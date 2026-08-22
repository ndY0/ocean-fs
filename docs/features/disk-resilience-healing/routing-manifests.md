---
feature: "Routing on Manifests (Read/Write Path)"
epic: "disk-resilience-healing"
status: proposed
priority: high
owner: ""
dependencies: ["failure-state-machine"]
adr: [0029]
perf: [2.4, 7.1]
created: 2026-08-22
updated: 2026-08-22
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
    successor; local write_degraded → 503;
  - unit (hint): sender skips a no-Healthy-data-pool target;
  - integration (local 3-node): mark node B `write_degraded` via the g2
    state machine (wal pool fault-injected) → new PUTs land on A/C only;
    reads of existing keys still served (read path unaffected by
    write_degraded).

### Out of Scope

- Status detection/state machine (g2) — this feature only *consumes* the
  flags.
- Announcement/reconciliation/healing (g3-g5).
- Capacity-aware *placement* within a node (Phase A f3/f5, already there).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | Write path manifest filters; local write_degraded 503; hint target filter |
| `oceanfs-node` | Read path filter live; manifest cache injection into the server |

## Interface (Public API)

- `WriteCoordinator::with_manifest_cache(cache: Arc<ManifestCache>)`.
- `ReadCoordinator::with_manifest_cache(cache: Arc<ManifestCache>)` (f7
  stub made live).
- `pub fn can_accept_writes(manifest: &NodeManifest) -> bool` — the shared
  filter (not write_degraded AND ≥1 Healthy data pool).

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

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-server`,
      `oceanfs-node`
- [ ] **Tests:** all listed green (write skips, 503, hint filter, read
      filter, integration)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D5 (cached routing = hint, failover on error) +
      §D3 role consequences (wal Dead → write rejection) satisfied
- [ ] **Perf:** 2.4 (manifest cache is ArcSwap — lock-free reads on the
      hot path), 7.1 (no locks added to the write/read paths; filters are
      manifest-field reads)
- [ ] **Integration:** the epic's "Degraded pool routes reads/writes
      around it" DoD — with a pool Degraded (not Dead), reads/writes avoid
      it with NO re-replication storm (g4 enqueues nothing for Degraded)

## Deviations (accepted)

- **`can_accept_writes` is node-granular, not pool-granular.** The
  manifest carries per-pool status but the write path selects *nodes*;
  a node with ≥1 Healthy data pool remains a valid target even if one of
  its pools is Degraded (its local placement picks the healthy pool).
  Pool-granular write routing is a Phase C refinement.
