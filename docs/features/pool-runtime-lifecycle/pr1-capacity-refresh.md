---
feature: "Pool Capacity Refresh"
epic: "pool-runtime-lifecycle"
status: proposed
priority: critical
owner: ""
dependencies: []
adr:
  - 0017-durability-task-abstraction
  - 0029-storage-pools-disk-resilience
  - 0033-manifest-aware-peer-selection
perf: []
created: 2026-09-12
updated: 2026-09-12
---

# Pool Capacity Refresh

## Summary

Add the **missing periodic caller** for `PoolRegistry::refresh_capacity` so
every registered pool's free/total capacity is re-read (`statvfs`) on a
node-global interval instead of only at boot, at runtime attach, and after an
intra-node relocation. A background ticker performs one `statvfs` per
registered pool per interval and updates the existing pool atomics and
`oceanfs_pool_bytes_*` gauges; the gossiped `NodeManifest.capacity_free_bytes`
is refreshed from the same values within the interval (the re-declare
mechanism is an implementer OQ), so placement, repair-target selection, and
f4's C2a/C2b capacity dataset stop operating on a snapshot that can be hours
old. The work is product-side: a new `[storage]
capacity_refresh_interval_secs` knob (`0` disables, default suggested 10) in
`oceanfs-core` plus the background task registration in `oceanfs-node`; the
storage crate's refresh path already exists and is unchanged.

This is **pr1 of the `pool-runtime-lifecycle` epic** (priority 1 of 2). pr2
(dead-pool runtime recovery) follows; the paused `fleet-degradation` f4
resumes after both land with review PASS.

## Root Causes & Evidence (verified 2026-09-12 at HEAD b3eba7f)

### R1 — `refresh_capacity` has no periodic caller

- `PoolRegistry::refresh_capacity` (`crates/oceanfs-storage/src/pool/mod.rs:1300-1310`)
  iterates the registered pools, calls `statvfs_capacity(pool.root())`, and
  writes the pool atomics plus the metric gauges.
- The only callers are:
  - boot/attach (`crates/oceanfs-storage/src/pool/mod.rs:1551-1583`; the root
    probe/capacity read is at `:1565-1566`), and
  - the intra-node drain mover after each successful relocate
    (`crates/oceanfs-storage/src/drain/intra_node.rs:241`), described there as
    "fresh statvfs so the next selection sees the space this copy consumed".
- No ticker, no interval task: `grep refresh_capacity` finds only the
  definition, the two call sites above, doc references, and tests.
- The module docs already assume the missing tick — strong evidence the
  periodic caller was intended but never landed:
  `pool/mod.rs:34-35` ("registration + the maintenance-tick capacity
  refresh") and `pool/mod.rs:1455-1462` ("The node's maintenance task
  normally drives capacity via `PoolRegistry::refresh_capacity` (`statvfs`)").

### R2 — Consumers read the stale snapshot

- **Placement** filters on `Healthy`, `!write_degraded`, `free_bytes >
  MIN_FREE_HEADROOM_BYTES` and scores `max free_bytes / weight` with
  pre-sized candidate vecs and integer math — no I/O in the selection itself
  (`crates/oceanfs-storage/src/pool/placement.rs:99-103`, `:179-192`; perf
  notes at `:112-114`).
- **Metrics** `oceanfs_pool_bytes_free{pool_id}` and
  `oceanfs_pool_bytes_total{pool_id}` are set only by refresh/setter paths
  (`pool/mod.rs:743-746`, `:789-798`; `set_pool_capacity` at `:1455-1495`).
- **Gossiped manifest** `capacity_free_bytes` is copied from
  `pool.free_bytes()` (`crates/oceanfs-node/src/pool_manifest.rs:89-96`),
  which feeds capacity-aware repair/peer selection (ADR-0033,
  `crates/oceanfs-node/src/repair.rs:124`, `:195`).
- The manifest is only **rebuilt/re-declared** at boot/join and recovery
  completion (`crates/oceanfs-node/src/modules/membership.rs:382-384`,
  `crates/oceanfs-node/src/node.rs:502-510`), on pool-set/lifecycle changes
  (attach/drain/detach hooks,
  `crates/oceanfs-node/src/modules/server.rs:529`, `:558`, `:582`, `:628`;
  `node.rs:651-662`), and on health **status** events
  (`crates/oceanfs-node/src/health.rs:166-176`).
  A capacity-only change therefore has no re-declare trigger today: without
  one, `refresh_capacity` would update the metrics/placement immediately but
  peers would keep seeing the last manifest's `capacity_free_bytes`. Whether
  the ticker should rebuild/re-declare the manifest itself (or the refresh
  should emit a health-style event) is an implementer OQ.

### R3 — Consequence under load

Disk space is consumed continuously by segment seals, WAL/event-WAL growth,
and recovery activity. Without a periodic refresh the snapshot decays from
the last probe until a relocate or restart, so:
- placement keeps selecting a pool that has since filled (or avoids one that
  has since freed space), and
- the manifest advertises stale `capacity_free_bytes` to peers, biasing
  repair-target selection, and
- the f4 C2a/C2b decision dataset ("per-node free capacity before/after each
  dynamic op", see
  [f4 C2a/C2b](../fleet-degradation/f4-pool-degradation-under-load.md)) would
  be built on stale numbers. After this feature, f4's dataset consumes the refreshed
  `oceanfs_pool_bytes_*`/manifest values instead of an SSH `df` probe.

### Decision and rejected alternative (recorded)

- **Approved 2026-09-12 (user): fix product-side with a periodic background
  task.**
- **Rejected: hot-path / probabilistic refresh** (refresh on a fraction of
  reads, or inside the write/placement path). Rationale: reads do not consume
  capacity, so read-triggered refresh misses exactly the write-driven growth
  this defect is about; and a `statvfs` syscall in the write/placement path
  makes placement latency non-deterministic and violates the perf
  guidelines' no-syscall hot-path discipline (the selection today is pure
  integer scoring, `placement.rs:112-114`, perf 1.x/7.1). The background
  ticker is deterministic, cheap (one syscall per pool per interval), and
  keeps all hot paths untouched.

## Scope

### In Scope

- **Background ticker**: a periodic task that calls
  `PoolRegistry::refresh_capacity()` for all registered pools every
  `capacity_refresh_interval_secs`, registered at the composition root so it
  is cancelled with the node's other background loops.
- **New node-global config**: `[storage] capacity_refresh_interval_secs`
  (default suggested **10**; `0` disables the periodic refresh), parsed and
  validated with a clear config error for unusable values. Note the
  `StorageConfig` derive caveat: the field must default to 10 in **both**
  serde deserialization and `StorageConfig::default()` (see Interface).
- **Scheduling under the existing background-task system**: the implementer
  grounds the exact seam under the ADR-0017 `DurabilityTask`/budget machinery
  in `crates/oceanfs-node/src/modules/durability.rs` (see Open Questions);
  no new scheduling framework.
- **Fresh values reach the consumers**: `refresh_capacity` updates the pool
  atomics and gauges immediately, so placement and metrics see fresh values
  on the next read; the gossiped manifest must also carry the refreshed
  `capacity_free_bytes` within the interval — the mechanism (ticker rebuilds
  and re-declares the manifest, or the refresh emits a health-style status
  event that triggers the existing re-declare path) is an implementer OQ
  (see below). Within one interval, placement, metrics, and peers see the
  refreshed capacity.
- **No hot-path syscall**: no `statvfs` (or any I/O) is added to the
  read/write/placement paths; placement stays pure integer scoring.
- **Tests** (see Definition of Done): config parse/validate, refresh effect
  (gauge + manifest after a tick on a temp-backed pool), disabled-interval
  behavior, and no regression in the existing suites.
- **Docs**: the new knob is documented (config reference) and every new or
  changed `pub` item carries `# Examples`.
- **Cross-link to f4**: its C2a/C2b dataset consumes the refreshed metrics
  instead of an SSH `df` probe; no f4 doc/scenario change is made in this
  feature (f4 is paused; the hand-off is recorded here).

### Out of Scope (for this feature)

- Hot-path / probabilistic / read- or write-triggered refresh (rejected
  alternative, recorded above).
- Capacity-based ring re-weighting or proactive rebalance (C2a/C2b
  implementation stays in `disk-resilience-capacity`; this feature supplies
  the fresh input only).
- Any f4 scenario or assertion change — f4's scope is unchanged and it
  re-provisions when the paused epic resumes.
- Any new metric beyond the existing `oceanfs_pool_bytes_*` gauges, unless
  grounding finds the ticker's errors unobservable (then the minimal
  counter/gauge is a recorded deviation).
- Fleet/load runs while paused (PIPELINE §6 / §7; no cloud provisioning).
- Refreshing config-declared-but-unregistered or detached pools (the ticker
  iterates the live registry only).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | `config/storage.rs`: new `StorageConfig::capacity_refresh_interval_secs` field (default 10, `0` disables) with field docs/`# Examples`, serde default, and validation; config-reference docs. |
| `oceanfs-node` | Register the background ticker (exact seam OQ: a `DurabilityTask`/scheduler adaptor in `modules/durability.rs`, or a dedicated loop in the storage/background module) and pass the configured interval; cancellation through the node's existing shutdown token/`BackgroundTasks`. |
| `oceanfs-storage` | No functional change expected — `refresh_capacity` and the gauges already exist. Only if the chosen seam needs a thin adapter/accessor; none is planned. |
| `oceanfs-server` | None. |
| `e2e` | None — f4 (when resumed) consumes `oceanfs_pool_bytes_*`/manifest capacity in its C2a/C2b dataset instead of an SSH `df` probe (cross-link). |

## Interface (Public API)

### Config surface (sketch — exact schema details are implementer OQs)

```toml
[storage]
# Capacity refresh cadence in seconds. One statvfs per registered pool per
# interval; 0 disables the periodic refresh (boot/attach probes still run).
capacity_refresh_interval_secs = 10   # default 10
```

```rust
// oceanfs-core/src/config/storage.rs (sketch)
pub struct StorageConfig {
    // ... existing fields ...
    /// Capacity refresh cadence in seconds (`0` disables the periodic
    /// refresh; boot/attach probes are unaffected).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::StorageConfig;
    ///
    /// assert_eq!(StorageConfig::default().capacity_refresh_interval_secs, 10);
    /// ```
    pub capacity_refresh_interval_secs: u64,
}
```

Caveat for the implementer: `StorageConfig` currently derives `Default`
(`crates/oceanfs-core/src/config/storage.rs:582-596`) and carries
`#[serde(default, ...)]`. A plain `u64` field would therefore default to `0`
(disabled) through both paths. The field needs a field-level
`#[serde(default = "...")]` **and** a `StorageConfig` default that yields 10
(manual `Default` impl or explicit defaulting in the accessor/path that reads
it) so serde and `StorageConfig::default()` agree. Validation rejects
unusable values with a clear config error; `0` is explicitly accepted as the
documented disable value.

### Reused (unchanged)

- `PoolRegistry::refresh_capacity()` (`crates/oceanfs-storage/src/pool/mod.rs:1300`)
  — updates pool atomics + gauges, I/O outside the registry lock.
- `PoolMetrics::bytes_free`/`bytes_total` gauges (`pool/mod.rs:743-746`).
- `pool_manifest::build_node_manifest` (`crates/oceanfs-node/src/pool_manifest.rs:53`)
  rebuilds the manifest from the live registry (so a re-declared manifest
  carries the refreshed `capacity_free_bytes`).

### Node-internal (sketch; names finalized in implementation)

- A background task/adaptor holding `Arc<PoolRegistry>` + the interval that
  calls `refresh_capacity()` per tick. If implemented as a `DurabilityTask`,
  it follows the existing adaptor shape in
  `crates/oceanfs-node/src/modules/durability.rs:522-587`; otherwise a
  dedicated ticker registered with the other background loops. Whether a
  statvfs-only cycle should take a Tier-1 permit is part of the seam OQ
  (ADR-0017 amendment meters heavy `.dat`/metadata I/O, which this is not).

## Data Flow

```
boot / attach → capability probed once (existing)
        │
        ▼
background ticker: every `[storage] capacity_refresh_interval_secs` (0 = disabled)
  → PoolRegistry::refresh_capacity()
      → statvfs_capacity(pool.root()) per registered pool (outside all locks)
      → pool.set_capacity → free_bytes / total_bytes atomics
      → oceanfs_pool_bytes_free / oceanfs_pool_bytes_total gauges
      → NodeManifest re-declare (mechanism OQ): capacity_free_bytes
          → placement: select_data_pool / select_from_pools (ADR-0029 §D5)
          → repair/peer selection: manifest-aware (ADR-0033)
          → f4 C2a/C2b dataset: metrics/manifest instead of SSH `df`
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in the affected
      crate(s) (`oceanfs-core`, `oceanfs-node`); the refresh task is
      registered at the composition root and cancelled with the node's
      shutdown; no `statvfs`/I/O is added to the read/write/placement hot
      paths; no test-only hooks.
- [ ] **Tests:** config parse/validate tests for
      `capacity_refresh_interval_secs` (default 10, an explicit value, `0`
      accepted as disabled, invalid values rejected with a clear error); a
      refresh-effect test on a temp-backed pool (write into a pool root,
      run one tick with a short test interval, assert the
      `oceanfs_pool_bytes_*` gauge/both pool atomics and the manifest's
      `capacity_free_bytes` reflect the new value); a disabled-interval (`0`)
      test showing capacity stays pinned to the last probe; no regression in
      the existing suites (`oceanfs-core`, `oceanfs-storage`,
      `oceanfs-node`, `oceanfs-server`; RocksDB-affected crates serialized
      with `--test-threads=1` per PIPELINE §4.6).
- [ ] **Docs:** every new/changed `pub` item has `# Examples`;
      `#![deny(missing_docs)]` passes; the config reference documents the new
      knob, its default, and the `0` disable semantics.
- [ ] **ADR:** ADR-0017 (scheduled background work; slow cycles skip rather
      than backlog), ADR-0029 §D2/D5 (capacity in the manifest; capacity-aware
      placement), and ADR-0033 (manifest-aware selection) constraints are
      addressed; the rejected hot-path/probabilistic alternative is recorded
      in this doc.
- [ ] **Perf:** `perf: []` — the refresh is one `statvfs` per registered pool
      per interval with no lock held across the I/O (the existing
      `refresh_capacity` shape), there is no hot-path change, and no
      throughput/latency claim is made.
- [ ] **Integration:** a node-level integration test exercises the complete
      path (registry → ticker → refreshed gauge + manifest on a temp-backed
      pool); f4's C2a/C2b dataset consumes the refreshed
      `oceanfs_pool_bytes_*`/manifest values instead of an SSH `df` probe
      (cross-link [f4](../fleet-degradation/f4-pool-degradation-under-load.md))
      when the paused epic resumes.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

Implementation-shape only — the periodic-task decision and the rejected
alternative above are settled.

- **Exact schedule seam.** ADR-0017 `DurabilityTask` under the existing
  scheduler (`crates/oceanfs-node/src/modules/durability.rs`, as the other
  Tier-1 adaptors) vs a dedicated lightweight ticker in the
  storage/background module. If it is a `DurabilityTask`: does a
  statvfs-only cycle take a Tier-1 permit (the scheduler's only path) given
  the ADR-0017 amendment's metering rule ("work that performs `.dat`
  reads/writes or metadata-CF batch writes")? Record the choice and
  rationale.
- **Default interval and validation bounds.** Confirm 10s as the default;
  define the accepted range and the rejection message, and confirm the
  serde-vs-derive default handling (see Interface caveat).
- **Manifest re-publish.** There is no periodic manifest publish today: the
  manifest is re-declared at boot/join, on pool-set changes, and on health
  **status** events (`health.rs:166-176`). Decide how the ticker makes peers
  see fresh capacity: the ticker rebuilds + re-declares the manifest itself
  (the attach-hook shape, `build_node_manifest` + `set_self_manifest`,
  `crates/oceanfs-node/src/modules/server.rs:515-536`), or the refresh emits
  a health-style event that rides the existing re-declare path. Consider
  version-bump/gossip churn when nothing changed (ties to the next OQ).
- **Skip unchanged values.** Whether the ticker should skip metric/gossip
  churn when `statvfs` returns the same values (e.g., only write atomics on
  change). It must not skip the syscall if the answer is no; record the
  chosen behavior.

## Deviations (accepted)

_None yet — filled at implementation close._
