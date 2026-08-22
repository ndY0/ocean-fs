# ADR-0029: Storage Pools — Disk-Granular Resilience with Node-Granular Membership

**Status:** Proposed
**Date:** 2026-08-22
**Deciders:** OceanFS architecture team

---

## Context

### The single-disk single-point-of-failure

Every OceanFS node stores all of its data under one `{data_dir}` root:
`segments/`, `wal/`, `event-wal/`, `metadata/` (RocksDB), `hints/`,
`membership_state.toml`. Consequences:

1. **No multi-disk layout** — a node's capacity is bounded by one disk, and
   WAL/metadata traffic shares a filesystem with segment I/O (latency
   contention, single durability domain).
2. **No failure classification** — a disk error surfaces as a generic
   `io::Error`; SWIM sees a "flaky node" and may suspect/evict a node whose
   other disks are perfectly healthy.
3. **Wrong failure granularity** — treating disk death as node death
   re-replicates *everything* on the node: a storm sized 1/N of the ring
   instead of 1/(N×disks), plus needless membership churn.
4. **No read/write routing awareness** — peers cannot route around a dying
   disk; the write path may place replicas onto it (torn-write risk even with
   WAL recovery).

### Scale context

OceanFS targets large deployments (100s–1000s of nodes; SWIM/gossip
membership). At that scale disk failure is a **routine event**: 500 nodes ×
10 disks → a disk dies every few days. The failure path must be automatic,
surgical, and cheap. Heterogeneous hardware (nodes with 4–16 disks of
different sizes/technologies) is the default, so placement must be
capacity-aware or it silently skews forever.

### The current resilience machinery (what we extend, not replace)

The cluster already heals node failure: replication (RF), hinted handoff
(ownership + delivery), data WAL (torn-write recovery), hint WAL
(self-healing replay), ADR-0028 membership plane (dedicated SWIM probes +
gossip). Disk resilience extends this machinery one granularity down.

### Forces

- **Membership state must stay small.** SWIM gossip carries per-node state;
  making each disk a ring member (Ceph OSD model) grows state to
  nodes × disks (10,000+ entries) and turns every rebalance into ring-table
  churn. For a gossip-based protocol this is the wrong place to spend state.
- **Failure isolation must be disk-granular.** A disk death must not evict a
  node or re-replicate its whole ring share.
- **Failure semantics are typed.** "Temporarily unreachable" (replicas
  intact, heal on rejoin) ≠ "confirmed loss" (replicas destroyed,
  re-replication is urgent) ≠ "slow/erroring" (route around, don't storm).
- **Announcement (push) alone is insufficient.** The announcer can die
  mid-broadcast or the network can swallow the message; under-replication is
  a data-loss risk and cannot afford non-propagation. A periodic
  reconciliation (pull) must guarantee RF restoration independently.
- **Routing is a hint, not a dependency.** Cached manifests may be stale in
  either direction; I/O errors must be the truth that routes around them.
- **Adding capacity must not require a restart.** Operators add disks to
  live nodes; the design must support runtime pool attach.
- **The WAL is local acceleration, not the system of record.** Durability's
  source of truth is the replica set; the local WAL only accelerates
  checkpointing. A replaced WAL device must therefore be survivable.
- **The metadata store is the node's only index.** Key → segment mapping
  lives in RocksDB; losing it makes the node's own segments unrecoverable
  junk locally — recovery must come from replicas.
- **Open trust model.** No authentication between nodes; decisions must not
  make spoofing easier.
- **Fleet is small (3–10 nodes) in tests, large in intent.** Protocol
  completeness and cost-shape at scale matter; test topology is a
  resource constraint, not a design target.

## Decision

**OceanFS introduces storage pools: a per-node disk abstraction where the
pool is the unit of placement, routing, failure semantics, and topology
configuration. Membership (SWIM) stays node-granular; ownership and loss
accounting live at pool granularity in the data plane.**

### D1. Pool-granular ownership under node-granular membership

- The SWIM plane (ADR-0028) is unchanged: probes, suspicion, incarnation,
  gossip — all node-granular.
- Each node declares **pools** (topology config, §D8). The ring still maps
  ranges to nodes (capacity-weighted vnodes); each node's placement layer
  spreads its ranges across its pools (role-aware, weight-aware,
  least-free-capacity within role).
- The pool is the unit of information: routing, placement, failure
  semantics, and config all speak in pools.

### D2. Versioned `NodeManifest` gossip attribute

Each node's gossip state carries a compact, schema'd, versioned manifest:

```rust
NodeManifest {                    // node attribute in gossip
    incarnation: u64,             // ties to SWIM incarnation; restart re-declares
    pools: Vec<PoolManifest>,     // one per configured pool
}

PoolManifest {
    id: u32,                      // stable pool id (topology config order)
    role: PoolRole,               // data | wal | metadata | hints
    status: PoolStatus,           // Healthy | Degraded | Dead
    write_degraded: bool,         // role consequence flag (D3)
    capacity_free_bytes: u64,     // capacity-aware placement
    weight: u32,                  // placement weight
    ext: Option<PoolExt>,         // reserved: failure-domain tags, SMART, ...
}
```

Cost: O(pools/node) — 5–20 entries. Schema + versioning from day one so the
wire format never forces a redesign.

### D3. Typed failure semantics with role-aware consequences

State machine:

```
Healthy ─(trend/spike)─▶ Degraded ─(confirmed loss)─▶ Dead
   ▲                         │
   └────(clean window)───────┘
```

- **Degraded is a suspicion, Dead is a confirmation.** Dead requires
  *confirmed loss*: ENOENT on an owned segment, EIO on fsync, device unplug.
  Latency alone never confirms Dead.
- **Detection is trend-based and tech-aware.** Absolute thresholds are a
  fast path only; the primary signal is a monotonic-worsening trajectory
  (error rate or latency doubling per window) so a disk failing
  exponentially *below* thresholds is still caught. Erratic/intermittent
  errors accumulate into the trend and SMART counters rather than flapping
  state. Technology defines the error profile: `tech = hdd` (SMART
  reallocated/pending sectors), `ssd`/`nvme` (ECC, wear), `cloud-ephemeral`
  (I/O signals only). Windows, baselines, and signal sets differ per tech
  with built-in defaults.
- **Role-aware consequences.** The same Dead status means different things
  per role (`write_degraded` in the manifest):

| Role | Dead consequence (local) | Cluster behavior | Recovery |
|---|---|---|---|
| data | Range copies lost; reads/writes of them fail → failover | Announce + targeted re-replication; new writes avoid node if no healthy data pool remains | Pool returns → healthy; old segments GC'd; placement resumes |
| wal | Cannot durably accept new writes; reads continue | `write_degraded`; coordinators route writes elsewhere | Fresh/replaced WAL + catch-up from replicas (D7) |
| metadata | Node serves nothing | Full unavailability announcement; peers re-replicate **without waiting for SWIM suspicion timeouts** (probes are socket-only — the attribute must drive it) | Fresh store + re-replication-in (D7) |
| hints | Cannot persist new debt | Avoid as hint target; reconciliation rebuilds any lost debt | Fresh hint WAL; debt rebuilt by reconciliation |

### D4. Propagation: targeted push announcement + periodic reconciliation

- **Loss announcement (push, fast path).** Pool P dies on node A → A emits a
  loss announcement carrying the **compact affected-range set** (ranges that
  lived on P), fanned out **only to the ring's replica holders** for those
  ranges (RF-bounded, not cluster-wide). Peers cross-check their local
  hold-set and re-replicate immediately.
  - Correction note: the ring maps ranges to nodes only; peers cannot derive
    "A's pool P ranges" locally — the exact mapping is A's local
    segment→pool table. Hence the range set rides the announcement (a rare
    event), never the NodeManifest (state).
- **Periodic reconciliation (pull, safety net).** Each node, on a fixed 5s
  tick (reusing hint-sweep machinery), processes a risk-prioritized work
  queue of its owned ranges: *live replica count < RF?* → repair.
  - Prioritized by live-copy count: single-copy ranges first, double-copy
    (RF=3) second, healthy ranges at slow background cadence (drift
    detection).
  - Does **not** depend on any announcement having arrived.
  - **Repair loop, not detection loop**: failed repairs retry next tick.
  - Worst-case RF-restoration bound after announcement loss =
    5s + repair time (single-copy ranges).

### D5. Cached routing state — hint, not dependency

- Peers cache the last-known NodeManifest per node (versioned, atomically
  replaced on fresher manifest). Stale-but-present beats absent.
- **Failover on error**: if the cached target fails at I/O time (timeout,
  connection error, disk error), the read/write falls through to the next
  replica regardless of the cache. The cache optimizes; the error path
  guarantees.
- Read path: prefer healthy pools, serve normally on `write_degraded` nodes.
- Write path: replica selection avoids degraded/dead pools and
  `write_degraded` nodes; respects role + capacity. Hinted-handoff fallback:
  hint target = "node + preferred pool"; the receiving node's local
  placement picks the healthy pool. Never choose a degraded pool when a
  healthy one exists.

### D6. RF-urgency and repair pacing

- **No timer windows.** Urgency = priority in the repair queue, bounded by a
  global repair concurrency budget.
- Single-copy ranges (RF=2 with one copy dead) repair immediately, at the
  front of the queue. RF=3 double-copy repairs run at lower priority, not on
  a deferral timer.
- This makes the RF=2 exposure window explicit and argues for RF=3 as the
  default at scale (see D7 metadata recovery).

### D7. WAL and metadata loss — the two durability-critical paths

- **WAL loss is recoverable by design.** The WAL is local durability
  *acceleration*; the replica set is the system of record. On wal-pool death
  the node rejects new writes (write_degraded) but keeps serving reads. On
  replacement/remount: **fresh WAL, zero trust of old contents**, then
  **catch-up from replicas** for accepted-but-uncheckpointed data; writes
  resume when caught up and the WAL is verified. Hint WAL: hints are delivery
  intent, not data — reconciliation rebuilds debt lost with the device.
- **Metadata loss is a catastrophic local event, not cluster data loss while
  RF is healthy.** The node's RocksDB is its only key→segment index; lost, the
  node serves nothing and its own segments become unreclaimable junk. The
  cluster does not lose data: the metadata-dead attribute triggers
  re-replication of the node's ranges onto healthy targets (or back onto the
  node after a fresh store + re-replication-in). The node rebuilds its index
  and rejoins serving.
  - **Residual hole, explicit**: RF=2 with the other replica already down,
    metadata loss on the last standing replica IS data loss. This is the
    standard RF=2 exposure, now attributable — and an argument for RF=3 as
    the default at scale.
  - Deferred mitigation: segment self-description (per-segment key index)
    would let a node rebuild its index locally without re-replication
    traffic. Correctness does not depend on it.

### D8. Topology configuration

```toml
[storage]
# zero-config fallback: no pools = single data_dir (today's behavior)

[[storage.pools]]
name = "fast-nvme-0"
role = "data"                    # data | wal | metadata | hints
root = "/mnt/nvme0"              # ONE root per pool = one failure domain
weight = 2                       # placement weight (default: by capacity)
tech = "nvme"                    # hdd | ssd | nvme | cloud-ephemeral

[[storage.pools]]
name = "journal"
role = "wal"                     # pinned: data WAL + hint WAL
root = "/mnt/optane0"

[[storage.pools]]
name = "meta"
role = "metadata"                # RocksDB
root = "/mnt/optane1"

# health is an INLINE table on each pool (per-pool overrides); there is no
# global [storage.pools.health] block.
[[storage.pools]]
name = "hot-nvme"
role = "data"
root = "/mnt/nvme2"
tech = "nvme"
health = { error_rate_threshold = 0.001, min_errors = 3, latency_factor = 5.0, trend_window_secs = 300, detection_window_secs = 30, recovery_window_secs = 300 }
```

Rules:
- **Mountpoints, not device paths** (Docker/systemd/VM/cloud-ephemeral safe).
- **No app-level RAID/striping** — JBOD-of-roots; failure domain = one root.
  ZFS/mdadm own the low-level work.
- **One pool = one root = one failure domain (v1).** Same-role devices =
  multiple pools. `failure_domain` tag reserved in `ext`.
- **Role pinning is the headline feature** — WAL/metadata isolation.
- **Weights with capacity auto-detect default** — 16-disk node > 4-disk node.
- **Per-node config, cluster-agnostic** — manifest summary is gossiped.
- **Startup validation** — probe each root (write+read); missing root =
  fatal vs degraded (configurable).
- **Runtime pool attach (no restart)** — admin API adds a pool at runtime:
  probe root → register → rebuild + gossip manifest → placement starts
  filling. Same path serves hot-swapped devices.

## Alternatives considered

- **Disks as ring members (Ceph OSD model)** — perfect failure isolation and
  capacity balance, but gossip state grows to nodes × disks and rebalance
  churns the ring table. Rejected: wrong place to spend membership state for
  a gossip-based protocol.
- **Node-granular only (status quo + health flags)** — cheapest, but disk
  death still triggers node-level consequences; cannot route around a disk;
  cannot scale capacity per node. Rejected.
- **Full data-plane isolation (no gossip attribute, peers probe on error)**
  — avoids manifest gossip but makes routing blind until first error, cannot
  do capacity-aware placement, and cannot distinguish disk death from node
  death. Rejected: the per-pool manifest is O(pools) — cheap enough to
  gossip and it makes all distributed decisions well-informed.

## Consequences

### Positive

- **Surgical disk failure**: 1/(N×disks) of data re-replicated on disk
  death, not 1/N; no node eviction; no SWIM churn from disk events.
- **Durability-critical paths made explicit and recoverable**: WAL loss
  (fresh WAL + catch-up), metadata loss (fresh store + re-replication-in),
  with the RF=2 residual hole named and attributable.
- **Scale-shaped state**: manifest is O(pools); announcements are
  RF-bounded events; reconciliation is O(owned ranges) locally.
- **Capacity-aware placement on heterogeneous hardware** via weights +
  capacity auto-detect; runtime attach without restart.
- **Detection that catches real failure patterns**: trend-based, tech-aware,
  erratic-tolerant — not just thresholds.

### Negative

- New config surface (`[storage.pools]`), admin API (attach), and manifest
  wire format.
- Placement becomes pool-aware: segment lifecycle code (allocation, GC,
  sealing) must carry pool context.
- Re-replication engine is new machinery on top of hinted handoff
  (shares its skeleton: 5s sweep, ownership, delivery).
- Health monitor adds per-pool signal collection + SMART reads where
  available.
- Test framework gains fault-injection levels (unit `DiskIo`, e2e
  filesystem-level, fleet loopback devices).

### Migration

Zero-config fallback preserves today's behavior: no pools = implicit single
pool at `data_dir`. Operators migrate to pools at their own pace via the
drain/rebalance workflow (add new pools → drain old → remove old config).
No automatic migration; no rolling-upgrade concern (fleet deploys
all-at-once).

## References

- Brainstorm: `docs/brainstorm/disk-resilience-pools.md` (design lineage,
  open-question resolutions).
- ADR-0028 (dedicated membership plane — probe isolation, node-granular
  gossip state that this ADR extends with the manifest attribute).
- ADR-0027 (hinted handoff ownership model — skeleton reused by
  re-replication).
- ADR-0018 (data WAL consolidation — WAL as local acceleration).
- ADR-0025 (segment lifecycle state machine — placement/GC extension points).
- Spec §13 (membership), §14.1 (node config), §5 (durability).
