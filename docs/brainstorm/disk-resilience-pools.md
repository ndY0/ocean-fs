# Disk Resilience & Multi-Disk Topology — Pre-Epic Brainstorm

**Author:** Implementer (design converged with stakeholder)
**Date:** 2026-08-22 (rev. 2 — open questions resolved, Phase C in scope)
**Status:** Pre-epic — design converged, decisions hardened; pending ADR-0029 + epic/feature breakdown
**Context:** OceanFS targets large scale (100s–1000s of nodes, SWIM/gossip membership).
Today every node stores all of its data under a single `{data_dir}` root
(`segments/`, `wal/`, `event-wal/`, `metadata/`, `hints/`). The system has no
concept of a disk: no multi-disk layout, no disk failure detection, no
disk-level loss accounting. This brainstorm captures the converged design for
the disk-resilience epic and the work it contains.

---

## 1. Problem Statement

At scale, disk failure is a **routine event**, not an incident:
500 nodes × 10 disks → a disk dies every few days. Today OceanFS cannot
express or survive that:

1. **No multi-disk layout** — a node cannot scale capacity beyond one disk,
   and WAL/metadata/segments share one filesystem (single point of failure,
   journal/segment traffic contention).
2. **No failure classification** — a disk error surfaces as a generic
   `io::Error`; the node just "looks flaky" to SWIM.
3. **Wrong failure granularity** — with node-granular handling only, a disk
   death triggers node-level consequences: the node may be suspected/evicted
   and the cluster re-replicates *everything* on it (a storm sized 1/N of the
   ring instead of 1/(N×disks)).
4. **No read/write routing awareness** — peers cannot route around a degraded
   disk, and the write path may place replicas onto a dying disk (silent data
   loss risk via torn writes).

The system already heals node failure (replication, hinted handoff, WAL
recovery, churn restarts). The disk-resilience epic extends that machinery one
granularity down.

---

## 2. Converged Design (the "pool" model)

### 2.1 The hybrid: node-granular SWIM, pool-granular ownership

```
┌──────────────────────────────────────────────────────────┐
│ MEMBERSHIP PLANE (SWIM — unchanged, node-granular)        │
│  probes / suspicion / incarnation                          │
│  + per-node attribute: NodeManifest (pools summary)        │
├──────────────────────────────────────────────────────────┤
│ DATA PLANE (ownership — pool-granular)                    │
│  segment → pool mapping (local to the node)                │
│  placement, routing, re-replication, loss accounting       │
└──────────────────────────────────────────────────────────┘
```

- **SWIM stays node-granular** (probes, suspicion, incarnation — unchanged).
- **Ownership/placement is pool-granular**: the ring maps ranges to nodes
  (capacity-weighted vnodes), and each node's placement layer spreads its
  ranges across its pools.
- **The pool is the unit of information** — routing, placement, failure
  semantics, and the topology config all speak in pools.
- **State vs events**: the NodeManifest (state) stays O(pools) — it is never
  range-granular. Affected-range sets ride the *loss announcement* (a rare,
  targeted event), never the manifest. (Rev. 2 correction: peers cannot fully
  derive "which ranges lived on A's pool P" from the ring alone — the
  ring maps ranges to nodes, the node's local segment→pool table has the
  exact mapping. See §2.4.)

### 2.2 The gossip attribute: versioned NodeManifest

```rust
NodeManifest {                    // carried as node attribute in gossip
    incarnation: u64,             // ties to SWIM incarnation; restart re-declares
    pools: Vec<PoolManifest>,     // one per configured pool
}

PoolManifest {
    id: u32,                      // stable pool id (topology config order)
    role: PoolRole,               // data | wal | metadata | hints
    status: PoolStatus,           // Healthy | Degraded | Dead
    write_degraded: bool,         // role-specific consequence flag (see §2.3)
    capacity_free_bytes: u64,     // capacity-aware placement
    weight: u32,                  // placement weight (config override)
    ext: Option<PoolExt>,         // future: failure-domain tags, SMART health, ...
}
```

Cost: O(pools/node) — 5–20 entries. Schema'd + versioned from day one so the
wire format never forces a redesign.

### 2.3 Typed failure semantics

```
Healthy ──(error rate ↑ / latency ↑)──▶ Degraded ──(confirmed loss)──▶ Dead
   ▲                                      │                             │
   └────(recovery, clean window)──────────┘                             │
                                                                        │
   Degraded: route around (reads/writes prefer other pools),           │
             NO re-replication yet                                      │
   Dead:     confirmed data loss → announce + reconcile + re-replicate  │
```

**State machine rules (hardened):**
- **Degraded is a suspicion, Dead is a confirmation.** Transition to Dead
  requires *confirmed loss*: ENOENT on a segment we own, EIO on fsync of a
  segment/WAL write, or device unplug detection. Latency alone can never
  confirm Dead.
- **Detection: thresholds are NOT sufficient — degradation is a trajectory.**
  Disk behavior is erratic: a disk can fail exponentially while every
  individual error stays below an absolute threshold, and pure thresholds
  miss it. The health monitor therefore tracks **trends over time**:
  - Per-window signal buckets (error rate, p99/p999 latency, SMART counters
    where available); a *monotonic worsening slope* (e.g., error rate or
    latency doubling per window) triggers Degraded **even while below the
    absolute threshold** — predictive, not reactive.
  - Absolute thresholds remain as a fast path (instant Degraded on a spike),
    but the trend detector is the primary signal.
  - Intermittent/erratic errors (bad sector remapped, transient) are tracked
    separately: they do not instantly trip Degraded, but they accumulate into
    the trend and into SMART counters (reallocated/pending sectors are the
    classic "disk is dying" tell).
- **Technology-aware error expectations.** The disk technology defines the
  error profile, and the monitor must factor it in:
  - `tech = hdd` — SMART reallocated/pending sector counts, seek errors,
    latency spikes under load; slow progressive failure with long warning.
  - `tech = ssd` / `nvme` — uncorrectable ECC errors, wear (TBW), bursty then
    catastrophic failure; SMART wear/uncorrectable counters as primary signal.
  - `tech = cloud-ephemeral` — no SMART; I/O-error trends and write-failure
    confirmation are the only signals (auto-configures the monitor
    accordingly).
  - Detection windows, baselines, and which signals count differ per tech
    (defaults built-in; operator overrides via config).
- **Recovery**: clean window — zero errors for 5 minutes (hysteresis prevents
  flapping). A pool returning to Healthy re-enters placement and resumes its
  role; stale segments from its pre-failure life are reclaimed by GC
  (re-replication may have replaced them).
- **Role-aware consequences** — the *same* Dead status means different things
  per role, and `write_degraded` is set accordingly:

| Pool role | Dead consequence (local) | Cluster behavior | Recovery |
|---|---|---|---|
| data | Those range copies are lost; reads/writes of them fail → failover | Announce affected ranges → targeted re-replication; new writes avoid node if no healthy data pool remains | Pool returns → healthy; old segments GC'd; placement resumes |
| wal | Journal unavailable → node cannot durably accept new writes; reads continue | Node sets `write_degraded`; coordinators route writes elsewhere (manifest) | **Replacement/remount is safe**: WAL is a local durability acceleration, NOT the source of truth (replication is). Fresh WAL + **catch-up from replicas** for any accepted-but-uncheckpointed data (§2.9). Node resumes writes when caught up + WAL verified. |
| metadata | Store unavailable → node serves nothing | Node announces full unavailability; peers re-replicate its ranges **without waiting for SWIM suspicion timeouts** (probes stay socket-only, so SWIM alone would never evict it — the attribute must) | **Fresh metadata store + re-replication-in** rebuilds the node's index; old segments become unreclaimable junk (GC by ownership handoff). See §2.9 — this is the durability-critical path. |
| hints | Cannot persist new debt; pending in-memory hints may still drain | Coordinators avoid node as hint target; reconciliation is the safety net for any debt lost with the WAL | Fresh hint WAL; debt is rebuilt by reconciliation (data is replicated; hints are delivery intent only) |

### 2.4 Propagation: push announcement + periodic reconciliation

- **Announcement (push) = fast path, targeted.** Node A loses pool P →
  emits a *loss announcement* carrying the **compact affected-range set**
  (range-id summary/bitmap) for the ranges that lived on P. Fan-out is
  **targeted**: for each affected range, the ring's replica set tells A who
  else holds a copy — A sends the announcement only to those replica holders,
  not cluster-wide. Peers cross-check against their local hold-set and
  re-replicate immediately.
  - Cost: proportional to affected ranges, but only on pool death (rare),
    and only to RF peers (bounded, not N-scaling).
- **Periodic reconciliation (pull) = safety net, complete.** Each node, on a
  fixed 5s tick (reusing the hint-sweep machinery), processes a
  **risk-prioritized work queue** of its owned ranges:
  *live replica count < RF?* → repair. Work is prioritized by live-copy
  count: single-copy ranges always first, double-copy (RF=3) second, healthy
  ranges scanned at a slow background cadence purely as drift detection.
  - Does **not** depend on any announcement having arrived.
  - It is a **repair loop, not a detection loop** — failed repairs retry on
    the next tick.
  - **Worst-case RF restoration bound after announcement loss = 5s + repair
    time** for single-copy ranges.
- Push alone is insufficient (announcer dies mid-broadcast, network partition
  swallows the message). The periodic layer guarantees RF restoration even
  when announcements fail.

### 2.5 Cached routing state (hint, not dependency)

- **Versioned cache**: peers keep the last-known NodeManifest per node;
  stale-but-present beats absent. Fresher manifest replaces older atomically.
- **Failover on error**: if the cached target fails at I/O time (timeout,
  connection error, disk error), the read/write falls through to the next
  replica regardless of the cache. The cache optimizes; the error path
  guarantees.
- Handles both staleness directions: "A healthy" but disk died → I/O error →
  failover (correct, one wasted attempt). "A degraded" but recovered → avoid
  A briefly (availability loss, never correctness loss).

### 2.6 Read path

- Routing table = cached manifests + ring. Prefer healthy pools; on error,
  failover to next replica.
- Reads on a `write_degraded` node are served normally (only writes are
  rejected) — routing must not conflate the two.

### 2.7 Write path

- Coordinator's replica selection consumes the manifest:
  avoid degraded/dead pools and `write_degraded` nodes, respect role +
  capacity (a node whose data pools are full is a bad target even if its WAL
  pool is empty).
- Hinted handoff is the write-path fallback: if the cache was stale and the
  write hits a dead pool, the hint target becomes "node + preferred pool";
  the receiving node's local placement picks the healthy pool.
- Ordering preference: never choose a degraded pool when a healthy one exists
  (a write landing on a dying disk risks torn writes even with WAL recovery).

### 2.8 Topology configuration (user-facing)

```toml
[storage]
# zero-config fallback: no pools = single data_dir (today's behavior)

[[storage.pools]]
name = "fast-nvme-0"
role = "data"                    # data | wal | metadata | hints
root = "/mnt/nvme0"              # ONE root per pool = one failure domain (v1 rule)
weight = 2                       # placement weight (default: by capacity)
tech = "nvme"                    # hdd | ssd | nvme | cloud-ephemeral (default: auto-detect)
# smart = true                  # read SMART counters where available (linux)

[[storage.pools]]
name = "fast-nvme-1"
role = "data"
root = "/mnt/nvme1"

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
health = { error_rate_threshold = 0.001, min_errors = 3,
           latency_factor = 5.0, trend_window_secs = 300,
           detection_window_secs = 30, recovery_window_secs = 300 }
```

Principles:
- **Mountpoints, not device paths** — survives Docker, systemd, VMs, cloud
  ephemeral disks.
- **No app-level RAID/striping** — JBOD-of-roots, failure domain = one root.
  ZFS/mdadm do the low-level work.
- **One pool = one root = one failure domain (v1 rule).** Multiple devices of
  the same role = multiple pools. A `failure_domain` tag is reserved in
  `PoolManifest.ext` for future grouping, but v1 never spans a pool across
  devices.
- **Role pinning is the headline feature** — WAL/metadata on their own device:
  durability isolation + latency win.
- **Weights with capacity auto-detect default** — a 16-disk node naturally
  owns more ring range than a 4-disk one.
- **Per-node config, cluster-agnostic** — each node declares its own; the
  gossip attribute carries the summary.
- **Startup validation** — probe each root (write+read); startup policy:
  missing root = fatal vs degraded (configurable).
- **Runtime pool attach — adding a pool must NOT require a restart.**
  An admin API accepts a new pool definition at runtime: the root is probed
  (write+read), the pool is registered, the NodeManifest is rebuilt and
  gossiped, and placement starts filling it (capacity-weighted, role-aware).
  Operators must never reboot a live node to gain capacity. The same path
  serves hot-swapped devices (a re-inserted disk is a new pool or a revived
  one).

### 2.9 WAL loss and metadata loss — the two durability-critical paths

**WAL loss is recoverable-by-design (not a durability hole).** The WAL is a
local durability *acceleration* for uncheckpointed writes — the source of
truth for durability is the replica set. When the wal pool dies, the node
stops accepting writes (write_degraded) but keeps serving reads. When the
pool is replaced or remounted, the node does NOT trust the WAL contents
(empty/replaced device = zero frames): it starts a fresh WAL and
**catch-up from replicas** for any accepted-but-uncheckpointed data. The
design discipline of not treating the local WAL as the system of record pays
off exactly here. Same for the hint WAL: hints are delivery intent, not data —
reconciliation rebuilds any debt lost with the device.

**Metadata loss is a catastrophic local event, but NOT cluster data loss
while RF is healthy — and the current code has NO recovery path for it.**
The honest framing:
- The node's metadata store (RocksDB) is its only index of key → segment.
  If it is lost, the node serves nothing and its own segments become
  unrecoverable junk (no self-describing key index — the index IS the
  metadata).
- **The cluster does not lose data**: every key is replicated, and the
  replica holders can re-serve/re-replicate it. The metadata-dead attribute
  triggers re-replication of the node's ranges onto healthy targets (or back
  onto the node after a fresh store + re-replication-in). The node rebuilds
  and rejoins serving.
- **The residual hole is the RF=2 window**: if the peer holding the second
  copy is already down, metadata loss on the last standing replica IS data
  loss. This is the standard RF=2 exposure, now explicit and attributable —
  and it argues for RF=3 as the default at scale, which the risk-prioritized
  repair (§2.4) supports.
- Long-term mitigation (deferred): segment self-description (per-segment key
  index) would let a node rebuild its index from its own segments without
  re-replication traffic. Not needed for correctness, only for recovery cost.

---

## 3. Epic Skeleton (work breakdown)

All three phases are **in scope**. Phase C is not optional: C2 (drain/
rebalance) is also the config-migration path (§5 Q6), and C3 (capacity-weighted
vnodes) is what makes the pool model pay off at heterogeneous scale.

### Phase A — Foundation (multi-disk storage layer)

| # | Work item | Crates | Notes |
|---|---|---|---|
| A1 | Topology config schema + validation | core, node | `[storage.pools]` (one root per pool), roles, weights, health thresholds, `tech`, startup probing, fatal-vs-degraded policy |
| A2 | `StoragePool` runtime | storage | Pool registry per node, root discovery, per-pool metrics (capacity, free, IO error counts) |
| A3 | Placement policy | storage | Range/segment → pool assignment: role-aware, weight-aware, least-free-capacity within role |
| A4 | WAL/metadata/hints isolation | storage, durability, node | Move WAL, RocksDB metadata, hint WAL onto role-pinned pools; keep today's behavior when single pool |
| A5 | `NodeManifest` builder + gossip integration | membership, node | Derive manifest from topology, attach to gossip state, schema/versioning |
| A6 | Cached routing state | node | Manifest cache per peer (versioned), consumed by read path + placement |
| A7 | **Runtime pool attach (no restart)** | node, admin | Admin API adds a pool at runtime: probe root, register, rebuild+gossip manifest, placement starts filling; hot-swap device path |

### Phase B — Failure semantics & healing

| # | Work item | Crates | Notes |
|---|---|---|---|
| B1 | Disk health monitor — trend + tech-aware | storage, node | Signal buckets, monotonic-worsening trend detector (doubling-slope), tech-specific profiles (hdd/ssd/nvme/cloud-ephemeral), SMART counters where available, hysteresis |
| B2 | Typed failure state machine + role consequences | core, node | Healthy → Degraded → Dead; `write_degraded` per role; metadata-dead → full unavailability announcement |
| B3 | Loss announcement (push, targeted) | membership, node | Compact affected-range set, fan-out to ring replica holders only; schema'd range summary |
| B4 | Periodic reconciliation (pull safety net) | node, durability | 5s tick, risk-prioritized work queue (single-copy first), repair loop with retries, no announcement dependency |
| B5 | Re-replication engine | node, durability | Capacity-aware target selection, RF restoration, risk-prioritized pacing, global concurrency budget |
| B6 | Read/write routing on manifests | node | Read: prefer healthy pools + failover on error. Write: avoid degraded pools + `write_degraded` nodes, role+capacity-aware targets, hint target = node + preferred pool |
| B7 | **WAL-loss recovery (catch-up from replicas)** | durability, node | Fresh/replaced wal pool: no trust of old WAL contents; catch-up from replicas for accepted-but-uncheckpointed data; resume writes when caught up + WAL verified |
| B8 | **Metadata-loss recovery (fresh store + re-replication-in)** | node, durability | Metadata-dead → unavailability announcement → re-replicate ranges onto fresh store (or healthy targets) → node rebuilds index and rejoins serving; GC of unreclaimable old segments |

### Phase C — Scale ops (in scope)

| # | Work item | Crates | Notes |
|---|---|---|---|
| C1 | Drain/rebalance | node, storage | Move segments off a pool (pre-replacement); also the **config migration path** (add new pools → drain old → remove old config) |
| C2 | Capacity-weighted vnodes refinement | membership, node | Ring weights derived from pool capacity; rebalance on topology change |
| C3 | Segment self-description (deferred mitigation) | storage | Per-segment key index so metadata loss recovery needs no re-replication traffic |

---

## 4. Test Framework Extension

The current framework (load-test-campaign brainstorm) tests node-level churn on
a fleet of single-disk VMs. Disk resilience needs fault injection at a new
granularity, at three levels:

### 4.1 Level 1 — Unit tests: trait-based fault injection (storage crate)

Introduce a thin `DiskIo` abstraction over file ops in the storage layer
(not on hot paths in release: zero-cost wrapper, or feature-gated). Unit tests
inject via a `FaultyIo` wrapper:

- `fail_next(n)` — fail the next n ops with a chosen error (ENOSPC, EIO,
  EPERM)
- `fail_after(trigger)` — fail all ops after a trigger (disk "dies")
- `delay(duration)` — inject latency (Degraded simulation)
- `die_on_read/write` — asymmetrical failures

Targets:
- Placement policy (role/weight/least-free)
- Health monitor transitions + hysteresis (Healthy → Degraded → Dead, clean
  window recovery)
- **Trend detection**: slowly-increasing error rate/latency that never crosses
  the absolute threshold must still trigger Degraded (doubling-slope
  detector); erratic/intermittent errors accumulate but do not flap
- **Tech-specific profiles**: same error sequence behaves per-tech
  (e.g., hdd reallocated-sector tell vs nvme uncorrectable-ECC tell);
  cloud-ephemeral = I/O-signals-only
- Dead-confirmation rules (ENOENT/EIO confirm; latency alone never does)
- Role-consequence matrix (data vs wal vs metadata vs hints Dead)
- **WAL-loss recovery**: fresh/replaced wal pool → no trust of old contents,
  catch-up from replicas, writes resume only when caught up + WAL verified
- **Metadata-loss recovery**: metadata-dead → unavailability → fresh store +
  re-replication-in → node serves again; old segments GC'd
- Torn-write handling on pool failure
- Startup validation (missing root fatal vs degraded)
- Manifest builder correctness + `write_degraded` flag
- **Runtime pool attach**: admin API adds a pool mid-run, manifest
  re-gossiped, placement fills it — no restart anywhere in the path

### 4.2 Level 2 — Integration tests (e2e crate, local multi-node)

Extend the existing e2e harness to launch N nodes each with a **multi-pool
topology** (temp dirs as pool roots — cheap and portable). Fault injection via
the filesystem itself:

- `chmod 000` a pool root → EPERM (Degraded)
- rename/remove a pool root → ENOENT (Dead)
- fill a pool with a small tmpfs → ENOSPC
- delete a pool's segment files → data loss simulation

New e2e scenarios (new test files):

| Scenario | Asserts |
|---|---|
| `disk_failure_healing.rs` | Write load → kill pool on node 1 → reads still served from other pools/nodes; re-replication restores RF; announcement propagated; no data loss |
| `disk_degraded_routing.rs` | Degraded pool → reads/writes route around it; no re-replication storm; clean-window recovery restores full routing |
| `disk_reconciliation_safety_net.rs` | **Suppress the announcement** (partition/filter) → periodic sweep still restores RF within the 5s+bound |
| `disk_write_path_placement.rs` | Writes avoid degraded/dead pools and `write_degraded` nodes; hint target lands on healthy pool |
| `disk_role_consequences.rs` | wal-pool Dead → writes rejected + reads continue; metadata-pool Dead → full unavailability, peers re-replicate **without** SWIM timeout |
| `disk_multi_pool_placement.rs` | Capacity-weighted placement across pools; role pinning (WAL/metadata never on data pool) |
| `disk_topology_config.rs` | Config validation: bad roots, duplicate roles, multi-root pool rejected, missing devices, fatal-vs-degraded startup policy |
| `disk_announcement_targeting.rs` | Announcement fan-out reaches only ring replica holders; range set is correct; non-holders never see it |
| `disk_wal_loss_recovery.rs` | Kill wal pool mid-load → writes rejected + reads continue → replace/remount (fresh, empty WAL) → catch-up from replicas → writes resume; **no data loss** |
| `disk_metadata_loss_recovery.rs` | Kill metadata pool → node unavailable, peers re-replicate without SWIM timeout → fresh store + re-replication-in → node serves again; **cluster data intact**; RF=2 + peer-down window documented |
| `disk_runtime_attach.rs` | Add a pool via admin API under live load → manifest re-gossiped, placement fills it, no restart on any node |

### 4.3 Level 3 — Fleet (load-test campaign extension)

The fleet SUT VMs currently have one volume each. Two options (cheap-first):

1. **Loopback devices** — attach loop files on the existing volume as extra
   "disks"; topology config maps each loop device to a pool. No new cloud
   volumes. Disk failure = `losetup -d` / remove the loop file / remount
   read-only.
2. **Extra cloud volumes** — attach a second Hetzner volume per SUT for a real
   device boundary (only if loop devices prove insufficient — e.g., we need
   true device-level EIO).

New fleet scenario: **phase 4 (degraded mode) of the load-test campaign** —
sustained load + disk kill on a node, asserting:
- reads/writes continue (no data loss)
- re-replication restores RF within the announcement-loss bound
- no node eviction storm (SWIM does not confuse disk death with node death;
  metadata-pool death DOES trigger attribute-driven re-replication)
- pool manifest propagates; routing table converges
- probe p99 stays below timeout (ADR-0028 isolation proof under disk failure)
- hot-add + drain/rebalance exercises (Phase C) under live load
- **wal-pool kill + replacement**: writes rejected during outage, resumed
  after catch-up — no data loss
- **metadata-pool kill**: node serves nothing, cluster heals it via
  re-replication-in, node rejoins

### 4.4 Metrics for assertions

Per-pool Prometheus metrics (new):
- `oceanfs_pool_status{pool_id, role}` — 0=Healthy 1=Degraded 2=Dead
- `oceanfs_pool_write_degraded{pool_id}` — role-consequence flag
- `oceanfs_pool_io_errors_total{pool_id}` — error counter (health monitor input)
- `oceanfs_pool_bytes_free{pool_id}` — placement input
- `oceanfs_ranges_under_replicated` — reconciliation loop output
- `oceanfs_ranges_re_replicated_total` — healing throughput
- `oceanfs_announcements_rx_tx_total` — propagation observability
- `oceanfs_repair_queue_depth{priority}` — risk-prioritized queue visibility

Assertions in e2e/fleet read these directly (existing `MetricsSnapshot`
pattern).

---

## 5. Resolved Design Decisions (formerly "open questions")

| # | Question | Decision (hardened) |
|---|---|---|
| 1 | RF=2 urgency pacing | **Risk-prioritized immediate repair, no timer windows.** Urgency = priority in the repair queue (single-copy ranges first), bounded by a global repair concurrency budget. RF=3 double-copy repairs proceed at lower priority, not on a deferral timer. |
| 2 | Degraded threshold tuning | **Thresholds are the fast path only; the primary signal is the trend.** Monotonic-worsening slope (error rate or latency doubling per window) triggers Degraded even below absolute thresholds. **Tech-aware**: hdd (SMART reallocated/pending sectors), ssd/nvme (ECC/wear), cloud-ephemeral (I/O-only). Dead requires *confirmed loss* (ENOENT, fsync EIO, unplug) — latency alone never confirms. |
| 3 | Affected-range derivation cost | **Corrected model**: the ring maps ranges to nodes only, so peers cannot derive "A's pool P ranges" locally. The loss announcement therefore carries the **compact affected-range set**, fanned out only to ring replica holders (RF-bounded, event-only). The NodeManifest stays O(pools). Reconciliation never needs the set — it recomputes live-copy counts locally per owned range. |
| 4 | Multi-root pools | **Forbidden in v1**: one pool = one root = one failure domain. Same-role devices = multiple pools. `failure_domain` tag reserved in `ext` for future grouping. |
| 5 | Reconciliation cadence | **Single 5s-tick loop** (reuses hint-sweep machinery) with a risk-prioritized work queue. Worst-case RF-restoration bound after announcement loss = 5s + repair time (single-copy). |
| 6 | Config migration | **Explicit, via the drain/rebalance workflow (C1)** — which is why Phase C is in scope. Zero-config fallback (no pools = today's `data_dir` behavior) makes migration non-urgent; operators migrate at their own pace. No automatic migration. |
| 7 | Adding a pool at runtime | **Admin API, no restart required** (A7). Probe root → register → rebuild+gossip manifest → placement fills it. Same path serves hot-swapped devices. |
| 8 | WAL pool loss | **Recoverable by design** (B7): WAL is local acceleration, not the system of record. Replacement/remount → fresh WAL, no trust of old contents, **catch-up from replicas** for accepted-but-uncheckpointed data; writes resume when caught up + verified. |
| 9 | Metadata pool loss | **Catastrophic local event, not cluster data loss while RF healthy** (B8): attribute-driven unavailability → re-replication (onto fresh store or healthy targets) → node rebuilds index and rejoins. **Residual hole = RF=2 with the other replica already down** — explicit, and an argument for RF=3 default at scale. Deferred mitigation: segment self-description (C3). |

**Remaining items are calibration, not design** (measured at fleet scale):
- Exact trend-detector slopes and window sizes per tech (doubling-slope defaults above are starting points)
- Announcement range-set encoding compactness at extreme scale (bitmap vs delta vs hash)
- Repair concurrency budget sizing

---

## 6. Summary

- **Pool model**: node-granular SWIM + pool-granular ownership, pool = unit of
  information; NodeManifest (state) stays O(pools); affected-range sets ride
  loss *announcements* (events), fanned out to ring replica holders only.
- **Versioned NodeManifest** attribute: schema'd now, room to grow via `ext`.
- **Typed failures**: Degraded (suspicion — route around) vs Dead
  (confirmation — announce + reconcile + re-replicate); urgency by remaining
  replica count; **role-aware consequences** (wal Dead → write-degraded,
  metadata Dead → full unavailability without SWIM timeout).
- **Detection is trend-based and tech-aware**: monotonic-worsening slope
  catches disks failing below absolute thresholds; per-tech error profiles
  (hdd/ssd/nvme/cloud-ephemeral) define which signals count.
- **Push + periodic**: announcement is the fast path; reconciliation is the
  mandatory safety net — 5s tick, risk-prioritized repair loop with retries.
- **Cached routing**: hint, not dependency; failover on error.
- **Write path**: placement avoids degraded pools + write-degraded nodes;
  hints fall back to healthy-pool targets.
- **Topology config**: mountpoint-based, one root per pool, role-pinned,
  weighted, tech-aware health, zero-config fallback.
- **Runtime pool attach (no restart)** via admin API — operators add capacity
  without rebooting a live node.
- **WAL loss = recoverable by design** (fresh WAL + catch-up from replicas);
  **metadata loss = catastrophic locally, cluster-safe while RF healthy**
  (fresh store + re-replication-in); residual RF=2 window explicit.
- **All three phases in scope**: A foundation, B failure semantics & healing
  (incl. WAL/metadata recovery), C scale ops (drain/rebalance = migration
  path, capacity-weighted vnodes, segment self-description).
- **Test framework**: three levels — trait-based fault injection (unit, incl.
  trend + tech-profile tests), multi-pool local e2e with filesystem-level
  fault injection (11 scenarios incl. WAL/metadata-loss recovery and runtime
  attach), fleet with loopback-device disk kills. New per-pool metric set.

Next step: write ADR-0029 (this design), then the epic + feature breakdown
starting with Phase A (A1–A7).
