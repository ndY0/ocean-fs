# Load Test Campaign — Phased Roadmap

**Author:** Brainstorm Agent (Architect)
**Date:** 2026-08-05
**Context:** Design of a layered, progressively-scaled load and stress testing
campaign for OceanFS. Each phase targets a distinct class of bugs, uses
incrementally more expensive environments, and gates on the previous phase
passing.
**References:** `ROADMAP.md` item #17 ("work on a stress test suite"),
`docs/spec.md` §15 (implementation phases), `guidelines/performance.md` §11-12
(instrumentation & CI regression detection).

---

## 0. Philosophy

### 0.1 Why Layered?

A single "stress test" that throws 1000 concurrent connections at a 50-node
cluster will find bugs, but it won't tell you _which_ bug. The failure will be
a timeout or a 503, and you'll spend days debugging. A layered campaign
isolates failure modes:

- **Phase 1 finds data races and deadlocks** — the bugs that corrupt data or
  crash processes. These are the most dangerous and the cheapest to catch.
- **Phase 2 finds resource leaks** — the bugs that degrade production over
  hours. Still single-node, still cheap.
- **Phase 3 finds distributed protocol bugs** — gossip divergence, quorum
  miscounting, replication gaps. Small cluster, still fits in CI.
- **Phase 4 finds degraded-mode bugs** — healing races, hinted handoff loss,
  timeout misconfiguration. Failure injection under load.
- **Phase 5 finds scaling bugs** — O(N²) algorithms, connection explosion,
  thundering herd. Requires real multi-machine deployment.
- **Phase 6 finds algorithmic worst-cases** — pathological failure cascades,
  protocol parameter boundaries. Simulation, not real hardware.

Each phase answers a different question. Passing Phase N is the prerequisite
for Phase N+1.

### 0.2 Guiding Principles

1. **Data integrity is non-negotiable.** Every phase must track a manifest of
   written keys and their BLAKE3 hashes, and verify every key is readable with
   correct content at the end of the run.
2. **Metrics, not logs.** The load harness asserts on Prometheus metrics
   (`/admin/metrics`), not log grep. A test that passes because "nothing looked
   wrong in the logs" is a test that lies.
3. **Deterministic when possible.** Fixed random seeds. Reproducible configs.
   A flaky load test that fails 10% of the time is worse than no test — the team
   learns to ignore it.
4. **Cheap phases gate expensive phases.** Don't run Phase 5 if Phase 2 is
   broken. The cheap phases are the fast feedback loop.
5. **Built on the existing harness.** Phase 0-4 extend the `e2e/` crate's
   `NodeProcess` and `Cluster` abstractions. No new testing framework.

---

## 1. Phase 0 — Micro-benchmark Regression Gates

**Status:** Partially exists (`benches/` crate), CI wiring missing.

**Bug class:** Performance regressions in hot-path functions (EC encode/decode,
BLAKE3 hashing, WAL append, metadata lookup, segment index lookup).

**What:**
- Run all criterion benchmarks on every PR.
- Compare against stored baseline (commit `main`).
- Fail CI on >3% regression in any benchmark.
- Guideline §11.5 mandates this; it is not yet implemented.

**Environment:** CI, any runner. **Duration:** <2 minutes. **Cost:** zero.

**Assertions:**
- `critcmp` or `codspeed` reports no regression >3%.
- All benchmarks compile and run without error.

**Deliverable:** CI job in `.github/workflows/benchmarks.yml`.

---

## 2. Phase 1 — Single-Node Concurrency Correctness

**Bug class:** Data races, deadlocks, use-after-free, data corruption under
concurrent access. These are the most dangerous bugs — they corrupt user data
or crash the node.

**What:**
- Single `oceanfs` process (spawned via `NodeProcess` harness).
- N concurrent tokio tasks (N = CPU count × 4) hammering the S3 API:
  - PUT objects with randomized blob sizes across all four tiers (inline,
    small, standard, multi-segment).
  - GET objects (both hot keys re-read and cold keys first-read).
  - DELETE objects (with re-PUT to exercise tombstone + compaction).
  - HEAD requests (exercises negative cache).
- Concurrent writes to the **same key** (tests HLC conflict resolution).
- Run under **TSAN** (`RUSTFLAGS="-Z sanitizer=thread"`) — catches data races
  that only manifest under concurrency.
- Run under **ASAN** and **UBSAN** as well (already mandated by guideline §12.3).

**What to assert:**
- Manifest integrity: every key written is readable with correct content
  (track `HashMap<Key, Blake3Hash>` in the harness).
- Zero panics. Zero deadlocks (global timeout on the run).
- `/admin/health` returns healthy at the end.
- `/admin/segments` shows coherent counts (no negative counts, no orphaned
  segments without backing data).
- All caches report non-negative hit/miss counts.
- No `accel_fallback_total` increments (no unexpected tier fallback during
  sustained EC encode/decode).

**Environment:** CI, single process, 30-60 seconds. **Cost:** fits in a PR
check.

**Deliverable:** `e2e/tests/load_concurrency.rs` — a single test function that
spawns a `NodeProcess`, launches N concurrent workers, and asserts the above.

**Prerequisites:**
- TSAN-compatible CI runner (nightly Rust).
- Fix accepted deviation #8 (2MB HTTP body size limit) to test large blobs.

---

## 3. Phase 2 — Single-Node Resource Stability (Sustained Load)

**Bug class:** Memory leaks, file descriptor leaks, RocksDB SST file
accumulation, buffer pool exhaustion, unbounded channel growth, GC/compaction
stalls, WAL growth without truncation.

**What:**
- Single node, sustained PUT+GET+DELETE loop for 30-60 minutes.
- Blob sizes randomized across all tiers. Key space large enough to exercise
  compaction (write-delete-rewrite cycles on overlapping key ranges).
- Background processes active with **shortened intervals** for testability:
  GC (10s cycle, 5s TTL), anti-entropy (10s), scrub (60s).
- Poll `/admin/metrics` every 10 seconds. Assert invariants on each poll.
- At the end of the run, kill the node (SIGKILL) and restart with the same
  data directory. Assert WAL recovery replays correctly and all objects are
  readable.

**What to assert:**
- **Memory:** RSS stabilizes (sawtooth pattern from GC is acceptable; monotonic
  upward drift over 30+ minutes is a leak).
- **File descriptors:** `/proc/{pid}/fd` count stabilizes. No linear growth.
- **RocksDB:** SST file count oscillates but doesn't grow unboundedly.
  `rocksdb.num-files-at-level{0,1,2,...}` metrics don't show level-0
  accumulation (write stall indicator).
- **Caches:** L1/L2/L3 show reasonable hit rates; cache memory usage stays
  within configured bounds (`object_cache_size_bytes`, etc.).
- **WAL:** WAL file count doesn't grow without bound (segments are being
  sealed and WAL truncated).
- **Segment sealing:** `segment_seal_errors_total` = 0. EC encoding errors = 0.
- **Post-crash:** WAL replay succeeds, all pre-crash objects readable.

**Environment:** CI with longer timeout, or dedicated "stress" runner. Still
single-node. **Duration:** 30-60 minutes. **Cost:** moderate (CI minutes).

**Deliverable:** `e2e/tests/load_sustained.rs` — a test function that runs the
sustained loop, polls metrics, and asserts at exit.

**Prerequisites:**
- Fix accepted deviations #2, #3, #4 (configurable GC/anti-entropy/scrub
  intervals in `NodeConfig`).
- Fix accepted deviation #6 (WAL recovery path).
- Expose the following metrics at `/admin/metrics` (see §8 Observability Gaps).
- Configurable `max_body_size` for large blob testing (deviation #8).

---

## 4. Phase 3 — Small Cluster Functional Stability Under Load + Churn

**Bug class:** Gossip divergence, ring inconsistency, quorum miscounting,
replication gaps, HLC clock skew under concurrency, cache invalidation races
across nodes, hinted-handoff queue buildup.

**What:**
- 3-5 node cluster (all processes on localhost via `Cluster` harness).
- Sustained concurrent PUT/GET/DELETE from all nodes simultaneously.
- Randomized key routing: some keys naturally route to each node as coordinator.
- **Churn:** Random node restart (graceful SIGTERM or crash SIGKILL + rejoin)
  every 10-30 seconds. Tests membership convergence under continuous load.
- Shortened gossip/SWIM/anti-entropy intervals for test speed (1s gossip,
  3s suspicion, 8s failure, 10s anti-entropy).

**What to assert:**
- **Membership convergence:** After every churn event, all alive nodes agree on
  membership list and ring generation within 10 gossip rounds. Poll
  `/admin/cluster` on every node.
- **Manifest integrity:** Every object written during the run (regardless of
  which node accepted the PUT) is readable from at least R nodes (where
  R = `read_quorum`).
- **Hinted handoff:** `/admin/metrics` shows `hinted_handoff_hints_delivered`
  approaches `hinted_handoff_hints_stored` over time (all hints eventually
  delivered after nodes return). `hinted_handoff_hints_expired` = 0 for
  short-downtime churn.
- **Cache invalidation:** After node B PUTs a new version of key K, node A's
  subsequent GET for K must return the new version (not stale L1 cache) within
  the cache TTL window.
- **HLC monotonicity:** Incarnation numbers never decrease. Timestamps never
  move backward for the same key.
- **No split-brain:** No two nodes simultaneously believe they are the
  coordinator for the same key range.
- **Ring consistency:** `ring.lookup(hash)` returns identical successor set on
  all nodes for the same key hash.

**Environment:** CI (spawns N child processes from the test binary). **Duration:**
2-5 minutes. **Cost:** moderate — nightly CI or merge-to-main.

**Deliverable:** `e2e/tests/load_cluster_churn.rs` — spawns a `Cluster`,
launches concurrent workers across all nodes, injects churn events, and asserts
post-run.

**Prerequisites:**
- Phase 1 and Phase 2 passing consistently.
- Fix accepted deviation #1 (segment metadata entries created in write path, so
  `/admin/segments` returns real data).
- All 43 cluster E2E tests passing (currently 39/43; remaining 4 are T21, T43,
  T45, and intermittent SWIM timing).

---

## 5. Phase 4 — Degraded Mode Under Load (Failure Injection)

**Bug class:** Healing races during concurrent writes, hinted handoff data loss,
quorum degradation under partial failure, timeout/deadline misconfiguration,
fastest-k read path failure when some shards are slow, GC/healing interaction
bugs.

**What:**
- 3-node cluster under sustained write+read load.
- **Failure injections (scripted, deterministic order):**

  1. **Mid-write kill:** PUT an object. Kill one replica node (SIGKILL) before
     it acks. Verify: write still completes via remaining W nodes (hinted
     handoff to fallback). Hint is delivered when killed node restarts.
     Object is readable from all nodes after heal.

  2. **Slow-node test:** Apply artificial latency (500ms) to one node via `tc
     netem`. Verify: reads use fastest-k (`FuturesUnordered`) and don't stall
     on the slow node. SWIM correctly distinguishes "slow" from "dead" — the
     slow node stays ALIVE if it responds within the suspicion timeout.

  3. **Disk-full simulation:** Fill the node's temp directory to 95%. Verify:
     writes fail gracefully (503, not panic). Reads from other replicas still
     work. GC doesn't panic on ENOSPC.

  4. **Partial data corruption:** Corrupt one segment shard file on a node
     (overwrite bytes in the segment data file). Trigger anti-entropy cycle.
     Verify: Merkle mismatch detected, heal enqueued, heal worker reconstructs
     the shard from surviving replicas, second anti-entropy pass finds no
     mismatch.

  5. **GC during healing:** Trigger GC compaction on a segment while it is
     being healed from another node. Verify: no race condition, no double-free,
     no use-after-free of segment data.

- Run GC + compaction during failures to verify no compaction/healing races.

**What to assert:**
- **Data integrity:** Every write that received HTTP 200 is readable after the
  experiment completes and all nodes have recovered.
- **No permanent hint loss:** After all nodes return, `hinted_handoff_hints_stored
  - hinted_handoff_hints_delivered` returns to zero.
- **Healing bounded:** All heal requests complete within `heal_timeout_sec`.
  No heal requests stuck in `pending` state.
- **No cascading failures:** One dead node doesn't cause surviving nodes to OOM,
  deadlock, or experience unbounded latency degradation.
- **Metrics clean:** No unexpected `accel_runtime_fallback_total` increments
  (GPU failures are expected if on GPU-capable hardware; CPU SIMD fallback
  during GPU cooldown is acceptable but must be counted).

**Environment:** Dedicated test runner or nightly CI. Requires `tc` (traffic
control), `kill`, and filesystem manipulation capabilities. **Duration:**
5-15 minutes. **Cost:** moderate-high (destructive operations, not safe to
run alongside other tests on the same machine).

**Deliverable:** `e2e/tests/load_degraded.rs` — a test function that orchestrates
failure injection with precise timing and asserts recovery.

**Prerequisites:**
- Phase 3 passing consistently.
- `tc netem` available on CI runner (or skip slow-node test on macOS).
- `Cluster::corrupt_shard(i, segment_id)` harness method to overwrite segment data.
- `Cluster::fill_disk(i, target_pct)` harness method.

---

## 6. Phase 5 — Scale Properties (Medium Cluster, Real Machines)

**Bug class:** O(N²) algorithmic complexity in gossip propagation, connection
pool exhaustion (full-mesh connectivity), ring convergence time nonlinear
blowup, thundering herd on topology changes, EC healing bandwidth saturation,
load imbalance across nodes.

**What:**
- 20-50 real nodes (cloud VMs or container orchestration). This is the
  smallest scale where algorithmic complexity issues become measurable.
- **This phase does not test functional correctness** (that's Phase 3-4).
  It tests **scaling properties**:

  1. **Ring convergence time vs. cluster size:** Measure time from node join
     until all nodes agree on membership for N = 1, 5, 10, 20, 50. Should
     be O(log N) amortized, not O(N) linear.

  2. **Gossip bandwidth per node vs. cluster size:** Measure bytes/sec of
     gossip traffic per node as cluster size grows. Should be bounded, not
     growing linearly with cluster size.

  3. **Connection count per node vs. cluster size:** Measure gRPC connections
     per node. Should be O(log N) or O(√N) via partial-view membership — not
     O(N) full mesh. Guideline §4.1 mandates `pool_size_per_peer = 4`; total
     connections = 4 × active_peers. If `active_peers` is O(N), this fails.

  4. **Rebalance impact:** Add 5 nodes to a 20-node cluster under sustained
     write load. Measure: key migration volume (should be O(N/M) per node),
     rebalance time, request latency p50/p99 during rebalance.

  5. **Disk-fill behavior:** Fill the cluster to 80% capacity with sustained
     writes. Measure: GC compaction throughput, segment sealing rate, write
     latency p50/p99. Verify no write stalls from RocksDB level-0 accumulation.

**What to assert:**
- Latency p50/p99 doesn't degrade more than 2× when cluster size doubles.
- No single node handles >2× the average request load (check
  `/admin/metrics` request counts per node; detect hot-spotting).
- Ring rebalance completes within time proportional to migrated data volume
  (not cluster size).
- Gossip bandwidth per node is bounded (not growing with cluster size).

**Environment:** Cloud VMs or Kubernetes cluster. **Duration:** 30-60 minutes
per scenario. **Cost:** real money (e.g., 50 × 2-vCPU VMs for 1 hour), but
only for pre-release or monthly cadence. **Not CI.**

**Deliverable:** `scripts/scale-test/` directory with:
- Terraform or Ansible for provisioning.
- A Rust load-generator binary (or extension of `e2e/` harness) that runs
  against the remote cluster.
- A results collector that produces a JSON report for historical comparison.

**Prerequisites:**
- Phase 4 passing consistently.
- Deployment automation for OceanFS cluster (Docker image, k8s manifest, or
  Ansible playbook).
- Prometheus + Grafana for metric collection across the cluster (or the
  harness scrapes `/admin/metrics` from every node).

---

## 7. Phase 6 — Extreme Scale Simulation (1000+ nodes)

**Bug class:** Worst-case algorithmic complexity, pathological failure
cascades, protocol parameter boundaries. Bugs that would cost thousands of
dollars to find with real hardware.

**What:**
- Build a **discrete-event simulator** as a new crate (`oceanfs-sim` or a
  `sim/` workspace member). The simulator runs the **actual** gossip protocol,
  ring routing, and failure detection code from `oceanfs-membership` and
  `oceanfs-routing` — but without real I/O: no RocksDB, no EC, no gRPC.
  Virtual nodes exchange messages over a virtual network with configurable
  latency, drop rate, and partition patterns.

  ```rust
  // Conceptual: the simulator reuses real protocol code.
  let mut sim = Simulation::new()
      .node_count(5000)
      .gossip_interval_ms(1000)
      .network_latency(Distribution::Uniform(1, 10)) // ms
      .network_drop_rate(0.001)
      .failure_rate(Distribution::Poisson(3600.0));  // 1 failure/hour/node

  sim.run(Duration::from_secs(3600)); // simulate 1 hour of wall-clock time
  sim.report(); // convergence time, message count, false-positive rate
  ```

- **Scenarios:**
  1. **Convergence:** 1000, 5000, 10000 nodes. Measure gossip rounds to full
     membership convergence. Should be O(log N).
  2. **Churn:** Poisson-distributed node failures (1-5% of cluster per hour).
     Measure: failure detection false-positive rate, healing request volume
     (should be linear in failures, not quadratic).
  3. **Network partition:** Split the virtual network into two halves, then
     heal. Measure: split-brain duration, merge convergence time, data
     inconsistency window.
  4. **Worst-case topology:** Adversarial node placement on the ring (all
     vnodes clustered). Measure: load imbalance factor
     (max_load / avg_load). Should stay bounded regardless of node placement.
  5. **Parameter sensitivity:** Sweep gossip interval (100ms → 10s), suspicion
     timeout (1s → 60s), vnodes per node (16 → 1024). Find parameter regimes
     where the protocol breaks down.

**What to assert:**
- Gossip message amplification factor bounded (not exponential).
- Ring converges to consistent state within O(log N) rounds.
- No failure cascade: killing 5% of nodes doesn't cause >1% additional
  failures due to overload.
- SWIM false-positive rate <0.1% under normal network conditions.
- Load imbalance factor <3.0 for uniform random key distribution.

**Environment:** Developer laptop. Simulation runs in seconds/minutes even for
10K nodes. **Cost:** essentially zero after the simulator is built.

**Deliverable:** `crates/oceanfs-sim/` (or `sim/` at workspace root):
- Reuses `oceanfs-membership` and `oceanfs-routing` as libraries.
- Virtual network layer: `trait VirtualNetwork { fn send(&mut self, from: NodeId, to: NodeId, msg: Message); }`
- Scenario runner that configures the simulation, runs it, and produces
  a JSON report.
- CI job that runs the simulation on every PR (it's fast — no real I/O).

**Prerequisites:**
- Phase 3 and Phase 4 passing (functional correctness validated first).
- `oceanfs-membership` and `oceanfs-routing` must be usable without a tokio
  runtime — the simulator provides its own event loop. This may require
  refactoring the crates to accept a generic `Clock` and `Network` trait.

---

## 8. Cross-Cutting Requirements

### 8.1 Manifest Tracking

Every load test in Phase 1-4 must maintain a `Manifest`:

```rust
struct Manifest {
    entries: DashMap<String, Blake3Hash>, // key → expected hash
}

impl Manifest {
    fn record_put(&self, bucket: &str, key: &str, body: &[u8]) {
        let hash = blake3::hash(body);
        self.entries.insert(format!("{}/{}", bucket, key), hash);
    }

    async fn verify_all(&self, cluster: &Cluster) -> Result<Vec<String>> {
        // For every entry, GET from a random node, hash response body,
        // compare. Returns list of mismatched keys (should be empty).
    }
}
```

Without this, you don't know if data was silently corrupted.

### 8.2 Metrics-Based Assertions

The harness must expose helpers to scrape and assert on `/admin/metrics`:

```rust
// e2e/src/metrics.rs — extension to the harness
impl NodeProcess {
    /// Returns parsed Prometheus metrics as a HashMap<metric_name, value>.
    pub async fn metrics(&self) -> Result<HashMap<String, f64>>;

    /// Asserts that a counter metric has not increased since the last snapshot.
    pub async fn assert_counter_stable(&self, name: &str, since: &MetricsSnapshot);
}
```

Key metrics to assert:
| Metric | Phase | Invariant |
|---|---|---|
| `accel_fallback_total` | 1-4 | Zero (or bounded count for GPU → CPU fallback) |
| `accel_runtime_fallback_total` | 2-4 | Zero in CPU-only tests; bounded in GPU tests |
| `segment_seal_errors_total` | 1-4 | Zero |
| `heal_requests_failed_total` | 3-4 | Zero |
| `gossip_messages_dropped_total` | 3-5 | Zero |
| `hinted_handoff_hints_expired_total` | 3-4 | Zero (for short-downtime churn) |
| `process_open_fds` | 2 | Bounded, not monotonically growing |
| `rocksdb_num_files_at_level_0` | 2 | Oscillates, doesn't accumulate |

### 8.3 Deterministic Seeding

```rust
// Accept --seed via env var, default to random but log the seed.
let seed: u64 = std::env::var("LOAD_TEST_SEED")
    .ok()
    .and_then(|s| s.parse().ok())
    .unwrap_or_else(|| {
        let seed = rand::random();
        eprintln!("LOAD_TEST_SEED={seed}"); // log for reproducibility
        seed
    });
```

A flaky test that can't be reproduced is worthless.

### 8.4 CI Integration Strategy

```
Every PR:
  ├── Phase 0 (micro-benchmarks, <2 min)
  └── Phase 1 (concurrency + TSAN, <2 min)

Merge to main:
  ├── Phase 0 + Phase 1
  └── Phase 2 (sustained single-node, <10 min)

Nightly:
  ├── Phase 0-2
  └── Phase 3 (3-node cluster + churn, <15 min)

Pre-release / weekly:
  ├── Phase 0-4 (including failure injection)

Monthly / pre-major-release:
  ├── Phase 5 (cloud deployment, manual trigger)

Ad-hoc / design-time:
  └── Phase 6 (simulation, run anytime on laptop)
```

---

## 9. Prerequisites — What Must Be Fixed First

Before Phase 1 can be truly useful, these accepted deviations from the
`broad-smoke-tests` feature must be resolved:

| # | Blocker | Phase Impacted | Effort |
|---|---|---|---|
| D2-D4 | Configurable GC, anti-entropy, scrub intervals not in `NodeConfig` | Phase 2, 3, 4 | Medium |
| D6 | WAL crash recovery not working (GET after crash returns 500) | Phase 2, 4 | High |
| D8 | 2MB default HTTP body size limit prevents testing >2MB blobs | Phase 1, 2 | Low |
| D1 | Write path doesn't create segment metadata entries | Phase 3 assertion on `/admin/segments` | Medium |
| — | No CI regression detection for benchmarks | Phase 0 | Low |
| — | TSAN/ASAN/UBSAN CI jobs not configured | Phase 1 | Medium |
| — | `/admin/metrics` not exposing all required metrics (see §8.2) | Phase 2+ | Medium |

---

## 10. Open Questions & Discussion Points

These are topics the implementer and architect must resolve before starting
each phase:

1. **Test harness evolution:** Phase 1-2 extend `NodeProcess`. Phase 3-4
   extend `Cluster`. Should Phase 5 use a separate load-generator binary, or
   should we extend the `e2e/` crate to work against remote clusters (passing
   `--target-hosts` instead of spawning local processes)?

2. **Observability gaps:** The current `/admin/metrics` endpoint exposes a
   subset of the metrics listed in §8.2. Which metrics are missing and need to
   be wired from `oceanfs-node`?

3. **Tooling for results:** How should test results be consumed — by a human
   reading CI output? By an agent scraping a JSON report? Should we produce
   a machine-readable results format (JSON with pass/fail, metric snapshots,
   timing data) alongside the human-readable assertion failures?

4. **Simulation vs. emulation:** For Phase 6, should the simulator reuse the
   actual protocol code (calling into `oceanfs-membership` and
   `oceanfs-routing`), or should it be a high-level model of the algorithms?
   Reusing real code catches real bugs but requires refactoring the crates to
   be runtime-agnostic. A model can be built faster but may diverge from the
   implementation.

5. **Hardware acceleration in load tests:** Phase 1-4 run on CI, which likely
   lacks GPUs. Should there be a separate GPU-specific load test in Phase 5
   (cloud GPU instances), or is the CPU fallback path sufficient for
   correctness testing? The GPU codepath introduces unique failure modes
   (device lost, OOM, semaphore contention) that won't be exercised on CPU.

6. **Time-compression for background processes:** Phase 2-4 need shortened
   intervals for GC, anti-entropy, and scrub. This requires those intervals to
   be configurable via `NodeConfig` (not hardcoded). Is there a design for
   a "testing mode" config flag that compresses all intervals to minimum
   values, or should each interval be individually configurable?

7. **Phase 3 churn model:** The current design restarts random nodes every
   10-30 seconds. Should the churn model be more sophisticated — e.g.,
   Poisson-distributed failures, correlated failures (rack-level), or
   adversarial patterns (kill the coordinator for the most-written key)?

---

## 11. Summary

| Phase | Bug Class | Environment | Duration | CI Cadence | Cost |
|---|---|---|---|---|---|
| 0 | Performance regressions | CI runner | <2 min | Every PR | Zero |
| 1 | Data races, deadlocks, corruption | CI (single proc, TSAN) | <2 min | Every PR | Zero |
| 2 | Resource leaks, sustained degradation | CI (single proc) | 30-60 min | Merge to main | Low |
| 3 | Distributed protocol bugs, churn | CI (3-5 procs) | 2-5 min | Nightly | Low |
| 4 | Degraded mode, failure injection | Dedicated runner | 5-15 min | Pre-release/weekly | Moderate |
| 5 | Algorithmic scaling, load imbalance | Cloud (20-50 VMs) | 30-60 min | Monthly/manual | Real money |
| 6 | Worst-case analysis, parameter tuning | Laptop (simulation) | Seconds-minutes | Ad-hoc/PR | Zero |
