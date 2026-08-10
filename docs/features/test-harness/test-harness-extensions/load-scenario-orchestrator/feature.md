---
feature: "Load Scenario Orchestrator — Worker Framework & Stats Collection"
epic: "test-harness-extensions"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure/config-system-fix
    reason: Need max_body_size configurable to test all 4 blob size tiers
  - epic: gap-closure/metrics-infrastructure
    reason: Need metrics endpoint for stats scraping during load
adr:
  - 0001-segment-packing
  - 0004-tiered-segment-sizing
  - 0019-test-harness-topology-cost-guardrails
perf:
  - "2.2 DashMap for concurrent caches"
  - "11.1 Atomic counters on hot paths"
created: 2026-08-05
updated: 2026-08-10
---

# Load Scenario Orchestrator — Worker Framework & Stats Collection

## Summary

Implement `LoadScenario`, `Worker`, `OpWeight`, `BlobSizeDist`, `KeySpace`, and stats types
in `e2e/src/load/generator.rs`. The `LoadScenario` describes a load test's concurrency, duration,
operation mix, blob size distribution, and key space strategy. `Worker` is a tokio task that
loops for the scenario duration, picks random operations/blobs, executes against the cluster,
and records stats via `AtomicU64`. The orchestrator spawns N workers, waits for the duration,
and collects `AggregateStats`. This is the engine behind every Phase 1-4 test.

## Scope

### In Scope

- `LoadScenario` struct: `concurrency`, `duration`, `operations` (vector of `OpWeight`), `blob_sizes` (`BlobSizeDist`), `key_space` (`KeySpace`), `seed`
- `OpWeight` struct: `op` (`Operation` enum: `Put`, `Get`, `Delete`, `Head`), `weight` (f64 probability)
- `BlobSizeDist` enum: `Fixed(usize)`, `Range(usize, usize)`, `Tiered { inline_pct, small_pct, standard_pct, multi_pct }` — covers all 4 segment tiers: inline ≤4KB, small 4-256KB, standard 256KB-4MB, multi >4MB
- `BlobSizeDist::sample(rng)` → `usize` — generates blob size from configured distribution
- `KeySpace` enum: `RandomUuid`, `Sequential { prefix, start, count }`, `Zipfian { hot_keys, cold_keys, skew }` — selection from a small set of keys with heavy skew
- `Worker` struct: `id`, cluster reference, `Arc<Manifest>`, `Arc<LoadScenario>`, seeded RNG (`ChaCha12Rng`), `WorkerStats`
- `Worker::run()` — main loop: pick random operation per weights, pick blob size per distribution, execute, record stats, sleep minimal yield
- `WorkerStats` struct: per-operation counters (`puts_total`, `puts_200`, `puts_5xx`, `gets_total`, `gets_200`, `gets_404`, `deletes_total`, `deletes_204`, `heads_total`, `heads_200`), latency histograms (using `hdrhistogram` or simple bucketed `AtomicU64` arrays)
- `AggregateStats` struct (sum/merge of N WorkerStats): aggregates all counters, computes p50/p99 from merged histograms
- `Orchestrator::run(scenario, cluster, manifest)` → `AggregateStats` — spawns N `Worker` tasks, waits for `duration`, joins all, merges stats
- Deterministic seeding: `LoadScenario::seed` feeds into `ChaCha12Rng`; same seed → same operation sequence
- `Worker` must handle PUT bodies: generate random bytes per `BlobSizeDist::sample()`, upload, record to manifest

### Out of Scope

- Remote cluster targeting (the `Worker` and `Orchestrator` are topology-agnostic — they operate against a `Cluster` handle which may be either locally spawned or remote-connected; the topology decision is made by the test entrypoint via `TARGET_HOST`/`TARGET_HOSTS` env vars per ADR-0019)
- Metrics scraping during the run (handled by `MetricsSnapshot` in separate feature)
- Churn injection (handled by `ChurnScheduler` in separate feature)
- Custom S3 operations beyond PUT/GET/DELETE/HEAD (no List, no multipart upload)

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New module `src/load/generator.rs`. Add `rand_chacha` and `rand` dependencies. Add `hdrhistogram` dependency (or use simple bucketed AtomicU64 arrays). |
| `e2e` | New module `src/load/mod.rs` — re-exports from `manifest.rs`, `generator.rs`, `metrics.rs`, `report.rs`. |

## Interface (Public API)

- `pub struct LoadScenario` — describes a load test: concurrency, duration, operations, blob sizes, key space, seed
- `pub struct OpWeight` — weighted operation entry: `op`, `weight`
- `pub enum Operation` — `Put`, `Get`, `Delete`, `Head`
- `pub enum BlobSizeDist` — `Fixed(usize)`, `Range(usize, usize)`, `Tiered { ... }`
- `pub enum KeySpace` — `RandomUuid`, `Sequential { ... }`, `Zipfian { ... }`
- `pub struct Worker` — single load-generator task
- `pub struct WorkerStats` — per-worker atomic counters and latency data
- `pub struct AggregateStats` — merged stats across all workers
- `pub struct Orchestrator` — spawns and manages workers

## Data Flow

```
Orchestrator::run(scenario, cluster, manifest):
  let manifest = Arc::new(Manifest::new());
  let scenario = Arc::new(scenario);

  // Spawn N workers
  for id in 0..scenario.concurrency:
    let worker = Worker::new(id, cluster.handle(), manifest.clone(), scenario.clone());
    handles.push(tokio::spawn(worker.run()));

  // Wait for duration or until all workers finish
  tokio::time::sleep(scenario.duration);
  // (Workers check elapsed time each tick and exit)

  // Collect stats
  let stats: Vec<WorkerStats> = futures::future::join_all(handles).await;
  let aggregate = AggregateStats::merge(&stats);

Worker::run():
  loop:
    if elapsed > scenario.duration: break

    op = pick_weighted(&scenario.operations)
    size = scenario.blob_sizes.sample(&mut rng)
    key = scenario.key_space.next_key(&mut rng)

    match op:
      Put:
        body = random_bytes(size)
        resp = cluster.put(random_node, key, body)
        if resp.status == 200: manifest.record("load-test", key, body)
        stats.puts_total += 1; stats.record_latency("put", elapsed)
      Get:
        resp = cluster.get(random_node, key)
        stats.gets_total += 1
      Delete:
        resp = cluster.delete(random_node, key)
        if resp.status == 204: manifest.record_delete("load-test", key)
        stats.deletes_total += 1
      Head:
        resp = cluster.head(random_node, key)
        stats.heads_total += 1

  return stats
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [ ] **Tests:** Unit test: `BlobSizeDist::Tiered` sampling — verify all 4 tiers are hit proportionally
- [ ] **Tests:** Unit test: `KeySpace::Zipfian` — verify hot keys appear more frequently than cold keys
- [ ] **Tests:** Unit test: `WorkerStats` — counters increment correctly from concurrent tasks
- [ ] **Tests:** Unit test: `AggregateStats::merge` — p50/p99 computed from merged histograms
- [ ] **Tests:** Unit test: Deterministic seeding — same seed produces identical operation sequence on two runs
- [ ] **Tests:** Integration test: spawn 1-node cluster, run 4-worker scenario for 10s, assert aggregate stats non-zero
- [ ] **Docs:** Every `pub` item has doc comments; `#![deny(missing_docs)]` passes
- [ ] **Perf:** Worker stats use `AtomicU64` (perf §11.1). Manifest uses `DashMap` (perf §2.2).
