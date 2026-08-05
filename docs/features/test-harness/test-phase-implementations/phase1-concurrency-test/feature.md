---
feature: "Phase 1 — Single-Node Concurrency Correctness Test"
epic: "test-phase-implementations"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure/config-system-fix
    reason: Need max_body_size configurable for multi-segment blob testing
  - epic: gap-closure/metrics-infrastructure
    reason: Need accel_fallback_total metric wired to assert zero fallbacks
  - epic: test-harness-extensions/manifest-tracker
    reason: Need Manifest for data integrity verification
  - epic: test-harness-extensions/load-scenario-orchestrator
    reason: Need Worker framework to generate concurrent load
  - epic: test-harness-extensions/load-report
    reason: Need LoadReport for structured results output
adr:
  - 0001-segment-packing
  - 0004-tiered-segment-sizing
perf:
  - "11.1 Atomic counters on hot paths"
  - "12.2 TSAN for concurrency bugs"
created: 2026-08-05
updated: 2026-08-05
---

# Phase 1 — Single-Node Concurrency Correctness Test

## Summary

Implement `e2e/tests/load_concurrency.rs` — a `#[tokio::test]` that validates
single-node concurrency correctness. Spawns one `NodeProcess`, launches N = CPU
count × 4 concurrent workers performing PUT/GET/DELETE/HEAD with randomized blob
sizes across all 4 segment tiers, including concurrent writes to the same key
(testing HLC conflict resolution). Runs for 60 seconds under TSAN. Asserts
manifest integrity (all written keys readable with correct content), zero panics,
zero deadlocks, `/admin/health` healthy, and `accel_fallback_total == 0` (if
metrics wired). This is the cheapest test that catches the most dangerous bugs:
data races, deadlocks, and data corruption under concurrent access.

## Scope

### In Scope

- `#[tokio::test]` function in `e2e/tests/load_concurrency.rs`
- Spawns 1 `NodeProcess` with standard config (`config_standard()`)
- N = `num_cpus::get() * 4` concurrent workers, minimum 8, maximum 64
- `LoadScenario` configuration:
  - Duration: 60 seconds (or `LOAD_TEST_DURATION_SECS` env var for CI override)
  - Operations: PUT 50%, GET 40%, DELETE 5%, HEAD 5%
  - Blob sizes: `Tiered` — 10% inline (≤4KB), 30% small (4-256KB), 40% standard (256KB-4MB), 20% multi (>4MB) — exercises all 4 segment tiers
  - Key space: `RandomUuid` (20% same-key writes by hashing to a smaller key pool of 100 keys)
- Same-key concurrency: 20% of PUTs target a pool of 100 shared keys to exercise HLC conflict resolution
- Concurrent key pool: `key_pool[worker_id % 100]` — each worker writes to a subset, some overlap
- Manifest: `Arc<Manifest>` shared across all workers; `record()` on successful PUT, `record_delete()` on successful DELETE
- Post-run verification phase: `manifest.verify(&cluster)` — all non-deleted keys readable with correct BLAKE3 hash
- Assertions:
  1. **manifest_integrity**: 0 mismatches (all written keys readable with correct content)
  2. **health**: `GET /admin/health` returns 200 at end
  3. **no_panics**: test completed without early termination (implied by reaching assertions)
  4. **accel_fallback_zero**: `MetricsSnapshot::scrape()` → `accel_fallback_total == 0` (if metrics infrastructure is wired; skip gracefully if /admin/metrics returns empty)
  5. **worker_stats_nonzero**: all workers performed at least some operations
- Deterministic seeding: `LOAD_TEST_SEED` env var; log seed at start
- TSAN: CI runs this test with `RUSTFLAGS="-Z sanitizer=thread"` (requires nightly Rust)
- Tag test with `#[ignore]` if `cfg(not(tsan))` to skip on non-TSAN CI runs? (No — always run; TSAN is a CI variant)

### Out of Scope

- ASAN/UBSAN runs (separate CI job; this test function also works under ASAN/UBSAN by design but is not specialized for them)
- Multi-node cluster (Phase 1 is strictly single-node)
- Sustained resource monitoring (RSS, FDs) — that's Phase 2
- Custom S3 operations beyond PUT/GET/DELETE/HEAD
- HEAL/GC/AE assertions beyond health endpoint

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New test file `tests/load_concurrency.rs`. |

## Interface (Public API)

No new `pub` items — this is a `#[tokio::test]` function, not library code.

## Data Flow

```
Test: load_concurrency
  1. Parse LOAD_TEST_SEED env var (or generate and log)
  2. Parse LOAD_TEST_DURATION_SECS env var (default 60)
  3. Build LoadScenario { concurrency = N, duration, operations, blob_sizes, key_space }
  4. Spawn NodeProcess with config_standard()
  5. Create Manifest, Orchestrator
  6. Run: orchestrator.run(scenario, &cluster, &manifest)
  7. After timeout: scrape /admin/metrics → MetricsSnapshot
  8. Verify: manifest.verify(&cluster) → Vec<Mismatch>
  9. Build LoadReport:
     - result = Pass if 0 mismatches AND health OK AND accel_fallback == 0
     - assertions: manifest_integrity, health, accel_fallback_zero
  10. Write LoadReport JSON + textfile
  11. assert!(report.result == Pass) — panics the test on failure
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [ ] **Code:** Test file `e2e/tests/load_concurrency.rs` compiles and links
- [ ] **Tests:** `cargo test -p e2e -- load_concurrency` passes on CI (single run, 60s)
- [ ] **Tests:** Test produces valid `LoadReport` JSON at `target/load-reports/`
- [ ] **Tests:** Manifest integrity: 1000+ written keys verified, 0 mismatches in controlled environment
- [ ] **Tests:** Same-key concurrent writes: no data corruption, HLC advances monotonically (verified implicitly by manifest)
- [ ] **Tests:** All 4 blob size tiers exercised (verified by worker stats showing counts for each tier)
- [ ] **Tests:** CI: TSAN variant (`RUSTFLAGS="-Z sanitizer=thread"`) runs without TSAN-detected data races
- [ ] **Docs:** Test doc comment explains what it validates and how to run locally
- [ ] **Perf:** Test completes in <2 minutes on CI (target), <5 minutes worst case
- [ ] **Integration:** `LOAD_TEST_SEED=42 cargo test -p e2e -- load_concurrency` produces reproducible behavior
