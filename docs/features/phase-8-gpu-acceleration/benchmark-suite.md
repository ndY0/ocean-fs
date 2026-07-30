---
feature: "Benchmark Suite & CI Regression Detection"
epic: "phase-8-gpu-acceleration"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: phase-3-erasure-coding
    reason: EC encode/decode benchmarks
  - epic: phase-1-storage-engine
    reason: Storage engine benchmarks (WAL, metadata, segment read/write)
  - epic: phase-2-distributed-connectivity
    reason: Network throughput benchmarks
adr: []
perf:
  - "11.4: Criterion benchmarks for hot-path functions"
  - "11.5: CI performance regression detection"
created: 2026-07-30
updated: 2026-07-30
---

# Benchmark Suite & CI Regression Detection

## Summary

Implement a comprehensive criterion benchmark suite covering all hot-path
functions, and integrate CI performance regression detection. Benchmarks
measure EC encode/decode, BLAKE3 hashing, metadata lookup, WAL append,
segment index lookup, and shard fetch. CI compares benchmark results
against a stored baseline; regressions >3% fail the pipeline.

## Scope

### In Scope
- Criterion benchmarks in `benches/` at workspace root
- `ec_benchmark.rs`: encode/decode with varying k, m, stripe sizes, data sizes
- `hash_benchmark.rs`: BLAKE3 streaming hash at 1 KB, 64 KB, 1 MB, 100 MB
- `storage_benchmark.rs`: WAL append throughput, metadata CRUD latency, segment index lookup, segment read/write
- `network_benchmark.rs`: gRPC throughput (unary + streaming), connection pool acquire/release
- `cache_benchmark.rs`: L1/L2 cache hit/miss latency, negative cache lookup
- CI benchmark job: runs `cargo bench --bench <name> -- --save-baseline main`
- Regression detection: `critcmp` (or `codspeed`) compares against stored baseline; fails if regression > 3%
- Baseline storage: Git LFS or dedicated artifact store for baseline JSON files
- Configurable regression threshold per benchmark group
- Documentation: how to run benchmarks locally, how to update baseline

### Out of Scope
- End-to-end cluster benchmarks (separate integration test framework)
- PGO workflow automation (script exists; not in this feature)
- Real-world workload simulation (YCSB, S3-benchmark — separate project)

## Crate Impact

| Crate | Change |
|---|---|
| Workspace root | New `benches/` directory with benchmark files |
| CI config | New workflow: `.github/workflows/benchmarks.yml` |

## Interface (Public API)

No public API. Benchmarks use `criterion` and internal crate APIs (with `pub(crate)` visibility or test-only re-exports).

## Data Flow

```
Running benchmarks locally:
  cargo bench --bench ec_benchmark
    → criterion warms up, measures, outputs:
        ec_encode_4_2_64k  time: [12.3 µs 12.5 µs 12.7 µs]
        ec_decode_4_2_64k  time: [15.1 µs 15.3 µs 15.6 µs]

CI regression check:
  1. Checkout PR branch
  2. Run benchmarks: cargo bench -- --save-baseline pr
  3. Checkout main branch
  4. Run benchmarks: cargo bench -- --save-baseline main
  5. critcmp main pr:
       ec_encode_4_2_64k: +2.1% (within 3% threshold) → PASS
       ec_decode_8_4_64k: +5.3% (exceeds 3% threshold) → FAIL
```

## Definition of Done

- [ ] **Code:** All benchmark files compile and run: `cargo bench --no-run` succeeds
- [ ] **Benchmarks:** EC: encode/decode at k=4,8,16, m=2,4,8, sizes 64KB–100MB; Hash: BLAKE3 at 1KB, 1MB, 100MB; Storage: WAL append (1000× 4KB), metadata get (1000 ops), segment index lookup (1000 ops); Cache: L1 get/miss, L2 get/miss, negative cache lookup; Network: gRPC unary round-trip, streaming throughput
- [ ] **CI:** Benchmark job in CI passes; intentionally introduced regression (>3%) fails CI
- [ ] **Docs:** `benches/README.md`: how to run, how to update baseline, what each benchmark measures
- [ ] **ADR:** N/A
- [ ] **Perf:** Rule 11.4 (criterion for hot-path functions), 11.5 (CI regression detection at 3%)
- [ ] **Integration:** N/A (benchmarks are the integration tests)
- [ ] **Manual:** `cargo bench` in workspace root runs all benchmarks without errors
