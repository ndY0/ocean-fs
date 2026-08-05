---
feature: "Manifest Tracker — PUT Recording & Post-Run Verification"
epic: "test-harness-extensions"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure/config-system-fix
    reason: Need configurable intervals to run background processes during verification
  - epic: gap-closure/metrics-infrastructure
    reason: Need metrics endpoint healthy to verify manifests after test
adr: []
perf:
  - "1.1 BytesMut for blob data"
  - "1.3 pre-size collections"
created: 2026-08-05
updated: 2026-08-05
---

# Manifest Tracker — PUT Recording & Post-Run Verification

## Summary

Implement the `Manifest` type in `e2e/src/load/manifest.rs`. This is the data-integrity
foundation for all load tests — it tracks every PUT operation's (bucket, key, BLAKE3 hash)
and provides a post-run `verify()` that GETs every recorded key from a random node and
compares the response hash. Must handle concurrent writes from multiple worker tasks,
gracefully skip keys deleted by DELETE workers during the run, and retry with exponential
backoff when nodes are unreachable during verification.

## Scope

### In Scope

- `Manifest` struct wrapping `DashMap<String, [u8; 32]>` — concurrent-safe key-to-hash mapping
- `Manifest::record(bucket, key, body)` — computes BLAKE3 hash and inserts entry
- `Manifest::record_delete(bucket, key)` — marks key as deleted so verify skips it
- `Manifest::verify(cluster)` → `Vec<Mismatch>` — GETs every non-deleted key, hashes response, compares
- Retry logic: exponential backoff (100ms, 200ms, 400ms, 800ms) for unreachable nodes; report unverified keys
- `ManifestSummary` struct — objects_written, objects_verified, mismatches, mismatch_details (Serialize)
- Concurrent safety: `record()` is called from multiple tokio worker tasks; `verify()` runs after all workers stop
- BLAKE3 dependency added to `e2e/Cargo.toml`
- `Mismatch` struct: `key`, `expected_hash` (hex), `actual_hash` (hex), `node` that was queried

### Out of Scope

- Cross-node manifest reconciliation (all workers share the same `Arc<Manifest>`)
- Manifest persistence between test runs (in-memory only; the LoadReport captures the summary)
- Partial-read verification (always reads full object)
- Version-stamped manifests (if same key is PUT multiple times, last-written hash is tracked)

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New module `src/load/manifest.rs`. Add `blake3` dependency to `Cargo.toml`. |

## Interface (Public API)

- `pub struct Manifest` — DashMap-based concurrent tracker for written objects
- `pub struct Mismatch` — a single verification failure: key, expected/actual hashes, queried node
- `pub struct ManifestSummary` — serializable aggregate: objects_written, objects_verified, mismatches, details
- `pub fn record(&self, bucket: &str, key: &str, body: &[u8])` — called by worker tasks after successful PUT
- `pub fn record_delete(&self, bucket: &str, key: &str)` — called by worker tasks after successful DELETE
- `pub fn len(&self) -> usize` — number of tracked keys (including deleted)
- `pub fn active_count(&self) -> usize` — number of non-deleted keys
- `pub async fn verify(&self, cluster: &Cluster) -> Vec<Mismatch>` — GET every key, compare BLAKE3

## Data Flow

```
Worker task (concurrent, N tasks):
  PUT /{bucket}/{key} → HTTP 200
    → manifest.record(bucket, key, body)
    → DashMap.insert("{bucket}/{key}", blake3::hash(body))

Worker task:
  DELETE /{bucket}/{key} → HTTP 204
    → manifest.record_delete(bucket, key)
    → DashMap entry marked as deleted (or value set to zero-hash sentinel)

Post-run verification (single-threaded):
  for each entry in manifest where not deleted:
    node = cluster.random_alive_node()
    GET http://{node}/{bucket}/{key} → response bytes
    actual = blake3::hash(response_bytes)
    if actual != expected:
      push Mismatch { key, expected, actual, node }
    on connection error:
      retry with backoff (100ms, 200ms, 400ms, 800ms)
      if all retries exhausted:
        push Mismatch { key, expected = hex, actual = "unreachable", node }
  return mismatches
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [ ] **Tests:** Unit test: `record()` + `verify()` with a mock cluster — 100 keys, all pass
- [ ] **Tests:** Unit test: `record_delete()` — deleted key skipped during verify, not reported as mismatch
- [ ] **Tests:** Unit test: concurrent `record()` from 16 tokio tasks — no data races, all entries present
- [ ] **Tests:** Unit test: node unreachable during verify — retry backoff, key reported as unverified
- [ ] **Tests:** Unit test: hash mismatch — Mismatch struct populated with correct hex values
- [ ] **Docs:** Every `pub` item has doc comments; `#![deny(missing_docs)]` passes in `e2e/src/load/manifest.rs`
- [ ] **Integration:** End-to-end: spawn 1-node cluster, PUT 10 keys, verify all passing, corrupt 1 response → verify reports 1 mismatch
