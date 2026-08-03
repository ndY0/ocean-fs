---
feature: "Broad Smoke Tests — Durability, Caching, Segment Lifecycle"
epic: "e2e-testing"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: final-integration
    feature: final-integration-composition-root
    reason: Node must start and serve S3 API
  - epic: final-integration
    feature: final-integration-durability-backgrounds
    reason: GC, anti-entropy, scrub, heal must be wired
  - epic: phase-6-caching-layer
    reason: L1/L2/L3 caches and prefetch must be wired
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Broad Smoke Tests — Durability, Caching, Segment Lifecycle

## Summary

After fixing the node startup and read/write metadata persistence bugs (commit
`a78a430`), the single-node S3 API path works end-to-end (PUT → GET → DELETE).
This feature validates the remaining subsystems that run in the background:
garbage collection, anti-entropy, scrubbing, healing, the three-tier cache
cascade, segment lifecycle, and WAL recovery.

All tests are **end-to-end tests against the release binary** — they spawn
`oceanfs` as a child process, exercise it via HTTP, and assert behavior
programmatically. No `TempDir` + `Node::start()` in-process patterns. No
shell scripts grepping logs. A single Rust crate (`e2e/`) containing all
tests, runnable via `cargo test -p e2e`.

## Scope

### In Scope

1. **`e2e/` test crate** — a new workspace member with a `NodeProcess`
   harness that spawns `oceanfs` as a child process, waits for it to
   become healthy, and exposes `get()`/`put()`/`delete()`/`kill()` helpers.
2. **Cache cascade** — L1/L2/L3 hits and misses tracked via `/admin/caches`.
3. **Negative cache** — DELETE inserts into L3 Bloom filter; subsequent
   GETs return 404 without touching RocksDB.
4. **Segment lifecycle** — all four size tiers (inline, small, standard,
   multi) produce correct segment counts in `/admin/segments`.
5. **Garbage collection** — shortened-interval config (10s cycle, 5s TTL);
   PUT, DELETE, poll `/admin/segments` until compaction reduces count.
6. **Orphan reaper** — unreferenced segments cleaned up after GC.
7. **Anti-entropy** — shortened interval (10s); Merkle trees built and
   verified; no false mismatches on clean data.
8. **Manual scrub** — `POST /admin/scrub` → 202 → all segments healthy.
9. **Heal pipeline** — manual enqueue if exposed via admin API; otherwise
   deferred (tested indirectly via scrub/anti-entropy in cluster tests).
10. **WAL crash recovery** — `kill()` → respawn with same data dir → objects
    readable.
11. **Prefetch engine** — LIST triggers prefetch → L2 metadata cache warmed
    → subsequent GETs hit cache.

### Out of Scope

- Multi-node cluster testing (separate epic: cluster-mode-debugging)
- EC encoding pipeline integration (codec is real, pipeline is stub)
- Shard distribution to remote nodes (placeholder)
- Bucket creation/deletion API (currently returns 404 — not yet
  implemented)
- Prometheus metrics wiring for GC/compaction stats (currently
  logged but not exported)
- Config hot-reload via SIGHUP
- GPU/CUDA and ISA-L acceleration backends (probed but not
  exercised beyond health check)
- Performance benchmarking

## Test Plan

### Test 1: Cache Cascade (L1 → L2 → L3 → RocksDB)

```
1. Start node with all caches enabled
2. PUT /smoke-bucket/hello.txt "Hello, OceanFS!"  → assert 200
3. GET /admin/caches → record baseline: L1, L2 all misses
4. GET /smoke-bucket/hello.txt → assert 200, body "Hello, OceanFS!"
5. GET /admin/caches → assert L1 hits increased (second GET hits L1)
6. PUT /smoke-bucket/small.bin (100 random bytes) → assert 200
7. GET /smoke-bucket/small.bin → assert 200
8. GET /smoke-bucket/small.bin → assert 200
9. GET /admin/caches → assert L1 hits increased further
   (third GET of small.bin hit L1)
```

### Test 2: Negative Cache (DELETE → L3 Bloom)

```
1. PUT /smoke-bucket/ephemeral.txt "will be deleted" → assert 200
2. GET /smoke-bucket/ephemeral.txt → assert 200
3. DELETE /smoke-bucket/ephemeral.txt → assert 204
4. GET /smoke-bucket/ephemeral.txt → assert 404
5. GET /smoke-bucket/ephemeral.txt → assert 404 (second attempt)
6. GET /admin/caches → assert L3 hits increased by 2
   (Bloom filter confirmed "definitely absent" on both attempts)
```

### Test 3: Segment Lifecycle (All Four Tiers)

```
1. PUT /smoke-bucket/inline.txt  (15 bytes)        → assert 200
2. PUT /smoke-bucket/small.bin   (100 KB random)   → assert 200
3. PUT /smoke-bucket/std.bin     (1 MB random)     → assert 200
4. PUT /smoke-bucket/big.bin     (10 MB random)    → assert 200
5. GET /admin/segments → assert:
   - total > 0
   - by_tier breakdown reflects written sizes
   - multi-segment blob produces multiple segment entries
6. GET all four objects → assert 200, body size matches original
7. HEAD all four objects → assert 200, content-length correct
```

### Test 4: Garbage Collection

```
1. Start node with shortened GC config (gc_interval_sec=10,
   tombstone_ttl_sec=5)
2. PUT smoke-bucket/keep.txt "important data" → 200
3. PUT smoke-bucket/delete-me.txt "garbage data" → 200
4. PUT smoke-bucket/also-delete.txt "more garbage" → 200
5. Wait for segments to seal (seal_timeout_ms=500)
6. Record baseline segment count from GET /admin/segments
7. DELETE smoke-bucket/delete-me.txt → 204
8. DELETE smoke-bucket/also-delete.txt → 204
9. Poll GET /admin/segments every second for up to 30 seconds
   until segment count decreases from baseline (GC compacted
   dead segments)
10. Assert: segment count < baseline
11. Assert: GET smoke-bucket/keep.txt → 200 (still readable)
12. Assert: GET smoke-bucket/delete-me.txt → 404 (reclaimed)
```

### Test 5: Orphan Reaper

```
1. After Test 4 completes, the orphan reaper should have cleaned
   up any fully-dead segments
2. Assert: GET /admin/segments shows no segments with zero
   live objects (reaper deleted orphans)
3. If reaper stats are exposed via an admin endpoint, assert
   orphans_deleted > 0; otherwise, the segment count decrease
   in Test 4 is sufficient evidence
```

### Test 6: Anti-Entropy Merkle Verification

```
1. Start node with shortened AE config (ae_interval_sec=10)
2. PUT several objects to create multiple sealed segments
3. Record baseline segment count from GET /admin/segments
4. Wait up to 15 seconds for an AE cycle to complete
5. Assert: GET /admin/segments shows same segment count (AE
   is read-only, shouldn't change segment inventory)
6. If AE stats are exposed via admin endpoint, assert
   segments_compared > 0 and mismatches_found = 0
```

### Test 7: Manual Scrub

```
1. PUT several objects to create sealed segments
2. POST /admin/scrub → assert 202 Accepted
3. Record baseline segment count from GET /admin/segments
4. Poll GET /admin/segments every 500ms for up to 10 seconds
5. Assert: segment count is unchanged (scrub is read-only)
6. Assert: all segments reported healthy (if scrub report is
   exposed via admin endpoint)
```

**Expected:** Scrub scans all segments, verifies BLAKE3 Merkle roots, reports
all segments healthy. No heal requests enqueued.

### Test 8: Heal Pipeline

```
1. If heal::enqueue_heal() is exposed via admin API:
   a. POST /admin/heal with a test segment_id → assert 202
   b. Poll /admin/segments or heal-specific endpoint to verify
      the heal worker processed the request
2. If not exposed: skip with justification per DK-003.
   Heal is tested indirectly via scrub (Test 7) triggering
   enqueue_heal on corrupt segments in cluster mode.
```

### Test 9: WAL Crash Recovery

```
1. Start node normally
2. PUT smoke-bucket/crash-test.txt "data before crash" → 200
3. PUT smoke-bucket/crash-large.bin (1MB random) → 200
4. Call node.kill() — sends SIGKILL before segments seal
5. Spawn a new node process with the same data directory
6. Assert: GET smoke-bucket/crash-test.txt → 200, body =
   "data before crash"
7. Assert: GET smoke-bucket/crash-large.bin → 200, content-length
   matches original size
```

**Expected:** After crash, WAL replay recovers unsealed segment data.
Objects written before the crash are readable after restart.

### Test 10: Prefetch Engine

```
1. Start node with prefetch_enabled=true
2. PUT smoke-bucket/a.txt ... smoke-bucket/f.txt (6 objects)
3. Record baseline L2 metadata cache stats from GET /admin/caches
4. GET smoke-bucket/ → LIST all 6 keys
5. Wait 2 seconds for prefetch worker to drain its queue
6. Record L2 stats from GET /admin/caches
7. Assert: L2 entry_count increased (prefetch warmed metadata)
8. GET smoke-bucket/c.txt → 200
9. Assert: data matches what was PUT
```

## Crate Impact

| Crate | Change |
|---|---|
| `e2e/` | **NEW** — E2E test crate. Depends on nothing from the workspace except `reqwest` + `serde_json`. Spawns the release binary via `std::process::Command`, hits it over HTTP, and asserts. Contains all 10 tests plus a shared `NodeProcess` harness. |
| `Cargo.toml` | MODIFIED — add `e2e/` to workspace members |
| Others | No changes unless a test exposes a bug |

## Test Harness Design

```rust
// e2e/src/harness.rs
pub struct NodeProcess {
    child: std::process::Child,
    http_addr: SocketAddr,
    grpc_addr: SocketAddr,
    data_dir: TempDir,
}

impl NodeProcess {
    /// Spawns `cargo run -p oceanfs --release` with the given config,
    /// waits for `/admin/health` to return 200, and returns a handle.
    pub async fn spawn(config: &str) -> Result<Self>;

    /// HTTP GET helper.
    pub async fn get(&self, path: &str) -> reqwest::Result<Response>;

    /// HTTP PUT helper.
    pub async fn put(&self, path: &str, body: &[u8]) -> reqwest::Result<Response>;

    /// HTTP DELETE helper.
    pub async fn delete(&self, path: &str) -> reqwest::Result<Response>;

    /// Sends SIGKILL to the child process (crash recovery tests).
    pub fn kill(&mut self) -> Result<()>;

    /// Graceful shutdown via SIGTERM, waits for exit.
    pub async fn shutdown(mut self) -> Result<()>;
}
```

Each test file in `e2e/tests/` uses this harness. Tests are independent:
each spawns its own node process with its own temp data directory and config.

## Key Decisions

### DK-001: Shortened Intervals for Testing

**Decision:** For tests 4 (GC) and 6 (anti-entropy), we create a test
configuration with shortened intervals (gc_interval_sec=10,
tombstone_ttl_sec=5, ae_interval_sec=10) rather than waiting for default
intervals (3600s, 259200s, 300s).

**Rationale:** Waiting 1 hour for GC or 5 minutes for anti-entropy is
impractical for a smoke test. The shortened intervals exercise the same
code paths without changing production behavior.

### DK-002: Test Ordering

**Decision:** Tests are ordered from simplest (cache cascade) to most
complex (WAL recovery). Each test can be run independently.

**Rationale:** If a test fails, we want to isolate the failure to one
subsystem. Running tests in dependency order (cache → segment → GC →
AE → scrub → heal → WAL) means later tests build on confidence from
earlier ones.

### DK-003: Heal Pipeline Testing

**Decision:** The heal pipeline smoke test is conditional. If the
internal `heal::enqueue_heal()` function is accessible from the admin
API, we test it directly. If not, we note that the heal pipeline is
tested indirectly via scrub and anti-entropy triggering heal on
corrupt segments.

**Rationale:** Introducing data corruption for a smoke test is risky and
complex. The heal worker's unit tests already verify the EC decode path.
We defer full end-to-end heal testing to the cluster mode tests where
node failure naturally creates heal scenarios.

### DK-004: E2E Harness — Spawn Binary, Not Library

**Decision:** All tests spawn the release binary as a child process and
communicate via HTTP. No `TempDir` + `Node::start()` in-process patterns.
No shell scripts grepping logs.

**Rationale:** Tests that call `Node::start()` in-process test the Rust API,
not the shipped artifact. They can hide bugs in CLI parsing, config loading,
signal handling, and process lifecycle that only manifest when running the
real binary. Spawning the binary and hitting it over HTTP is the same
interaction a real user has. The small overhead of process spawn (~50ms)
is acceptable for a suite that runs in CI on every push.

## Definition of Done

- [ ] **Harness:** `e2e/Cargo.toml` created and added to workspace members.
  `e2e/src/harness.rs` provides `NodeProcess::spawn()`, `get()`, `put()`,
  `delete()`, `kill()`, `shutdown()`. Config helpers for each test scenario
  (standard, short-GC, short-AE, prefetch-enabled, etc.).
- [ ] **Test 1:** Cache cascade — L1/L2/L3 hits/misses change correctly
  after PUT/GET operations, asserted via `/admin/caches`
- [ ] **Test 2:** Negative cache — DELETE inserts into Bloom filter,
  subsequent GETs hit L3 "definitely absent", asserted via `/admin/caches`
- [ ] **Test 3:** Segment lifecycle — all four tiers produce expected
  segment counts in `/admin/segments`, all objects readable
- [ ] **Test 4:** GC — shortened-interval GC cycle runs, segment count
  decreases after compaction (polled via `/admin/segments`), live objects
  still readable, deleted objects return 404
- [ ] **Test 5:** Orphan reaper — unreferenced segments are cleaned up
  after GC compaction completes
- [ ] **Test 6:** Anti-entropy — Merkle trees built and verified, no
  mismatches on clean data, segment inventory unchanged
- [ ] **Test 7:** Scrub — `POST /admin/scrub` returns 202, segments
  reported healthy, no corruption detected
- [ ] **Test 8:** Heal pipeline — manually enqueued heal processed
  (or deferred with justification per DK-003)
- [ ] **Test 9:** WAL recovery — `kill()` + respawn recovers unsealed
  data, objects readable after restart
- [ ] **Test 10:** Prefetch engine — LIST triggers prefetch, L2 cache
  entry count increases, subsequent GETs serve correct data
- [ ] **CI:** `cargo test -p e2e` passes in CI. Tests are independent
  (each spawns its own node) and can run in parallel.
- [ ] **Bugs:** Any bugs found are fixed and committed (or documented
  as known issues with ADRs)
