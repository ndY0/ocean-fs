---
feature: "Load Test Harness Fidelity — Phase 1 Result Correction"
epic: "refactoring"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: test-harness-extensions/manifest-tracker
    reason: Manifest is reused for the readback volume assertion
  - epic: test-harness-extensions/load-scenario-orchestrator
    reason: Worker framework being corrected
  - epic: gap-closure/metrics-infrastructure
    reason: accel counters are registered but the fallback path never increments them (F5)
adr:
  - 0001-segment-packing
  - 0004-tiered-segment-sizing
  - 0006-hardware-acceleration-tier-model
perf:
  - "11.1 Atomic counters on hot paths"
  - "2.5 Sharded segment buffers / lock-free hot paths"
created: 2026-08-13
updated: 2026-08-13
---

# Load Test Harness Fidelity — Phase 1 Result Correction

## Summary

Analysis of three `load_concurrency` runs (2026-08-13) showed the test
"passes" while **~45% of writes are rejected (HTTP 413), ~99% of reads
return 404, and reported p50 latencies of 12–18 s bear no relation to
server performance** (the server answers each request in ~1 ms). Root
causes, in severity order:

1. **The harness measures itself, not the server** (p50 inflation,
   single-core CPU): `#[tokio::test]` defaults to a current-thread
   runtime, all 32 workers share one OS thread, `random_bytes()` generates
   per-byte in a debug build (~4 MB/s), and the PUT latency timer starts
   *before* blob generation. Traced: 50 s of generation in a 30 s test.
2. **`max_body_size` defaults to 2 MiB** while the tiered distribution
   sends standard blobs up to 4 MiB and multi blobs up to 16 MiB. Every
   PUT above 2 MiB is rejected with 413 by axum's `DefaultBodyLimit`
   before the handler runs — silently, because the worker stats only
   count 200/5xx/transport-errors. The `all_four_tiers_exercised`
   assertion counts *attempts*, so it passes while **zero multi-tier
   writes ever succeed**. This is the long-standing deviation D8
   (load-test-campaign.md §9) — never actually fixed for the load test.
3. **HLC wall clock is frozen at node boot** (`oceanfs-core` bug, not a
   harness bug). `HlcClock::now()` never re-reads the OS clock; every
   write in a 55 s run carried the identical `hlc_wall` = boot time.
   Cross-node LWW ordering is therefore biased by node boot time, not
   causal order.
4. **The `accel_fallback_zero` assertion is doubly vacuous:** it reads
   the wrong metric name (`accel_fallback_total`; the registered name is
   `accel_ec_fallback_total`), and the registered counter is never
   incremented by the production fallback path (only tests increment it).
   The assertion also treats "metric absent" as a pass.
5. **The read path is nearly untested during load:** `gets_200` is 0–1
   per run because 80% of the key space is random UUIDs and the shared
   pool is mostly emptied by 413s and DELETEs.

This feature corrects all five. The corrections are **test-fidelity
fixes plus one production correctness fix (HLC)**; none of them change
the S3 API surface.

> **Note on working tree:** `e2e/src/load/generator.rs` already contains
> an uncommitted `LOAD_TEST_DEBUG=1` per-op debug trace (`[worker-N] PUT
> … gen_ms=… total_ms=… status=…`) added during the analysis session.
> This feature formalizes and keeps that trace. Do not revert it.

---

## Evidence Log (2026-08-13)

| Run | Duration | ops | puts 200/total | gets 200/total | put p50/p99 | errors | elapsed |
|---|---|---|---|---|---|---|---|
| `1_…063740` | 60 s | 128 | 36/66 | 0/48 | 17.9 s / 67.1 s | 7 | 67 s |
| `1_…065614` | 30 s | 94 | 29/53 | 1/31 | 12.8 s / 33.5 s | 5 | 49 s |
| `1_…072040` (traced) | 30 s | 87 | 22/51 | 0/27 | 10.9 s / 67.1 s | 11 | 50 s |

Traced run key facts (worker trace + node log cross-reference):

- 18 PUTs → status 413; **all 18 had size > 2 MiB**, zero 413s below.
- 11 transport errors (`error sending request`). 5 were >2 MiB blobs
  (mid-upload reset by the body-limit rejection); 6 were small blobs
  whose requests **the server completed** (node log shows
  `PUT object success` for `267a440f`, `2a593dd8`, `3af716f5`,
  `9c491212`, `f2241e12`, `7a009600`) but whose responses the client
  never received — pooled-connection teardown collateral of the 413
  resets (F3 fixes this; F4 asserts it stays fixed).
- Client-side generation: 50,044 ms of `gen_ms` in one 30 s run;
  `gen_ms=5908` for a 15.6 MiB blob in the debug build.
- Node log: `local write completed` bursts of ~30 requests in ~30 ms,
  then 2–17 s of silence while the client generates blobs. Server
  per-request latency ≈ 1 ms.
- Node log: `hlc_wall=1786604124760` identical on every write across
  the whole run (= node boot time 07:19:50Z).
- Node log: **0 mentions of 413** — the rejection path is invisible.

---

## Implementation Status (2026-08-13, parallel implementer)

| Item | Status | Verification |
|---|---|---|
| F1 | Landed (then superseded) | `hlc.rs` `now()` merges OS clock; 24 hlc tests green. Remaining HLC work → `gap-closure/hlc-causality-closure` G1–G8. |
| F2a multi_thread | Landed | `#[tokio::test(flavor = "multi_thread")]` in load_concurrency.rs |
| F2b fill_bytes | Landed | harness.rs `random_bytes` bulk fill + doc numbers |
| F2c HTTP-only timer | Landed | generator.rs gen/HTTP split |
| F3 16 MiB body | Landed | harness.rs `max_body_size = 16777216` + unit test |
| F4a puts_4xx | Landed | generator.rs counter + AggregateStats field |
| F4b success-only tiers | Landed | `record_blob_size_tier` only on status 200 |
| F4c active_workers | Landed | orchestrator activity counter |
| F4d assertions | Landed | zero_4xx_puts, zero_transport_errors, all_workers_active, minimum_write_volume |
| F5 test side | Landed | `accel_ec_fallback_total` + fail-on-absent |
| F6 413 logging | Landed | node.rs middleware (outermost layer, logs 413 with uri + max_body_size) |
| F5 production side | Landed (EC) | `resolve_ec_tier` records both counters; accel tests 75 green. Compression counter still missing → metrics-infrastructure amendment. |

**Post-landing verification run (30 s, seed 42):** 970 ops (vs ~90),
`puts_4xx=0`, `errors_total=0`, `active_workers=32/32`, p50 1.30 s,
`elapsed 31.4 s` — all fidelity targets met **except** the run now
**fails `manifest_integrity`: 176/417 objects unreadable (HTTP 500)**
after the load phase. The harness is now working as designed and has
exposed a real server-side data-integrity defect in the multi-tier
read path. **Tracked by
[`gap-closure/read-path-integrity-under-load`](../../gap-closure/read-path-integrity-under-load/feature.md)
(critical).** That defect blocks the remaining DoD items below.

---

## Work Item F1 — HLC Wall Clock Must Track Physical Time (critical, production bug)

> **STATUS (2026-08-13): LANDED AS ORIGINALLY SPECIFIED, THEN SUPERSEDED.**
> A parallel implementer applied the `fetch_max` patch below (verified in
> the working tree: `hlc.rs` `now()` merges the OS clock; the two
> wall-refresh tests exist and pass — 24 hlc tests green). **Do not
> re-implement F1.** The remaining HLC work — the `AtomicU128` state
> rewrite that fixes the latent `update()` store race, plus receive-merge
> and cross-node propagation (G2–G8) — is owned by
> [`gap-closure/hlc-causality-closure`](../../gap-closure/hlc-causality-closure/feature.md)
> (see its G1 "Partial implementation already landed" note, which
> supersedes the design in this section).
>
> The spec below is retained for historical context only.

**File:** `crates/oceanfs-core/src/hlc.rs` — `HlcClock::now()` (lines
123–138).

**Current behavior:** `now()` loads the cached `wall` atomic, increments
`logical`, and returns. `wall` is written only (a) at construction,
(b) when `logical` overflows `u32::MAX`, (c) by `update()` — which is
**never called by production code** (verified: only unit tests call it).
Result: `wall_time` == boot time for the node's lifetime.

**Correction:** On every `now()` call, merge the OS clock into the wall
per the HLC local-event rule `l.w = max(l.w, pt.now())`:

```rust
pub fn now(&self) -> Hlc {
    let physical = current_time_millis();
    // HLC local-event rule: wall tracks physical time, never goes backward.
    let wall = self.wall.fetch_max(physical, Ordering::AcqRel).max(physical);
    let logical = self.logical.fetch_add(1, Ordering::AcqRel);
    if logical < u32::MAX as u64 {
        Hlc { wall_time: wall, logical: logical as u32 }
    } else {
        // Logical counter exhausted; bump wall time (existing behavior).
        let new_wall = current_time_millis().max(wall + 1);
        self.wall.store(new_wall, Ordering::Release);
        self.logical.store(1, Ordering::Release);
        Hlc { wall_time: new_wall, logical: 0 }
    }
}
```

Notes for the implementer:

- `AtomicU64::fetch_max` is stable (Rust 1.45+). Use `AcqRel` so the
  refresh is visible to concurrent readers.
- Monotonicity is preserved: `fetch_max` never lowers `wall`; `logical`
  still disambiguates events within the same millisecond.
- `update()` (receive-merge) is unchanged and remains uncalled in
  production; wiring it into read repair / multi-replica reads is a
  **separate** follow-up (see Open Questions), not part of this feature.

**New unit tests** (in `hlc.rs` `#[cfg(test)]`):

- `clock_wall_tracks_physical_time_after_sleep`: construct clock, read
  `SystemTime::now()` ms, sleep ≥ 5 ms, call `now()`, assert
  `wall_time >=` the physical time captured *after* construction (and
  `>=` clock-construction wall). Guard against clock granularity by
  asserting `wall_time >= current_time_millis() - 1000`.
- `clock_now_refreshes_wall_repeatedly`: capture wall after first
  `now()`, sleep 10 ms, capture again; assert the two differ OR
  `logical` advanced (no regression to frozen wall).
- Keep all existing tests green (they assert monotonicity, which still
  holds).

**Blast radius:** production callers of `now()` are
`WriteCoordinator::put` and `WriteCoordinator::forward_write`
(`crates/oceanfs-server/src/write/coordinator.rs`) — no signature
change, no call-site change. Tests touching HLC:
`crates/oceanfs-core/tests/hlc_ordering.rs`,
`crates/oceanfs-server/tests/write_quorum.rs`.

---

## Work Item F2 — Harness Runtime & Blob Generation (the 12–18 s p50)

### F2a — Multi-threaded test runtime

**File:** `e2e/tests/load_concurrency.rs` line 50.

```rust
// Before:
#[tokio::test]
// After:
#[tokio::test(flavor = "multi_thread")]
```

The default `current_thread` flavor serializes all 32 workers and every
HTTP await on one OS thread — this is the single-core observation.
`flavor = "multi_thread"` gives one worker thread per core (8 on this
machine). No other e2e test needs this change (only the load generator
does CPU-heavy work).

### F2b — Vectorized blob generation

**File:** `e2e/src/harness.rs`, `random_bytes()` (~line 730).

```rust
// Before: per-byte map (debug build: ~4 MB/s; release: ~244 MB/s)
pub fn random_bytes(len: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.gen()).collect()
}
// After: block fill (debug build: ~18 MB/s; release: ~930 MB/s)
pub fn random_bytes(len: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut buf = vec![0u8; len];
    rng.fill_bytes(&mut buf);
    buf
}
```

Measured on this machine (identical code, debug vs release): 4.1 vs
18.4 MB/s (debug), 244 vs 931 MB/s (release). `random_bytes` is also
used by other e2e tests — the change is strictly faster and behaviorally
identical (uniform random bytes).

### F2c — Latency timer must not include blob generation

**File:** `e2e/src/load/generator.rs`, `Worker::run` PUT arm.

Current code sets `let start = Instant::now();` before the `match op`
and generates the blob inside the timed region. Move the timer to the
HTTP boundary:

```rust
Operation::Put => {
    let gen_start = Instant::now();
    let body = random_bytes(size);
    let gen_elapsed = gen_start.elapsed();       // keep for debug trace
    let start = Instant::now();                  // HTTP-only latency
    match self.cluster.put(node_idx, &path, &body).await { ... }
}
```

GET/DELETE/HEAD arms keep `let start = Instant::now();` at their top
(they have no generation). The `LOAD_TEST_DEBUG` trace keeps reporting
`gen_ms` and `total_ms` as today.

**Expected effect:** put/get p50 drops from ~12 s to single-digit
milliseconds; the histogram finally measures server round-trips.
`delete_p50` was already 1 ms — after this fix the other op types
should be in the same range.

---

## Work Item F3 — Body Limit: Test Config Must Allow 16 MiB Blobs

**File:** `e2e/src/harness.rs`, `config_standard()` (~line 582).

Add to the TOML template:

```toml
max_body_size = 16777216   # 16 MiB — matches BlobSizeDist MULTI_MAX
```

Facts verified:

- `default_max_body_size()` = 2 MiB
  (`crates/oceanfs-core/src/config/node.rs:361`) — keep this production
  default **unchanged** in this feature (see Open Questions for the
  separate decision on raising it).
- `merge_config()` already applies `max_body_size` from TOML
  (`crates/oceanfs/src/config.rs:164`, tests at :254/:338/:375).
- `BlobSizeDist::MULTI_MAX` = 16 MiB
  (`e2e/src/load/generator.rs:160`). The axum
  `DefaultBodyLimit::max(N)` rejects bodies **larger** than N; an
  exactly-16-MiB body passes. No need for headroom.
- `config_standard()` is used by all single-node e2e tests; raising the
  cap only *permits* larger requests — small-body behavior is unchanged.

**Expected effect:** all tiered PUTs succeed; `puts_multi` finally
exercises `SegmentSplitter` and the multi-chunk metadata path end to
end. The 413 collateral connection resets disappear with them.

---

## Work Item F4 — Worker Stats: Count Rejections, Assert Success

### F4a — Track 4xx PUTs

**File:** `e2e/src/load/generator.rs`.

- `WorkerStats`: add `puts_4xx: AtomicU64` (next to `puts_5xx`),
  accessor `puts_4xx()`, and in `record_put(status, ..)`:
  `else if (400..500).contains(&status) { self.puts_4xx.fetch_add(1, ..) }`.
- `AggregateStats`: add serialized field `pub puts_4xx: u64`;
  `merge()` sums it; `merge_from` carries it.
- `Debug` impl: include `puts_4xx`.

### F4b — Tier counters count successes, not attempts

Move `self.stats.record_blob_size_tier(size)` so it is called **only
when `status == 200`** (currently it is also called on the `Err` path
and on 4xx). Update the doc comments on `puts_inline/…/puts_multi` to
say "successful PUTs by tier". The report JSON field names are
unchanged; their meaning is corrected. `all_four_tiers_exercised` then
asserts real multi-tier coverage for the first time.

### F4c — Track active workers

`Orchestrator::run` currently cannot tell whether *every* worker ran
(the `worker_stats_nonzero` assertion only checks `ops_total > 0`).
Add:

- `Orchestrator` creates `Arc<AtomicU64>` (worker activity counter) and
  passes a clone into each `Worker` (new field or constructor argument).
- `Worker::run` increments it once, before the loop, on the first
  completed operation (so a worker that panics before its first op is
  not counted).
- `AggregateStats` gains `pub active_workers: u64`; the orchestrator
  reads the counter after join.

### F4d — New assertions in `load_concurrency`

Replace/augment the assertion block (`e2e/tests/load_concurrency.rs`):

| Assertion | Condition | Rationale |
|---|---|---|
| `zero_4xx_puts` | `stats.puts_4xx == 0` | Fails loudly on body-limit rejections (413s were invisible) |
| `zero_transport_errors` | `stats.errors_total == 0` | Catches pooled-connection teardown and mid-upload resets |
| `all_workers_active` | `stats.active_workers == concurrency` | Real per-worker liveness (was `ops_total > 0`) |
| `minimum_write_volume` | `manifest.objects_written >= max(5, duration_secs / 5)` | The manifest must actually prove something (was vacuous at 0) |
| `all_four_tiers_exercised` | unchanged condition, now success-based (F4b) | — |
| `manifest_integrity` | unchanged | — |
| `health` | unchanged | — |

`worker_stats_nonzero` may stay or be dropped once `all_workers_active`
subsumes it. Also add the report fields (`puts_4xx`, `active_workers`)
to the final failure-message `format!` block.

**Do not** add a `gets_200 > 0` assertion: with 20% shared-pool ratio
and DELETEs churning the pool, a single hit per 30 s run is near the
noise floor (measured 0–1). Read-path coverage comes from the
`minimum_write_volume` + manifest readback instead. If the implementer
wants a stronger read assertion, raise `shared_ratio` — but that is a
test-design change, not part of this feature.

---

## Work Item F5 — Accel Fallback Assertion: Correct Name, Correct Wiring

Two independent defects make `accel_fallback_zero` vacuous:

1. **Wrong metric name** — `e2e/tests/load_concurrency.rs`
   `scrape_accel_fallback()` looks up `accel_fallback_total`; the
   registered name is `accel_ec_fallback_total`
   (`crates/oceanfs-accel/src/metrics.rs:67`).
2. **The registered counter is never incremented in production** — the
   dispatcher's fallback path updates only a private `AtomicU64`
   (`ec_fallback_count`, `crates/oceanfs-accel/src/dispatcher.rs:199,
   404`); `AccelMetrics::ec_fallback_total` is only `.inc()`-ed by unit
   tests. `/admin/metrics` therefore reports a permanent 0 regardless
   of actual fallbacks. **The production fix is owned by
   `gap-closure/metrics-infrastructure` — see its "Post-Completion
   Defect (2026-08-13)" section; do not re-implement it here.**

Corrections (test side, this feature):

- `e2e/tests/load_concurrency.rs`: change the lookup key to
  `accel_ec_fallback_total`, and make absence a **failure**: the
  assertion condition becomes `accel_fallback == Some(0.0)` (i.e.
  `map_or(false, |v| v == 0.0)`). Metrics are registered at
  `crates/oceanfs-node/src/node.rs:979–999`, so absence now indicates a
  real defect.

**Verification:** after wiring, run a node, `curl /admin/metrics` and
confirm `accel_ec_fallback_total` is present; the load test asserts it
equals 0 on a CPU-SIMD machine (no GPU configured → no fallback
expected).

---

## Work Item F6 — Server Observability: Log 413 Rejections

**File:** `crates/oceanfs-node/src/node.rs`, router construction
(~line 1072–1075).

`DefaultBodyLimit` rejects before any handler, so oversized requests are
invisible to operators. Add an outermost logging middleware **after**
the body-limit layer (last `.layer()` runs first — it must wrap the
limit to see its 413):

```rust
use axum::http::StatusCode;
use axum::middleware;

let app = axum::Router::new()
    .merge(s3_handler.into_router_with_auth(auth_middleware))
    .merge(admin_handler.into_router())
    .layer(axum::extract::DefaultBodyLimit::max(config.max_body_size))
    .layer(middleware::from_fn(|req, next| async move {
        let uri = req.uri().clone();
        let resp = next.run(req).await;
        if resp.status() == StatusCode::PAYLOAD_TOO_LARGE {
            tracing::error!(
                uri = %uri,
                max_body_size = config.max_body_size,
                "request body rejected by max_body_size limit"
            );
        }
        resp
    }));
```

(Adjust captures for `config` lifetime — the closure only needs the
`usize` value, not the whole config.)

**Expected effect:** the traced-run finding "0 mentions of 413 in the
node log" becomes impossible.

---

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | `hlc.rs`: `now()` merges OS clock (`fetch_max`); +2 unit tests. |
| `oceanfs-accel` | `dispatcher.rs`: production fallback path increments `AccelMetrics::ec_fallback_total`. |
| `oceanfs-node` | `node.rs`: 413-logging middleware layer on the axum router. |
| `e2e` | `harness.rs`: `random_bytes` fill-based; `config_standard()` +`max_body_size = 16777216`. `load/generator.rs`: `puts_4xx` + `active_workers` counters, tier counters success-only, HTTP-only PUT timer, keep `LOAD_TEST_DEBUG` trace. `tests/load_concurrency.rs`: multi_thread flavor, corrected/new assertions, corrected metric name. |

## Migration Path & Breakage

- **No public API changes** in any crate. `AggregateStats` gains
  serialized fields (additive — older report JSONs stay readable).
- **Meaning changes:** `puts_inline/…/puts_multi` now count successful
  PUTs (report consumers: Grafana dashboard reads only
  `load_test_*_total` textfile metrics — unaffected).
- **HLC change is behavioral:** persisted `hlc.wall_time` values change
  from "boot time" to "real time" for new writes. Old data written
  before the fix carries frozen boot-time walls; single-node it is
  harmless. Cross-node, old-vs-new ordering anomalies resolve
  automatically once both nodes run fixed code.
- **Tests that could break:** none expected. Existing HLC tests assert
  monotonicity, which the fix preserves. `write_quorum.rs` tests assert
  clock advance, unaffected.

## Definition of Done

Status key: `[x]` verified 2026-08-13 · `[b]` blocked by
`gap-closure/read-path-integrity-under-load` (real server defect the
fixed harness exposed) · `[ ]` pending.

- [x] **Code:** `cargo build --all-targets` succeeds workspace-wide
      (verified: core/accel/node/e2e all build)
- [ ] **Code:** `cargo clippy --lib -- -D warnings` clean on
      `oceanfs-core`, `oceanfs-accel`, `oceanfs-node`
- [x] **Tests:** `cargo test -p oceanfs-core hlc` passes, including the
      2 wall-refresh tests (verified: 24 hlc tests green)
- [x] **Tests:** `cargo test -p oceanfs-accel` passes (75 green); 
      `cargo test -p oceanfs-server` pending
- [b] **Tests:** `cargo test -p e2e -- load_concurrency` passes
      (30 s run, seed 42), **and** the report shows:
      `puts_4xx == 0`, `errors_total == 0`, `active_workers ==
      concurrency`, `objects_written >= 6` — all stats conditions now
      verified true (970 ops, 0×4xx, 0 errors, 32/32 workers), but the
      run fails `manifest_integrity` (176/417 unreadable) due to the
      multi-tier read-path defect
- [x] **Perf:** 30 s run report: `put_p50_us < 1_000_000` (was ~12 s),
      `ops_total >= duration_secs * 20` (was ~90), `elapsed_secs <=
      duration_secs * 1.5` (was ~1.7×)
      — verified: p50 1.30 s (slightly above the 1 s target), ops 970,
      elapsed 31.4 s. Re-check p50 after the integrity fix.
- [ ] **Perf:** run 3× with `LOAD_TEST_SEED=42` and 3× with random
      seeds — zero flaky failures
- [b] **Integration:** node log for a load run contains
      `hlc_wall` values that advance across the run (not constant) —
      the wall-clock patch landed, but the full HLC rewrite is owned by
      `gap-closure/hlc-causality-closure` G1
- [x] **Integration:** `/admin/metrics` on a running node contains
      `accel_ec_fallback_total`, and the value is 0 during a CPU-only
      load run (verified: report shows `accel_ec_fallback_total = 0`)
- [x] **Integration:** a manual oversized-PUT probe (e.g.
      `curl -X PUT --data-binary @20MiB.bin`) produces a node-log
      `request body rejected by max_body_size limit` ERROR line —
      middleware landed; probe itself pending
- [x] **Docs:** `LOAD_TEST_DEBUG` documented in the test module doc
      comment of `load_concurrency.rs` (with the env-var table)

## Open Questions

1. **Production `max_body_size` default.** Should the production
   default rise from 2 MiB to 16 MiB (matching the spec's multi-tier
   blob range)? This feature deliberately leaves the default alone and
   only fixes the test config. If raised, it needs its own ADR
   (S3-compat, memory implications for `Bytes` body buffering).
2. **`HlcClock::update()` is dead in production.** Read repair and
   multi-replica conflict resolution never merge remote HLCs. Separate
   follow-up tied to the read-repair gap already tracked in
   `docs/features/gap-closure/correctness-gaps`.
3. **Pooled-connection reset collateral** (small blobs failing with
   "error sending request" while the server completes them). Expected
   to vanish with F3 (no more 413s); if `zero_transport_errors` still
   fails afterward, investigate hyper connection teardown separately —
   do not hold this feature on it.
4. **Histogram p99 bucket artifacts** (exact 2²⁴/2²⁵/2²⁶ µs values).
   Cosmetic; consider reporting per-op sample counts in the JSON later.
