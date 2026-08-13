---
feature: "E2E Binary Verification & Default Log Capture"
epic: "test-harness"
status: proposed
priority: high
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-08-13
updated: 2026-08-13
---

# E2E Binary Verification & Default Log Capture

## Summary

Two test-harness reliability fixes in the `e2e` crate. (A) Binary
verification: `resolve_binary_path()` (e2e/src/harness.rs:540) prefers
`target/release/oceanfs` over `target/debug/oceanfs`; during gap-closure
the release binary was stale (built pre-fix) while debug was current, so
several e2e load runs silently tested the OLD binary and produced false
failure signatures that cost a full forensics round. After resolution,
verify the binary matches the sources (mtime comparison against the newest
source file under crates/, or a build-id/hash recorded at build time) and
fail the test with a clear message when stale. (B) Default log capture:
node logs are captured only when `E2E_CAPTURE_NODE_LOGS=1`, and the default
`E2E_NODE_LOG_LEVEL` is "error" (harness.rs:302–305) — the seal-worker
"skipping seal", "seal queue full", and read-path failure signatures
(warn/info/debug) are invisible in default runs. Capture node
stdout+stderr into `e2e/target/e2e-logs/` by default for all
node-spawning tests, default the level to "info" ("debug" for
load_concurrency), add a grep helper over captured logs, and make the load
test assert on log cleanliness rather than leaving it to manual review.

## Evidence/Motivation

**Problem A — stale binary silently tested (proven during gap-closure):**

- `resolve_binary_path()` resolution order (harness.rs:536–539 doc,
  540–567): `OCEANFS_BIN` env → `target/release/oceanfs` →
  `target/debug/oceanfs` (relative to cwd, then `CARGO_MANIFEST_DIR`),
  then PATH fallback. Nothing verifies the binary matches the sources.
- The release binary was stale (built pre-fix) while debug was current;
  several e2e load runs silently exercised the old binary, producing
  false failure signatures that cost a full forensics round before the
  cause was identified.

**Problem B — failure signatures invisible in default runs:**

- Log capture is opt-in (`E2E_CAPTURE_NODE_LOGS=1`, harness.rs:303–305);
  without it, stdout/stderr are discarded to null (harness.rs:331).
- Default `E2E_NODE_LOG_LEVEL` is "error" (harness.rs:302) — the
  signatures the gap-closure reviews grep for are warn/info/debug:
  "skipping seal" (debug, coordinator.rs:641), "seal queue full" (warn,
  pool.rs:519), `BadDigest` / `cannot fetch chunk` read-path failures.
- The pool-backpressure DoD's 5xx gate had to be log-based, and reviewers
  had to set env vars manually to produce the captured node logs their
  verification relied on.
- **Log-dir path quirk:** `PathBuf::from("target/e2e-logs")`
  (harness.rs:311) is relative to the TEST BINARY's cwd (the e2e crate
  root when run via `cargo test -p e2e`), so files land in
  `e2e/target/e2e-logs`. The convention is implicit and undocumented —
  this feature decides and documents a single convention.
- `e2e/tests/load_concurrency.rs` asserts manifest_integrity, health,
  accel_fallback_zero, zero_4xx_puts, zero_transport_errors,
  all_workers_active, minimum_write_volume, all_four_tiers_exercised —
  no log-based assertion exists.

## Design & Scope

### A — binary verification

1. After `resolve_binary_path()` resolves a non-`OCEANFS_BIN` path,
   compare the binary's mtime against the newest source file under
   `crates/` (recursively: `.rs`, plus workspace `Cargo.toml` /
   `Cargo.lock` / `build.rs`).
2. If the binary is older than the newest source → fail the test with a
   message naming the binary, its mtime, the newest source file and its
   mtime, and the fix (`cargo build --release`, or pin a known-good
   binary via `OCEANFS_BIN`).
3. Prefer the mtime check for simplicity. If it proves flaky (clock skew,
   checkout mtimes), fall back to a build-id/hash recorded at build time —
   Open Question 1.
4. `OCEANFS_BIN` behavior: keep it — verify the path exists, do NOT
   staleness-check it (the operator's responsibility); document this in
   the function doc comment.
5. The staleness failure must be distinct (a clear panic before spawning),
   never a silent skip.

### B — default log capture

1. Capture node stdout+stderr into the log dir BY DEFAULT for all
   node-spawning tests (drop the `E2E_CAPTURE_NODE_LOGS` opt-in gate, or
   make it default-on with an explicit opt-out).
2. Default `E2E_NODE_LOG_LEVEL` = "info"; load_concurrency requests
   "debug" (per-test override — Open Question 3 for the exact mechanism).
3. Decide and document a single convention for the log dir:
   `e2e/target/e2e-logs` (the current de-facto location, relative to the
   e2e crate root) — document that quirk explicitly, or anchor the path to
   `CARGO_MANIFEST_DIR`.
4. Add a harness helper to read/grep captured logs: per-node
   `captured_logs()` / `grep_logs(pattern)`, plus a cluster-level
   "any node log contains pattern" helper.
5. `load_concurrency`: add a `logs_clean` assertion — captured node logs
   contain none of `no appending segment available in pool`, `BadDigest`,
   `cannot fetch chunk` (the three signatures from the gap-closure
   post-review). Must fail pre-fix by construction — mutation note: verify
   the assertion fails when a matching signature is injected or the test
   is pointed at a deliberately broken binary.

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | `src/harness.rs`: binary staleness check + `OCEANFS_BIN` doc, default log capture + level default + log-dir convention, grep helper(s). `src/load` (if needed): log-clean data plumbing. `tests/load_concurrency.rs`: `logs_clean` assertion. Unit tests for the staleness check. |
| oceanfs crates | None. |

## Definition of Done

- [ ] **Code:** `cargo build -p e2e --tests` succeeds
- [ ] **Tests:** new unit test — a stale mtime triggers the failure
      message; a fresh binary passes
- [ ] **Tests:** existing e2e suites still pass (at minimum load_concurrency
      30 s seed 42)
- [ ] **Behavior:** after a default run (no env vars), node logs are
      present in `e2e/target/e2e-logs`
- [ ] **Behavior:** the log-clean assertion in load_concurrency is
      verified, and verified to fail pre-fix by construction (mutation
      note: inject a signature or point at a deliberately broken binary)

## Open Questions

1. mtime vs build-id/hash for staleness detection?
2. Delete old logs per run, or accumulate (append, like today)?
3. Default log level per test: "info" everywhere with load_concurrency at
   "debug", or "debug" as the harness default?
