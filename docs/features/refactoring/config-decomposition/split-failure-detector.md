---
feature: "Split Failure Detector"
epic: "refactoring/config-decomposition"
status: done
priority: medium
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Failure detector types reference NodeId and shared types from oceanfs-core
  - feature: split-membership
    reason: Membership module should be split first for clean member-state access
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-05
---

# Split Failure Detector

## Summary

`crates/oceanfs-membership/src/failure_detector.rs` is 519 lines implementing
the SWIM failure detection protocol. The SWIM protocol has natural boundaries
between its ping phase (probing peers for liveness) and its suspicion phase
(escalating unresponsive peers to suspected-then-failed states). Split into a
`failure_detector/` directory with `ping.rs` (ping logic and timeout handling),
`suspicion.rs` (suspicion mechanism and failure declaration), and `mod.rs`
(coordinator tying the phases together, plus re-exports). Tests move alongside
their respective phases.

## Scope

### In Scope

- Delete `src/failure_detector.rs`
- Create `src/failure_detector/` directory with:
  - `failure_detector/mod.rs` — coordinator tying ping and suspicion phases
    together; the main `FailureDetector` struct (or equivalent coordinator
    type); re-exports for downstream consumers
  - `failure_detector/ping.rs` — ping logic: selecting ping targets, sending
    pings, indirect ping (ping-req), timeout handling, round-trip time tracking
  - `failure_detector/suspicion.rs` — suspicion mechanism: transitioning a node
    from alive → suspected → failed; suspicion timeout; gossip dissemination of
    suspicion state
- Migrate all `#[cfg(test)]` tests from the old `failure_detector.rs` into the
  appropriate phase file (`ping.rs` or `suspicion.rs`). Tests that exercise the
  full protocol (ping → suspicion → failure) go in `mod.rs`.
- Update `src/lib.rs` to declare `pub mod failure_detector;` (this already
  exists — it transparently points to `failure_detector/mod.rs`)

### Out of Scope

- Changing SWIM protocol behavior, timeouts, or state transitions. Pure
  structural refactor.
- Moving the failure detector between crates. It stays in
  `oceanfs-membership`.
- Changing the public API or trait boundaries. The `FailureDetector` type
  (or equivalent) retains the same `pub` surface.
- Adding new tests or expanding test coverage.
- Integrating with the gossip protocol beyond the existing integration points.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-membership` | Delete `src/failure_detector.rs`; create `src/failure_detector/` directory with 3 files: `mod.rs`, `ping.rs`, `suspicion.rs` |

## Interface (Public API)

No public API additions or removals. The re-export facade in
`failure_detector/mod.rs` exports the same types previously exported from
`failure_detector.rs`. Downstream consumers
(`use oceanfs_membership::failure_detector::FailureDetector`) continue to work.

```rust
// oceanfs-membership/src/failure_detector/mod.rs
mod ping;
mod suspicion;

pub use ping::{PingState, PingTarget, PingResult};
pub use suspicion::{SuspicionState, FailureVerdict};

// The main coordinator type (adjust names to match actual codebase)
pub struct FailureDetector {
    ping: PingState,
    suspicion: SuspicionState,
    // ...
}
```

If `FailureDetector` is already re-exported from `oceanfs-membership/src/lib.rs`
(since the architecture §1.2 lists `FailureDetector` as a public API item of
`oceanfs-membership`), the lib.rs re-export chain is preserved.

## Data Flow

Pure structural refactor. No runtime data flow changes. The SWIM protocol
phases remain identical:

```
FailureDetector::run_cycle()
  → ping::select_targets()         // choose peers to probe
  → ping::send_pings()             // direct pings
  → ping::handle_timeouts()        // collect unresponsive peers
  → suspicion::evaluate()          // escalate to suspected
  → suspicion::check_timeouts()    // escalate suspected → failed
  → suspicion::declare_failure()   // notify membership layer
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds workspace-wide; no new
  warnings
<!-- REVIEW: VERIFIED — `cargo build --all-targets -p oceanfs-membership` passes cleanly -->
- [x] **Tests:** `cargo test -p oceanfs-membership` passes; all tests from the
  old `failure_detector.rs` pass in their new file locations
<!-- REVIEW: VERIFIED — all 47 tests (39 unit + 8 integration) pass including failure detector tests -->
- [x] **Docs:** Every `pub` item in `ping.rs`, `suspicion.rs`, and `mod.rs` has
  a doc comment; `#![deny(missing_docs)]` passes for `oceanfs-membership`
<!-- REVIEW: VERIFIED — RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p oceanfs-membership passes cleanly -->
- [x] **ADR:** N/A — implements existing guideline §3.3, no new architectural
  decision required. The SWIM protocol's natural phase boundaries make this
  a straightforward split.
- [x] **Perf:** N/A — no behavioral change; the SWIM protocol runs identically
- [x] **Integration:** Existing integration tests pass unchanged;
  `cargo test -p oceanfs-membership` green including integration tests
<!-- REVIEW: VERIFIED — all membership tests pass. `cargo test --workspace` fails on oceanfs-server (pre-existing Epic 5). -->
- [x] **Line counts:** `ping.rs` under 300 lines, `suspicion.rs` under 250
  lines, `mod.rs` under 100 lines (excluding tests). No file exceeds 500
  lines
<!-- REVIEW: ACCEPTED DEVIATION — ping.rs: 203 lines ✅; suspicion.rs: 52 lines ✅; mod.rs: 314 total, 184 production lines (over the under-100 target). A `types.rs` file was added for DetectorConfig, DetectorCommand, and FailureDetector struct, which accounts for the mod.rs exceeding the target. No file over 500 lines. Reviewer accepted this structure. -->
- [x] **Re-exports:** `failure_detector/mod.rs` re-exports all previously
  public types; `cargo doc --no-deps -p oceanfs-membership` shows identical
  public API for the `failure_detector` module
<!-- REVIEW: VERIFIED — FailureDetector and DetectorConfig are pub(crate); this matches pre-split visibility (not re-exported from lib.rs). -->

## Accepted Deviations

- **`types.rs` added:** A new `failure_detector/types.rs` file was added
  to hold `DetectorConfig`, `DetectorCommand`, and the `FailureDetector`
  struct definition, keeping the protocol logic files (`ping.rs`,
  `suspicion.rs`) focused and small.
- **`mod.rs` at 184 production lines:** Over the under-100 target due to
  the coordinator logic tying ping and suspicion phases together. The
  `types.rs` extraction mitigates this. Reviewer accepted the structure
  given the natural cohesion of the SWIM protocol coordinator.
