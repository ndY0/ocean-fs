---
feature: "Miscellaneous File Splits"
epic: "refactoring/long-term-hygiene"
status: proposed
priority: low
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Type file layout must be settled before splitting files that import these types
  - epic: refactoring/storage-decomposition
    reason: Storage crate anti-entropy and GC splits (C3) may change the pool.rs context
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Miscellaneous File Splits

## Summary

Three small, independent file-split tasks from the structural audit that improve
codebase navigability without changing behavior. Each task takes a moderately
sized file (224–797 lines) and splits it into 2–3 smaller files, following the
architecture guideline §3.3 ("each public type gets its own file" where
applicable, and "if a file is growing, split at natural boundaries"). These are
low-risk, mechanical refactors suitable for batching into a single PR or doing
individually as stretch work.

## Scope

### In Scope: Three Split Tasks

#### M5: Split `oceanfs-storage/src/segment/pool.rs` (649 lines)

Split into two files under a `segment/pool/` subdirectory:

- `segment/pool/manager.rs` — pool lifecycle: initialization, active segment
  acquisition, segment recycling, pool shutdown
- `segment/pool/shard.rs` — sharding logic: shard assignment, load-aware
  segment distribution, shard statistics
- `segment/pool/mod.rs` — re-exports `manager` and `shard` public items

The existing `pub(crate)` items in `pool.rs` remain `pub(crate)` within the
`segment::pool` module. No public API change.

#### M6: Split `oceanfs-server/src/admin.rs` (797 lines)

Split into two files under an `admin/` subdirectory:

- `admin/handlers.rs` — admin API endpoint handlers: health check, node status,
  configuration management, shutdown trigger
- `admin/metrics.rs` — metrics endpoints: Prometheus scrape endpoint, metrics
  aggregation, admin dashboard data
- `admin/mod.rs` — re-exports `handlers` and `metrics` public items; keeps any
  shared helper types

The admin module provides internal server management endpoints. Split boundary
follows the natural separation between operational commands (handlers) and
observability data (metrics).

#### M9: Extract `cli.rs` and `signals.rs` from `oceanfs/src/main.rs` (224 lines)

Extract two concerns from the binary entrypoint:

- `cli.rs` — argument parsing (clap or manual), configuration file path
  resolution, `--version`, `--config`, `--data-dir` flags
- `signals.rs` — OS signal handling: `SIGTERM`, `SIGINT` graceful shutdown,
  `SIGHUP` config reload (if implemented), signal-to-shutdown-token bridge
- `main.rs` — reduced to: parse CLI args → load config → init tracing →
  construct `Node` → start → wait for shutdown signal → graceful teardown

This follows the architecture guideline §3.3 spirit for the binary crate:
`main.rs` orchestrates; `cli.rs` and `signals.rs` own their respective concerns.

### Out of Scope

- **M7** (`oceanfs-membership/src/membership.rs` split) — this is tracked in
  Epic 6, feature `split-membership`
- **M10** (`oceanfs-membership/src/failure_detector.rs` split) — tracked in
  Epic 6, feature `split-failure-detector`
- **L1** (`oceanfs-routing/src/ring.rs` monitoring) — the audit explicitly
  recommends monitoring, not action; ring.rs at 316 lines is well-sized
- Changing any function signatures, visibility, or behavior — these are pure
  file-split (code motion) refactors
- Adding or removing tests — existing `#[cfg(test)] mod tests` blocks move
  with the code they test

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | Create `src/segment/pool/` directory with `mod.rs`, `manager.rs`, `shard.rs`; delete `src/segment/pool.rs` |
| `oceanfs-server` | Create `src/admin/` directory with `mod.rs`, `handlers.rs`, `metrics.rs`; delete `src/admin.rs` |
| `oceanfs` | Create `src/cli.rs` and `src/signals.rs`; reduce `src/main.rs` to orchestration only |

## Interface (Public API)

No new public items. No removed public items. The re-export facades in
`segment/pool/mod.rs` and `admin/mod.rs` ensure identical public API surfaces
to the existing files. The `oceanfs` binary has no public API — `main.rs`,
`cli.rs`, and `signals.rs` are all binary-internal modules.

### Re-export Facade Pattern (for M5 and M6)

```rust
// oceanfs-storage/src/segment/pool/mod.rs
mod manager;
mod shard;

pub(crate) use manager::SegmentPool;
pub(crate) use shard::ShardAssigner;
// ... all items previously pub(crate) in pool.rs
```

```rust
// oceanfs-server/src/admin/mod.rs
mod handlers;
mod metrics;

pub(crate) use handlers::{health_check, node_status, ...};
pub(crate) use metrics::{prometheus_scrape, metrics_dashboard, ...};
```

## Data Flow

These are pure structural refactors. No runtime data flow changes.

For each split task:

```
1. Read the existing file to understand its structure
2. Identify natural split boundaries (type boundaries, concern boundaries)
3. Create new files with the split code
4. Create mod.rs as re-export facade
5. Delete the old monolithic file
6. Verify cargo build, cargo test, and cargo clippy
```

The tasks are entirely independent — they can be done in any order, by
different developers, or batched into a single PR.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds workspace-wide with no
  new warnings
- [ ] **Tests:** `cargo test` passes for all affected crates; all existing
  tests continue to pass in their new file locations
- [ ] **Docs:** `#![deny(missing_docs)]` still passes for all affected crates;
  no new `missing_docs` warnings introduced
- [ ] **ADR:** N/A — file splits implement existing guidelines, no new decisions
  required
- [ ] **Perf:** N/A — no behavioral change
- [ ] **Integration:** Existing cross-crate integration tests
  (`oceanfs-node/tests/`) pass unchanged; `cargo test --workspace` green
- [ ] **M5 Complete:** `oceanfs-storage/src/segment/pool.rs` replaced with
  `segment/pool/{mod.rs, manager.rs, shard.rs}`
- [ ] **M6 Complete:** `oceanfs-server/src/admin.rs` replaced with
  `admin/{mod.rs, handlers.rs, metrics.rs}`
- [ ] **M9 Complete:** `oceanfs/src/main.rs` reduced to orchestration;
  `cli.rs` and `signals.rs` created

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) are non-blocking per
> `guidelines/coding.md` §9.2.1.
