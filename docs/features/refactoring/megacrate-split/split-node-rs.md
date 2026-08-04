---
feature: "Split Node.rs"
epic: "refactoring/megacrate-split"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Shared type re-exports must be stable
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Split Node.rs

## Summary

`crates/oceanfs-node/src/node.rs` is 1,012 lines containing three distinct
concerns: the `Node` struct and its startup/wiring logic (`Node::start` wires
12+ subsystems), the `BackgroundTasks` struct (8 task handles + 8 cancel tokens
= 16 fields), configuration validation (`validate_config`), and a
`PrefetchStoreAdapter`. Split this monolithic file into three focused files —
`node.rs`, `background_tasks.rs`, and `config.rs` — so each file has a single
responsibility and is navigable under 500 lines. Tests move alongside their
types. `node.rs` gains smaller `build_*` helper functions to decompose the
large `Node::start` method.

## Scope

### In Scope

- Create `src/background_tasks.rs`:
  - Move `BackgroundTasks` struct and its `impl` block
  - Move `spawn_background_tasks` function
  - Move all background-task-related tests from the `#[cfg(test)]` block
- Create `src/config.rs`:
  - Move `validate_config` function
  - Move config-validation tests
  - Evaluate whether this function should merge with `oceanfs-core` config
    validation or stay in `oceanfs-node`. If it belongs in core, defer that
    move and note the decision for the `split-config` feature (Epic 6).
- Refactor `src/node.rs` (keeping ~400–500 lines):
  - Retain `Node` struct (6 fields), `PrefetchStoreAdapter`, `Node::start`,
    and `build_*` helper functions extracted from `Node::start`
  - Decompose `Node::start` into smaller `build_*` helper functions:
    - `build_metadata_store`
    - `build_segment_store`
    - `build_encoder`
    - `build_ring`
    - `build_membership`
    - `build_connection_pool`
    - `build_server`
    - Each helper constructs one subsystem and returns it, following the
      dependency-injection pattern per ADR-0005
  - Retain Node-level tests
- Update `src/lib.rs`:
  - Add `mod background_tasks;` and `mod config;` (or `pub mod` as needed)
  - Ensure re-exports from `lib.rs` continue to work

### Out of Scope

- Changing `Node::start` behavior or startup order. This is a pure structural
  refactor — no semantic changes.
- Moving types between crates. Everything stays in `oceanfs-node`.
- Merging `validate_config` with `oceanfs-core` config validation. If that is
  desirable, it is tracked as a follow-up item to be coordinated with Epic 6.
- Adding new tests. Existing tests are moved, not expanded.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | Refactor `src/node.rs` → `src/node.rs` (leaner) + new `src/background_tasks.rs` + new `src/config.rs` |

## Interface (Public API)

No public API additions or removals. The existing facade in
`oceanfs-node/src/lib.rs` re-exports `Node`, `NodeConfig`, `BackgroundTasks`
unchanged. Internal `pub(crate)` visibility is adjusted so that
`background_tasks.rs` and `config.rs` can be imported by `node.rs`.

Updated `lib.rs` module declarations:

```rust
mod background_tasks;
mod config;
mod node;
// re-exports unchanged
pub use node::Node;
pub use node::NodeConfig;
pub use background_tasks::BackgroundTasks;
```

## Data Flow

This is a structural refactor. No runtime data flow changes. The startup
sequence in `Node::start` is identical — the `build_*` helpers are inlined
decompositions of the existing inline construction code.

```
Node::start(config)
  → build_metadata_store(&config)
  → build_segment_store(&config, &metadata)
  → build_encoder(&config)
  → build_ring(&config)
  → build_membership(&config, &ring)
  → build_connection_pool(&config)
  → build_server(segment_store, ring, membership, pool, ...)
  → spawn_background_tasks(&config, server)
  → Ok(Node { config, server, background_tasks })
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets -p oceanfs-node` succeeds; no new
  warnings
- [ ] **Tests:** `cargo test -p oceanfs-node` passes; all tests from the old
  `node.rs` pass in their new file locations
- [ ] **Docs:** Every `pub` item in the new files has a doc comment;
  `#![deny(missing_docs)]` passes for `oceanfs-node`
- [ ] **ADR:** N/A — internal refactor within one crate, no architectural
  decision required
- [ ] **Perf:** N/A — no behavioral change; startup sequence is identical
- [ ] **Integration:** Cross-crate integration tests (`oceanfs-node/tests/`)
  pass unchanged; `oceanfs-node/tests/e2e_single_node.rs` and all other
  integration tests green
- [ ] **Line count:** No single file exceeds 500 lines after the split
  (excluding tests). `node.rs` ~400–500 lines with `build_*` helpers,
  `background_tasks.rs` ~200 lines, `config.rs` ~100 lines
- [ ] **Re-exports:** `oceanfs-node/src/lib.rs` re-exports `Node`,
  `NodeConfig`, and `BackgroundTasks` from correct modules;
  `cargo doc --no-deps -p oceanfs-node` shows identical public API
