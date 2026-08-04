---
feature: "Split Config Module"
epic: "refactoring/config-decomposition"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Config types may reference types from the split types/ directory;
      the types/ split must complete first
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Split Config Module

## Summary

`crates/oceanfs-core/src/config.rs` is 504 lines containing seven distinct
config structs (`NodeConfig`, `MetadataConfig`, `RingConfig`, `WalConfig`,
`AccelConfig`, `AuthConfig`, `CompressionConfig`) all in one file. These
structs share no internal logic — each is mostly `#[derive]` with a few
`Default` impls. Split into a `config/` directory with one file per config
struct and a `mod.rs` re-export facade. The file `config.rs` is replaced by the
`config/` directory. All existing imports (`use oceanfs_core::config::NodeConfig`)
continue to work unchanged.

## Scope

### In Scope

- Delete `src/config.rs`
- Create `src/config/` directory with:
  - `config/mod.rs` — re-export facade (replaces `config.rs`)
  - `config/node.rs` — `NodeConfig` struct + `Default` impl
  - `config/metadata.rs` — `MetadataConfig` + `Default`
  - `config/ring.rs` — `RingConfig` + `Default`
  - `config/wal.rs` — `WalConfig` + `Default`
  - `config/accel.rs` — `AccelConfig` + `Default`
  - `config/auth.rs` — `AuthConfig` + `Default`
  - `config/compression.rs` — `CompressionConfig` + `Default`
- Migrate all `#[cfg(test)]` tests from the old `config.rs` into the file
  owning the config type under test
- Update `src/lib.rs` to declare `pub mod config;` (this already exists —
  the declaration points to `config/mod.rs` transparently)

### Out of Scope

- Changing any config struct's fields, derives, defaults, or semantics.
  This is a pure mechanical split.
- Moving config types between crates. All config types remain in
  `oceanfs-core`.
- Merging `oceanfs-node` config validation (`validate_config`) into
  `oceanfs-core` config. That is evaluated as part of feature `split-node-rs`
  and is a separate decision.
- Changing the serialization format or adding new config types.
- Any downstream crate changes — the re-export facade ensures zero breakage.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | Delete `src/config.rs`; create `src/config/` directory with 8 files: `mod.rs`, `node.rs`, `metadata.rs`, `ring.rs`, `wal.rs`, `accel.rs`, `auth.rs`, `compression.rs` |

## Interface (Public API)

No new public items. No removed public items. The facade in `config/mod.rs`
re-exports every public config struct previously exported from `config.rs`.
All downstream consumers continue to work unchanged:

```rust
// oceanfs-core/src/config/mod.rs
mod node;
mod metadata;
mod ring;
mod wal;
mod accel;
mod auth;
mod compression;

pub use node::NodeConfig;
pub use metadata::MetadataConfig;
pub use ring::RingConfig;
pub use wal::WalConfig;
pub use accel::AccelConfig;
pub use auth::AuthConfig;
pub use compression::CompressionConfig;
```

## Data Flow

Pure structural refactor. No runtime data flow changes.

```
Old:  use oceanfs_core::config::NodeConfig
            ↓
      oceanfs-core/src/config.rs (504 lines, all configs)

New:  use oceanfs_core::config::NodeConfig
            ↓
      oceanfs-core/src/config/mod.rs (re-exports)
            ↓
      oceanfs-core/src/config/node.rs (NodeConfig definition)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds workspace-wide; no new
  warnings
- [ ] **Tests:** `cargo test` passes; all config tests from the old
  `config.rs` pass in their new file locations
- [ ] **Docs:** Every `pub` config struct in each new file has a doc comment;
  `cargo doc --no-deps` produces no `missing_docs` warnings for
  `oceanfs-core::config`
- [ ] **ADR:** N/A — implements existing guideline §3.3, no new architectural
  decision required
- [ ] **Perf:** N/A — no behavioral change
- [ ] **Integration:** Existing cross-crate integration tests pass unchanged;
  `cargo test --workspace` green
- [ ] **Facade:** `oceanfs-core/src/config/mod.rs` re-exports every public
  item from the old `config.rs` — verified via `cargo doc` showing identical
  public API for the `oceanfs_core::config` module
