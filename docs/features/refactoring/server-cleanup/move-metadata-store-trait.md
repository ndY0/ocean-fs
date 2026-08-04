---
feature: "Rename MetadataStore Concrete Struct"
epic: "server-cleanup"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: type-system-cleanup
    reason: MetadataStore trait lives in oceanfs-core/src/types.rs; split-core-types must complete first
  - epic: server-cleanup
    reason: Feature split-s3-handler and move-coordinators should complete first to minimize merge conflicts
adr:
  - 0005-trait-in-consuming-crate
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Rename MetadataStore Concrete Struct

## Summary

**Decision (2026-08-03):** The `MetadataStore` trait qualifies for ADR-0005's
cross-cutting exception. It is consumed by 3+ crates in different DAG branches:
`oceanfs-server` (coordinators), `oceanfs-cache` (negative-cache, prefetch),
and `oceanfs-node` (composition root). Per ADR-0005, cross-cutting traits stay
in `oceanfs-core`. The trait does **not** move.

This feature reduces to a single rename: the concrete RocksDB-backed struct
`oceanfs_storage::metadata::store::MetadataStore` is renamed to
`RocksDbMetadataStore` to eliminate the naming collision with the
`oceanfs_core::MetadataStore` trait. No trait moves, no dependency graph
changes.

## Scope

### In Scope

- **Rename the concrete struct:** Rename `oceanfs_storage::metadata::store::MetadataStore`
  to `RocksDbMetadataStore` to eliminate the naming collision with the
  `oceanfs_core::MetadataStore` trait.
- **Update `oceanfs-storage`:** All internal references, `mod.rs` re-exports,
  and `lib.rs` facade must use the new name `RocksDbMetadataStore`.
- **Update `oceanfs-node`:** Composition root wires `RocksDbMetadataStore` —
  update type references from `MetadataStore` to `RocksDbMetadataStore` for
  the concrete struct. The trait import from `oceanfs_core::MetadataStore` is
  unchanged.
- **Document the cross-cutting exception:** ADR-0005 already updated to list
  `MetadataStore` as a cross-cutting trait that stays in `oceanfs-core`.

### Out of Scope

- Moving the `MetadataStore` trait out of `oceanfs-core` — it stays (cross-cutting exception per ADR-0005)
- Changing the trait's method signatures — rename only
- Moving `SegmentStore` or `WalWriter` traits
- Any changes to `MetadataOps` trait in `oceanfs-server`

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | No change. Trait stays, cross-cutting exception documented in ADR-0005. |
| `oceanfs-storage` | Rename concrete `MetadataStore` struct → `RocksDbMetadataStore` in `src/metadata/store.rs`; update all internal refs, `mod.rs` re-exports, and `lib.rs` facade. |
| `oceanfs-node` | Update concrete type references: `oceanfs_storage::MetadataStore` → `oceanfs_storage::RocksDbMetadataStore`. Trait import (`oceanfs_core::MetadataStore`) unchanged. |
| `oceanfs-server` | No change. |
| `oceanfs-cache` | No change. Import `oceanfs_core::MetadataStore` unchanged. |

## Interface (Public API)

### oceanfs-core

**Unchanged** — `pub trait MetadataStore: Send + Sync` stays.

### oceanfs-storage

**Renamed:**
- `pub struct MetadataStore` → `pub struct RocksDbMetadataStore`
- All `impl MetadataStore for RocksDbMetadataStore` → `impl RocksDbMetadataStore` (inherent)
- `impl oceanfs_core::MetadataStore for MetadataStore` → `impl oceanfs_core::MetadataStore for RocksDbMetadataStore` (trait impl)

## Migration Path

### Step 1: Rename in oceanfs-storage

Rename the concrete struct in `oceanfs-storage/src/metadata/store.rs`:
```
-o pub struct MetadataStore {
+o pub struct RocksDbMetadataStore {
```
Update all internal references within `oceanfs-storage`:
- `src/metadata/mod.rs`: `pub use store::MetadataStore;` → `pub use store::RocksDbMetadataStore;`
- `src/lib.rs`: update re-export
- All `#[cfg(test)]` blocks: `MetadataStore::open(...)` → `RocksDbMetadataStore::open(...)`

### Step 2: Update oceanfs-node

- `oceanfs-node/src/node.rs`: `oceanfs_storage::MetadataStore` → `oceanfs_storage::RocksDbMetadataStore`
- `oceanfs-node/src/metadata_adapter.rs`: update concrete type reference
- All integration tests under `oceanfs-node/tests/`: update concrete type references

### Step 3: Verify

- `cargo build --workspace --all-targets`
- `cargo test --workspace`

## Data Flow

Unchanged at runtime. Rename-only change — the trait stays in core, the
concrete struct is renamed for clarity.

```
Before:  oceanfs_core::MetadataStore (trait) ← impl by oceanfs_storage::MetadataStore (struct)
After:   oceanfs_core::MetadataStore (trait) ← impl by oceanfs_storage::RocksDbMetadataStore (struct)
```

## Definition of Done

- [ ] **Code:** `cargo build --workspace --all-targets` succeeds
- [ ] **Tests:** `cargo test --workspace` passes; no test behavioral changes
- [ ] **Docs:** ADR-0005 documents `MetadataStore` as a cross-cutting exception
- [ ] **Rename:** All references to the concrete `MetadataStore` struct in
  `oceanfs-storage` and `oceanfs-node` updated to `RocksDbMetadataStore`
- [ ] **Perf:** Not applicable (rename only)
- [ ] **Integration:** All `oceanfs-node/tests/` integration tests pass
