---
feature: "Resolve oceanfs-server → oceanfs-storage Dependency"
epic: "server-cleanup"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: type-system-cleanup
    reason: Auditing optional deps requires stable shared type imports
  - epic: server-cleanup
    reason: Should run after move-coordinators and split-s3-handler to have a clean server codebase to audit
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Resolve oceanfs-server → oceanfs-storage Dependency

## Summary

**Decision (2026-08-03):** `oceanfs-server` legitimately uses `oceanfs-storage`,
`oceanfs-ec`, `oceanfs-cache`, and `oceanfs-accel` for both type re-exports
(§2.4) and concrete type instantiation. Architecture guideline §4.1 has been
revised to remove the prohibition — `oceanfs-server` may import concrete crates.
All four optional dependencies are kept with justification documented in
`Cargo.toml`. The DAG diagram in §1.1 accurately reflects reality.

This feature is reduced to: document each optional dep's justification, add
inline comments to `Cargo.toml`, and verify the DAG diagram is consistent.

## Scope

### In Scope

- **Document each optional dependency's justification** in
  `oceanfs-server/Cargo.toml` with inline comments explaining why the dep
  exists (type re-export, concrete type construction, or both)
- **Verify architecture docs consistency:** The DAG diagram §1.1 already
  shows `server → storage` — no change needed. §4.1 has been revised to
  remove the prohibition (see architecture.md update).
- **Audit feature gates:** Confirm that code using optional deps is
  correctly `#[cfg(feature = "...")]` guarded.

### Out of Scope

- Removing any optional dependency — all four are legitimate and kept
- Writing a new ADR — the decision is documented here and in the
  updated architecture guideline §4.1
- Changing the crate dependency graph

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | `Cargo.toml`: add inline comments documenting each optional dep's justification. No code changes required — deps are already legitimate. |
| `guidelines/architecture.md` | §4.1 revised to remove the prohibition. DAG diagram §1.1 unchanged (already correct). |

## Interface (Public API)

No changes. The server's public API is unchanged — the optional deps were
already part of the public surface. This feature only adds documentation.

## Audit Summary (Pre-Decided)

The decision has been made by authority. The server uses these crates for:

| Dependency | Feature | Default | Usage |
|---|---|---|---|
| `oceanfs-storage` | `storage` | on | Type re-exports + concrete type construction |
| `oceanfs-ec` | `ec` | on | Type re-exports |
| `oceanfs-cache` | `cache` | on | Type re-exports + concrete type construction |
| `oceanfs-accel` | `accel` | off | Feature-gated concrete type construction (GPU probing, dispatcher) |

Architecture §4.1 prohibition has been removed — server may import concrete crates.

## Implementation Plan

1. Add inline comments to each optional dependency in `oceanfs-server/Cargo.toml`
   documenting why it exists
2. Verify `architecture.md` §4.1 has been revised (done — see architecture.md update)
3. Run `cargo build -p oceanfs-server && cargo test -p oceanfs-server` to confirm

## Definition of Done

- [ ] **Cargo.toml:** Each optional dep in `oceanfs-server/Cargo.toml` has an
  inline comment documenting its justification
- [ ] **Architecture:** §4.1 revised — no longer prohibits server from importing
  concrete crates. DAG diagram §1.1 unchanged (already correct).
- [ ] **Build:** `cargo build --workspace --all-targets` succeeds
- [ ] **Tests:** `cargo test --workspace` passes
