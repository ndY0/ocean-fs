---
feature: "f3: Durability Crate Folder Hygiene"
epic: "refactoring/review-wave-4"
status: proposed
priority: low
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f3: Durability Crate Folder Hygiene

## Summary

The durability crate's layout is inconsistent: `gc/`, `heal/`,
`anti_entropy/`, `hinted_handoff/`, `merkle/` are folders while
`scrub.rs`, `scrub_service.rs`, `reconcile.rs`, and `repair.rs` are
top-level files (review `node.rs:8` header remark: "cleaner durability
crate architecture: the scrub service has not its own folder,
reconciliation neither"). Folder-hygiene pass: group scrub and
reconcile/repair into folders, matching the crate's existing convention.

## Scope

### In Scope
- `scrub.rs` + `scrub_service.rs` → `scrub/mod.rs`, `scrub/coordinator.rs`,
  `scrub/service.rs` (split the coordinator from the gRPC service).
- `reconcile.rs` + `repair.rs` + `compaction_recovery.rs`-adjacent code →
  a `reconcile/` folder (or `repair/`) following the same pattern.
- Pure `git mv` + `mod` path + `use` updates; no behavior change.

### Out of Scope
- Moving code between crates (ADR-0009 boundary already decided).
- Any behavioral/algorithmic change to scrub or reconcile (those are in
  `manifest-aware-repair` epic).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | Module re-organization only |

## Definition of Done

- [ ] `cargo build --all-targets` + durability tests green.
- [ ] No `pub` path changes outside the crate (lib.rs re-exports
      preserved).
- [ ] File layout consistent with `gc/`/`heal/` convention.
