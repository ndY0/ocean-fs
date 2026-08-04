---
feature: "Audit Doc Comments"
epic: "refactoring/long-term-hygiene"
status: proposed
priority: low
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Type splits will change file locations; doc comment audit should run after types are settled
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Audit Doc Comments

## Summary

All crates in the OceanFS workspace include `#![deny(missing_docs)]` in their
`lib.rs` files (audit finding L2 confirms this). However, code-graph symbol
analysis reveals many `pub` items with empty `symbol_doc: ""`, suggesting that
doc comments may be missing on individual public items despite the lint passing.
This could happen because test-only code is allowed to omit docs
(`#[allow(missing_docs)]` in test modules) or because some items use blanket
allow attributes. This feature runs a workspace-wide `cargo doc --no-deps`
build, captures all `missing_docs` warnings, enumerates every `pub` item lacking
a doc comment, and adds `///` doc comments with `# Examples` sections to all
identified items. The work is mechanical but high volume — the codebase has
~162 source files across 12 crates.

## Scope

### In Scope

- Run `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features` against
  the workspace to surface all `missing_docs` warnings as hard errors
- Enumerate every `pub` item (struct, enum, trait, function, method, type alias,
  module) across all crates that produces a `missing_docs` warning
- Add doc comments (`///`) to all identified items. Every doc comment must:
  - Describe what the item is and why it exists
  - Include a `# Examples` section showing basic usage (even if tagged
    `` ```ignore `` for items that cannot be tested in doc tests)
  - Follow the existing doc-comment style in the codebase (single-line `///`
    for short descriptions, multi-line `///` with sections for complex items)
- Verify with a second pass: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
  --all-features` must succeed with zero warnings across all crates
- Prioritize items in high-coupling files first: `oceanfs-core/src/types/`
  (the post-split type files), `oceanfs-storage`, and `oceanfs-server`
- For any item where adding a doc comment would be purely boilerplate (e.g.,
  a newtype wrapper that is self-describing), document why it exists and its
  invariants

### Out of Scope

- Rewriting or improving existing doc comments — this is gap-filling only, not
  content improvement
- Adding module-level documentation (`//!`) — the audit finding is about
  individual `pub` items, not module docs
- Fixing other `cargo doc` warnings (broken intra-doc links, unresolved
  references) — only `missing_docs` is in scope. Other warnings should be
  captured and reported as a separate issue
- Changing the `#![deny(missing_docs)]` configuration or adding blanket
  `#[allow(missing_docs)]` attributes
- Adding tests — doc examples may be tagged `` ```ignore `` if they cannot
  execute in a doc-test environment; no new test infrastructure is required

## Crate Impact

| Crate | Change |
|---|---|
| All 12 crates | Doc comments added to `pub` items across all source files; no API changes, no `Cargo.toml` changes |

The volume will vary by crate. Estimated impact based on the audit:

| Crate | Files | Estimated Missing Docs |
|---|---|---|
| `oceanfs-core` | ~15 (post-split) | 20–40 items |
| `oceanfs-storage` | ~30 | 25–50 items |
| `oceanfs-server` | ~26 | 20–40 items |
| `oceanfs-node` | ~10 | 10–20 items |
| `oceanfs-accel` | ~20 | 15–30 items |
| Other crates | ~61 | 10–30 items (combined) |

## Interface (Public API)

No new public items. No removed public items. No signature changes. Every
change is an additive `///` doc comment on an existing `pub` item.

## Data Flow

```
1. Run audit:
   $ RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features 2>&1 | tee doc-warnings.txt
   
   Expected output: a list of warnings like:
   warning: missing documentation for a struct
     --> crates/oceanfs-core/src/types/id.rs:23:1
      |
   23 | pub struct SegmentId(u64);
      | ^^^^^^^^^^^^^^^^^^^^^^^^^^

2. Categorize by crate and file:
   - Parse doc-warnings.txt into a structured list: {crate, file, line, item_name, item_type}
   - Sort by crate (priority) and file (grouping for batch edits)

3. For each item, write a doc comment:
   - Research: what does this item do? Read the type definition, its impl blocks,
     and its usage sites (code-graph get_type_usages)
   - Write: `/// Description.` + `/// # Examples` section
   - For simple types (newtypes, fieldless enums): one-line doc is sufficient
   - For complex types (structs with multiple fields, traits with async methods):
     describe each field/method and invariants

4. Verify:
   $ RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
   # Must exit 0 with zero warnings

5. Final check:
   $ cargo test --workspace
   # All existing tests continue to pass (doc comments don't change behavior)
```

## Definition of Done

- [ ] **Audit:** `cargo doc --no-deps --all-features` is run and all
  `missing_docs` warnings are captured in a structured report (file path,
  line number, item name, item type)
- [ ] **Code:** Every `pub` item identified in the audit has a doc comment
  (`///`) with a description and `# Examples` section; `cargo build --all-targets`
  succeeds workspace-wide with no new compilation errors
- [ ] **Tests:** `cargo test --workspace` passes; all existing tests continue
  to pass unchanged
- [ ] **Docs:** `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features`
  succeeds with **zero warnings** across all crates
- [ ] **ADR:** N/A — this is guideline compliance work, no architectural decision
  required
- [ ] **Perf:** N/A — doc comments have zero runtime impact
- [ ] **Integration:** Existing cross-crate integration tests
  (`oceanfs-node/tests/`) pass unchanged

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and
> `ignore`-tagged doc examples are non-blocking (tracked separately per
> `guidelines/coding.md` §9.2.1).
