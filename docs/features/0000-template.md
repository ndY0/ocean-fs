---
feature: "Feature Template"
epic: "phase-N-epic-slug"
status: proposed
priority: medium
owner: ""
dependencies: []
adr: []
perf: []
created: YYYY-MM-DD
updated: YYYY-MM-DD
---

# Feature Template

## Summary

One paragraph describing what is built, why, and where.

## Scope

### In Scope
- item 1
- item 2

### Out of Scope
- item 1 (deferred to feature X)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-xxx` | New module `foo.rs` |

## Interface (Public API)

- `pub struct Foo` — description
- `pub trait FooStore` — description

## Data Flow

```
Input → Step 1 → Step 2 → Output
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds
- [ ] **Tests:** `cargo test` passes; new tests cover all `pub` API paths
- [ ] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]` passes
- [ ] **ADR:** Constraints from referenced ADRs are satisfied
- [ ] **Perf:** Performance guidelines cited in frontmatter are followed
- [ ] **Integration:** Integration test exercises a complete scenario

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking — they are structural codebase hygiene tracked
> separately (see `guidelines/coding.md` §9.2).
