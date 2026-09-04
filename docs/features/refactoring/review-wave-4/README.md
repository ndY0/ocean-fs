---
feature: "Wave 4: Mechanical Hygiene — Config, Dead Code, Folders, Docs"
epic: "refactoring/review-wave-4"
status: proposed
priority: medium
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# Wave 4: Mechanical Hygiene — Config, Dead Code, Folders, Docs

> Coordination doc for wave 4 of the 2026-09 review program. These items
> are mechanical and low-risk; they can interleave with wave 3 (different
> files) but should NOT be mixed into wave-2 refactor PRs (they would
> obscure the pure-move diffs).

## Summary

Four independent hygiene streams that the review surfaced but that are not
structural gates:

1. **Config plumbing** (Theme 3) — thread `NodeConfig` values into
   subsystems instead of `XxxConfig::default()`.
2. **Dead-code / test-only purge** — remove production-binary bloat.
3. **Durability folder hygiene** — scrub/reconcile get folders.
4. **Documentation + interaction graphs** — the review author's stated
   need (architecture docs, Mermaid interaction diagrams).

## Feature DAG

All four are independent; land in any order.

```
f1-config-plumbing
f2-dead-code-test-purge
f3-durability-folder-hygiene
f4-docs-and-interaction-graphs
```

## Acceptance bar (epic DoD)

- [ ] Config plumbing lands without behavior change (subsystems read what
      they used to, now from user config where present).
- [ ] Dead-code purge removes only items proven unused (grep + tests); no
      `#[allow(dead_code)]` production items remain without justification.
- [ ] Folder moves are pure `git mv` + module-path updates (no behavior
      change).
- [ ] Docs: interaction diagrams committed; rustdoc builds clean.
