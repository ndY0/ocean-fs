---
feature: "Wave 0+1: Stale-Comment Closure & Correctness Bug Batch"
epic: "refactoring/review-wave-0-1"
status: proposed
priority: critical
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# Wave 0+1: Stale-Comment Closure & Correctness Bug Batch

> Coordination doc for waves 0 and 1 of the 2026-09 review program. They
> are treated together: both are small, independent of the structure gate,
> and both should land before any wave-2 refactor starts (a clean tree
> with no review markers makes the later pure-move refactors reviewable).

## Summary

Two concerns:

1. **Wave 0 — close stale / wrong / resolved review comments.** ~17 of the
   112 `[review]` blocks are factually wrong against today's code or were
   closed by recent commits. Leaving them in the tree forces every later
   implementer to re-litigate ghosts. They are deleted (not annotated).
2. **Wave 1 — correctness bug batch.** Six small, independent defects that
   are real today and need no design discussion. They are bundled here so
   they do not ride along with (and obscure) the wave-2 refactors.

## Feature DAG

```
f0-close-stale-comments
        └── f1-correctness-bug-batch   (independent, but lands after f0
                                        so the tree is marker-free)
```

## Acceptance bar (epic DoD)

- [ ] Every comment listed in f0 is removed from the tree.
- [ ] Every bug listed in f1 is fixed with a regression test.
- [ ] Full workspace build + tests green (RocksDB crates with
      `--test-threads=1`, PIPELINE.md §4.6).
- [ ] Review roadmap wave 0/1 items marked closed.
