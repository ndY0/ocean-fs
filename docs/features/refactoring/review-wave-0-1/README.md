---
feature: "Wave 0+1: Stale-Comment Closure & Correctness Bug Batch"
epic: "refactoring/review-wave-0-1"
status: in_progress
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

- [x] Every comment listed in f0 is removed from the tree.
- [x] Every bug listed in f1 is fixed with a regression test (B1
      excepted — deferred to composition-root c1's `NodeLeaveHandler`
      deletion, DECISION 2026-09-04; disposition recorded in f1 + c1).
- [x] Full workspace build + tests green (RocksDB crates with
      `--test-threads=1`, PIPELINE.md §4.6). Pre-existing failures
      outside this epic's control are recorded in f1's Deviations note.
- [x] Review roadmap wave 0/1 items marked closed.
