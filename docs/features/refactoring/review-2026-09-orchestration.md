---
feature: "2026-09 Review Program — Implementer Orchestration"
epic: "refactoring"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring/review-wave-0-1
  - epic: refactoring/composition-root-decomposition
  - epic: refactoring/store-unification
  - epic: refactoring/legacy-mode-removal
  - epic: refactoring/durability-scheduler
  - epic: refactoring/manifest-aware-repair
adr:
  - 0017-durability-task-abstraction
  - 0031-remove-single-datadir-legacy-mode
  - 0032-unify-segment-data-access
  - 0033-manifest-aware-peer-selection
  - 0034-bounded-metadata-accounting
created: 2026-09-04
updated: 2026-09-04
---

# 2026-09 Review Program — Implementer Orchestration

> **START HERE.** This is the navigation file for the 2026-09 review
> program. It tells an implementer the global ordering of every epic and
> feature, what each epic depends on, and what "done" means at each gate.
> The per-epic READMEs are the map inside each epic; this document is the
> map of maps.

## Program shape

Source: the 2026-08-25/09-03 whole-project review (112 in-code comments).
Triage: `docs/features/refactoring/review-2026-09-roadmap.md` (8 themes,
6 waves, per-comment verdicts). Decisions: ADR-0017 (scheduler, accepted),
ADR-0031 (legacy removal, accepted), ADR-0032 (store unification,
accepted), ADR-0033 (manifest-aware selection, accepted).

## Global dependency chain (read top to bottom)

```
                      ┌─────────────────────────────────────────────┐
                      │ WAVE 0+1: stale-comment closure + bug batch │  ← independent, land first
                      │ (review-wave-0-1/)                           │
                      └───────────────┬─────────────────────────────┘
                                      ▼
              ┌───────────────────────────────────────────┐
              │ WAVE 2 GATE                                 │
              │ ① composition-root c1 (single wiring point) │
              └───────────────┬───────────────────────────┘
                              ▼
   ┌──────────────────────────────┬──────────────────────────────────┐
   ▼                              ▼                                  ▼
┌──────────────────────┐  ┌────────────────────┐  ┌─────────────────────────────┐
│ store-unification    │  │ legacy-mode-removal│  │ bounded-metadata-scans      │
│ f1→f2→f3 (ADR-0032)  │  │ f1→f2→f3           │  │ f1→f3→f4/f2 (ADR-0034,      │
│  (f2 AFTER legacy f2 │  │ (ADR-0031; f2 must │  │  accounting-based)          │
│   — same files)      │  │  precede store f2) │  │                             │
└──────────────────────┘  └────────────────────┘  └───────────────┬─────────────┘
        └──────────────────────────┬───────────────────────────────┘
                                   ▼
              ┌───────────────────────────────────────┐
              │ durability-scheduler f1→f2→f3→f4      │   (ADR-0017; runs on unified store)
              └───────────────────────────────────────┘
                                   ▼
              ┌───────────────────────────────────────┐
              │ manifest-aware-repair f1→f2/f3        │   (ADR-0033; needs store + holder sets)
              └───────────────────────────────────────┘
                                   ▼
              ┌───────────────────────────────────────┐
              │ RESUME healing epics: g7 wal-loss,    │
              │ g8 metadata-loss (+ #30 replicated     │
              │ lifecycle state ADR)                  │
              └───────────────────────────────────────┘
```

### Wave order and entry points

| Wave | Epic / item | Entry doc | Depends on | Gate to pass |
|---|---|---|---|---|
| 0+1 | Stale-comment closure + bug batch | `review-wave-0-1/README.md` | — | all stale markers deleted; bug fixes green |
| 2 ① | Composition root | `composition-root-decomposition/README.md` (c1 first) | — | `start()` < 300 lines (after c5); one store wiring point (after c1) |
| 2 ② | Store unification | `store-unification/README.md` (f1→f2→f3) | c1 | one trait, one impl, one instance |
| 2 ⑤ | Legacy removal | `legacy-mode-removal/README.md` (f1→f2→f3) | c1 (parallel w/ ②) | pools mandatory; no legacy branches |
| 2 ⑥ | Bounded metadata scans (accounting) | `bounded-metadata-scans/README.md` (f1→f3→f4/f2) — **ADR-0034 Accepted** | ②, ⑤ (event-WAL/checkpoint format) | reaper/GC/remap no longer O(all objects) |
| 2 ③ | Durability scheduler | `durability-scheduler/README.md` (f1→f2→f3→f4) | ②, ⑥ | global semaphore; loops out of node.rs |
| 2 ④ | Manifest-aware AE/scrub | `manifest-aware-repair/README.md` (f1→f2/f3) | ②, ③ (holder sets), c2/c3 | AE/scrub select by storage_locations |
| 3 | Healing g7/g8 + #30 ADR | `features/disk-resilience-healing/` | all of wave 2 | per-feature DoD |
| 4 | Config plumbing, dead-code, folder hygiene, docs/graphs | `review-wave-4/README.md` | can interleave | mechanical, low risk |
| 5 | Deferred design ADRs | `review-wave-5/README.md` | backlog | ADRs written |

\* ~~Wave 2 ⑥ needs an ADR~~ — **resolved**: ADR-0034 (bounded metadata
accounting, Accepted 2026-09-04) + the `bounded-metadata-scans` epic are
written. It lands before `durability-scheduler/f3-keyspace-sharding.md` and
before g7's catch-up enumeration at scale.

## Gate definitions

- **Wave 2 is the structure gate.** g7/g8 must not start before ② store
  unification and ③ scheduler are done (they would add uncoordinated
  `.dat` writers and per-task loops to the exact surfaces being fixed).
- Each epic's README has its own epic DoD. The program-level gate for
  wave 2 is: **one store, one scheduler, manifest-aware selection, pools
  mandatory, node.rs decomposed.**

## Comment → item map

| Review theme | Lands in |
|---|---|
| Store/data-access proliferation, single-writer | store-unification (②) |
| Background orchestration / concurrency / reactor | durability-scheduler (③); reactor rejected — see roadmap §wave 5 |
| Config not plumbed | review-wave-4/config-plumbing |
| durable/background orchestration | durability-scheduler (③); reactor rejected — see roadmap §wave 5 |
| Full-space scans / unbounded memory | **bounded-metadata-scans (⑥, ADR-0034)** + store-unification |
| Manifest-aware AE/scrub | manifest-aware-repair (④) |
| Streaming read path | review-wave-5 (deferred) |
| Seal signaling / space efficiency | review-wave-5 (audit written: `audits/2026-09-04-seal-on-zero-space-waste.md`) |
| Correctness / hardening | review-wave-0-1 bug batch |
| Legacy mode | legacy-mode-removal (⑤) |
| Composition root / DI | composition-root-decomposition (①) |

## Status board

| Epic | Status |
|---|---|
| review-wave-0-1 | **done (2026-09-04)** — f0+f1 implemented, review PASS; B1 deferred to composition-root c1's leave-handler deletion |
| composition-root-decomposition | c1–c5 docs, not started |
| store-unification | f1–f3 docs, not started |
| legacy-mode-removal | f1–f3 docs, not started |
| durability-scheduler | f1–f4 docs, not started |
| manifest-aware-repair | f1–f3 docs, not started |
| bounded-metadata-scans | f1–f4 docs, not started (ADR-0034 accepted) |
| g7 / g8 (healing) | proposed (blocked on wave 2) |

## References

- Triage: `docs/features/refactoring/review-2026-09-roadmap.md`
- ADRs: 0017, 0031, 0032, 0033
- In-flight healing: `docs/features/disk-resilience-healing/`
