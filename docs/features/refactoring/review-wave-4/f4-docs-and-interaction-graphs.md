---
feature: "f4: Architecture Documentation & Interaction Graphs"
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

# f4: Architecture Documentation & Interaction Graphs

## Summary

The review author identified (2026-09-04) that the urge to build a reactor
partly arose from a **lack of documentation of subsystem interactions**
and **lack of architecture graphs**. This feature produces Mermaid
interaction diagrams + module-interaction docs for the main paths, so the
system's structure is graspable without reading every crate. It directly
serves future reviews and the composition-root refactor (each module
builder gets a documented interaction surface).

## Scope

### In Scope
- **Interaction diagrams (Mermaid)** for:
  - Write path: HTTP → S3 handler → WriteCoordinator → pools/sealer →
    lifecycle coordinator → event WAL → data WAL → replicator → peers.
  - Read path: HTTP → ReadCoordinator → pool-slot probe → DiskSegmentReader
    (local) → gRPC fetch fallback → EC decode → assembly.
  - Durability background tasks: GC, orphan reaper, scrub, AE, heal,
    reconcile, re-replication — with their store/lifecycle dependencies
    (post-wave-2 shape).
  - Healing epic: loss announcement, holder index, re-replication,
    manifest/routing cache.
  - Membership/gossip plane vs data plane.
- **Module-interaction docs** per crate describing which subsystems talk to
  which (the "map" the reviewer wanted).
- Diagrams committed under `docs/diagrams/` (or alongside the relevant
  READMEs), referenced from `guidelines/architecture.md`.

### Out of Scope
- Doc-comment sweep (`long-term-hygiene/audit-doc-comments` already tracks
  rustdoc coverage).
- Behavioral changes.

## Crate Impact

| Crate | Change |
|---|---|
| `docs/` | New `diagrams/` + interaction docs; update architecture guideline references |

## Definition of Done

- [ ] Mermaid diagrams render (valid syntax checked) for all paths listed.
- [ ] Each diagram is referenced from the owning module README or the
      architecture guideline.
- [ ] Diagrams stay accurate after the composition-root decomposition
      lands (regenerate in c5 if the module map changed).
