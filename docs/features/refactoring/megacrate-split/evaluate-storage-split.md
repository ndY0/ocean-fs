---
feature: "Evaluate Storage Crate Split"
epic: "refactoring/megacrate-split"
status: accepted
priority: high
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Shared type re-exports must be stable before crate boundary evaluation
adr:
  - 0005-trait-in-consuming-crate
  - 0009-storage-crate-split
perf: []
created: 2026-08-03
updated: 2026-08-04
---

# Evaluate Storage Crate Split

## Summary

**Decision (2026-08-03): Proposal A — create `oceanfs-durability` crate.**
Durability background tasks (anti_entropy, scrub, gc, heal) will be extracted
from `oceanfs-storage` into a new `oceanfs-durability` crate. This reduces
`oceanfs-storage` from ~12.7K to ~7K lines and separates low-level storage
primitives from high-level maintenance logic. The ADR produced by this feature
must document the split boundary, crate responsibilities, dependency edges,
and migration plan. No implementation occurs in this feature — it produces
the ADR and follow-up implementation feature brief.

## Scope

### In Scope

- **Document the decision:** Proposal A is selected — create `oceanfs-durability`
  crate. Write the ADR capturing:
  - Exact module mapping: which modules move from `oceanfs-storage` to
    `oceanfs-durability` (anti_entropy, scrub, gc, heal) and which stay
    (buffer_pool, segment, wal, metadata, blob_store)
  - New crate sizes (~7K for storage-core, ~5.6K for durability)
  - Dependency edges: `oceanfs-durability → oceanfs-storage` (durability
    reads segments to verify/repair) and `oceanfs-server → oceanfs-durability`
    (server triggers heal/scrub)
  - Trait placement per ADR-0005: where do `SegmentStore`, `MetadataStore`,
    and durability-related traits live?
  - Coordination with Feature 9 (protobuf stubs): healing, scrub service
    stubs already moving to `oceanfs-storage` — after split, they move to
    `oceanfs-durability`
- **Rejection rationale for Proposal B:** Move durability to `oceanfs-node`
  would make the node crate excessively large and blur the line between
  composition root and business logic. A dedicated durability crate keeps
  concerns separated.

### Out of Scope

- **Implementation of the split.** A separate feature (`execute-storage-split`)
  will implement it once the ADR is accepted.
- Splitting individual files — already done by Epic 2.

## Crate Impact

No crate changes in this feature. Analysis only. If the resulting ADR
approves a split, the implementation feature will have its own Crate Impact
table.

## Interface (Public API)

No public API changes. This feature produces an ADR document.

## Data Flow

```
Audit H2 finding
  → Analyze Proposal A (storage-core + durability crates)
  → Analyze Proposal B (durability → oceanfs-node)
  → Evaluate DAG impact, ADR-0005 compliance, compilation boundaries
  → Produce ADR with decision
  → (if accepted) Create follow-up implementation feature brief
```

## Decision (2026-08-04)

Proposal A accepted with refinement: create both `oceanfs-durability` (for
background maintenance tasks) and `oceanfs-storage-api` (for storage interface
contracts). See [ADR-0009](../../adr/0009-storage-crate-split.md) for full
rationale.

Implementation feature: [Execute Storage Crate Split](./execute-storage-split.md).

## Definition of Done

- [x] **ADR:** ADR-0009 written under `docs/adr/0009-storage-crate-split.md`
  documenting the decision, module mapping, DAG edges, trait placement, and
  migration plan
- [x] **Cross-reference:** ADR references ADR-0005 (trait-in-consuming-crate),
  architecture §4.1, and §1.2 (crate responsibilities)
- [x] **Follow-up:** ADR appendix references `execute-storage-split` implementation feature
- [x] **Docs:** ADR indexed in doc-graph
