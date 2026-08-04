---
feature: "Evaluate Server Crate Split"
epic: "refactoring/megacrate-split"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Shared type re-exports must be stable before crate boundary evaluation
  - epic: refactoring/server-cleanup
    reason: Server crate intra-crate splits (H4, H6, M2) must complete first for clean module boundaries
adr:
  - 0005-trait-in-consuming-crate
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Evaluate Server Crate Split

## Summary

`oceanfs-server` is 11,130 lines across 26 source files and 6 submodules. It
mixes HTTP handlers (`s3_handler`, `s3_xml`), coordination logic
(`read_coordinator`, `write_coordinator`), auth, bucket config, admin, hinted
handoff, and gRPC service implementations — all in one crate. The architecture
§1.2 expects only `S3Handler`, `WriteCoordinator`, `ReadCoordinator`, and
`AdminHandler` — but the actual crate has grown far beyond this. This feature
evaluates splitting the server crate into an S3 API surface crate and a
coordination crate, analyzes the impact on the DAG and ADR-0005 compliance,
and produces an ADR with the final decision. No implementation occurs in this
feature.

## Scope

### In Scope

- Analyze splitting `oceanfs-server` into:
  - **`oceanfs-server`** (S3 API surface): `s3_handler`, `s3_xml`, `router`,
    `auth` submodule, plus the `AppState` struct and HTTP-layer types.
  - **`oceanfs-coordination`** (business logic): `read_coordinator`,
    `write_coordinator`, `hinted_handoff`, `metadata_ops`, `bucket_config`,
    `admin`, and the `read/` + `write/` subdirectories.
- **Boundary analysis for critical types:**
  - Where do the read/write submodules belong? `read/assembly.rs`,
    `write/replication.rs` — are these coordination-level or server-level?
  - What about `AppState` — does it belong in `oceanfs-server` (holds HTTP
    state) or `oceanfs-coordination` (holds coordinator handles)?
  - Where does `hinted_handoff` fit? It coordinates between nodes — is it
    server-scoped coordination or genuinely cross-cutting?
- **Are coordination types genuinely separable?** Analyzed by mapping every
  type in `oceanfs-server` to "S3 surface" vs "coordination." If a significant
  number of types are shared between the two proposed crates (e.g., response
  types used by both handlers and coordinators), the split may create more
  indirection than it removes.
- **Dependency analysis:** Does `oceanfs-coordination` become an intermediary
  layer between `oceanfs-server` and `oceanfs-storage` (or the storage traits)?
  Does this create a chain `server → coordination → storage` that is worse
  than the current `server → storage` optional dependency?
- **ADR-0005 compliance:** If `oceanfs-coordination` exists, where do the
  coordinator traits (`SegmentStore`, `MetadataStore`, `RingCache`) live?
  Currently these are consumed by `oceanfs-server` — do they move to
  `oceanfs-coordination` as the consumer?
- **Interaction with `resolve-server-storage-dep` (H1):** The optional
  `oceanfs-server → oceanfs-storage` dependency must be resolved. Does
  extracting `oceanfs-coordination` make the resolution cleaner or more complex?
- **Impact on `oceanfs-node`:** `oceanfs-node` is the composition root. With
  a new `oceanfs-coordination` crate, `oceanfs-node` must construct and wire
  coordination types explicitly. Does this increase wiring complexity?
- Produce an ADR documenting the analysis, the chosen approach, and the
  rationale. If a split is rejected, the ADR must document why the current
  single-crate structure is preferred and what alternative mitigations apply.

### Out of Scope

- **Implementation of the split.** If the ADR approves a split, a separate
  feature (`execute-server-split`) will implement it.
- Intra-crate file splits within `oceanfs-server`. The server-cleanup epic
  (Epic 3) handles those — they are prerequisites for this evaluation so the
  source tree has clean module boundaries to evaluate.
- Evaluating the storage split (that is `evaluate-storage-split`).
- Moving protobuf stubs (Epic 4) — though the analysis must consider how the
  split affects proto ownership.

## Crate Impact

No crate changes in this feature. Analysis only.

## Interface (Public API)

No public API changes. This feature produces an ADR document.

## Data Flow

```
Audit H3 finding + Epic 3 completion (clean server boundaries)
  → Map all server-crate types to "S3 surface" vs "coordination"
  → Analyze boundary coherence: shared types, circular dep risk, wiring impact
  → Evaluate whether an intermediary coordination layer helps or hurts
  → Consider ADR-0005 trait placement: do coordinator traits move?
  → Produce ADR with decision
  → (if accepted) Create follow-up implementation feature brief
```

## Definition of Done

- [ ] **Analysis:** A written analysis maps every source file in
  `crates/oceanfs-server/src/` to either "S3 API surface" or "coordination"
  (or "shared/cross-cutting"). The analysis identifies types that resist
  clean categorization and evaluates whether a split creates more indirection
  than it removes.
- [ ] **ADR:** A new ADR (next available number) is written under `docs/adr/`
  documenting the evaluated options, the decision, and the rationale. The ADR
  follows the `docs/adr/0000-template.md` format.
- [ ] **Cross-reference:** The ADR references ADR-0005 (trait-in-consuming-crate),
  architecture §4.1 (construction in node), §1.2 (crate responsibilities), and
  the `resolve-server-storage-dep` (H1) feature.
- [ ] **Open questions resolved:** The structural roadmap question 4 ("Do
  read/write coordinators belong in server or coordination? What about auth?")
  is answered in the ADR.
- [ ] **Follow-up:** If a split is approved, the ADR appendix includes a brief
  feature outline for the implementation feature.
- [ ] **Docs:** The ADR is indexed in the doc-graph.
