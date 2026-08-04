---
feature: "Evaluate Accel Sub-Crates"
epic: "refactoring/long-term-hygiene"
status: proposed
priority: low
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Shared types from `oceanfs-core` must be stable before altering crate boundaries
adr:
  - 0006-hardware-acceleration-tier-model
  - 0007-compression-tier-governance
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Evaluate Accel Sub-Crates

## Summary

`oceanfs-accel` is 20 files and ~6,878 lines containing 6 distinct backends
(tier0 CPU SIMD, ISA-L, ARM SVE, CUDA, nvCOMP, igzip) plus the dispatcher,
compressor, and metrics. Each backend is feature-gated in dedicated modules per
architecture guideline §2.3, but 6 backends in one crate creates compilation
coupling: changing any backend's FFI requires rebuilding the entire accel crate.
This feature **evaluates** whether to split `oceanfs-accel` into a dispatcher-only
core crate (`oceanfs-accel`) plus per-backend sub-crates (`oceanfs-accel-isal`,
`oceanfs-accel-cuda`, `oceanfs-accel-arm-sve`, `oceanfs-accel-nvcomp`,
`oceanfs-accel-igzip`). The output is an ADR with the decision; no code changes
occur unless the ADR is accepted and a follow-up implementation feature is created.

## Scope

### In Scope

- Analyze the current `oceanfs-accel` crate structure: module boundaries, feature
  gate configuration (`Cargo.toml` features), compilation unit sizes, and inter-module
  coupling between backends and the dispatcher
- Measure compilation time for `oceanfs-accel` under each feature combination:
  `default` (tier0 only), `cuda`, `isa-l`, `arm-sve`, `nvcomp`, `igzip`, and
  `all` — capture baseline wall-clock build times with `cargo build --timings`
- Evaluate the proposed split architecture:
  - `oceanfs-accel` (dispatcher + tier0 CPU SIMD + metrics) — the always-present core
  - `oceanfs-accel-isal` — ISA-L backend, gated on `isal` feature
  - `oceanfs-accel-cuda` — CUDA backend, gated on `cuda` feature
  - `oceanfs-accel-arm-sve` — ARM SVE backend, gated on `arm-sve` feature
  - `oceanfs-accel-nvcomp` — nvCOMP compression, gated on `nvcomp` feature
  - `oceanfs-accel-igzip` — igzip compression, gated on `igzip` feature
- Analyze the impact on:
  1. **Compilation time benefit**: How much build time is saved when changing one
     backend (vs rebuilding the entire accel crate)?
  2. **FFI risk isolation**: How does a sub-crate boundary prevent unsafe FFI
     bugs from crossing into other backends?
  3. **Workspace complexity**: Going from 12 to 17 crates. Evaluate maintenance
     overhead: additional `Cargo.toml` files, version synchronization (workspace
     versions), CI matrix expansion, and new-crate boilerplate.
  4. **Feature-gating across crate boundaries**: In the current single-crate
     model, `#[cfg(feature = "cuda")]` modules are compiled together under one
     crate. In the split model, each sub-crate is conditionally compiled via
     `Cargo.toml` `[dependencies]` with `optional = true`. Analyze whether this
     creates feature-propagation complexity in `oceanfs-node` (the composition
     root that wires backends into the dispatcher).
  5. **Developer experience**: How does navigation, IDE support, and documentation
     generation change when backends are in separate crates vs one crate?
- Compare against the status quo: document what the current structure costs and
  what the split would cost
- Produce an ADR (`docs/adr/0008-accel-subcrate-split.md` or next available
  number) with a clear **Accept** or **Reject** decision, rationale, and
  implementation plan if accepted

### Out of Scope

- Implementing the split — this feature only produces an ADR. If accepted, a
  follow-up implementation feature will be created
- Changing any backend implementation, API, or feature-gate behavior
- Evaluating crate splits for other subsystems (storage, server) — those are
  separate evaluation features in Epic 5
- Benchmarking runtime performance — this is about build-time and codebase
  architecture, not hot-path speed
- Adding new backends or removing existing ones

## Crate Impact

| Crate | Change |
|---|---|
| None | **Evaluation only.** No source files, `Cargo.toml`, or `lib.rs` changes. The only output is an ADR document in `docs/adr/`. |

## Interface (Public API)

No new public items. No removed public items. This is an evaluation feature
with zero code changes.

## Data Flow

This is an analysis feature. The workflow:

```
1. Profile current state:
   $ cargo build --timings -p oceanfs-accel --features "cuda,isal,nvcomp,igzip,arm-sve"
   $ cargo build --timings -p oceanfs-accel --features "cuda"   # single backend change
   
   Capture: compilation wall-clock, crate graph, artifact sizes.

2. Draft proposed workspace structure:
   $ tree crates/oceanfs-accel*/
   crates/oceanfs-accel/       (dispatcher + tier0 + metrics — ~1,500 lines)
   crates/oceanfs-accel-isal/  (ISA-L FFI — ~800 lines)
   crates/oceanfs-accel-cuda/  (CUDA FFI — ~1,200 lines)
   crates/oceanfs-accel-arm-sve/ (ARM SVE — ~600 lines)
   crates/oceanfs-accel-nvcomp/  (nvCOMP — ~900 lines)
   crates/oceanfs-accel-igzip/   (igzip — ~800 lines)

3. Analyze five dimensions:
   a. Compilation time delta (single-backend change: rebuild 1 tiny crate vs 1 large crate)
   b. FFI isolation (unsafe code in one crate cannot corrupt another crate's compilation)
   c. Workspace complexity (5 additional Cargo.toml, 5x CI matrix entries, version sync)
   d. Feature-propagation ergonomics (dependency chains through `oceanfs-node`)
   e. Developer experience (navigation, IDE, doc generation)

4. Formulate recommendation:
   - ACCEPT: "Split into sub-crates because [reason], implementation to follow in feature `impl-accel-subcrates`"
   - REJECT: "Keep single crate because [reason], revisit if compilation time exceeds [threshold]"

5. Write ADR and submit for review.
```

## Definition of Done

- [ ] **Analysis:** Compilation time baselines captured and documented for all
  feature combinations of `oceanfs-accel`
- [ ] **Evaluation:** All five dimensions (compilation time, FFI isolation,
  workspace complexity, feature-propagation, developer experience) are analyzed
  with concrete data, not speculation
- [ ] **ADR:** A new ADR is written in `docs/adr/` following the ADR template
  (`0000-template.md`) with fields: Status, Date, Deciders, Context, Decision
  (Accept or Reject with rationale), and Consequences
- [ ] **Comparison:** The ADR includes a structured comparison table of status
  quo vs proposed split across all evaluated dimensions
- [ ] **ADR Constraints:** ADR-0006 (acceleration tier model) and ADR-0007
  (compression tier governance) are referenced and their constraints are
  preserved in any proposed split
- [ ] **Review:** The ADR is submitted to the architecture team for review;
  status remains `accepted` or is changed to `rejected` based on review outcome
- [ ] **Roadmap:** If accepted, a follow-up implementation feature
  (`impl-accel-subcrates`) is created in `docs/features/refactoring/long-term-hygiene/`;
  if rejected, the rationale is recorded and this feature is marked `cancelled`

> **Lint & Doc Examples (non-gating):** Not applicable — this feature produces
> no Rust code. The ADR is the sole deliverable.
