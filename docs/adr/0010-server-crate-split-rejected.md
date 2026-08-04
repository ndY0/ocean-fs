# ADR-0010: Server Crate Split — Rejected

**Status:** Proposed
**Date:** 2026-08-04
**Deciders:** OceanFS design team

---

## Context

`oceanfs-server` has grown to ~11.1K lines across 28 source files and 6
submodules. The architecture §1.2 crate-responsibility table promises
`S3Handler`, `WriteCoordinator`, `ReadCoordinator`, and `AdminHandler`. The
actual crate has expanded to include `s3_xml`, `router`, `hinted_handoff`,
`metadata_ops`, `bucket_config`, `auth/` (4 files), `grpc/` (5 files including
service stubs), and submodule groups for `read/` (5 files), `write/` (3
files), `s3_handler/` (3 files).

The structural audit (finding H3, 2026-08-03) rated this as high severity and
recommended evaluating a split into:

- **`oceanfs-server`** (S3 API surface): `s3_handler/`, `s3_xml`, `router`,
  `auth/`, `AppState`, HTTP-layer types
- **`oceanfs-coordination`** (business logic): `read/`, `write/`,
  `hinted_handoff`, `metadata_ops`, `bucket_config`, `admin`, `grpc/`

This ADR documents the evaluation, its outcome, and the rationale.

## Decision

**The server crate split is rejected.** `oceanfs-server` remains a single
crate. The S3 API surface and coordination logic form a cohesive bounded
context that is more valuable together than apart.

The structural concerns identified by the audit are addressed through
intra-crate organization (Epic 3: server-cleanup) rather than a crate-boundary
change:

1. **`s3_handler.rs` is split** into `s3_handler/handlers.rs` (axum handlers),
   `s3_handler/mime.rs` (MIME type resolution), and `s3_handler/mod.rs`
   (already completed or in Epic 3).

2. **`read_coordinator.rs` and `write_coordinator.rs` are moved** into their
   respective `read/` and `write/` subdirectories, consolidating coordinator
   logic with the assembly/fetch/repair and replication code they orchestrate
   (Epic 3: `move-coordinators`).

3. **`admin.rs` approaches 800 lines** — if it continues growing, it is split
   into `admin/handlers.rs` and `admin/metrics.rs` within the same crate.

4. **The existing subdirectory structure is documented** as the canonical crate
   layout:

   ```
   oceanfs-server/src/
   ├── lib.rs              (facade — architecture §3.1)
   ├── error.rs            (crate error type)
   ├── router.rs           (S3 API routing to handlers)
   ├── s3_xml.rs           (S3 XML response serialization)
   ├── s3_handler/         (axum HTTP handlers + MIME map)
   ├── auth/               (SigV4 signing, key store, middleware)
   ├── read/               (read coordinator, assembly, fetch, repair)
   ├── write/              (write coordinator, replication)
   ├── grpc/               (gRPC service implementations)
   ├── hinted_handoff.rs   (write-path handoff to peer nodes)
   ├── metadata_ops.rs     (metadata CRUD for coordinators)
   ├── bucket_config.rs    (per-bucket configuration store)
   └── admin.rs            (admin API: health, metrics, trigger heal/scrub)
   ```

### Rationale for Rejection

**1. The S3 API and coordination form a natural bounded context.** Every
handler in `s3_handler/` directly invokes the read or write coordinator. The
split would introduce a trait boundary (`CoordinationApi`) that exists solely
for the crate split — the trait methods would mirror the coordinators' existing
public methods one-to-one. This adds indirection without improving testability
(the handlers and coordinators are already tested together in integration
tests).

**2. `hinted_handoff` is genuinely cross-cutting within the server domain.**
It sits between the write coordinator (which initiates handoffs) and the S3
handler (which may need to surface handoff status to clients). Splitting it
into a coordination crate forces awkward trait abstractions that span both
crates.

**3. Shared types make a clean split expensive.** `AppState` holds both HTTP
state and coordinator references. Response types (`PutObjectResponse`,
`GetObjectResponse`) are used by both handlers and coordinators. The error type
(`ServerError`) spans all layers. A split would require either duplicating
these types, creating a third shared-types crate, or accepting that the S3
surface crate depends on the coordination crate for response types — which is
backwards.

**4. The storage split (ADR-0009) already addresses the primary concern.**
The modules that least belonged in a single-responsibility sense
(`anti_entropy`, `gc`, `heal`, `scrub`) are being extracted into
`oceanfs-durability`. What remains in `oceanfs-server` — S3 handlers,
coordinators, auth, admin — are genuinely coherent: they all exist to serve the
S3 API.

**5. The crate is already well-organized internally.** Epic 3 (server-cleanup)
has addressed the mega-file problems (`s3_handler.rs`, `read_coordinator.rs`,
`write_coordinator.rs`). The remaining files are each under 800 lines and have
clear singular responsibilities. The subdirectory structure (`read/`, `write/`,
`auth/`, `s3_handler/`, `grpc/`) is clean and follows the architecture
guideline §3.3 (one-type-per-file).

## Consequences

### Positive

- **No new crate overhead.** The workspace stays at 14 crates (with the storage
  split adding `oceanfs-durability` + `oceanfs-storage-api`).
- **No trait indirection.** Handlers call coordinator methods directly — no
  `CoordinationApi` trait that exists only to satisfy a crate boundary.
- **Simpler composition root.** `oceanfs-node` constructs one server object,
  not two.
- **Consistent error handling.** `ServerError` spans all layers without
  conversion at an artificial crate boundary.
- **The architectural intent is documented.** This ADR captures *why* the
  server crate is organized as it is, making future decisions informed.

### Negative

- **Crate remains large (~11K lines).** A single crate at this size requires
  discipline to keep organized. Mitigation: the documented module structure and
  Epic 3 splits provide that discipline.
- **No compile-time isolation between S3 and coordination layers.** Changing a
  coordinator implementation recompiles the entire server crate. Mitigation:
  this is acceptable because the two layers are tightly coupled in practice;
  isolating them would not meaningfully reduce recompilation because changes to
  coordinators nearly always affect handlers.

### Neutral

- **The `evaluate-server-split` feature is completed with a rejection.** The
  evaluation itself was valuable — it confirmed that the current structure,
  post-Epic-3 cleanup, is architecturally sound.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Split into `oceanfs-server` + `oceanfs-coordination`** | Follows architecture guideline pattern; separates concerns at crate boundary | Adds a `CoordinationApi` trait boundary that mirrors existing coordinators; creates shared-type problems (AppState, response types, error types); `hinted_handoff` resists clean categorization | The indirection cost exceeds the structural benefit. S3 + coordination are a single bounded context. |
| **Three-way split: server, coordination, server-types** | Solves shared-types problem | Three crates for what is currently one coherent domain; excessive for 11K lines | Over-engineering. If the crate reaches 30K+ lines, this should be re-evaluated. |
| **Move coordinators into `oceanfs-storage-api`** | Single dependency for coordination traits | Server business logic (read path assembly, write quorum management) is not a storage concern. Storage API should define storage contracts, not coordination logic. | Category error: coordinators consume storage, they are not storage. |

## References

- [Structural Audit (2026-08-03), finding H3](../audits/2026-08-03-two-stage-structural-audit.md)
- [ADR-0005: Trait-in-Consuming-Crate Pattern](./0005-trait-in-consuming-crate.md)
- [ADR-0009: Storage Crate Split](./0009-storage-crate-split.md) (the storage split reduces pressure on the server crate)
- [Architecture Guideline §1.2: Crate Responsibilities](../guidelines/architecture.md#12-crate-responsibilities)
- [Architecture Guideline §4.1: Construction Happens in `oceanfs-node`](../guidelines/architecture.md#41-construction-happens-in-oceanfs-node)
- [Epic 3: Server Crate Cleanup](../features/refactoring/structural-roadmap.md#epic-3-server-crate-cleanup-short-term--sprint-3)
- [Feature: Evaluate Server Crate Split](../features/refactoring/megacrate-split/evaluate-server-split.md)
