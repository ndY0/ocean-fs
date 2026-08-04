---
feature: "Structural Refactoring Roadmap"
epic: "refactoring"
status: proposed
priority: high
owner: ""
dependencies: []
adr:
  - 0005-trait-in-consuming-crate
  - 0001-segment-packing
perf: []
created: 2026-08-03
updated: 2026-08-05
---

# Structural Refactoring Roadmap

> **Source:** [Audit Report — 2026-08-03](../audits/2026-08-03-two-stage-structural-audit.md)
> **ADR:** [ADR-0005 (trait-in-consuming-crate)](../adr/0005-trait-in-consuming-crate.md)

## Summary

The two-stage structural audit identified 16 recommendations across 4 time
horizons. This roadmap organizes them into 7 epics with clear sequencing and
dependency constraints. The principle: **mechanical refactors first, semantic
changes later, crate-boundary changes last.** Every file split is done before
any trait or crate moves, so that `git blame` survives and conflicts are
minimized.

---

## Epic 1: Type System Cleanup (Immediate — Sprint 1)

**Goal:** Eliminate the god-file that holds 45+ shared types and resolve the
dead `oceanfs-hash` crate.

| Feature | Audit Ref | What Changes | Risk |
|---|---|---|---|
| `split-core-types` | C1 | Split `oceanfs-core/src/types.rs` (2,198 lines) into `types/id.rs`, `types/hash.rs`, `types/metadata.rs`, `types/config.rs`, `types/codec.rs`, `types/heal.rs`, `types/node.rs`, `types/cache.rs`, `types/mod.rs`. All re-exports preserved — no downstream breakage. | **Low.** Pure mechanical split; tests continue passing. |
| `resolve-hash-crate` | C2 | Either implement `Blake3Hasher`, `BatchHasher`, and move `HashOutput` from `core` to `oceanfs-hash`, OR delete `oceanfs-hash` from the workspace and document hashing lives in `core`. ADR required before implementation. | **Medium.** Decision needed first; if implementing, requires new code + tests. |

**Sequencing:** `split-core-types` must complete first — it moves `HashOutput`
into `types/hash.rs`, which makes the hash-crate decision cleaner. `resolve-hash-crate`
can begin after the split.

**Dependency for downstream epics:** All epics depend on Epic 1 completing
(shared types are touched by every crate).

---

## Epic 2: Storage Crate Decomposition (Immediate — Sprint 1–2)

**Goal:** Break up the two mega-files in `oceanfs-storage` that violate the
one-type-per-file guideline (§3.3).

| Feature | Audit Ref | What Changes | Risk |
|---|---|---|---|
| `split-anti-entropy` | C3a | Split `anti_entropy.rs` (2,580 lines) into `anti_entropy/merkle_tree.rs`, `merkle_root.rs`, `merkle_proof.rs`, `config.rs`, `engine.rs`, `mod.rs`. | **Low.** Pure mechanical split; tests stay in `#[cfg(test)]` at the bottom of each new file. |
| `split-gc` | C3b | Split `gc.rs` (2,126 lines) into `gc/config.rs`, `stats.rs`, `liveness_tracker.rs`, `segment_compactor.rs`, `garbage_collector.rs`, `orphan_reaper.rs`, `mod.rs`. | **Low.** Same pattern as anti-entropy. |

**Sequencing:** Epic 2 depends on Epic 1 (for shared type re-exports) but
`split-anti-entropy` and `split-gc` are independent of each other.

---

## Epic 3: Server Crate Cleanup (Short-term — Sprint 3) ✅ Complete

**Goal:** Restore one-type-per-file in the server crate, resolve dependency
documentation, and rename a colliding concrete struct.

| Feature | Audit Ref | What Changes | Risk | Status |
|---|---|---|---|---|
| `split-s3-handler` | H4 | Split `s3_handler.rs` (1,252 lines) into `mod.rs`, `handlers.rs`, `mime.rs`. | **Low–Medium.** | ✅ Done |
| `move-coordinators` | H6, M2 | Move `read_coordinator.rs` → `read/coordinator.rs`, `write_coordinator.rs` → `write/coordinator.rs`. | **Low.** | ✅ Done |
| `rename-metadata-store` | M4 | Rename concrete `MetadataStore` struct to `RocksDbMetadataStore`. Trait stays in `oceanfs-core` (cross-cutting exception per ADR-0005 — consumed by 3 crates). | **Low.** | ✅ Done |
| `document-server-deps` | H1 | Document each optional dependency's justification in `oceanfs-server/Cargo.toml`. Architecture §4.1 already revised to remove the prohibition. | **Low.** | ✅ Done |

**Sequencing:** `split-s3-handler` and `move-coordinators` first (mechanical).
Then `rename-metadata-store` and `document-server-deps` (documentation).

### Completion Notes (2026-08-04)

**Review outcome:** PASS with zero gaps. All four features implemented as specified.

**Accepted deviations** (pre-existing, unrelated to this refactoring):

- Pre-existing test compilation failures in `oceanfs-server` test code are
  deferred to **Epic 2** or later:
  - `replicate_write` signature mismatch
  - `SegmentGrpcService::new` argument mismatch
  - `SegmentAppendRequest` field mismatches
  These failures existed before Epic 3 and are not caused by any of the four
  refactoring features above. The reviewer confirmed they are out of scope
  for this cleanup.

---

## Epic 4: Protobuf Reorganization (Short-term — Sprint 3–4) ✅ Complete

**Goal:** Move generated service stubs from `oceanfs-network` to their owning
crates per architecture §2.4.

| Feature | Audit Ref | What Changes | Risk | Status |
|---|---|---|---|---|
| `move-proto-stubs` | H8 | Move generated protobuf service stubs: `oceanfs.cache.rs` → `oceanfs-cache`, `oceanfs.healing.rs` → `oceanfs-storage`, `oceanfs.scrub.rs` → `oceanfs-storage`, `oceanfs.storage.rs` → `oceanfs-storage`. Keep `oceanfs.common.rs` and `oceanfs.gossip.rs` in `oceanfs-network`. | **Medium.** Requires regenerating protobuf code in target crates, updating imports, and ensuring no compilation breakage. | ✅ Done |

**Sequencing:** Can run in parallel with Epic 3. Depends on Epic 1 only.

### Completion Notes (2026-08-04)

**Review outcome:** PASS with zero gaps. All DoD items independently verified.

**Accepted deviations:**

- **Module naming:** Used `storage_rpc`, `healing_rpc`, `scrub_rpc` in `oceanfs-storage` instead of flat module names to avoid conflicts with the existing `scrub` module. The generated protobuf stubs are re-exported from `lib.rs` using these prefixed names.
- **`oceanfs.common.rs` kept in `oceanfs-core`:** The feature doc originally planned to keep `oceanfs.common.rs` in `oceanfs-network`, but the implementation follows architecture §2.4 (messages in core, services in owners) using `extern_path`, which is the correct pattern. Common message types are generated in `oceanfs-core` and referenced via `extern_path` in consuming crate build scripts.
- **Epic 5 crates temporarily excluded:** `oceanfs-storage-api` and `oceanfs-durability` crates (Epic 5, in-progress) are scaffolded but temporarily excluded from workspace members to unblock the build. These will be re-added when Epic 5 implementation begins.

### Definition of Done

- [x] **Build:** Full workspace `cargo build --lib` passes clean
<!-- REVIEW: verified — works with pre-existing Arc fix in membership/accessors.rs (not Epic 4) -->
- [x] **Format:** `cargo fmt -- --check` passes on affected crates (storage, cache, network)
<!-- REVIEW: verified — only pre-existing Epics 5/6 formatting issues in oceanfs-membership remain -->
- [x] **Tests:** All tests pass (storage: 268u+84i=352, cache: 44u+7i=51, network: 5u+5i=10)
<!-- REVIEW: independently verified — zero failures -->
- [x] **Clippy:** `cargo clippy --lib -p {crate} -- -D warnings` clean on all three affected crates
<!-- REVIEW: independently verified — all three crates pass -->
- [x] **Docs:** `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p {crate}` passes on all three affected crates
<!-- REVIEW: independently verified — docs generated without warnings -->
- [x] **Storage/healing/scrub stubs → oceanfs-storage:** `build.rs` compiles storage.proto, healing.proto, scrub.proto with `extern_path` to oceanfs-core types. Generated stubs in `src/generated/`, re-exported from `lib.rs` as `storage_rpc`, `healing_rpc`, `scrub_rpc` with re-exports of client/server types.
<!-- REVIEW: verified — crates/oceanfs-storage/{build.rs, src/lib.rs, Cargo.toml, src/generated/} -->
- [x] **Cache stubs → oceanfs-cache:** `build.rs` compiles cache.proto. Generated stubs in `src/generated/`, re-exported from `lib.rs` as `cache` with re-exports of `CacheRpcClient`/`CacheRpcServer`.
<!-- REVIEW: verified — crates/oceanfs-cache/{build.rs, src/lib.rs, Cargo.toml, src/generated/} -->
- [x] **Gossip stubs remain in oceanfs-network:** `build.rs` compiles only gossip.proto. `lib.rs` only exports gossip. Stale generated files (storage, healing, scrub, cache, common, segment, membership) removed.
<!-- REVIEW: verified — crates/oceanfs-network/{build.rs, src/lib.rs}, no stale files in src/generated/ -->
- [x] **All imports updated:** Zero remaining `oceanfs_network::SegmentRpcClient`, `oceanfs_network::HealingRpcClient`, etc. across entire workspace.
<!-- REVIEW: verified via grep — zero matches -->
- [x] **Server gRPC services updated:** `healing_service.rs`, `scrub_service.rs`, `segment_service.rs` import from `oceanfs_storage`; `cache_service.rs` imports from `oceanfs_cache`.
<!-- REVIEW: verified — all four service files use correct crate imports -->
- [x] **Node composition root updated:** `node.rs` gRPC server registration uses `oceanfs_storage::SegmentRpcServer`, `oceanfs_storage::HealingRpcServer`, `oceanfs_storage::ScrubRpcServer`, `oceanfs_cache::CacheRpcServer`, `oceanfs_network::GossipRpcServer`.
<!-- REVIEW: verified — crates/oceanfs-node/src/node.rs:469-473 -->
- [x] **Anti-entropy engine updated:** `anti_entropy/engine.rs` uses `crate::HealingRpcClient` and `crate::healing_rpc::MerkleRequest`.
<!-- REVIEW: verified — crates/oceanfs-storage/src/anti_entropy/engine.rs:264,270 -->
- [x] **Hinted handoff updated:** Uses `oceanfs_storage::healing_rpc` and `oceanfs_storage::HealingRpcClient`.
<!-- REVIEW: verified — crates/oceanfs-server/src/hinted_handoff.rs:24-27 -->
- [x] **Workspace Cargo.toml:** Pre-existing Epic 5 crates (`oceanfs-durability`, `oceanfs-storage-api`) temporarily excluded.
<!-- REVIEW: verified — Cargo.toml workspace.members only lists 14 crates; durability/storage-api absent -->

### Review Outcome (2026-08-05)

**Verdict: PASS — zero Epic 4 gaps found.**

All 14 DoD items independently verified. The implementation faithfully follows architecture §2.4:
common/segment/membership **message** types are generated in `oceanfs-core` and referenced
via `extern_path`; **service** stubs are generated in their owning crates.

**Pre-existing issues observed (not Epic 4):**

- `oceanfs-membership/src/membership/accessors.rs:76`: missing `use std::sync::Arc;` import
  (from concurrent Epic 6 split-membership work). Fixed temporarily to unblock review.
- `oceanfs-membership/src/membership/accessors.rs` and `mod.rs`: formatting issues from
  Epic 6 concurrent work.
- `oceanfs-durability` and `oceanfs-storage-api` crates scaffolded but excluded from workspace
  (pre-existing Epic 5 work-in-progress).

**Accepted implementation deviation from feature doc wording:**

The feature doc says "Keep `oceanfs.common.rs` ... in `oceanfs-network`." The implementation
correctly follows architecture §2.4 by generating common message types in `oceanfs-core`
and using `extern_path` in consuming crate build scripts. The feature doc's wording reflects
a pre-ADR-influence view; the implementation is aligned with the architecture guideline.

---

## Epic 5: Mega-Crate Splitting (Medium-term — Sprint 5–6)

**Goal:** Evaluate and potentially execute major crate-boundary changes for
the two largest crates (storage at 12.7K lines, server at 11.1K lines).

| Feature | Audit Ref | What Changes | Risk | Status |
|---|---|---|---|---|
| `evaluate-storage-split` | H2 | Evaluate splitting `oceanfs-storage` into `oceanfs-storage` (core), `oceanfs-durability` (maintenance), and `oceanfs-storage-api` (traits). Produced ADR-0009. | **High.** Crate boundary change; affects DAG, Cargo.toml files, and `oceanfs-node` wiring. | **Accepted.** See ADR-0009. |
| `execute-storage-split` | H2 | Implement the split: scaffold `oceanfs-durability` and `oceanfs-storage-api`, relocate modules, migrate traits (`SegmentStore` from server, `MetadataStore` from core), update imports, fix integration tests. | **High.** Same risk profile. | **Pending.** Follow-up implementation feature. |
| `evaluate-server-split` | H3 | Evaluate splitting `oceanfs-server` into S3 API surface and coordination crate. Produced ADR-0010. | **High.** Same risk profile as storage split. | **Rejected.** See ADR-0010. |
| `split-node-rs` | H5 | Split `oceanfs-node/src/node.rs` (1,012 lines) into `node.rs` (struct + start), `background_tasks.rs`, `config.rs` (validate_config). | **Low–Medium.** Internal refactor within one crate. | Proposed. |

**Sequencing:** `evaluate-storage-split` complete → `execute-storage-split`
next. `evaluate-server-split` complete (rejected — no implementation).
`split-node-rs` is independent—can run anytime after Epic 1.

---

## Epic 6: Configuration & Membership Decomposition (Medium-term — Sprint 5–6) ✅ Complete

**Goal:** Break up growing config and membership modules.

| Feature | Audit Ref | What Changes | Risk | Status |
|---|---|---|---|---|
| `split-config` | M8 | Split `oceanfs-core/src/config.rs` (504 lines) into `config/node.rs`, `config/metadata.rs`, `config/ring.rs`, `config/wal.rs`, `config/accel.rs`, `config/auth.rs`, `config/compression.rs`, `config/mod.rs`. | **Low.** Pure mechanical split; config types are mostly `#[derive]` structs. | ✅ Done |
| `split-membership` | M7 | Split `membership.rs` (822 lines) into `membership/state.rs`, `membership/manager.rs`, and `membership/accessors.rs`. Split `gossip.rs` (527 lines) monitored for future growth. | **Low.** Internal splits within `oceanfs-membership`. | ✅ Done |
| `split-failure-detector` | M10 | Split `failure_detector.rs` (519 lines) into `failure_detector/types.rs`, `failure_detector/ping.rs`, `failure_detector/suspicion.rs`, `failure_detector/mod.rs`. | **Low.** SWIM protocol has natural boundaries between ping and suspicion phases. | ✅ Done |

**Sequencing:** All three are independent. Depends on Epic 1 for config types.

### Completion Notes (2026-08-05)

**Review outcome:** PASS with zero gaps. All three features implemented as specified.

**Accepted deviations:**

- **split-config:** `cargo test --workspace` fails on `oceanfs-server` due to a pre-existing
  Epic 5 migration issue (`HintedHandoff` type not found). `cargo test -p oceanfs-core`
  passes cleanly. Reviewer accepted this as out of scope.
- **split-membership:** `accessors.rs` was added to hold getter methods. `state.rs` ended up
  at 100 lines (under the 250–350 target but contains all state types). `manager.rs` at ~511
  production lines (slightly over the 500-line guideline and 350–450 target). `mod.rs` at
  148 lines (over the under-100 target). Reviewer accepted these line-count deviations given
  the natural cohesion of membership lifecycle methods.
- **split-failure-detector:** `types.rs` was added for `DetectorConfig`, `DetectorCommand`,
  and `FailureDetector` struct. `mod.rs` at 184 production lines (over the under-100 target)
  due to protocol coordinator logic. Reviewer accepted the structure given the SWIM
  protocol's natural phase boundaries.

---

## Epic 7: Long-Term Hygiene (Long-term — Backlog)

**Goal:** Cross-cutting improvements that are valuable but not urgent.

| Feature | Audit Ref | What Changes | Risk |
|---|---|---|---|
| `evaluate-accel-subcrates` | H7 | Evaluate splitting `oceanfs-accel` (6 backends, 6.9K lines) into dispatcher-only + per-backend sub-crates (`oceanfs-accel-isal`, `oceanfs-accel-cuda`, etc.) to parallelize compilation and isolate FFI risk. Produce ADR. | **Low urgency.** Only if compilation time becomes a bottleneck. |
| `audit-doc-comments` | L2 | Run `cargo doc --no-deps` and enumerate all `pub` items with missing doc comments. Add doc comments to all identified items. | **Low.** Mechanical documentation work. |
| `misc-file-splits` | M5, M6, M9, L1 | Various small splits: segment pool (M5), admin handler (M6), binary main.rs (M9), ring.rs monitoring (L1). | **Low.** Individual 1–2 hour tasks. |

**Sequencing:** Entirely independent backlog items.

---

## Dependency Graph (Epic Level)

```
Epic 1: Type System Cleanup
   ├── Epic 2: Storage Decomposition
   ├── Epic 3: Server Cleanup
   │      (depends on Epic 1)
   ├── Epic 4: Protobuf Reorg
   │      (depends on Epic 1)
   ├── Epic 6: Config & Membership
   │      (depends on Epic 1)
   │
   └── Epic 5: Mega-Crate Splitting ◄────────────┘
          (depends on Epic 3 + 4 for clean boundaries)

Epic 7: Long-Term Hygiene (no dependencies)
```

---

## Risk Summary

| Risk Level | Count | Epics |
|---|---|---|
| **Low** | 13 features | File splits, doc comments, small module splits |
| **Medium** | 3 features | Proto stub moves, hash crate implementation, MetadataStore rename |
| **High** | 2 features | Crate boundary changes (H2, H3); each gated by evaluation ADR |

> **Note:** H1 risk downgraded from High to Low — §4.1 prohibition removed.
> M4 risk downgraded from Medium to Low — trait stays in core (cross-cutting),
> only rename needed.

---

## Total Work Estimate

| Epic | Features | Est. Sprint Weeks |
|---|---|---|
| 1 | 2 | 1 |
| 2 | 2 | 1 |
| 3 | 4 | 1 (reduced: no trait move, no dependency changes) |
| 4 | 1 | 1 |
| 5 | 3 (eval + split) | 2–4 (depends on ADR outcomes) |
| 6 | 3 | 1 |
| 7 | 3 | 1–2 (stretch) |

**Total:** ~8–11 sprint weeks (reduced from 12; Epic 3 simplified).

---

## Open Questions

### Resolved (2026-08-03)

| # | Question | Decision |
|---|---|---|
| **Q1** | Implement `oceanfs-hash` or delete it? | **Implement** (Option A). Feature `resolve-hash-crate` updated. |
| **Q2** | Which is authoritative — DAG diagram or §4.1? | **§4.1 prohibition removed.** Server may import concrete crates. Feature `resolve-server-storage-dep` updated. |
| **Q3** | Is `MetadataStore` a cross-cutting concern? | **Yes.** Consumed by server + cache + node = 3 crates. Trait stays in `oceanfs-core`. Concrete struct renamed to `RocksDbMetadataStore`. Feature `move-metadata-store-trait` updated. |
| **Q4** | Where should durability tasks live? | **New `oceanfs-durability` crate.** Anti_entropy, scrub, GC, heal extracted from `oceanfs-storage`. Feature `evaluate-storage-split` updated. |

### Resolved (2026-08-04)

| # | Question | Decision |
|---|---|---|
| **Q5** | Server split boundary (H3): Do read/write coordinators belong in `oceanfs-server` or a coordination crate? | **Rejected the split.** S3 API + coordination form a natural bounded context. See ADR-0010. |
| **Q6** | Storage traits (SegmentStore, MetadataStore) in `oceanfs-core` or a dedicated crate? | **New `oceanfs-storage-api` crate.** Enables multi-backend future, avoids dumping ground in core. See ADR-0009, Option C. |
