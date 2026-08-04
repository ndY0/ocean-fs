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
updated: 2026-08-03
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

## Epic 3: Server Crate Cleanup (Short-term — Sprint 3)

**Goal:** Restore one-type-per-file in the server crate, resolve dependency
documentation, and rename a colliding concrete struct.

| Feature | Audit Ref | What Changes | Risk |
|---|---|---|---|
| `split-s3-handler` | H4 | Split `s3_handler.rs` (1,252 lines) into handler functions, response types, mime map. | **Low–Medium.** |
| `move-coordinators` | H6, M2 | Move read/write coordinators into existing `read/` and `write/` subdirectories. | **Low.** Internal move. |
| `rename-metadata-store` | M4 | Rename concrete `MetadataStore` struct to `RocksDbMetadataStore`. Trait stays in `oceanfs-core` (cross-cutting exception per ADR-0005 — consumed by 3 crates). | **Low.** Rename only; no trait move, no DAG change. |
| `document-server-deps` | H1 | Document each optional dependency's justification in `oceanfs-server/Cargo.toml`. Architecture §4.1 already revised to remove the prohibition. | **Low.** Documentation-only. |

**Sequencing:** `split-s3-handler` and `move-coordinators` first (mechanical).
Then `rename-metadata-store` and `document-server-deps` (documentation).

---

## Epic 4: Protobuf Reorganization (Short-term — Sprint 3–4)

**Goal:** Move generated service stubs from `oceanfs-network` to their owning
crates per architecture §2.4.

| Feature | Audit Ref | What Changes | Risk |
|---|---|---|---|
| `move-proto-stubs` | H8 | Move generated protobuf service stubs: `oceanfs.cache.rs` → `oceanfs-cache`, `oceanfs.healing.rs` → `oceanfs-storage`, `oceanfs.scrub.rs` → `oceanfs-storage`, `oceanfs.storage.rs` → `oceanfs-storage`. Keep `oceanfs.common.rs` and `oceanfs.gossip.rs` in `oceanfs-network`. | **Medium.** Requires regenerating protobuf code in target crates, updating imports, and ensuring no compilation breakage. |

**Sequencing:** Can run in parallel with Epic 3. Depends on Epic 1 only.

---

## Epic 5: Mega-Crate Splitting (Medium-term — Sprint 5–6)

**Goal:** Evaluate and potentially execute major crate-boundary changes for
the two largest crates (storage at 12.7K lines, server at 11.1K lines).

| Feature | Audit Ref | What Changes | Risk |
|---|---|---|---|
| `evaluate-storage-split` | H2 | Evaluate splitting `oceanfs-storage` into `oceanfs-storage-core` (buffer_pool, segment, wal, metadata, blob_store) and `oceanfs-durability` (anti_entropy, scrub, gc, heal). Produce ADR with decision. If approved, execute the split in a follow-up feature. | **High.** Crate boundary change; affects DAG, Cargo.toml files, and `oceanfs-node` wiring. |
| `evaluate-server-split` | H3 | Evaluate splitting `oceanfs-server` into `oceanfs-server` (S3 API surface: s3_handler, s3_xml, router, auth) and `oceanfs-coordination` (read_coordinator, write_coordinator, hinted_handoff, metadata_ops, bucket_config, admin). Produce ADR. If approved, execute in follow-up feature. | **High.** Same risk profile as storage split. |
| `split-node-rs` | H5 | Split `oceanfs-node/src/node.rs` (1,012 lines) into `node.rs` (struct + start), `background_tasks.rs`, `config.rs` (validate_config). | **Low–Medium.** Internal refactor within one crate. |

**Sequencing:** Evaluation features produce ADRs first; implementation follows
only if the ADR is accepted. Both evaluation features can run in parallel.
`split-node-rs` is independent—can run anytime after Epic 1.

---

## Epic 6: Configuration & Membership Decomposition (Medium-term — Sprint 5–6)

**Goal:** Break up growing config and membership modules.

| Feature | Audit Ref | What Changes | Risk |
|---|---|---|---|
| `split-config` | M8 | Split `oceanfs-core/src/config.rs` (504 lines) into `config/node.rs`, `config/metadata.rs`, `config/ring.rs`, `config/wal.rs`, `config/accel.rs`, `config/auth.rs`, `config/compression.rs`, `config/mod.rs`. | **Low.** Pure mechanical split; config types are mostly `#[derive]` structs. |
| `split-membership` | M7 | Split `membership.rs` (822 lines) into `membership/state.rs` and `membership/manager.rs`. Split `gossip.rs` (527 lines) monitored for future growth. | **Low.** Internal splits within `oceanfs-membership`. |
| `split-failure-detector` | M10 | Split `failure_detector.rs` (519 lines) into `failure_detector/ping.rs`, `failure_detector/suspicion.rs`, `failure_detector/mod.rs`. | **Low.** SWIM protocol has natural boundaries between ping and suspicion phases. |

**Sequencing:** All three are independent. Depends on Epic 1 for config types.

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

### Still Open

| # | Question |
|---|---|
| **Q5** | Server split boundary (H3): Do read/write coordinators belong in `oceanfs-server` or a coordination crate? Feature `evaluate-server-split` will produce an ADR. |
