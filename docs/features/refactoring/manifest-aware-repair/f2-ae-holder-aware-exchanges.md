---
feature: "f2: Holder-Aware Anti-Entropy Exchanges"
epic: "refactoring/manifest-aware-repair"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: f1-peer-selector-trait
    epic: refactoring/manifest-aware-repair
    reason: run_cycle/continuous/sampling consume the injected PeerSelector and its node-layer ManifestPeerSelector impl
  - epic: refactoring/composition-root-decomposition
    reason: AntiEntropy::new signature changes; the single call site is the c2 durability builder (node.rs §7 → modules/durability.rs)
  - epic: refactoring/store-unification
    reason: The holder-set reads (registry Sealed entries) and the SegmentDataStore reads inside run_cycle must be consistent — one shared store after ADR-0032
adr:
  - 0033-manifest-aware-peer-selection
  - 0015-anti-entropy-merkle-protocol
  - 0029-storage-pools-disk-resilience
perf:
  - "2.4 lock-free reads on the hot path"
  - "2.6 bounded queues / no per-peer O(all segments) fan-out"
created: 2026-09-04
updated: 2026-09-04
---

# f2: Holder-Aware Anti-Entropy Exchanges

> **INTERFACE / SEQUENCING CONSTRAINT (roadmap §4, C3):** this feature
> wires `peer_selector` into `AntiEntropy`. The durability-scheduler epic
> (`f1` `AeTask` adaptor + `f4` wiring) also constructs/registers
> `AntiEntropy` inside the SAME c2 durability builder. To keep the two
> epics non-conflicting, prefer injecting the selector via a builder
> `with_peer_selector(Arc<dyn PeerSelector>)` setter (the codebase's
> `with_*` style) so `AntiEntropy::new`'s signature stays stable and the
> scheduler adaptor is untouched. If a constructor parameter is used
> instead, this feature MUST land before `durability-scheduler/f4`
> wiring.

## Summary

Reworks `AntiEntropy` (`crates/oceanfs-durability/src/anti_entropy/engine.rs`)
so that **the segment → holder map is the entry point** of every cycle,
per ADR-0033 D1/D2. Today the engine picks `peer_count` random alive
members (`select_alive_peers`, `engine.rs:863-878`) and sends its **full
sealed-segment list** to each (`engine.rs:538-547`); review
`engine.rs:226` calls this wrong under partial replication. After f2, each
sealed segment's comparison peers are `PeerSelector::eligible_holders(
segment, metadata.storage_locations)` — actual holders that are alive and
manifest-healthy — and a segment with no eligible remote holder is
**excluded from remote exchange** and left to the existing
`local_merkle_verify` fallback + local scrub. The ADR-0015 Merkle
protocol, incremental tree, sampling fraction/rates, and the gRPC
`MerkleExchange` wire calls are **unchanged**; only peer/segment selection
changes. All three modes (`run_cycle`, `run_continuous_cycle`,
`run_sampling_cycle`) are preserved.

## Scope

### In Scope
- Add `peer_selector: Arc<dyn PeerSelector>` to `AntiEntropy` (field +
  constructor parameter at `engine.rs:94-121`); update the node wiring
  (`node.rs:1238`, later the c2 durability builder).
- **D2 entry-point flip in `run_cycle`** (`engine.rs:178-291`): after
  gathering Sealed segments from the registry (`for_each` +
  `SegmentState::Sealed`, unchanged), derive per-segment holders from
  `metadata.storage_locations` and group segments **by eligible holder**;
  for each peer, exchange roots over *only the segments that peer holds*
  (replaces the random `select_alive_peers` at `engine.rs:233` and the
  full-list `try_grpc_merkle_exchange` send at `engine.rs:538-547`).
- **Continuous mode** (`run_continuous_cycle`, `engine.rs:307-354`):
  iterate the tracked Sealed segments once (the current per-peer
  `registry.for_each` re-scan inside the peer loop is a per-peer O(n)
  repeat — move the scan out), and for each segment compare roots against
  up to `min(peer_count, |eligible holders|)` eligible holders. No remote
  exchange for local-only segments. Trigger cadence
  (`on_segment_sealed`, write-counter, gossip interval) unchanged.
- **Sampling mode** (`run_sampling_cycle`, `engine.rs:412-472`): keep the
  fraction sample of tracked segments; for each sampled segment pick up to
  one eligible holder (preserving the existing `break` after the first
  peer, `engine.rs:463`). Local-only sampled segments are counted but not
  remotely exchanged.
- **Fallback**: `local_merkle_verify` (`engine.rs:638-661`) remains the
  no-peer / local-only coverage path; when a cycle yields zero eligible
  remote holders for its whole set the existing fallback condition
  (`engine.rs:268`) covers it unchanged.
- Delete `AntiEntropy::select_alive_peers` (`engine.rs:859-878`) and its
  tests (`engine.rs:1534-1643`); it has no caller after the flip.
- Remove the resolved review block at `engine.rs:226`. Leave
  `engine.rs:184` and `engine.rs:199` untouched (Theme 4/1 — different
  wave).
- Test updates: `make_anti_entropy` and every `AntiEntropy::new` call in
  `engine.rs` tests gain a test double (`AllEligible` — returns holders as
  given), plus new holder-aware tests below.

### Out of Scope
- Merkle tree, `MerkleExchange` RPC, `exchange_single_root`,
  `try_grpc_merkle_exchange` internals, heal-enqueue behavior — unchanged
  (ADR-0015).
- Scrub (`f3`), GC/reader consolidation (ADR-0032 epic), scheduler
  (durability-scheduler epic).
- The `ec_repair_segment` / `merkle_repair_diverged_leaves` /
  `MerkleExchangeProtocol` dead-code cleanup (roadmap wave 4 — separate).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `anti_entropy/engine.rs`: new `peer_selector` field + ctor param; holder-aware grouping in all three cycles; `select_alive_peers` deleted; review block at :226 removed. `lib.rs`: unchanged exports. |
| `oceanfs-node` | `node.rs:1238` `AntiEntropy::new(...)` gains the injected `Arc<dyn PeerSelector>` (constructed in the c2 durability builder); no other node change. |

## Interface (Public API)

- `AntiEntropy::new(config, membership, registry, pool, segment_store,
  merkle_tree, peer_selector)` — **adds** one `Arc<dyn PeerSelector>`
  argument after `merkle_tree` (mirrors the ctor shape of
  `ReconciliationLoop::new`, which already takes injected deps).
- Removed: `AntiEntropy::select_alive_peers` (pub, `engine.rs:859`). All
  remaining pub methods (`run_cycle`, `run_continuous_cycle`,
  `run_sampling_cycle`, `start_background`, `on_segment_sealed`,
  `register_metrics`, `merkle_tree`) keep their signatures.
- The `#[cfg(test)]` module gains an `AllEligible` test selector
  implementing `PeerSelector` (returns the input holder list) — reused by
  every existing engine test that does not exercise eligibility.

## Data Flow

```
run_cycle / continuous / sampling
  registry.for_each(Sealed)                       ← "segments I hold" (entry point, D2)
    for segment in sealed:
      holders = segment.metadata.storage_locations
      eligible = peer_selector.eligible_holders(segment, holders)   // holders ∩ alive+healthy, − self
      if eligible.is_empty(): local_only += 1; skip remote            // → local_merkle_verify / scrub
      else: group[eligible_peer].push(segment)                        // cap per-segment peers by peer_count
  for (peer, shared_segments) in group:                              // ≤ full list, only shared
    try_grpc_merkle_exchange(peer, shared_segments)                   // wire call unchanged
  if no peers (or no mismatch): local_merkle_verify(sealed, local_trees, …)   // fallback unchanged
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds for
      `oceanfs-durability` and `oceanfs-node`.
- [ ] **No random-peer path:** a workspace grep finds no remaining
      production call to `select_alive_peers` or to a peer list derived
      from `Membership::nodes()` inside `anti_entropy/engine.rs`.
- [ ] **Entry point flip:** all three cycles derive the peer set
      per-segment from `metadata.storage_locations` via the injected
      `PeerSelector`; `try_grpc_merkle_exchange` is never called with a
      full sealed-segment list for a peer that is not an eligible holder
      of those segments.
- [ ] **Modes intact:** `continuous_enabled` / `sampling_enabled` /
      `sampling_interval_sec` / `sampling_fraction` /
      `continuous_max_segments` semantics unchanged; `on_segment_sealed`
      unchanged.
- [ ] **Local-only handling test:** a registry with a segment whose
      `storage_locations == {self}` (or whose only holders are Dead) is
      counted as local-only and produces no gRPC Merkle exchange, while a
      second segment listing one eligible holder produces exactly one
      exchange group.
- [ ] **Grouping test:** with `storage_locations = [self, A, B]` and
      `peer_count = 1`, each cycle exchanges with at most one of {A, B}
      per segment (ADR-0015 cost model — no full-mesh fan-out to every
      holder).
- [ ] **Merkle protocol regression:** `exchange_single_root` and the
      mismatch→`crate::heal::enqueue_heal` path still covered by the
      existing mismatch tests.
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `cargo test -p oceanfs-node --lib -- --test-threads=1` pass
      (RocksDB caveat, PIPELINE.md §4.6).
- [ ] **Docs:** no pub item loses its `# Examples`; `#![deny(missing_docs)]`
      passes.
- [ ] **ADR:** ADR-0033 D1/D2 satisfied (holder-set entry point, no
      exchange with non-holders, local-only excluded); ADR-0015 sampling/
      continuous + cost model preserved.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and
> `ignore`-tagged doc examples are non-blocking (see
> `guidelines/coding.md` §9.2).
