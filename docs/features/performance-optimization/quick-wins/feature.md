---
feature: "Quick Wins — Low Effort, High Impact Fixes"
epic: "performance-optimization"
status: proposed
priority: critical
owner: ""
dependencies: []
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "1.1 Bytes/BytesMut for blob data"
  - "1.2 Arena/buffer pool for segment append"
  - "1.3 Pre-size collections with known capacity"
  - "1.5 Zero-copy protobuf deserialization"
  - "3.4 Group commit for WAL fsync"
  - "6.4 Static dispatch over dynamic dispatch on hot paths"
  - "9.1 Accept borrowed data, never require ownership"
created: 2026-08-05
updated: 2026-08-05
---

# Quick Wins — Low Effort, High Impact Fixes

## Summary

Seven high-impact, low-effort performance fixes identified across all five
performance audits (write path, read path, network, storage I/O, and
acceleration). These fixes span `oceanfs-server`, `oceanfs-storage`,
`oceanfs-accel`, and `oceanfs-ec` crates. Each is an implementation detail
change — no architectural redesigns. Total estimated effort: ~7 hours.
All seven can ship before the gap-closure sprints, and collectively address
7 of the top 10 bottlenecks from the [perf synthesis](../../audits/2026-08-05-perf-synthesis.md#4-top-10-bottlenecks-by-estimated-impact).

## Scope

### In Scope

- **QW-1: Replace `.to_vec()` on `Bytes` with `.clone()` (refcount bump).**
  Identified in 5+ locations: `WriteCoordinator::forward_write()` (coordinator.rs:275),
  `replicate_to_single()` (replication.rs:126), S3 GET handler L1 cache-hit paths
  (handlers.rs:208,219,228,250), and gRPC `append_segment` handler stream accumulation
  (segment_service.rs:83-84). Each `.to_vec()` allocates a new `Vec<u8>` and copies the
  entire blob. For a 4 MB blob replicated to 2 remotes, that's 12 MB of unnecessary
  allocate+copy per PUT. Source: write-path C3, read-path C3, network C2/H3.

- **QW-2: Wire actual `sync_all()` into WAL group commit closure.**
  `WalSyncGroup::create_sync_group()` (writer.rs:220-229) passes a closure that
  returns `Ok(())` — a no-op. The group-commit infrastructure is architecturally
  correct (batches up to 64 waiters with 5ms timeout), but the fsync function
  never calls `sync_all()` or `sync_data()`. Source: write-path C1, storage-IO C1.

- **QW-3: Replace `MultiChunkAssembler` `Vec<u8>` + `Bytes::from()` with `BytesMut::freeze()`.**
  `MultiChunkAssembler` (assembly.rs:50,142) accumulates chunks in `Vec<u8>`
  then converts `Bytes::from(self.buffer)` — a double allocation that copies
  the entire blob twice. Using `BytesMut` with `extend_from_slice` + `freeze()`
  eliminates both the intermediate allocation and the final copy. Source: read-path C4.

- **QW-4: Wire `ReadTuningConfig` fields to actually control read behavior.**
  `ReadTuningConfig` fields `parallel_fetch`, `use_fastest_k`, and `stripe_parallelism`
  are parsed from bucket policy but discarded at `read_coordinator.rs:403` with
  `let _ = (parallel_fetch, use_fastest_k);`. Wire `parallel_fetch` to control
  serial vs parallel fetch, `use_fastest_k` to implement k-of-m early termination
  in `FuturesUnordered`, and `stripe_parallelism` to bound concurrency with a
  `tokio::sync::Semaphore`. Source: read-path C1.

- **QW-5: Replace `encode_to_vec()` with `encode(&mut BytesMut)` in protobuf serialization.**
  All RPC handlers use `encode_to_vec()` which allocates a fresh `Vec<u8>` per
  serialization. The `prost::Message::encode()` method accepts `&mut BufMut`
  including `BytesMut`, enabling zero-copy protobuf encoding. Guideline §1.5
  mandates this. Source: network audit §1.5 violation, synthesis §3.

- **QW-6: Replace `dyn Encoder`/`dyn Decoder` in `AccelDispatcher` with generic dispatch.**
  `AccelDispatcher` holds `Arc<dyn Encoder>` and `Arc<dyn Decoder>`, incurring
  vtable lookup + indirect call on every encode/decode (dispatch.rs:871-878).
  Replace with `<E: Encoder>` generic or an enum-based dispatch
  (`enum EncoderBackend { Cpu(CpuEncoder), Isal(IsalEncoder), Cuda(CudaBackend) }`)
  to enable monomorphization and cross-crate inlining. Source: accel C1.

- **QW-7: Change `Encoder::encode()` return type from `Vec<Vec<u8>>` to `Vec<Bytes>`.**
  The trait method returns owned, heap-allocated `Vec<Vec<u8>>` (traits.rs:27),
  producing ~62 allocations and ~8.3MB copy per 4MB segment encode (k=4, m=2,
  strip_size=64KB, 16 stripes). Switching to `Vec<Bytes>` or a `Bytes`-based
  container eliminates the per-stripe allocation avalanche. Source: accel C2.

### Out of Scope (for this feature)

- **Architectural wiring of write path** (gap-closure Epic 3: write-path-unification)
- **WAL crash recovery** (gap-closure Epic 4: correctness-gaps)
- **EC decode integration** (gap-closure Epic 4: correctness-gaps)
- **O(N²) gossip delta-only push** (gap-closure Epic 6: codebase-hygiene)
- **Other `.to_vec()` occurrences in non-hot paths** — handled by gap-closure Epic 6
- **Full `dyn Trait` purge across all crates** — handled by gap-closure Epic 6
- **Return type signature verification** — QW-7 must not break the `Encoder` trait's
  existing implementors: `CauchyEncoder`, `IsalEncoder`, `ArmEncoder`, `CudaBackend`

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | QW-1: Replace 4× `.to_vec()` in `handlers.rs` with `Body::from()`. QW-1: Replace `.to_vec()` in `coordinator.rs:275` and `replication.rs:126` with `Bytes::clone()`. QW-4: Wire `ReadTuningConfig` fields in `read/coordinator.rs`. QW-5: Replace `encode_to_vec()` in all gRPC handlers. |
| `oceanfs-storage` | QW-2: Wire `sync_all()` into `WalSyncGroup` closure in `wal/writer.rs`. QW-3: Replace `Vec<u8>` with `BytesMut` in `read/assembly.rs` (MultiChunkAssembler). QW-5: Replace `encode_to_vec()` in gRPC segment service. |
| `oceanfs-accel` | QW-6: Replace `dyn Encoder`/`dyn Decoder` in `dispatcher.rs` with generic/enum dispatch. |
| `oceanfs-ec` | QW-7: Change `Encoder::encode()` return type in `traits.rs` from `Vec<Vec<u8>>` to `Vec<Bytes>`. Update all implementors. |

## Interface (Public API)

- `pub trait Encoder` in `oceanfs-ec` — **changed return type** from `Vec<Vec<u8>>` to `Vec<Bytes>` for `encode()` method (QW-7)
- `WalSyncGroup` closure — internal change only, no public API change (QW-2)
- `ReadTuningConfig` — no API change; fields already exist, just need wiring (QW-4)
- `AccelDispatcher` — internal dispatch change; public API unchanged (QW-6)
- No new public types introduced by this feature

## Data Flow

QW-1 (`.to_vec()` → `.clone()`):
```
S3 GET handler L1 hit:
  cached_data: Bytes (from DashMap)
  ↓ .clone() (refcount bump, ~0ns) instead of .to_vec() (4MB allocation + copy)
  → Body::from(cached_data) → HTTP response (zero-copy)

WriteCoordinator::forward_write():
  req.data: Bytes
  ↓ Bytes::clone() instead of .to_vec()
  → SegmentAppendRequest { data: Bytes } → gRPC streaming

replicate_to_single():
  data: &[u8] → accept Bytes instead
  ↓ passed by reference
  → gRPC call
```

QW-2 (WAL fsync):
```
WalWriter::append() → WalSyncGroup::submit()
  ↓ wait on oneshot
  ↓ [flusher task] file.sync_all() ← WAS: Ok(()) no-op
  ↓ wake all waiters
```

QW-4 (ReadTuningConfig):
```
ReadCoordinator::assemble_chunks()
  config.parallel_fetch?  → FuturesUnordered (parallel) or sequential loop
  config.use_fastest_k?   → collect k results, drop slow m
  config.stripe_parallelism? → Semaphore::new(stripe_parallelism).acquire()
```

## Definition of Done

- [ ] **QW-1:** All `.to_vec()` on `Bytes` in hot paths replaced with `.clone()`, all `Bytes`-to-`Vec<u8>` cache-hit conversions replaced with `Body::from(Bytes)`. Verified by `cargo build --all-targets` and manual review of 6 identified sites.
- [ ] **QW-2:** WAL `fsync_fn` closure calls `file.sync_data()` or `file.sync_all()`. File handle shared between `WalWriter` and `WalSyncGroup` via `Arc<File>`. `cargo test` in `oceanfs-storage` passes; WAL tests exercise the sync path.
- [ ] **QW-3:** `MultiChunkAssembler::buffer` changed from `Vec<u8>` to `BytesMut`. `finalize()` calls `.freeze()` instead of `Bytes::from(buffer)`. Existing assembly tests pass with zero-copy semantics.
- [ ] **QW-4:** `parallel_fetch=false` triggers sequential chunk fetch. `use_fastest_k=true` triggers k-of-m early termination in `FuturesUnordered`. `stripe_parallelism > 0` creates a `Semaphore` bounding decode concurrency. Read coordinator tests verify all three modes.
- [ ] **QW-5:** All `encode_to_vec()` in gRPC handlers replaced with `encode(&mut BytesMut)`. Pre-sized `BytesMut` with `with_capacity()` per guideline §1.3. gRPC integration tests pass.
- [ ] **QW-6:** `AccelDispatcher` uses enum dispatch (`EncoderBackend`) or generic dispatch instead of `Arc<dyn Encoder>`. All 44 unit tests in `oceanfs-accel` pass. No vtable dispatch on encode/decode hot path.
- [ ] **QW-7:** `Encoder::encode()` returns `Vec<Bytes>`. All implementors updated: `CauchyEncoder`, `IsalEncoder`, `ArmEncoder`, `CudaBackend`. All 40+ tests in `oceanfs-ec` pass with `Bytes`-based output.
- [ ] **Code:** `cargo build --all-targets` succeeds across all affected crates
- [ ] **Tests:** All existing tests pass. New tests added for QW-2 (real fsync), QW-4 (config wiring), QW-6 (enum dispatch).
- [ ] **Docs:** `#![deny(missing_docs)]` passes on all affected crates. `Encoder::encode()` doc updated for new return type.
- [ ] **ADR:** Constraints from ADR-0006 (acceleration tier model) satisfied — QW-6 must not break the trait-based pluggability contract.
- [ ] **Perf:** Performance guidelines cited in frontmatter are followed.
- [ ] **Integration:** End-to-end S3 PUT/GET flow exercises QW-1, QW-3, QW-4 together. WAL integration test verifies QW-2 durability.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and `ignore`-tagged
> doc examples are non-blocking (see `guidelines/coding.md` §9.2.1).
