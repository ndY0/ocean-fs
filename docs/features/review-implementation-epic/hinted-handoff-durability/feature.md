---
feature: "Hinted Handoff Durability"
epic: "review-implementation-epic"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure-addendum
    reason: Config pass-through and trait-object conversions (Item 6) must be
      complete before HintWal can consume MetadataStore trait and read its
      configuration from NodeConfig
adr:
  - 0009-storage-crate-split
  - 0005-trait-in-consuming-crate
created: 2026-08-09
updated: 2026-08-09
---

# Hinted Handoff Durability

## Summary

Currently, hinted handoffs are held in memory only (review finding #25). A node
crash before delivery loses all pending hints. This feature adds a persistent
`HintWal` — a write-ahead log for hinted handoff records that survives crashes.
The `HintWal` implements the `WalWriter` trait from `oceanfs-storage-api`
(ADR-0009 Part 2). Two record types are supported: `HintInline` for small
blobs stored directly in the WAL, and `HintSegmentRef` for large blobs that
reference a sealed segment. Delivery is batched: when a node returns (ALIVE
event), all pending hints for that node are drained and sent via a single
gRPC `HintedHandoff` RPC with a repeated field. On successful delivery, the
`HintWal` is truncated. The `HintWal` lives in `oceanfs-durability`; wiring
happens in `oceanfs-node`.

## Scope

### In Scope
- `HintWal` type in `oceanfs-durability` implementing `WalWriter` trait
- Two protobuf record types: `HintInline` and `HintSegmentRef`
- Entry serialization: length-prefixed protobuf with CRC32 footer
- Replay on startup: reconstruct in-memory hint queues from `HintWal`
- Batched delivery: group hints by `intended_for`, one gRPC call per node
- Truncation: delete `HintWal` file after successful delivery
- Node lifecycle: drain-and-deliver on ALIVE event from SWIM failure detector

### Out of Scope (for this feature)
- Hint querying or visibility via admin API (future)
- Hint TTL or expiry (all hints are delivered or lost on node replacement)
- Changing the gRPC `HintedHandoff` proto definition to a repeated field
  (that's a gap-closure proto change)
- Adaptive delivery timing (delivery happens on ALIVE event, always)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage-api` | Already has `WalWriter` trait; no changes needed |
| `oceanfs-durability` | New module `hinted_handoff/` with `hint_wal.rs`, `hint_record.rs`, `hint_delivery.rs` |
| `oceanfs-core` | New protobuf message types `HintRecord`, `HintInline`, `HintSegmentRef` (if not already present) |
| `oceanfs-node` | In `node.rs:start()`, construct `HintWal`, wire to `HintedHandoff` manager, register ALIVE event handler |

## Interface (Public API)

- `pub struct HintWal` — implements `oceanfs_storage_api::WalWriter`
  - `pub fn open(path: &std::path::Path) -> Result<Self>`
  - `pub fn write_hint(&self, record: HintRecord) -> Result<u64>` — append a hint record, return log position
  - `pub fn replay(&self) -> Result<Vec<(u64, HintRecord)>>` — replay all records, return (position, record) pairs
  - `pub fn truncate_after(&self, position: u64) -> Result<()>` — truncate WAL at position

- `pub enum HintRecord` (protobuf-backed)
  - `HintInline { intended_for: NodeId, bucket_id: BucketId, object_key: String, data: Bytes }`
  - `HintSegmentRef { intended_for: NodeId, bucket_id: BucketId, object_key: String, segment_id: SegmentId, offset: u64, length: u32 }`

- `pub struct HintedHandoffManager` — manages in-memory hint queues and delivery
  - `pub fn new(hint_wal: Arc<HintWal>, grpc_client: Arc<dyn NodeRpcClient>, config: HintedHandoffConfig) -> Self`
  - `pub async fn enqueue(&self, record: HintRecord) -> Result<()>` — write to WAL + add to in-memory queue
  - `pub async fn drain_and_deliver(&self, target: NodeId) -> Result<usize>` — drain all hints for target, send batched gRPC, truncate WAL on success
  - `pub fn pending_count(&self, target: NodeId) -> usize`

- `pub struct HintedHandoffConfig`
  - `pub wal_path: PathBuf`
  - `pub inline_threshold_bytes: u64` — blobs ≤ this use HintInline; larger use HintSegmentRef (default 4096)
  - `pub max_batch_size: usize` — max hints per batched gRPC call (default 256)

## Data Flow

```
PUT request → WriteCoordinator::put()
  ↓ (one successor unreachable)
WriteCoordinator::handoff_hint(record)
  ↓
HintedHandoffManager::enqueue(record)
  ├→ HintWal::write_hint(record)       [persist to WAL]
  └→ in-memory queue per intended_for   [fast lookup for delivery]

... node returns ...

SWIM FailureDetector emits ALIVE(node_id) event
  ↓
oceanfs-node event loop receives ALIVE
  ↓
HintedHandoffManager::drain_and_deliver(node_id)
  ├→ collect all pending hints for node_id
  ├→ group into HintedHandoffRequest { hints: repeated }
  ├→ gRPC call: client.hinted_handoff(request).await
  │    ↓ success
  ├→ HintWal::truncate_after(last_delivered_position)
  └→ return count of delivered hints

... node restart ...

oceanfs-node::start()
  ↓
HintWal::open(path)
  ↓
HintWal::replay() → Vec<(u64, HintRecord)>
  ↓
Rebuild in-memory queues from replayed records
  ↓
HintedHandoffManager ready → resume delivery on ALIVE events
```

## Definition of Done

- [ ] **D1.1** In `crates/oceanfs-durability/src/hinted_handoff/hint_record.rs`, define protobuf-backed types:
  ```rust
  // Protobuf message in proto/hinted_handoff.proto:
  // message HintRecord {
  //   oneof record {
  //     HintInline inline = 1;
  //     HintSegmentRef segment_ref = 2;
  //   }
  // }
  // message HintInline {
  //   bytes intended_for = 1;  // NodeId
  //   bytes bucket_id = 2;      // BucketId
  //   string object_key = 3;
  //   bytes data = 4;
  // }
  // message HintSegmentRef {
  //   bytes intended_for = 1;
  //   bytes bucket_id = 2;
  //   string object_key = 3;
  //   bytes segment_id = 4;     // SegmentId
  //   uint64 offset = 5;
  //   uint32 length = 6;
  // }
  ```
  Use `prost` to generate Rust types. Re-export from `hinted_handoff/mod.rs`.

- [ ] **D1.2** In `crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs`, implement `struct HintWal`:
  ```rust
  pub struct HintWal {
      file: Arc<Mutex<std::fs::File>>,
      path: PathBuf,
      write_offset: AtomicU64,
      config: HintWalConfig,
  }
  ```
  With methods:
  - `pub fn open(path: impl AsRef<Path>) -> Result<Self>` — opens or creates the WAL file with `OpenOptions::new().create(true).append(true).read(true)`.
  - `pub fn write_hint(&self, record: &HintRecord) -> Result<u64>` — encodes `record` as length-delimited protobuf: `[varint_len: u32][protobuf_bytes][crc32: u32]`. Writes to file, fsyncs, returns new `write_offset`.
  - `pub fn replay(&self) -> Result<Vec<(u64, HintRecord)>>` — reads entire file from offset 0, decodes each length-delimited protobuf record, verifies CRC32, returns `Vec<(byte_offset, HintRecord)>`.
  - `pub fn truncate_after(&self, position: u64) -> Result<()>` — calls `file.set_len(position)`, seeks to end.

- [ ] **D1.3** In `crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs`, implement `WalWriter` trait from `oceanfs-storage-api` for `HintWal`:
  ```rust
  impl oceanfs_storage_api::WalWriter for HintWal {
      fn write(&self, data: &[u8]) -> Result<u64>;
      fn sync(&self) -> Result<()>;
      fn truncate(&self, position: u64) -> Result<()>;
      fn replay(&self) -> Result<Vec<(u64, Vec<u8>)>>;
  }
  ```
  The `write()` method wraps `data` into a length-delimited + CRC32 frame identical to the `HintRecord` encoding.
  The `replay()` method returns raw `Vec<u8>` entries (generic WAL replay), while `HintWal::replay()` returns decoded `HintRecord` entries (type-specific).

- [ ] **D1.4** In `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs`, implement `struct HintedHandoffManager`:
  ```rust
  pub struct HintedHandoffManager {
      hint_wal: Arc<HintWal>,
      grpc_client: Arc<dyn NodeRpcClient>,
      queues: DashMap<NodeId, VecDeque<(u64, HintRecord)>>,
      config: HintedHandoffConfig,
  }
  ```
  - `pub fn new(...) -> Self` — constructor accepting `Arc<HintWal>`, `Arc<dyn NodeRpcClient>`, `HintedHandoffConfig`.
  - `pub async fn enqueue(&self, record: HintRecord) -> Result<()>` — locks WAL, writes record via `hint_wal.write_hint()`, computes `position`, pushes `(position, record)` into `queues[record.intended_for()]`.
  - `pub async fn drain_and_deliver(&self, target: NodeId) -> Result<usize>` — locks `queues[target]`, drains all `(position, record)` tuples, builds `HintedHandoffRequest { hints: repeated }`, calls `grpc_client.hinted_handoff(request)`. On success: calls `hint_wal.truncate_after(last_position)` where `last_position` is the max position among drained records. Returns count of delivered hints. On failure: re-enqueue records at front of queue, return error.
  - `pub fn pending_count(&self, target: NodeId) -> usize` — returns `queues[target].len()`.

- [ ] **D1.5** In `crates/oceanfs-core/src/config/node.rs`, add to `NodeConfig`:
  ```rust
  /// Path to hinted handoff WAL file. Default: "{data_dir}/hints.wal".
  #[serde(default)]
  pub hint_wal_path: Option<PathBuf>,
  /// Maximum blob size stored inline in hinted handoff WAL (bytes).
  /// Blobs above this threshold are stored as segment references.
  /// Default: 4096 (4 KB).
  #[serde(default = "default_hint_inline_threshold_bytes")]
  pub hint_inline_threshold_bytes: u64,
  /// Maximum hints per batched gRPC delivery call. Default: 256.
  #[serde(default = "default_hint_max_batch_size")]
  pub hint_max_batch_size: usize,
  ```
  Add `fn default_hint_inline_threshold_bytes() -> u64 { 4096 }` and `fn default_hint_max_batch_size() -> usize { 256 }`.

- [ ] **D1.6** In `crates/oceanfs-node/src/node.rs`, function `Node::start()`:
  - After constructing the gRPC client pool, construct `HintWal`:
    ```rust
    let hint_wal_path = config.hint_wal_path
        .unwrap_or_else(|| config.data_dir.join("hints.wal"));
    let hint_wal = Arc::new(HintWal::open(&hint_wal_path)?);
    ```
  - Replay existing hints:
    ```rust
    let replayed = hint_wal.replay()?;
    ```
  - Construct `HintedHandoffManager`:
    ```rust
    let hint_config = HintedHandoffConfig {
        wal_path: hint_wal_path,
        inline_threshold_bytes: config.hint_inline_threshold_bytes,
        max_batch_size: config.hint_max_batch_size,
    };
    let hinted_handoff = Arc::new(HintedHandoffManager::new(
        hint_wal,
        grpc_client.clone(),
        hint_config,
    ));
    ```
  - Rebuild in-memory queues from `replayed` records by calling `hinted_handoff.enqueue()` for each replay entry.
  - Register the ALIVE event handler: subscribe to `failure_detector.events()`, filter for `MembershipEvent::Alive(node_id)`, call `hinted_handoff.drain_and_deliver(node_id)`.
  - Pass `hinted_handoff` to `WriteCoordinator` so it can call `enqueue()` on write-path handoff.

- [ ] **D1.7** Wire the in-process `WriteCoordinator` handoff path: when a successor is unreachable during write, instead of enqueuing into an in-memory-only `Vec`, call `hinted_handoff.enqueue(record)`.

- [ ] **D1.8** Verify the gRPC `HintedHandoff` proto RPC accepts a repeated field:
  ```protobuf
  message HintedHandoffRequest {
    repeated HintRecord hints = 1;
  }
  ```
  If it currently uses single-record, update the proto and regenerate stubs. Place the proto at `proto/hinted_handoff.proto` if it does not exist.

## Tests Required

- [ ] **T1.1** `test_hint_wal_write_and_replay_roundtrip` — In `crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs` test module:
  - Create `HintWal::open(temp_path)`.
  - Write 5 `HintInline` records with distinct `intended_for`, `bucket_id`, `object_key`, and `data`.
  - Write 3 `HintSegmentRef` records with distinct `segment_id`, `offset`, `length`.
  - Close `HintWal` (drop).
  - Reopen `HintWal::open(temp_path)`.
  - Call `replay()`.
  - Assert 8 records returned.
  - Assert each recovered record has correct `intended_for`, `bucket_id`, `object_key` matching the original.
  - For `HintInline` records, assert `data` bytes match.
  - For `HintSegmentRef` records, assert `segment_id`, `offset`, `length` match.

- [ ] **T1.2** `test_hint_wal_truncate_after_delivery` — In same test module:
  - Write 10 records.
  - Call `replay()` and record the position of the 5th record.
  - Call `truncate_after(position_of_5th)`.
  - Call `replay()` again.
  - Assert only 5 records returned (positions 1–5).
  - Assert file size equals the byte offset of record 5 + its serialized length.

- [ ] **T1.3** `test_hint_wal_corrupt_record_crc_mismatch_skipped` — In same test module:
  - Write 3 valid records.
  - Manually corrupt the CRC32 of the 2nd record by writing garbage into the file at the CRC offset.
  - Call `replay()`.
  - Assert `replay()` returns an error OR returns only records 1 and 3 (skipping 2 with a WARN log). Design decision: return `Err` with a `HintWalError::CorruptRecord { position }` variant.

- [ ] **T1.4** `test_hint_wal_implements_wal_writer_trait` — In same test module:
  - Assert `HintWal: WalWriter` compiles.
  - Call `WalWriter::write(hint_wal.as_ref(), b"raw_bytes")`.
  - Call `WalWriter::replay(hint_wal.as_ref())`.
  - Assert one raw entry with value `b"raw_bytes"`.

- [ ] **T1.5** `test_hinted_handoff_batched_delivery` — In `crates/oceanfs-durability/tests/hinted_handoff_integration.rs`:
  - Create `HintedHandoffManager` with a mock gRPC client that records incoming `HintedHandoffRequest` values.
  - Enqueue 5 hints for `node_a` and 3 hints for `node_b`.
  - Call `drain_and_deliver(node_a)`.
  - Assert mock client received exactly 1 `HintedHandoffRequest` containing exactly 5 hints.
  - Assert `pending_count(node_a) == 0`.
  - Assert `pending_count(node_b) == 3` (unchanged).
  - Call `drain_and_deliver(node_b)`.
  - Assert mock client received 1 `HintedHandoffRequest` with exactly 3 hints.
  - Assert `pending_count(node_b) == 0`.

- [ ] **T1.6** `test_hinted_handoff_delivery_failure_reenqueues` — In same integration test:
  - Configure mock gRPC client to return `Err(...)` on the first call and `Ok(...)` on the second.
  - Enqueue 3 hints for `node_a`.
  - Call `drain_and_deliver(node_a)` — first attempt fails.
  - Assert `pending_count(node_a) == 3` (re-enqueued).
  - Call `drain_and_deliver(node_a)` — second attempt succeeds.
  - Assert `pending_count(node_a) == 0`.

- [ ] **T1.7** `test_hint_wal_survives_restart_and_delivers` — In `crates/oceanfs-node/tests/hinted_handoff_restart.rs`:
  - Create a 2-node cluster.
  - Take node_b offline.
  - PUT 10 objects to node_a (which hands off to node_b's hints since node_b is unreachable).
  - Kill node_a (SIGKILL).
  - Restart node_a.
  - Bring node_b back online.
  - Verify all 10 objects are readable from node_b (hints delivered after restart).

## ADR References

- [ADR-0009](../adr/0009-storage-crate-split.md) — Part 2 establishes `WalWriter` trait in `oceanfs-storage-api`; `HintWal` is the second implementation (alongside `SegmentWal`)
- [ADR-0005](../adr/0005-trait-in-consuming-crate.md) — `WalWriter` trait originally defined in consuming crate; after ADR-0009, the trait lives in `oceanfs-storage-api` for multi-consumer support
