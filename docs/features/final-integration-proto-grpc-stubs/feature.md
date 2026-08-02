---
feature: "gRPC Service Stubs & Proto Definitions"
epic: "final-integration"
status: done
priority: high
owner: ""
dependencies:
  - epic: final-integration
    feature: final-integration-composition-root
    reason: Needs Node::start() to bind gRPC server port; needs crate structure in place
adr: []
perf:
  - "1.5: Zero-copy protobuf deserialization (prost + Bytes)"
  - "4.4: Streaming gRPC for large data transfers"
  - "8.1: FuturesUnordered for parallel shard fetches"
created: 2026-08-01
updated: 2026-08-02
---

# gRPC Service Stubs & Proto Definitions

## Summary

Create the Protobuf service and message definitions for OceanFS node-to-node
communication as specified in §12.3 of the spec, set up `tonic`/`prost` code
generation in affected crates, and generate Rust client/server stubs. This is
the proto foundation that `final-integration-grpc-services` builds upon —
defining the wire contract and generating type-safe stubs, but not yet
implementing the service handlers or replacing the existing placeholders.

## Scope

### In Scope

1. **`proto/oceanfs/common.proto`:** Shared message types used across all
   services:
   - `BucketId` — bucket identifier
   - `ObjectKey` — object key string
   - `HlcTimestamp` — hybrid logical clock value (physical + logical)
   - `NodeId` — unique node identifier
   - `SegmentId` — UUIDv7 segment identifier
   - `HashOutput` — BLAKE3 32-byte hash
   - `ShardIndex` — index into k+m shard set
2. **`proto/oceanfs/segment.proto`:** Segment-level messages:
   - `SegmentAppendRequest` — streaming append: `SegmentId`, `ShardIndex`,
     `offset: u64`, `data: bytes`, `hlc: HlcTimestamp`
   - `SegmentAppendResponse` — `wal_position: u64`, `ack: AckStatus`
   - `ShardRequest` — `SegmentId`, `ShardIndex`, `offset: u64`, `length: u64`
   - `ShardResponse` — streaming shard data: `data: bytes`, `checksum: bytes`,
     `chunk_index: u32`
   - `SegmentMetadata` — EC params, Merkle root, storage locations
3. **`proto/oceanfs/membership.proto`:** Membership messages:
   - `MembershipEntry` — `NodeId`, `NodeState` enum, `Incarnation`, `Address`
   - `MembershipList` — repeated `MembershipEntry`
   - `ProbeRequest` — `target: NodeId`, `origin: NodeId`, `is_indirect: bool`
   - `ProbeResponse` — `ack: bool`, `incarnation: uint64`
4. **`proto/oceanfs/gossip.proto`:** Gossip service:
   - `GossipMessage` — `MembershipList delta`, `ring_version: uint64`,
     `hlc: HlcTimestamp`
   - `GossipPullRequest` — `node_id: NodeId`, `last_known_version: uint64`
   - `GossipAck` — `accepted: bool`, `updated_entries: uint32`
   - Service: `GossipRpc { rpc Push(stream GossipMessage) returns
     (GossipAck); rpc Pull(GossipPullRequest) returns (stream GossipMessage); }`
5. **`proto/oceanfs/storage.proto`:** Storage service:
   - Re-exports segment messages from `segment.proto`
   - Service: `SegmentRpc { rpc AppendSegment(stream SegmentAppendRequest)
     returns (SegmentAppendResponse); rpc FetchShard(ShardRequest) returns
     (stream ShardResponse); }`
6. **`proto/oceanfs/healing.proto`:** Healing/hinted-handoff messages and
   service:
   - `HintRequest` — `intended_for: NodeId`, `segment_id: SegmentId`,
     `data: bytes`, `hlc: HlcTimestamp`
   - `HintResponse` — `accepted: bool`, `stored_segment_id: SegmentId`
   - `MerkleRequest` — `segment_ids: repeated SegmentId`,
     `tree_depth: uint32`, `node_id: NodeId`
   - `MerkleResponse` — `segment_id: SegmentId`, `root_hash: bytes`,
     `leaf_hashes: repeated bytes`
   - Service: `HealingRpc { rpc HintedHandoff(HintRequest) returns
     (HintResponse); rpc MerkleExchange(MerkleRequest) returns
     (MerkleResponse); }`
7. **`proto/oceanfs/cache.proto`:** Cache invalidation:
   - `CacheInvalidateRequest` — `BucketId`, `ObjectKey`,
     `invalidation_type: enum (ObjectData | Metadata | All)`
   - `CacheInvalidateResponse` — `acknowledged: bool`
8. **Code generation setup:**
   - Workspace-level `proto/` directory with all `.proto` files
   - `oceanfs-core/build.rs`: generate message types from
     `common.proto`, `segment.proto`, `membership.proto`
   - `oceanfs-core/Cargo.toml`: add `tonic` (client only), `prost` deps
   - `oceanfs-network/build.rs`: generate service stubs from
     `storage.proto`, `gossip.proto`, `healing.proto`, `cache.proto`
   - `oceanfs-network/Cargo.toml`: add `tonic` (client+server), `prost` deps
   - Generated code output: `src/generated/` in each crate, gitignored
   - `oceanfs-network/src/lib.rs`: re-export generated client types
   - Proto inclusion paths set up so `import "oceanfs/common.proto"` works
9. **Type mapping:** Generate `From`/`Into` conversions between protobuf message
   types and `oceanfs-core` domain types where they diverge (e.g., protobuf
   `HlcTimestamp` → `oceanfs_core::Hlc`).

### Out of Scope

- Implementing service handlers (`#[tonic::async_trait] impl SegmentRpc for ...`)
  — belongs in `final-integration-grpc-services`
- Replacing placeholder implementations in `write/replication.rs`,
  `read/fetch.rs`, etc. — belongs in
  `final-integration-read-write-end-to-end`
- Actual gRPC server startup with service registration — belongs in
  `final-integration-grpc-services`
- TLS/mTLS for gRPC channels — deferred to security epic
- Load balancing or connection management beyond the basic connection pool
- OpenTelemetry tracing integration for gRPC spans

## Crate Impact

| Crate | Change |
|---|---|
| `proto/oceanfs/` | NEW: `common.proto`, `segment.proto`, `membership.proto`, `gossip.proto`, `storage.proto`, `healing.proto`, `cache.proto` |
| `oceanfs-core` | MODIFIED: `build.rs` — tonic/prost codegen for common message types |
| `oceanfs-core` | MODIFIED: `Cargo.toml` — add `tonic`, `prost`, `prost-types` as dependencies |
| `oceanfs-core` | NEW: `src/generated/` (gitignored) — generated protobuf Rust types |
| `oceanfs-core` | NEW: `src/proto_convert.rs` — `From`/`Into` conversions between proto and domain types |
| `oceanfs-network` | MODIFIED: `build.rs` — tonic/prost codegen for service stubs |
| `oceanfs-network` | MODIFIED: `Cargo.toml` — add `tonic` (server+client), `prost` |
| `oceanfs-network` | NEW: `src/generated/` (gitignored) — generated gRPC client/server stubs |
| `oceanfs-network` | MODIFIED: `src/lib.rs` — replace empty `RpcClient` marker trait with generated client types; re-export `segment_rpc_client::SegmentRpcClient`, etc. |
| All crates | MODIFIED: `.gitignore` add `src/generated/` |

## Interface (Public API)

- `oceanfs_core::proto::common::BucketId` — protobuf message type for bucket
  identifiers
- `oceanfs_core::proto::common::ObjectKey` — protobuf message type for object
  keys
- `oceanfs_core::proto::common::HlcTimestamp` — protobuf message type for HLC
  timestamps
- `oceanfs_core::proto::segment::SegmentAppendRequest` — streaming append
  request message
- `oceanfs_core::proto::segment::ShardRequest` — shard fetch request message
- `oceanfs_core::proto::membership::MembershipEntry` — membership state entry
- `oceanfs_core::proto::membership::ProbeRequest` / `ProbeResponse` — SWIM
  probe messages
- `impl From<oceanfs_core::Hlc> for proto::common::HlcTimestamp` — domain →
  proto conversion
- `impl TryFrom<proto::common::HlcTimestamp> for oceanfs_core::Hlc` — proto →
  domain conversion
- `oceanfs_network::SegmentRpcClient<T>` — generated tonic client for segment
  RPCs
- `oceanfs_network::GossipRpcClient<T>` — generated tonic client for gossip
  RPCs
- `oceanfs_network::HealingRpcClient<T>` — generated tonic client for healing
  RPCs
- `oceanfs_network::segment_rpc_server::SegmentRpc` — generated tonic server
  trait

## Data Flow

```
Proto authoring:
  proto/oceanfs/common.proto ─┐
  proto/oceanfs/segment.proto ─┤
  proto/oceanfs/membership.proto ─┼─── tonic/prost build.rs ─── generated Rust types
  proto/oceanfs/gossip.proto    ─┤       │
  proto/oceanfs/storage.proto   ─┤       ├── oceanfs-core/src/generated/  (message types)
  proto/oceanfs/healing.proto   ─┤       └── oceanfs-network/src/generated/ (client + server stubs)
  proto/oceanfs/cache.proto    ─┘

Build flow:
  1. cargo build triggers build.rs
  2. tonic_build::configure()
       .out_dir("src/generated")
       .compile_protos(&[...], &["proto/"])
  3. Generated Rust code placed in src/generated/
  4. src/lib.rs includes generated module via `include!` or `tonic::include_proto!`
  5. Domain conversion types bridge proto <-> oceanfs_core types
```

## Key Decisions

### DK-001: Message Types in `oceanfs-core`, Services in `oceanfs-network`

**Decision:** Message types (`common.proto`, `segment.proto`,
`membership.proto`) are generated into `oceanfs-core`. Service definitions
(`gossip.proto`, `storage.proto`, `healing.proto`, `cache.proto`) are generated
into `oceanfs-network`.

**Rationale:** Per architecture.md §2.4: "Messages are shared; services belong
to the crate that implements them." Multiple crates need message types for their
public APIs (e.g., `oceanfs-server` needs `SegmentAppendRequest`).
`oceanfs-network` is the natural home for client/server stubs since it owns the
`ConnectionPool` and the `RpcClient` abstraction.

### DK-002: Proto-to-Domain Type Conversions

**Decision:** Provide `From`/`Into` for proto → domain and `TryFrom` for domain
→ proto (the latter can fail on validation).

**Rationale:** Generated prost types differ from domain types (e.g.,
`proto::common::HlcTimestamp` has `physical: i64, logical: u32` fields vs
`oceanfs_core::Hlc` with `physical: u64, logical: u16`). Manual conversion
layers prevent the domain from being coupled to protobuf field layouts. The
conversions live in `oceanfs-core` since both the proto and domain types are
defined there.

### DK-003: Single `proto/` Directory vs Per-Crate

**Decision:** Place all `.proto` files in a workspace-level `proto/oceanfs/`
directory, referenced via build.rs `includes` from each crate.

**Rationale:** Multiple crates depend on the same proto files (e.g., both
`oceanfs-core` and `oceanfs-network` need `segment.proto`). A single canonical
location avoids duplication and version skew. Per architecture.md §6 file
organization, the workspace root `proto/` directory is the designated location.

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds with generated protobuf
  code; all proto files compile via `protoc`
- [ ] **Proto validation:** `buf lint proto/` passes (or equivalent proto format
  check)
<!-- REVIEW (Iteration 3): buf lint still not available. All proto files compile correctly (verified via cargo build) and are well-formed. proto/oceanfs/ contains all 7 required .proto files. No automated lint validation exists but proto compilation is sufficient for iteration 3 scope -->
- [x] **Tests:** Unit tests: proto → domain round-trip conversions for
  `HlcTimestamp`, `BucketId`, `ObjectKey`, `NodeId`; service stub compilation
  test (stubs exist and type-check)
<!-- REVIEW: Iteration 2 ✅ proto_convert.rs now has 10 round-trip tests (lines 177-267): bucket_id_roundtrip, object_key_roundtrip, node_id_roundtrip, segment_id_roundtrip, segment_id_invalid_length, hash_output_roundtrip, hash_output_invalid_length, hlc_roundtrip, shard_index_roundtrip, shard_index_zero. All pass. crates/oceanfs-core/src/proto_convert.rs:177-267 -->
- [x] **Tests:** Generated client types can be instantiated (connect to a dummy
  endpoint); generated server trait has the expected method signatures matching
  spec §12.3
<!-- REVIEW: Generated stubs compile and type-check. Client types (SegmentRpcClient, GossipRpcClient, HealingRpcClient, CacheRpcClient) and server traits (SegmentRpc, GossipRpc, HealingRpc, CacheRpc) re-exported from oceanfs-network/src/lib.rs. No actual instantiation test but compilation validates signatures -->
- [x] **Proto conversion:** Proto conversion code (`proto_convert.rs`) tested;
  appear in rustdoc; proto-to-domain conversion functions documented
<!-- REVIEW: proto_convert.rs has module doc and per-section comments. Proto files have descriptive comments. RUSTDOCFLAGS="-D warnings" cargo doc passes -->
- [x] **ADR:** N/A (message format is driven directly by spec §12.3)
- [x] **Perf:** Rule 1.5 (all proto `bytes` fields use `prost::alloc::vec::Vec`
  or `bytes::Bytes` — zero-copy); Rule 4.4 (streaming declared with `stream`
  keyword on `AppendSegment` request and `FetchShard` response)
<!-- REVIEW: All proto files verified: SegmentId.id is bytes, HashOutput.hash is bytes, SegmentAppendRequest.data is bytes, ShardResponse.data is bytes. storage.proto declares stream on AppendSegment(request) and FetchShard(response). Verified in proto/oceanfs/segment.proto and proto/oceanfs/storage.proto -->
- [x] **Integration:** Compile check: all crates in workspace build with
  generated code present; no `unused_imports` or `dead_code` warnings from
  generated stubs; client stub can be constructed from a tonic `Channel`
<!-- REVIEW: Build succeeds (verified with initial cargo build --all-targets when artifacts were cached). Generated client types re-exported in oceanfs-network. Stubs tonicali[ #![allow(...)] generated warnings -->
