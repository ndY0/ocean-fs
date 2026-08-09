---
feature: "Fetch Shard Batching"
epic: "review-implementation-epic"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: gap-closure-addendum
    reason: Item 9 provides the initial group_by_node() utility and
      proto definition update; this feature completes the integration
      into both read and heal paths with full batching semantics
adr: []
created: 2026-08-09
updated: 2026-08-09
---

# Fetch Shard Batching

## Summary

The current fetch implementation issues one gRPC `FetchShard` RPC per shard,
even when multiple shards for the same read or heal operation reside on the
same remote node (review findings #26, #30). The transport already supports
batching — the `FetchShard` RPC uses server-side streaming (`returns (stream
ShardResponse)` in the proto), meaning multiple shards from one node can
flow back over a single gRPC connection. The gap is on the client side: shard
requests are not grouped by target node before issuing RPCs.

This feature implements a `group_by_node()` utility in `oceanfs-core` (shared
by both `oceanfs-server` and `oceanfs-durability`) that groups shard requests
by their owning node. The read path (`fetch.rs`) and heal path (`worker.rs`)
are refactored to use this utility: for each group, issue one batched gRPC
call with a repeated `shard_ids` field instead of one call per shard.

## Scope

### In Scope
- `group_by_node()` function in `oceanfs-core::shard::routing`
- `ShardRequest` struct (defined or reused from existing code)
- Refactor read path in `oceanfs-server/src/read/fetch.rs` to use batched fetching
- Refactor heal path in `oceanfs-durability/src/heal/worker.rs` to use batched fetching
- Proto update: ensure `FetchShardRequest` has a `repeated ShardId shard_ids` field
- Server-side handler: iterate over `request.shard_ids` and stream each shard sequentially

### Out of Scope (for this feature)
- Changing the gRPC streaming protocol semantics (already streaming)
- Batching across different segments (each FetchShardRequest is scoped to a single segment)
- Connection pooling changes (batched calls reuse existing pooled connections)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New module `shard/routing.rs`: `group_by_node()`, `ShardRequest` struct |
| `oceanfs-server` | Modify `read/fetch.rs`: replace per-shard loop with `group_by_node()` + batched-per-node loop |
| `oceanfs-durability` | Modify `heal/worker.rs`: replace per-shard fetch with `group_by_node()` + batched-per-node loop |
| `proto/shard.proto` | Update `FetchShardRequest`: add `repeated ShardId shard_ids = 1` (if not already present) |
| `oceanfs-server` | Modify `grpc/segment_service.rs`: handler iterates `request.shard_ids`, streams each |

## Interface (Public API)

- `pub struct ShardRequest` in `oceanfs_core::shard::routing`
  - `pub shard_index: u32`
  - `pub segment_id: SegmentId`
  - `pub shard_id: ShardId`

- `pub fn group_by_node(shards: &[ShardRequest], membership: &Membership) -> HashMap<NodeId, Vec<ShardRequest>>` in `oceanfs_core::shard::routing`
  - For each shard, calls `membership.lookup_shard_owner(segment_id, shard_index)` to determine owning node
  - Groups shards into per-node `Vec<ShardRequest>`
  - Silently drops shards whose owner is not found in membership (node is DEAD or unknown; those shards will be reconstructed via EC)

## Data Flow

```
Read path (oceanfs-server):
  assemble_chunks(segment_id, chunk_indices)
    ↓
  Build list of ShardRequest { shard_index, segment_id, shard_id }
    ↓
  group_by_node(shards, membership) → HashMap<NodeId, Vec<ShardRequest>>
    ↓
  For each (node_id, node_shards):
    ├→ FetchShardRequest { shard_ids: node_shards.iter().map(|s| s.shard_id).collect() }
    ├→ grpc_client.fetch_shards(request).await → streaming response
    └→ while let Some(ShardResponse) = stream.next():
         └→ collect shard_data into result vec
    ↓
  EC decode if fewer than k shards received

Heal path (oceanfs-durability):
  execute_heal(segment_id, missing_shards)
    ↓
  Build ShardRequest list for k surviving shards
    ↓
  group_by_node(k_shards, membership) → HashMap<NodeId, Vec<ShardRequest>>
    ↓
  For each (node_id, node_shards):
    ├→ FetchShardRequest { shard_ids: [...] }
    ├→ grpc_client.fetch_shards(request).await → streaming
    └→ collect shard_data
    ↓
  EC reconstruct missing shards
```

## Definition of Done

- [ ] **D6.1** In `crates/oceanfs-core/src/shard/routing.rs`, implement:
  ```rust
  use std::collections::HashMap;
  use oceanfs_membership::Membership;
  use oceanfs_routing::NodeId;

  /// A single shard to fetch from a remote node.
  #[derive(Debug, Clone)]
  pub struct ShardRequest {
      pub shard_index: u32,
      pub segment_id: SegmentId,
      pub shard_id: ShardId,
  }

  /// Group shard fetch requests by their owning node.
  ///
  /// Resolves each shard to its owning node via the membership view.
  /// Returns a map from NodeId to all shards that reside on that node.
  /// Shards whose owner is not in membership (DEAD/unknown) are silently dropped —
  /// the caller is expected to reconstruct them via EC from surviving shards.
  pub fn group_by_node(
      shards: &[ShardRequest],
      membership: &Membership,
  ) -> HashMap<NodeId, Vec<ShardRequest>> {
      let mut groups: HashMap<NodeId, Vec<ShardRequest>> = HashMap::new();
      for shard in shards {
          if let Some(node_id) = membership.lookup_shard_owner(&shard.segment_id, shard.shard_index) {
              groups.entry(node_id).or_default().push(shard.clone());
          }
      }
      groups
  }
  ```
  Re-export from `crates/oceanfs-core/src/shard/mod.rs` and `crates/oceanfs-core/src/lib.rs`.

- [ ] **D6.2** In `crates/oceanfs-core/src/shard/mod.rs`, create if it doesn't exist:
  ```rust
  pub mod routing;
  ```

- [ ] **D6.3** Verify the protobuf `FetchShardRequest` message supports batched shard IDs.
  Read `proto/shard.proto` (or `proto/segment.proto`):
  ```protobuf
  message FetchShardRequest {
    repeated ShardId shard_ids = 1;  // one or more shard IDs to fetch
    bytes segment_id = 2;
    // ... existing fields ...
  }
  ```
  If it currently uses a single `shard_id` field:
  - Change `ShardId shard_id = 1;` to `repeated ShardId shard_ids = 1;`
  - Regenerate proto stubs with `cargo build -p oceanfs-core` (or whichever crate hosts proto stubs)

- [ ] **D6.4** In `crates/oceanfs-server/src/grpc/segment_service.rs`, update the `fetch_shards` handler:
  ```rust
  async fn fetch_shards(
      &self,
      request: Request<FetchShardRequest>,
  ) -> Result<Response<Self::FetchShardsStream>, Status> {
      let req = request.into_inner();
      let (tx, rx) = tokio::sync::mpsc::channel(16);

      let segment_store = self.segment_store.clone();
      tokio::spawn(async move {
          for shard_id in req.shard_ids {
              match segment_store.read_shard(&req.segment_id, &shard_id).await {
                  Ok(data) => {
                      let response = ShardResponse {
                          shard_id: Some(shard_id),
                          shard_data: data,
                          ..Default::default()
                      };
                      if tx.send(Ok(response)).await.is_err() {
                          break; // client disconnected
                      }
                  }
                  Err(e) => {
                      let response = ShardResponse {
                          shard_id: Some(shard_id),
                          error: Some(e.to_string()),
                          ..Default::default()
                      };
                      let _ = tx.send(Ok(response)).await;
                  }
              }
          }
      });

      Ok(Response::new(ReceiverStream::new(rx)))
  }
  ```
  Note: handler iterates over all `shard_ids` in the request and streams each back.

- [ ] **D6.5** In `crates/oceanfs-server/src/read/fetch.rs`, locate the shard fetch loop (currently iterates over shards and issues one gRPC per shard). Replace with:
  ```rust
  use oceanfs_core::shard::routing::{group_by_node, ShardRequest};

  // Build shard requests
  let shard_requests: Vec<ShardRequest> = /* existing logic to build request list */;

  // Group by node
  let node_groups = group_by_node(&shard_requests, &self.membership);

  // One RPC per node
  for (node_id, node_shards) in node_groups {
      let shard_ids: Vec<ShardId> = node_shards.iter()
          .map(|s| s.shard_id.clone())
          .collect();
      let request = FetchShardRequest {
          segment_id: segment_id.to_bytes().into(),
          shard_ids,
          ..Default::default()
      };
      let client = self.grpc_pool.get_client(&node_id)?;
      let mut stream = client.fetch_shards(request).await?;
      while let Some(response) = stream.message().await? {
          if let Some(err) = response.error {
              tracing::warn!(%node_id, shard_id = ?response.shard_id, error = %err,
                  "Shard fetch error, will reconstruct from remaining k");
          } else {
              collected_shards.push(response.shard_data);
          }
      }
  }
  ```

- [ ] **D6.6** In `crates/oceanfs-durability/src/heal/worker.rs`, locate the heal fetch loop. Apply the same batching pattern:
  ```rust
  use oceanfs_core::shard::routing::group_by_node;

  let node_groups = group_by_node(&surviving_shard_requests, &self.membership);
  for (node_id, node_shards) in node_groups {
      let shard_ids: Vec<ShardId> = node_shards.iter()
          .map(|s| s.shard_id.clone())
          .collect();
      let request = FetchShardRequest {
          segment_id: segment_id.to_bytes().into(),
          shard_ids,
          ..Default::default()
      };
      let client = self.grpc_pool.get_client(&node_id)?;
      let mut stream = client.fetch_shards(request).await?;
      while let Some(response) = stream.message().await? {
          if response.error.is_none() {
              collected_shards.push(response.shard_data);
          }
      }
  }
  ```

- [ ] **D6.7** Verify after implementation:
  ```bash
  grep -rn "group_by_node" crates/oceanfs-server/src/read/fetch.rs
  # Expected: at least 1 match
  grep -rn "group_by_node" crates/oceanfs-durability/src/heal/worker.rs
  # Expected: at least 1 match
  ```

## Tests Required

- [ ] **T6.1** `test_group_by_node_clusters_shards_by_owner` — In `crates/oceanfs-core/src/shard/routing.rs` test module:
  - Create a `Membership` with 3 nodes: `n1`, `n2`, `n3`.
  - Configure `lookup_shard_owner` to return:
    - shard 0 → n1, shard 1 → n2, shard 2 → n1, shard 3 → n3, shard 4 → n2, shard 5 → n1
  - Create 6 `ShardRequest`s.
  - Call `group_by_node()`.
  - Assert returned `HashMap` has 3 keys.
  - Assert `groups[n1].len() == 3` (shards 0, 2, 5).
  - Assert `groups[n2].len() == 2` (shards 1, 4).
  - Assert `groups[n3].len() == 1` (shard 3).

- [ ] **T6.2** `test_group_by_node_handles_empty_input` — In same module:
  - Call `group_by_node(&[], &membership)`.
  - Assert returned `HashMap` is empty.

- [ ] **T6.3** `test_group_by_node_drops_unowned_shards` — In same module:
  - Create `Membership` where `lookup_shard_owner` returns `None` for shard 2.
  - Create 3 `ShardRequest`s: shard 0 → n1, shard 1 → n2, shard 2 → None.
  - Call `group_by_node()`.
  - Assert map has 2 keys (n1, n2).
  - Assert total shards across all groups == 2 (shard 2 dropped).

- [ ] **T6.4** `test_read_fetch_batches_per_node` — In `crates/oceanfs-node/tests/read_write_roundtrip.rs`:
  - Set up a 3-node cluster. Write data with k=4, m=2 so shards are distributed across nodes.
  - Instrument or mock the gRPC client to count `fetch_shards` calls.
  - Issue a read that requires shards from 2 of the 3 nodes.
  - Assert exactly 2 gRPC `fetch_shards` calls were made (not per-shard, which would be 4+).
  - Assert the read succeeds with correct data.

- [ ] **T6.5** `test_heal_fetch_batches_per_node` — In `crates/oceanfs-durability/tests/heal_batched_fetch.rs`:
  - Set up scenario: 4 surviving shards across 2 nodes (2 each), need k=3 for EC decode.
  - Instrument mock gRPC client to count calls.
  - Trigger heal for the segment.
  - Assert exactly 2 gRPC `fetch_shards` calls made (1 per node).
  - Assert heal succeeds (missing shard reconstructed).

- [ ] **T6.6** `test_fetch_shards_handler_streams_multiple_shards` — In `crates/oceanfs-server/tests/segment_service_test.rs`:
  - Set up `SegmentGrpcService` with a mock `SegmentStore` that returns data for 3 shard IDs.
  - Send a `FetchShardRequest` with `shard_ids = [s1, s2, s3]`.
  - Collect all streamed `ShardResponse` messages.
  - Assert 3 responses received.
  - Assert each has the correct `shard_id` and `shard_data`.

## ADR References

- No specific ADR for this feature. The batching principle is from review findings #26 and #30. The implementation follows the existing gRPC streaming semantics in spec §12.3.
