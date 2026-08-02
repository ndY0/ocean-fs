---
feature: "Read-Write Path End-to-End Integration"
epic: "final-integration"
status: in_progress
priority: critical
owner: ""
dependencies:
  - epic: final-integration
    feature: final-integration-composition-root
    reason: Needs Node struct with Wiring of all coordinators and caches
  - epic: final-integration
    feature: final-integration-grpc-services
    reason: Needs real gRPC AppendSegment and FetchShard for inter-node communication
  - epic: phase-4-distributed-read-write
    feature: read-coordinator-parallel
    reason: ReadCoordinator scaffolding exists but returns hardcoded data
  - epic: phase-4-distributed-read-write
    feature: write-coordinator-quorum
    reason: WriteCoordinator scaffolding exists but rejects non-local writes
  - epic: phase-6-caching-layer
    reason: L1/L2/L3 caches must be wired into the read path
adr:
  - 0001-segment-packing
perf:
  - "5.2: Streaming hash — never buffer the full blob"
  - "5.4: Batch verify for multi-chunk reads"
  - "8.1: FuturesUnordered for parallel shard fetches"
  - "8.2: tokio::select! with timeout branches"
  - "9.3: Pre-compute key hash once"
  - "1.1: Bytes/BytesMut for blob data"
created: 2026-08-01
updated: 2026-08-02
---

# Read-Write Path End-to-End Integration

## Summary

Replace every remaining placeholder and stub in the read and write coordinator
paths with real implementations. The `ReadCoordinator` currently returns
hardcoded `"[segment data]"` for all reads; the `WriteCoordinator` rejects
non-local writes; replication simulates writes; hinted handoff delivery is a
no-op; and `Router::try_forward()` validates but never forwards. Wire the L1/L2/L3
caches into the read path, connect the prefetch engine to LIST/GET operations,
and apply the auth middleware to S3 routes. After this feature, `PUT /{bucket}/{key}`
and `GET /{bucket}/{key}` work end-to-end across multiple nodes.

## Scope

### In Scope

1. **`ReadCoordinator` — real read path:**
   - Replace hardcoded `"[segment data]"` (line 154-197 of
     `read_coordinator.rs`) with:
     - Query `MetadataStore` (via `MetadataOps` adapter) for
       `ObjectMetadata`
     - If `inline_data` is present: extract and return directly (zero segment
       I/O)
     - If chunk references exist: for each chunk, fetch k of k+m shards in
       parallel via `FetchShard` gRPC calls (using `FuturesUnordered`)
     - EC decode if reading from parity shards (when some data shards fail or
       are slow)
     - Multi-chunk assembly: concatenate chunk data from multiple segment
       reads in order
     - Streaming BLAKE3 verification: feed chunk data through a single
       `blake3::Hasher` as it arrives; compare final hash against stored
       `blake3_hash` in `ObjectMetadata`
     - On hash mismatch: log `ERROR`, initiate segment healing, return 500 to
       client
     - Read repair: when `read_quorum > 1`, compare responses from multiple
       replicas (via HLC timestamps), serve the latest, and asynchronously push
       the corrected version to stale nodes
   - Configuration-driven behavior:
     - `read_parallel_fetch = true` → fetch all k+m shards simultaneously
     - `read_use_fastest_k = true` → return as soon as k shards arrive (via
       `FuturesUnordered` completion-order semantics)
     - `read_stripe_parallelism = N` → limit concurrent stripe decode to N
       tasks via a semaphore (perf §2.7)

2. **`WriteCoordinator` — real forwarding:**
   - Replace "non-local writes rejected" error with actual gRPC forwarding:
     - When the current node is not in the replica set for a key, call
       `Router::try_forward()` to send the write to the correct coordinator
       node
     - The forwarding target becomes the write coordinator; the forwarding node
       acts as a proxy returning the coordinator's `WriteResult` to the client

3. **`write/replication.rs` — real replication:**
   - Already addressed in `final-integration-grpc-services` (replaced with
     actual `AppendSegment` gRPC streaming calls)
   - Verify that the integration works: local append + remote replication →
     all W replicas have the data → quorum satisfied → client ack

4. **`hinted_handoff.rs` — real delivery:**
   - Already addressed in `final-integration-grpc-services` (replaced no-op
     with actual `HintedHandoff` gRPC calls)
   - Verify: when a returning node receives its handoff data, it writes the
     segment locally and the hint is cleared

5. **`router.rs` — real forwarding:**
   - Already addressed in `final-integration-grpc-services` (replaced
     validation-only with actual gRPC forwarding)
   - Verify: `Router::try_forward()` opens a streaming `AppendSegment` RPC to
     the target, streams the write data, returns the target's response

6. **Wire L1/L2/L3 caches into the read path:**
   - **L1 Object Cache:** Before any storage lookup, check the object cache
     for the requested key:
     - HIT → verify BLAKE3 hash against cached data (cheap, in-memory) → serve
       from memory
     - MISS → proceed to L2 metadata cache
     - On successful GET (from any path), populate L1 cache if blob size ≤
       `object_cache_max_blob_size`
   - **L2 Metadata Cache:** Before RocksDB metadata lookup, check the metadata
     cache:
     - HIT → extract `inline_data` or `chunk_list` from cached metadata →
       if inline, return immediately; if chunks, proceed to shard fetch
     - MISS → proceed to L3 negative cache
   - **L3 Negative Cache:** Before RocksDB metadata lookup, check the Bloom
     filter:
     - "definitely not present" → return 404 immediately (avoids RocksDB
       query)
     - "maybe present" → proceed to RocksDB metadata lookup
     - On DELETE, add key to the negative cache
   - Cache invalidation on write: PUT or DELETE on a key invalidates that key's
     entries in L1 and L2 caches (best-effort, node-local). Remote cache
     invalidation propagates through the `CacheInvalidate` gRPC (implemented
     in `final-integration-grpc-services`).

7. **Wire prefetch engine into LIST/GET:**
   - After `LIST` returns object keys: if `prefetch_enabled`, the prefetch
     engine warms metadata cache entries for the next `prefetch_after_list`
     objects in the list result
   - After `GET`: prefetch metadata for the next `prefetch_after_get` keys in
     the bucket's key ordering
   - Prefetch runs on a background task (spawned by the composition root in
     feature `final-integration-composition-root`); the read path enqueues
     prefetch hints into a bounded channel consumed by the engine

8. **Apply auth middleware to S3 routes:**
   - The auth middleware already exists in `oceanfs-server/src/auth.rs` but is
     not applied to any routes
   - Apply it as axum middleware on the S3 route group:
     - `PUT /{bucket}/{key}` → auth required
     - `GET /{bucket}/{key}` → auth required
     - `DELETE /{bucket}/{key}` → auth required
     - `HEAD /{bucket}/{key}` → auth required
     - `GET /{bucket}` (list) → auth required
     - `PUT /{bucket}` (create bucket) → auth required
   - Admin routes and health check remain unauthenticated
   - Auth is configurable: `auth_enabled = true/false` in `NodeConfig`; when
     disabled, the middleware is a no-op pass-through

9. **End-to-end flow integration test:**
   - Single-node: PUT blob → GET blob → hash matches, blob bytes equal
   - Single-node: PUT inline blob (≤4 KB) → GET → served from metadata cache
     (zero segment I/O)
   - Single-node: PUT small blob → GET → segment read + EC decode → hash
     matches
   - Multi-node: PUT on node 1 → GET on node 2 → hash matches (data replicated
     via gRPC)
   - Multi-node: PUT on node 1, kill node 1 → GET on node 2 → data available
     from replica
   - Cache behavior: GET twice → first MISS (populates cache), second HIT
     (served from L1/L2)
   - Negative cache: HEAD nonexistent key → 404 without RocksDB query
   - Prefetch: LIST → subsequent GETs for listed keys → metadata cache HIT

### Out of Scope

- Range requests (HTTP Range header) — spec §16 future work
- Object versioning — spec §16 future work
- Object locking & retention — spec §16 future work
- Multi-region routing — spec §16 future work
- S3 multipart upload (CreateMultipartUpload / UploadPart /
  CompleteMultipartUpload)
- Bucket lifecycle policies (auto-expire, auto-tier)
- Event notifications (S3-compatible event triggers)
- IAM-style multi-tenancy policies

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | MODIFIED: `src/read_coordinator.rs` — replace hardcoded data with real metadata lookup, shard fetch, EC decode, hash verify, read repair |
| `oceanfs-server` | MODIFIED: `src/write_coordinator.rs` — replace forwarding error with actual gRPC forwarding via Router |
| `oceanfs-server` | MODIFIED: `src/s3_handler.rs` — wire L1/L2/L3 cache lookups into GET/HEAD; wire cache population on successful GET; wire cache invalidation on PUT/DELETE; enqueue prefetch hints after LIST/GET; apply auth middleware |
| `oceanfs-server` | MODIFIED: `src/router.rs` — verify integration with gRPC forwarding (already implemented in gRPC services feature) |
| `oceanfs-server` | NEW: `src/read/assembly.rs` — multi-chunk assembler: concatenates chunk data from multiple segment reads, verifies combined BLAKE3 hash |
| `oceanfs-server` | NEW: `src/read/read_repair.rs` — read repair: compare multi-replica responses, serve latest, async push to stale nodes |
| `oceanfs-cache` | MODIFIED: expose cache types for wiring (already public; verify integration works) |
| `oceanfs-node` | MODIFIED: `src/node.rs` — verify cache wiring in Node::start() passes caches to S3Handler |

## Interface (Public API)

- `pub struct ReadCoordinator` — updated signature:
  - `pub fn new(ring: Arc<RingCache>, node_id: NodeId, metadata: Arc<dyn MetadataOps>, pool: Arc<ConnectionPool>, object_cache: Option<Arc<ObjectCache>>, metadata_cache: Option<Arc<MetadataCache>>, negative_cache: Option<Arc<NegativeCache>>, decoder: Arc<dyn Decoder>, conflict_resolver: Option<Arc<dyn ConflictResolver>>) -> Self`
  - `pub async fn get(&self, bucket: &BucketId, key: &ObjectKey, hash_key: &HashKey, policy: &BucketPolicy) -> Result<GetResult>`
- `pub struct GetResult` — `data: Bytes`, `metadata: ObjectMetadata`, `cache_hit: CacheHitLevel`, `hash: HashOutput`
- `pub enum CacheHitLevel` — `L1Object`, `L2MetadataInline`, `L2MetadataChunks`, `L3Negative`, `Miss`
- `pub struct MultiChunkAssembler` — accumulates chunk data in order, verifies BLAKE3
  - `pub fn new(expected_hash: HashOutput) -> Self`
  - `pub fn push_chunk(&mut self, index: usize, data: Bytes) -> Result<()>`
  - `pub fn finalize(self) -> Result<Bytes>`

## Data Flow

```
GET /{bucket}/{key} — full read path after integration:

S3Handler::get_object(bucket, key):
  1. Pre-compute HashKey from key (once)
  2. Auth middleware → validate credentials
  3. L1 Object Cache check:
     HIT → verify BLAKE3, serve from memory, <1ms latency
     MISS → continue
  4. L2 Metadata Cache check:
     HIT → extract metadata from cache
       ├── inline_data present → return data, populate L1
       └── chunk_list → jump to step 6
     MISS → continue
  5. L3 Negative Cache check:
     "definitely not present" → 404 Not Found, <0.1ms
     "maybe present" → continue
  6. Router → determine replica set from ring → select R nodes
  7. MetadataStore::get_object(bucket, key) via MetadataOps adapter:
     ├── Found (inline) → return data, populate L1+L2 caches, done
     └── Found (chunks) → proceed to step 8
  8. For each chunk in chunk_list:
     a. Fetch k of k+m shards in parallel via FuturesUnordered:
        ├── For each shard index: locate node from ring, FetchShard gRPC
        ├── Collect until k complete shards received (fastest k)
        └── Verify per-chunk checksums as data arrives
     b. EC decode if reading from parity shards (Decoder::decode)
     c. Push decoded chunk data into MultiChunkAssembler
  9. MultiChunkAssembler::finalize() → verify BLAKE3:
     ├── Match  → populate L1 cache, return 200 + data
     └── Mismatch → log ERROR, trigger segment healing, return 500
  10. (Async, if read_repair enabled) Read repair:
      Compare responses from R replicas; push latest to stale nodes
  11. Enqueue prefetch hint (if enabled): warm next N keys' metadata
```

## Key Decisions

### DK-001: Cache Lookup Order

**Decision:** L1 (object) → L2 (metadata) → L3 (negative) → RocksDB. This is
the order of increasing latency and decreasing specificity.

**Rationale:** L1 is fastest (~microsecond) but only covers exact blob payloads.
L2 covers all metadata (including inline blobs) but is larger and slower. L3
prevents unnecessary RocksDB lookups for non-existent keys. This ordering
minimizes average latency: hot objects served from L1, warm metadata from L2,
non-existent objects rejected at L3.

### DK-002: Read Repair Strategy

**Decision:** Read repair is triggered when `read_quorum > 1` and responses
disagree (stale HLC or checksum mismatch). Serve the latest version to the
client synchronously; push corrected data to stale replicas asynchronously in a
`tokio::spawn` task.

**Rationale:** The client should never wait for repair to complete — that adds
tail latency proportional to the slowest replica. Asynchronous repair ensures
the read path remains fast while eventually restoring consistency.

### DK-003: Auth Middleware Placement

**Decision:** Apply auth middleware as axum route-layer middleware on the S3
route group. The middleware extracts credentials from the request (AWS Signature
V4, configurable), validates them, and injects a `RequestContext` into request
extensions for downstream handlers.

**Rationale:** Middleware runs before the handler, so unauthenticated requests
never reach the S3 handlers. This prevents accidental unauthenticated access and
centralizes auth logic. When `auth_enabled = false`, the middleware layer is
replaced with a no-op pass-through.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds; `ReadCoordinator::get()`
  compiles with full real implementation
<!-- REVIEW (Iteration 3): ✅ cargo build --all-targets -p oceanfs-server passes. ✅ cargo build --all-targets -p oceanfs-node passes. ✅ ReadCoordinator::get() and get_object() compile and return real data. ✅ Metadata lookup via MetadataOps adapter works. ✅ Inline data served directly. ✅ Multi-chunk assembly with streaming BLAKE3 via MultiChunkAssembler. ✅ L1/L2/L3 cache checks in s3_handler GET handler (lines 292-344). ✅ Cache invalidation on PUT/DELETE. ✅ Cache population on successful GET. ✅ Auth middleware wired in node.rs:301-303 via AuthMiddleware::passthrough(). ✅ L2 metadata cache populated after GET (s3_handler.rs:373-375). ⚠️ Interface deviation persists: ReadCoordinator::new() takes 3 params (ring, node_id, conflict_resolver) not 9 as spec'd; get() takes ReadRequest struct not individual params. Declared BY DESIGN by implementer. ❌ No gRPC shard fetching — fetch.rs:30-88 is #[allow(dead_code)], returns placeholder zero-bytes. Not called from read path. ❌ No EC decode in read path — requires real shard fetch. ❌ No read repair — read/repair.rs:26-69 stubbed with #[allow(dead_code)], schedule_repair never called. ❌ WriteCoordinator forwarding returns ForwardFailed error (write_coordinator.rs:135-139) — requires gRPC services feature. ❌ Prefetch hints NOT enqueued from LIST/GET handlers — zero hits for "prefetch" in oceanfs-server/src/. PrefetchEngine exists in node.rs but s3_handler never calls it. -->
- [ ] **Tests:** Unit tests per component:
  - `ReadCoordinator::get()`: inline blob → returned directly, single-chunk
    blob → fetches k shards, multi-chunk blob → assembles correctly, hash
    mismatch → error, cache hit L1 → skips storage, cache hit L2 inline →
    skips segment fetch, negative cache hit → 404, missing blob → 404 from
    RocksDB
  - `MultiChunkAssembler`: correct order, wrong order → error, hash match,
    hash mismatch, empty chunks → ok
  - `WriteCoordinator::put()`: local write → succeeds, non-local write →
    forwarded via Router → succeeds
  - Cache wiring: PUT → invalidates L1/L2; GET MISS → populates L1/L2;
    DELETE → adds to L3 negative cache
  - Auth middleware: valid credentials → 200, invalid → 401, disabled → all
    pass through
<!-- REVIEW (Iteration 3): ✅ MultiChunkAssembler unit tests (7 tests, assembly.rs:156-252) all pass. ✅ ReadCoordinator chunk assembly tests (read_coordinator.rs:511-649): single chunk, multi-chunk, hash mismatch, missing segment. ✅ ReadCoordinator classify tests: inline, not_found, single_chunk, multi_chunk. ✅ ReadCoordinator metadata_only, default, GetResult cache hit tests. ✅ SegmentReader trait object safety test (read_coordinator.rs:768-774). ✅ WriteCoordinator local write tests: local write, hash generation, quorum, HLC advance, quorum capped. ✅ Auth middleware unit tests (auth/middleware.rs:118-131). ✅ Auth middleware integration tests (auth_middleware.rs: 5 tests). ⚠️ ReadCoordinator get_object() inline path tested indirectly via classify test but no dedicated test for get_object() with inline metadata. ❌ WriteCoordinator non-local write test expects ForwardFailed error — NOT actual forwarding success (write_coordinator.rs:294-323). ❌ No ReadCoordinator unit tests for L1/L2 cache hit, negative cache, or missing blob from RocksDB in co-located test module — these paths are only exercised in the s3_handler integration tests. ⚠️ make_app_state() sets all caches to None (s3_handler.rs:849-853); make_app_state_with_caches() (line 1098-1132) exists and is used by cache_l1_hit and cache_l3_negative tests. -->
- [ ] **Tests:** Integration tests:
  - `oceanfs-node/tests/read_write_roundtrip.rs`: PUT → GET → hash matches
    for inline, small, standard, and multi-segment blobs
  - `oceanfs-node/tests/cache_behavior.rs`: verify L1/L2/L3 hit/miss
    counters
  - `oceanfs-node/tests/read_repair.rs`: R=2, one replica stale → latest
    served, stale repaired
  - `oceanfs-node/tests/auth_middleware.rs`: auth enabled → 401 on missing
    credentials
<!-- REVIEW (Iteration 3): ✅ read_write_roundtrip.rs: 7 tests all pass — 1KB, 100KB, small, empty, multiple blobs, overwrite, 1MB all with hash verification. ✅ cache_behavior.rs: 9 tests all pass — L1 put/get/miss/invalidate/stats, L2 put/get/miss/invalidate, L3 insert/query/disabled. ✅ e2e_single_node.rs: 4 tests all pass — 1KB, 100KB, 1MB, hash verification. ✅ auth_middleware.rs: 5 tests all pass — passthrough, enabled/disabled, clone, layer type. ✅ read_repair.rs exists with 3 tests (LWW resolver: newer wall time, equal wall time + higher logical, tie-break). ⚠️ read_repair.rs tests only the ConflictResolver logic — does NOT test the actual read repair flow (comparing R replicas, async push to stale nodes) because the repair module is stubbed (read/repair.rs:26-69, #[allow(dead_code)]). ❌ No multi-node integration tests (PUT on node1 → GET on node2; kill node1 → GET from replica). ❌ No full HTTP handler path with wired L1→L2→L3 caches exercised through the axum router (tests use coordinator layer directly, not HTTP). ⚠️ Cache stats verification exists in cache_behavior.rs but e2e_single_node.rs does not verify cache stats reflect hits. ⚠️ No tests verify prefetch wiring (prefetch engine not wired into LIST/GET). -->
  `ReadCoordinator` and `S3Handler` paths covered
<!-- REVIEW (Iteration 3): ✅ clippy passes clean for both oceanfs-server and oceanfs-node (all-targets, -D warnings). ✅ No hardcoded `"[segment data]"` string remaining — confirmed with grep. ✅ Errors are descriptive (s3_error_response uses proper S3 XML). ✅ Cache cascade tests (make_app_state_with_caches) exist and work (s3_handler.rs:1098-1175). -->
- [ ] **Docs:** Every `pub` item has `# Examples`; `ReadCoordinator::get()`
  documented with the full cache → storage → EC decode → hash verify flow
<!-- REVIEW (Iteration 3): ✅ cargo doc --no-deps -p oceanfs-server passes with zero warnings (denied). ✅ MultiChunkAssembler has runnable doc-example (assembly.rs:27-42). ✅ SegmentReader trait has doc-example (read_coordinator.rs:124-146). ✅ Router has doc-example (router.rs:44-59) — ignore-annotated. ✅ HintedHandoff has doc-example (hinted_handoff.rs:52-59) — runnable. ✅ ReadCoordinator has module-level docs describing full read path (read_coordinator.rs:1-17). ✅ GetResult, CacheHitLevel, ReadRequest, ReadResult, ReadOutcome all have doc comments. ✅ Auth middleware module has module-level docs (auth/mod.rs:1-18). ⚠️ ReadCoordinator::get_object() (line 232) and ::get() (line 284) lack `# Examples` blocks. ⚠️ S3Handler doc-example is `ignore`-annotated (s3_handler.rs:93). ⚠️ Module docs describe EC decode and shard fetch flow that is not functional (dead code in fetch.rs). -->
- [ ] **ADR:** ADR-0001 (segment packing): inline storage path exercised; small
  blob reads go through packed segment path correctly
<!-- REVIEW (Iteration 3): ✅ Inline storage exercised — inline_data served directly (read_coordinator.rs:237-238). ✅ inline_threshold_bytes=4096 matches ADR-0001 via SegmentSizeConfig::default(). ✅ small_threshold_bytes=262144, default_target_size=4MB. ✅ BLAKE3 hash computed on write, verified on read via MultiChunkAssembler::finalize(). ✅ Small blob reads through packed segments work with InMemorySegmentReader in tests. ❌ No end-to-end packed segment path with real gRPC shard fetch — chunk reads via InMemorySegmentReader only; actual distributed packed segment reads are not functional. -->
- [ ] **Perf:** Rule 5.2 (streaming BLAKE3 — never buffer full blob before
  hashing), Rule 5.4 (single hasher for multi-chunk reads), Rule 8.1
  (FuturesUnordered for parallel shard fetch), Rule 8.2 (tokio::select! with
  timeout for shard fetch deadline), Rule 9.3 (HashKey pre-computed once in
  handler), Rule 1.1 (Bytes not Vec<u8> on read/write hot paths)
<!-- REVIEW (Iteration 3): ✅ 5.2: MultiChunkAssembler uses streaming blake3::Hasher with .update() per chunk (assembly.rs:108). ✅ 5.4: Single hasher for multi-chunk reads (assembly.rs:45). ✅ 9.3: HashKey pre-computed once per request in s3_handler.rs (line 221 for PUT, 289 for GET, 410 for HEAD). ✅ 1.1: Bytes used for blob data in all API signatures. Zero hits for std::sync::Mutex/RwLock in oceanfs-server/src/ (parking_lot used instead). ⚠️ 8.1: FuturesUnordered exists in fetch.rs:49-53 but is #[allow(dead_code)] — never called from the read path. ⚠️ 8.2: tokio::select! exists in fetch.rs:49-53 but is dead code — timeout handling not exercised in actual read path. ⚠️ 1.1: MultiChunkAssembler buffer uses Vec<u8> (assembly.rs:50), converts via Bytes::from at finalize() — acceptable tradeoff per prior reviews. ⚠️ 2.3: parking_lot::RwLock and parking_lot::Mutex used consistently (zero std::sync::Mutex/RwLock in server crate) ✅. -->
- [ ] **Integration:** Full end-to-end: `oceanfs-node/tests/e2e_single_node.rs`
  — PUT of 1 KB, 100 KB, 1 MB blobs; GET each; verify BLAKE3 hash; verify
  cache stats reflect hits
<!-- REVIEW (Iteration 3): ✅ e2e_single_node.rs exists (234 lines): 4 tests all pass — 1KB, 100KB, 1MB roundtrips with hash verification. ✅ 1MB blob test passes in both read_write_roundtrip.rs and e2e_single_node.rs. ⚠️ Tests exercise coordinator layer directly (not HTTP handler). ⚠️ No cache stats verification in e2e tests — cache stats assertion missing. ⚠️ Full HTTP handler with L1→L2→L3 caches not exercised — e2e tests use TestNode helper which doesn't wire caches. ❌ No multi-node tests exist (require gRPC). -->
  → `curl http://localhost:9000/test-bucket/test-key` → returns testfile with
  matching hash
<!-- REVIEW (Iteration 3): Not manually verified. Node starts successfully (node_lifecycle test passes: node starts, health check, shutdown in ~10s). S3Handler routes wired in node.rs:214-216 with HTTP listener on configurable port. gRPC server is bound but services are stubs. Single-node write path works (write_coordinator local). Read path works for inline data and chunk assembly via InMemorySegmentReader. Full manual verification of HTTP PUT→200→GET→hash_match would require a running node with RocksDB. -->
