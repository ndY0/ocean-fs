---
feature: "S3-Compatible HTTP Handlers"
epic: "phase-5-s3-api"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: write-coordinator-quorum
    reason: PUT handler delegates to write coordinator
  - feature: read-coordinator-parallel
    reason: GET handler delegates to read coordinator
  - feature: rocksdb-metadata-store
    reason: HEAD, DELETE, and LIST use metadata store
adr: []
perf:
  - "4.2: HTTP/2 multiplexing for client API"
  - "4.3: TCP_NODELAY on all sockets"
  - "3.6: sendfile / splice for blob responses"
  - "13.2: anyhow / eyre only at application boundary"
created: 2026-07-30
updated: 2026-07-30
---

# S3-Compatible HTTP Handlers

## Summary

Implement the S3-compatible HTTP API in `oceanfs-server`. Expose the standard S3
operations (PUT, GET, HEAD, DELETE, bucket create/list/delete) via an HTTP/2
server. Responses follow S3 XML conventions for compatibility with existing S3
SDKs. This is the external-facing API that clients interact with.

## Scope

### In Scope
- HTTP/2 server (axum or hyper) listening on `listen_addr`
- `PUT /{bucket}/{key}`: create or overwrite object; delegates to `WriteCoordinator`
- `GET /{bucket}/{key}`: retrieve object; delegates to `ReadCoordinator`
- `HEAD /{bucket}/{key}`: object metadata without body; delegates to metadata store
- `DELETE /{bucket}/{key}`: soft-delete (tombstone); delegates to metadata store
- `PUT /{bucket}`: create bucket with default policy
- `GET /{bucket}?list-type=2`: list objects (prefix, delimiter, continuation-token)
- `DELETE /{bucket}`: delete empty bucket
- S3-compatible XML error responses for all status codes
- S3-compatible XML response for list operations
- Content-Type detection from file extension (configurable MIME map)
- `ETag` header (BLAKE3 hash, hex-encoded)
- `Content-Length` header
- Streaming response body for GET (zero-copy via `sendfile`/`splice`)
- `anyhow::Result` at HTTP handler boundary; concrete errors below
- Unit tests for all HTTP endpoints with mock coordinators

### Out of Scope
- S3 authentication (separate feature)
- Multi-part uploads (future work)
- Object versioning (future work)
- Range requests (future work)
- Bucket policy via POST (separate feature)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | New modules: `http/handlers.rs`, `http/s3_response.rs`, `http/s3_error.rs`, `http/xml.rs` |
| `oceanfs-server` | New facade export: `pub use http::S3Handler` |

## Interface (Public API)

- `pub struct S3Handler` — `pub fn new(write: Arc<WriteCoordinator>, read: Arc<ReadCoordinator>, metadata: Arc<dyn MetadataStore>) -> Self`, `pub fn into_router(self) -> axum::Router`
- `pub(crate) mod s3_response` — XML serialization for `ListBucketResult`, `ErrorResponse`
- `pub(crate) mod s3_error` — error-to-S3-status-code mapping (`NoSuchKey` → 404, `AccessDenied` → 403, etc.)

## Data Flow

```
PUT /photos/cat.jpg  (body: <JPEG bytes>)
  HTTP handler:
    ├─ Parse bucket, key from URI path
    ├─ Pre-compute HashKey
    ├─ Read body into Bytes (streaming)
    ├─ Delegate to WriteCoordinator::put(req)
    │    └─ (distributed write path from Phase 4)
    └─ Return 200 OK with ETag: <blake3_hex>

GET /photos/cat.jpg
  HTTP handler:
    ├─ Parse bucket, key
    ├─ Delegate to ReadCoordinator::get(req)
    │    └─ (distributed read path from Phase 4)
    └─ Stream response body (sendfile/splice if file-backed)
         Headers: Content-Type: image/jpeg, ETag, Content-Length

HEAD /photos/cat.jpg
  → Metadata lookup only → return headers without body

DELETE /photos/cat.jpg
  → MetadataStore::delete_object + put tombstone → 204 No Content

GET /photos?list-type=2&prefix=cat
  → MetadataStore::list_objects(bucket, prefix) → XML response
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-server`
- [ ] **Tests:** Unit tests: PUT → 200 + ETag, GET → 200 + body matches, HEAD → 200 + headers no body, DELETE → 204, GET deleted → 404, list with prefix, list pagination (continuation-token), error XML format, content-type detection
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-server`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes
- [ ] **ADR:** N/A
- [ ] **Perf:** Rule 4.2 (HTTP/2), 4.3 (TCP_NODELAY), 3.6 (sendfile for GET), 13.2 (anyhow at boundary only)
- [ ] **Integration:** `tests/s3_api.rs`: full S3 client (rusoto or aws-sdk) against local server: PUT, GET, HEAD, DELETE, list operations; verify ETag consistency, verify GET after PUT returns identical data
- [ ] **Manual:** `curl` examples in docs: PUT, GET, DELETE cycle works
