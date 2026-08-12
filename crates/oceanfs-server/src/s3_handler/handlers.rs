//! S3-compatible HTTP handler functions.
//!
//! Contains all the individual request handler functions for the
//! S3 REST API: PUT, GET, HEAD, DELETE on objects, and PUT, GET,
//! DELETE on buckets.

use std::collections::HashMap;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use oceanfs_core::{BucketId, HashKey, HashOutput, ObjectKey, ObjectMetadata};
use oceanfs_routing::hash_key;
use tracing::{debug, error, info, warn};

use super::{header_val, infer_content_type, s3_error_response, AppState};
use crate::{
    error::Error, read::coordinator::ReadRequest, s3_xml, write::coordinator::WriteRequest,
};

// ---------------------------------------------------------------------------
// Branch prediction hints (stable Rust)
// ---------------------------------------------------------------------------

/// Hint to the CPU branch predictor that `b` is very likely true.
///
/// Uses the `#[cold]` trick: the unlikely path is extracted into a
/// separate function marked `#[cold]` and `#[inline(never)]`. The
/// compiler optimizes the calling code as if the `cold` path is
/// never taken, placing the hot path in the fast code region.
#[inline(always)]
fn likely(b: bool) -> bool {
    if !b {
        cold_path();
    }
    b
}

/// Hint to the CPU branch predictor that `b` is very likely false.
#[inline(always)]
fn unlikely(b: bool) -> bool {
    if b {
        cold_path();
    }
    b
}

#[cold]
#[inline(never)]
fn cold_path() {}

// ---------------------------------------------------------------------------
// Object handlers
// ---------------------------------------------------------------------------

/// PUT /{bucket}/{key} — create or overwrite an object.
///
/// Delegates to [`WriteCoordinator::put`] for blob storage with
/// quorum replication. Returns `200 OK` with the `ETag` header.
///
/// Cache behaviour: invalidates L1 object cache and L2 metadata
/// cache for the written key to prevent stale reads.
///
/// # Errors
///
/// Returns S3-compatible XML error responses for all failure modes.
pub(crate) async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    state.s3_put_counter.inc();
    let bucket_id = BucketId::new(&bucket);
    let object_key = ObjectKey::new(&key);

    let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

    let policy = state.buckets.get(&bucket);
    let write_quorum = policy.as_ref().map(|p| p.consistency.write_quorum).unwrap_or(1);

    let req = WriteRequest {
        bucket: bucket_id.clone(),
        key: object_key.clone(),
        hash_key: hk,
        data: body.clone(),
        write_quorum,
        ack_after_wal: true,
        ec_async: false,
        policy,
    };

    match state.write.put(req).await {
        Ok(result) => {
            let etag = result.blake3_hash.map(|h| h.to_hex()).unwrap_or_default();

            // Segment data is persisted by the SegmentSealer (O_DIRECT
            // or buffered I/O) during the seal worker. The WAL provides
            // crash recovery for data between PUT completion and seal.
            // No synchronous disk write on the hot path.

            // Persist object metadata so reads can locate the data.
            // For inline-tier blobs (≤ threshold), the WriteCoordinator
            // has already stored the complete ObjectMetadata (with
            // inline_data populated) — skip the redundant write to avoid
            // overwriting it with inline_data: None.
            if !result.chunks.is_empty() {
                let meta = ObjectMetadata {
                    object_key: object_key.clone(),
                    size: result.size,
                    blake3_hash: result.blake3_hash,
                    chunks: result.chunks.clone(),
                    inline_data: None,
                    created_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64,
                    hlc: result.hlc,
                };
                if let Err(e) = state.metadata.put_object(&bucket_id, meta) {
                    error!(key = %key, error = %e, "failed to persist object metadata");
                    return s3_error_response(
                        &Error::Internal(format!("metadata write failed: {e}")),
                        &bucket,
                        &key,
                    );
                }
            }

            // Invalidate caches for this key — locally and on replicas.
            if let Some(ref l1) = state.object_cache {
                l1.invalidate(&bucket_id, &object_key);
            }
            if let Some(ref l2) = state.metadata_cache {
                l2.invalidate(&bucket_id, &object_key);
            }
            state.write.invalidate_cache_on_replicas(&bucket_id, &object_key, &hk).await;

            // Register segment metadata for each unique segment so
            // /admin/segments reflects created segments.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let size_config = oceanfs_core::SegmentSizeConfig::default();
            for chunk in &result.chunks {
                let tier = size_config.classify(result.size);
                let seg_meta = oceanfs_core::SegmentMetadata {
                    segment_id: chunk.segment_id,
                    ec_k: 1,
                    ec_m: 0,
                    size_tier: tier,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: Some(now_ms),
                };
                if let Err(e) = state.metadata.put_segment(seg_meta) {
                    error!(segment_id = %chunk.segment_id, error = %e, "failed to persist segment metadata");
                }
            }

            info!(key = %key, size = result.size, etag = %etag, "PUT object success");

            let mut headers = HeaderMap::new();
            headers.insert(header::ETAG, header_val(&etag));
            headers.insert(header::CONTENT_LENGTH, header_val("0"));

            (StatusCode::OK, headers).into_response()
        }
        Err(e) => s3_error_response(&e, &bucket, &key),
    }
}

/// GET /{bucket}/{key} — retrieve an object.
///
/// Delegates to [`ReadCoordinator::get`]. Returns the object body
/// with `Content-Type`, `ETag`, and `Content-Length` headers.
///
/// Cache lookup order (per DK-001):
/// 1. L1 Object Cache — hit → serve from memory
/// 2. L2 Metadata Cache — hit → serve inline or proceed to chunks
/// 3. L3 Negative Cache — "definitely absent" → 404
/// 4. ReadCoordinator → metadata lookup + chunk assembly
///
/// On success, populates L1 and L2 caches.
///
/// # Errors
///
/// Returns `404` with S3 XML if the object does not exist.
pub(crate) async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    state.s3_get_counter.inc();
    // When key is empty, this is a bucket listing request (GET /{bucket}).
    // Route the request to list_objects handler.
    if key.is_empty() {
        return list_objects(State(state), Path(bucket), Query(std::collections::HashMap::new()))
            .await;
    }

    let bucket_id = BucketId::new(&bucket);
    let object_key = ObjectKey::new(&key);

    let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

    // ---- L1 Object Cache ----
    if let Some(ref l1) = state.object_cache {
        if let Some(cached_data) = l1.get(&bucket_id, &object_key) {
            tracing::debug!(key = %key, "L1 cache hit");
            // Verify BLAKE3 hash against L2 metadata if available.
            if let Some(ref l2) = state.metadata_cache {
                if let Some(ref meta) = l2.get(&bucket_id, &object_key) {
                    if let Some(ref stored_hash) = meta.blake3_hash {
                        let computed = blake3::hash(&cached_data);
                        if *computed.as_bytes() != *stored_hash.as_bytes() {
                            error!(
                                key = %key,
                                "L1 cache BLAKE3 mismatch — evicting and falling through"
                            );
                            l1.invalidate(&bucket_id, &object_key);
                            // Fall through to ReadCoordinator below.
                        } else {
                            let content_type = infer_content_type(&state.mime_types, &key);
                            let mut headers = HeaderMap::new();
                            headers.insert(header::CONTENT_TYPE, header_val(&content_type));
                            headers.insert(
                                header::CONTENT_LENGTH,
                                header_val(&cached_data.len().to_string()),
                            );
                            return (StatusCode::OK, headers, Body::from(cached_data))
                                .into_response();
                        }
                    } else {
                        // No stored hash to verify — serve from cache.
                        let content_type = infer_content_type(&state.mime_types, &key);
                        let mut headers = HeaderMap::new();
                        headers.insert(header::CONTENT_TYPE, header_val(&content_type));
                        headers.insert(
                            header::CONTENT_LENGTH,
                            header_val(&cached_data.len().to_string()),
                        );
                        return (StatusCode::OK, headers, Body::from(cached_data)).into_response();
                    }
                }
            }
            // No L2 cache available — serve from L1 without verification.
            let content_type = infer_content_type(&state.mime_types, &key);
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, header_val(&content_type));
            headers.insert(header::CONTENT_LENGTH, header_val(&cached_data.len().to_string()));
            return (StatusCode::OK, headers, Body::from(cached_data)).into_response();
        }
    }

    // ---- L2 Metadata Cache ----
    let l2_cache_hit = state.metadata_cache.as_ref().and_then(|l2| l2.get(&bucket_id, &object_key));

    // L2 hit is the common case for hot metadata — >99% for frequently
    // accessed keys. The branch hint keeps the fast path in the predictor.
    if let Some(ref cached_meta) = l2_cache_hit {
        // L2-hit warm: suggest to CPU that we took the hot path
        let _ = likely(true);
        tracing::debug!(key = %key, "L2 metadata cache hit");

        // If the cached metadata has inline data, serve it directly.
        if let Some(ref inline) = cached_meta.inline_data {
            let content_type = infer_content_type(&state.mime_types, &key);
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, header_val(&content_type));
            headers.insert(header::CONTENT_LENGTH, header_val(&inline.len().to_string()));

            // L1 population: small blobs may be worth caching in L1.
            if let Some(ref l1) = state.object_cache {
                l1.put(bucket_id.clone(), object_key.clone(), inline.clone());
            }

            return (StatusCode::OK, headers, Body::from(inline.clone())).into_response();
        }
    }

    // ---- L3 Negative Cache ----
    // Most keys exist — a negative-cache hit is the unlikely path.
    // The branch hint tells the CPU to speculatively execute the
    // RocksDB fallback (the common case).
    if let Some(ref l3) = state.negative_cache {
        if unlikely(l3.contains(&bucket_id, &object_key)) {
            tracing::debug!(key = %key, "L3 negative cache hit — key definitely absent");
            return s3_error_response(
                &Error::NotFound(format!("{}/{}", bucket, key)),
                &bucket,
                &key,
            );
        }
    }

    // ---- ReadCoordinator ----
    let policy = state.buckets.get(&bucket);
    let req = ReadRequest {
        bucket: bucket_id.clone(),
        key: object_key.clone(),
        hash_key: hk,
        metadata_only: false,
        policy,
    };

    match state.read.get(req).await {
        Ok(result) => {
            let etag =
                result.metadata.blake3_hash.as_ref().map(|h| h.to_hex()).unwrap_or_else(|| {
                    let hash = blake3::hash(&result.data);
                    HashOutput::from_bytes(*hash.as_bytes()).to_hex()
                });
            let content_type = infer_content_type(&state.mime_types, &key);

            // Populate L1 cache on success.
            if let Some(ref l1) = state.object_cache {
                l1.put(bucket_id.clone(), object_key.clone(), result.data.clone());
            }
            // Populate L2 metadata cache on success.
            if let Some(ref l2) = state.metadata_cache {
                l2.put(bucket_id.clone(), object_key.clone(), result.metadata.clone());
            }

            // Enqueue prefetch hint for adjacent keys (fire-and-forget).
            if let Some(ref prefetch) = state.prefetch_engine {
                let bucket_clone = bucket_id.clone();
                let key_clone = object_key.clone();
                let prefetch_clone = prefetch.clone();
                tokio::spawn(async move {
                    // M8: Discover adjacent keys from the metadata store
                    // and prefetch them into L2 cache (best-effort).
                    prefetch_clone.discover_and_prefetch_adjacent(&bucket_clone, &key_clone);
                });
            }

            info!(key = %key, size = result.data.len(), "GET object success");

            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, header_val(&content_type));
            headers.insert(header::ETAG, header_val(&etag));
            headers.insert(header::CONTENT_LENGTH, header_val(&result.data.len().to_string()));

            // Response body: when data is file-backed (mmap or O_DIRECT),
            // wrap in SegmentFileBody for sendfile path via reverse proxy.
            // For true kernel-space sendfile(2), deploy nginx in front.
            let body = {
                #[cfg(feature = "sendfile")]
                {
                    match &result.segment_source {
                        Some(
                            oceanfs_storage::io::SegmentReadSource::MmapBacked { .. }
                            | oceanfs_storage::io::SegmentReadSource::DirectIo { .. },
                        ) => {
                            let file_body = oceanfs_storage::io::SegmentFileBody::new(
                                result.data.clone(),
                                0,
                                result.data.len() as u64,
                            );
                            Body::new(file_body)
                        }
                        _ => Body::from(result.data),
                    }
                }
                #[cfg(not(feature = "sendfile"))]
                {
                    let _ = &result.segment_source;
                    Body::from(result.data)
                }
            };

            (StatusCode::OK, headers, body).into_response()
        }
        Err(e) => s3_error_response(&e, &bucket, &key),
    }
}

/// HEAD /{bucket}/{key} — retrieve object metadata only.
///
/// Delegates to [`ReadCoordinator::get`] with `metadata_only = true`.
/// Returns headers without a response body.
///
/// Also checks L3 negative cache before querying the metadata store.
///
/// # Errors
///
/// Returns `404` if the object does not exist.
pub(crate) async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    state.s3_head_counter.inc();
    let bucket_id = BucketId::new(&bucket);
    let object_key = ObjectKey::new(&key);

    let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

    // ---- L3 Negative Cache ----
    // See get_object for explanation of the inverted Bloom filter check.
    if let Some(ref l3) = state.negative_cache {
        if l3.contains(&bucket_id, &object_key) {
            debug!(key = %key, "L3 negative cache hit — key definitely absent");
            return s3_error_response(
                &Error::NotFound(format!("{}/{}", bucket, key)),
                &bucket,
                &key,
            );
        }
    }

    let policy = state.buckets.get(&bucket);
    let req = ReadRequest {
        bucket: bucket_id,
        key: object_key,
        hash_key: hk,
        metadata_only: true,
        policy,
    };

    match state.read.get(req).await {
        Ok(result) => {
            let etag = result.metadata.blake3_hash.as_ref().map(|h| h.to_hex()).unwrap_or_default();
            let size = result.metadata.size;

            debug!(key = %key, size = size, "HEAD object success");

            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                header_val(&infer_content_type(&state.mime_types, &key)),
            );
            headers.insert(header::ETAG, header_val(&etag));
            headers.insert(header::CONTENT_LENGTH, header_val(&size.to_string()));
            headers.insert(header::ACCEPT_RANGES, header_val("bytes"));

            (StatusCode::OK, headers).into_response()
        }
        Err(e) => s3_error_response(&e, &bucket, &key),
    }
}

/// DELETE /{bucket}/{key} — soft-delete an object.
///
/// Writes a tombstone via [`MetadataOps::delete_object`], replicates the
/// deletion to the remote replicas, and returns `204 No Content` when the
/// confirmed deletions (local + remote) meet the bucket's `write_quorum`.
/// Returns `503 Service Unavailable` when the quorum is not met — a
/// DELETE is no longer reported as successful when replicas never saw
/// it (F3b).
///
/// Invalidates L1 and L2 caches for the deleted key.
/// Inserts the key into the L3 negative cache.
///
/// # Errors
///
/// Returns `404` if the object does not exist.
pub(crate) async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    state.s3_delete_counter.inc();
    let bucket_id = BucketId::new(&bucket);
    let object_key = ObjectKey::new(&key);

    match state.metadata.delete_object(&bucket_id, &object_key) {
        Ok(()) => {
            // Replicate deletion to other replicas in the ring. The local
            // tombstone counts as one confirmed deletion; `write.delete`
            // returns the number of remote confirmations.
            let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));
            let remote_deleted = match state.write.delete(&bucket_id, &object_key, &hk).await {
                Ok(count) => count,
                Err(e) => {
                    warn!(error = %e, key = %key, "delete replication failed");
                    return s3_error_response(&e, &bucket, &key);
                }
            };

            // Quorum check: local (1) + confirmed remote deletions. The
            // required quorum is capped at the replica count — a
            // single-node cluster cannot confirm more than one deletion
            // (mirrors the write path's capping).
            let write_quorum =
                state.buckets.get(&bucket).map(|p| p.consistency.write_quorum).unwrap_or(1);
            let required_quorum =
                (write_quorum as usize).min(state.write.replica_count(&hk)).max(1);
            let confirmed = 1 + remote_deleted;
            if confirmed < required_quorum {
                let err =
                    Error::QuorumNotMet { required: required_quorum as u8, received: confirmed };
                warn!(
                    key = %key,
                    required = required_quorum,
                    received = confirmed,
                    "DELETE quorum not met"
                );
                return s3_error_response(&err, &bucket, &key);
            }

            // Invalidate caches for this key — locally and on replicas.
            if let Some(ref l1) = state.object_cache {
                l1.invalidate(&bucket_id, &object_key);
            }
            if let Some(ref l2) = state.metadata_cache {
                l2.invalidate(&bucket_id, &object_key);
            }
            state.write.invalidate_cache_on_replicas(&bucket_id, &object_key, &hk).await;
            // Add to negative cache so subsequent HEAD/GET skip RocksDB.
            if let Some(ref l3) = state.negative_cache {
                l3.insert(&bucket_id, &object_key);
            }

            info!(
                key = %key,
                remote_deleted,
                "DELETE object success"
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            let srv_err = Error::Metadata(e);
            s3_error_response(&srv_err, &bucket, &key)
        }
    }
}

// ---------------------------------------------------------------------------
// Bucket handlers
// ---------------------------------------------------------------------------

/// PUT /{bucket} — create a new bucket with default policy.
///
/// # Errors
///
/// Returns `409` if the bucket already exists.
pub(crate) async fn create_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Response {
    if state.buckets.exists(&bucket) {
        let err = Error::BucketAlreadyExists(bucket.clone());
        return s3_error_response(&err, &bucket, "");
    }

    state.buckets.put(bucket.clone(), crate::bucket_config::BucketPolicy::default());
    info!(bucket = %bucket, "bucket created");

    let mut headers = HeaderMap::new();
    headers.insert(header::LOCATION, header_val(&format!("/{}", bucket)));
    (StatusCode::OK, headers).into_response()
}

/// POST /{bucket}?policy — sets or updates a bucket's policy.
///
/// Accepts a JSON body with the bucket policy configuration.
/// Returns `200` on success, `400` on invalid JSON, `404` if the
/// bucket does not exist.
pub(crate) async fn put_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: String,
) -> Response {
    // Only handle ?policy query parameter.
    if !params.contains_key("policy") {
        let err = Error::Internal("missing ?policy query parameter".into());
        return s3_error_response(&err, &bucket, "");
    }

    if !state.buckets.exists(&bucket) {
        let err = Error::NotFound(format!("bucket {bucket} not found"));
        return s3_error_response(&err, &bucket, "");
    }

    let policy: crate::bucket_config::BucketPolicy = match serde_json::from_str(&body) {
        Ok(p) => p,
        Err(e) => {
            let err = Error::Internal(format!("invalid bucket policy JSON: {e}"));
            return s3_error_response(&err, &bucket, "");
        }
    };

    if let Err(e) = policy.validate() {
        let err = Error::Internal(format!("invalid bucket policy: {e}"));
        return s3_error_response(&err, &bucket, "");
    }

    state.buckets.put(bucket.clone(), policy);
    info!(bucket = %bucket, "bucket policy updated");

    (StatusCode::OK).into_response()
}

/// GET /{bucket}?list-type=2&prefix=... — list objects in a bucket.
///
/// Supports `prefix` query parameter. Returns an S3-compatible
/// `ListBucketResult` XML response.
///
/// # Errors
///
/// Returns `404` if the bucket does not exist.
pub(crate) async fn list_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    state.s3_list_counter.inc();
    let bucket_id = BucketId::new(&bucket);
    let prefix = params.get("prefix").map(|s| s.as_str()).unwrap_or("");

    match state.metadata.list_objects(&bucket_id, prefix) {
        Ok(objects) => {
            let entries: Vec<(String, u64, String)> = objects
                .iter()
                .map(|m| {
                    let etag = m.blake3_hash.as_ref().map(|h| h.to_hex()).unwrap_or_default();
                    (m.object_key.as_str().to_string(), m.size, etag)
                })
                .collect();

            // Enqueue prefetch hints for subsequent keys (fire-and-forget).
            if let Some(ref prefetch) = state.prefetch_engine {
                let keys: Vec<ObjectKey> = objects.iter().map(|m| m.object_key.clone()).collect();
                let entry_count = entries.len();
                let bucket_clone = bucket_id.clone();
                let prefetch_clone = prefetch.clone();
                tokio::spawn(async move {
                    prefetch_clone.after_list(bucket_clone, &keys, entry_count);
                });
            }

            let xml = s3_xml::list_bucket_xml(&bucket, &entries, false, None, prefix);
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, header_val("application/xml"));
            (StatusCode::OK, headers, Body::from(xml)).into_response()
        }
        Err(e) => {
            let srv_err = Error::Metadata(e);
            s3_error_response(&srv_err, &bucket, "")
        }
    }
}

/// DELETE /{bucket} — delete an empty bucket.
///
/// # Errors
///
/// Returns `409` if the bucket is not empty.
/// Returns `404` if the bucket does not exist.
pub(crate) async fn delete_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Response {
    if !state.buckets.exists(&bucket) {
        let err = Error::NotFound(format!("bucket {bucket}"));
        return s3_error_response(&err, &bucket, "");
    }

    // Check if bucket is empty by listing with no prefix.
    let bucket_id = BucketId::new(&bucket);
    match state.metadata.list_objects(&bucket_id, "") {
        Ok(objects) if !objects.is_empty() => {
            let err = Error::BucketNotEmpty(bucket.clone());
            s3_error_response(&err, &bucket, "")
        }
        Ok(_) => {
            state.buckets.delete(&bucket);
            info!(bucket = %bucket, "bucket deleted");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            let srv_err = Error::Metadata(e);
            s3_error_response(&srv_err, &bucket, "")
        }
    }
}
