//! S3-compatible HTTP handlers.
//!
//! Implements the standard S3 REST API operations (PUT, GET, HEAD,
//! DELETE, LIST) with S3-compatible XML responses and error formatting.
//!
//! ## Architecture
//!
//! The handler delegates to:
//! - [`WriteCoordinator`] for PUT (blob writes with quorum)
//! - [`ReadCoordinator`] for GET and HEAD (blob reads + metadata)
//! - [`MetadataOps`] for DELETE and LIST (tombstone + prefix scan)
//! - [`BucketConfigStore`] for bucket CRUD
//!
//! Per performance guideline §4.2 (HTTP/2), §4.3 (TCP_NODELAY),
//! and §13.2 (`anyhow` only at application boundary — we use
//! concrete [`Error`] types).

use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use bytes::Bytes;
use oceanfs_core::{BucketId, HashKey, ObjectKey};
use oceanfs_routing::hash_key;
use tracing::{debug, error, info};

use crate::{
    bucket_config::BucketConfigStore,
    error::Error,
    metadata_ops::MetadataOps,
    read_coordinator::{ReadCoordinator, ReadRequest},
    s3_xml,
    write_coordinator::{WriteCoordinator, WriteRequest},
};

// ---------------------------------------------------------------------------
// Helper: safe HeaderValue construction
// ---------------------------------------------------------------------------

/// Builds a [`HeaderValue`] from a string, falling back to an
/// empty static value on failure.
fn header_val(s: &str) -> HeaderValue {
    HeaderValue::from_str(s).unwrap_or(HeaderValue::from_static(""))
}

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

/// Shared application state for all S3 HTTP handlers.
///
/// Each field is wrapped in `Arc` so it can be shared across
/// concurrent connections without synchronization overhead.
#[derive(Clone)]
pub(crate) struct AppState {
    /// Write coordinator for PUT operations.
    pub write: Arc<WriteCoordinator>,
    /// Read coordinator for GET/HEAD operations.
    pub read: Arc<ReadCoordinator>,
    /// Metadata store for DELETE/LIST operations.
    pub metadata: Arc<dyn MetadataOps>,
    /// Bucket configuration store for bucket CRUD.
    pub buckets: Arc<BucketConfigStore>,
    /// MIME type map by file extension.
    pub mime_types: Arc<MimeMap>,
}

// ---------------------------------------------------------------------------
// S3Handler
// ---------------------------------------------------------------------------

/// S3 API HTTP handler.
///
/// Wraps an axum `Router` that serves the S3-compatible REST API.
/// Construct with [`S3Handler::new`] and mount with
/// [`S3Handler::into_router`].
///
/// # Examples
///
/// ```ignore
/// # use std::sync::Arc;
/// # use oceanfs_server::{S3Handler, WriteCoordinator, ReadCoordinator};
/// # use oceanfs_server::metadata_ops::MetadataOps;
/// # use oceanfs_server::bucket_config::BucketConfigStore;
/// # async fn example(
/// #     write: Arc<WriteCoordinator>,
/// #     read: Arc<ReadCoordinator>,
/// #     metadata: Arc<dyn MetadataOps>,
/// #     buckets: Arc<BucketConfigStore>,
/// # ) {
/// let handler = S3Handler::new(write, read, metadata, buckets);
/// let router = handler.into_router();
/// // Mount in an axum server: axum::serve(listener, router).await
/// # }
/// ```
pub struct S3Handler {
    state: AppState,
}

impl S3Handler {
    /// Creates a new S3 handler with the given dependencies.
    ///
    /// All dependencies are injected via `Arc` for testability and
    /// to support the composition-root pattern in `oceanfs-node`.
    pub fn new(
        write: Arc<WriteCoordinator>,
        read: Arc<ReadCoordinator>,
        metadata: Arc<dyn MetadataOps>,
        buckets: Arc<BucketConfigStore>,
    ) -> Self {
        let state =
            AppState { write, read, metadata, buckets, mime_types: Arc::new(MimeMap::default()) };
        Self { state }
    }

    /// Consumes the handler and returns an axum `Router`.
    ///
    /// The returned router can be mounted in an axum `Server` via
    /// `axum::serve(listener, router)`.
    pub fn into_router(self) -> Router {
        let state = self.state;

        Router::new()
            // Object operations: /{bucket}/{*key}
            .route(
                "/{bucket}/{*key}",
                axum::routing::put(put_object)
                    .get(get_object)
                    .head(head_object)
                    .delete(delete_object),
            )
            // Bucket operations: /{bucket}
            .route(
                "/{bucket}",
                axum::routing::put(create_bucket).get(list_objects).delete(delete_bucket),
            )
            .with_state(state)
    }
}

// ---------------------------------------------------------------------------
// Object handlers
// ---------------------------------------------------------------------------

/// PUT /{bucket}/{key} — create or overwrite an object.
///
/// Delegates to [`WriteCoordinator::put`] for blob storage with
/// quorum replication. Returns `200 OK` with the `ETag` header.
///
/// # Errors
///
/// Returns S3-compatible XML error responses for all failure modes.
async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let bucket_id = BucketId::new(&bucket);
    let object_key = ObjectKey::new(&key);

    let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

    let req = WriteRequest {
        bucket: bucket_id,
        key: object_key,
        hash_key: hk,
        data: body,
        write_quorum: 2,
        ack_after_wal: true,
        ec_async: false,
        policy: None,
    };

    match state.write.put(req).await {
        Ok(result) => {
            let etag = result.blake3_hash.map(|h| h.to_hex()).unwrap_or_default();

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
/// # Errors
///
/// Returns `404` with S3 XML if the object does not exist.
async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let bucket_id = BucketId::new(&bucket);
    let object_key = ObjectKey::new(&key);

    let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

    let req = ReadRequest {
        bucket: bucket_id,
        key: object_key,
        hash_key: hk,
        metadata_only: false,
        policy: None,
    };

    match state.read.get(req).await {
        Ok(result) => {
            let etag =
                result.metadata.blake3_hash.as_ref().map(|h| h.to_hex()).unwrap_or_else(|| {
                    let hash = blake3::hash(&result.data);
                    oceanfs_core::HashOutput::from_bytes(*hash.as_bytes()).to_hex()
                });
            let content_type = infer_content_type(&state.mime_types, &key);

            info!(key = %key, size = result.data.len(), "GET object success");

            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, header_val(&content_type));
            headers.insert(header::ETAG, header_val(&etag));
            headers.insert(header::CONTENT_LENGTH, header_val(&result.data.len().to_string()));

            (StatusCode::OK, headers, Body::from(result.data)).into_response()
        }
        Err(e) => s3_error_response(&e, &bucket, &key),
    }
}

/// HEAD /{bucket}/{key} — retrieve object metadata only.
///
/// Delegates to [`ReadCoordinator::get`] with `metadata_only = true`.
/// Returns headers without a response body.
///
/// # Errors
///
/// Returns `404` if the object does not exist.
async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let bucket_id = BucketId::new(&bucket);
    let object_key = ObjectKey::new(&key);

    let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

    let req = ReadRequest {
        bucket: bucket_id,
        key: object_key,
        hash_key: hk,
        metadata_only: true,
        policy: None,
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
/// Writes a tombstone via [`MetadataOps::delete_object`].
/// Returns `204 No Content` on success.
///
/// # Errors
///
/// Returns `404` if the object does not exist.
async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let bucket_id = BucketId::new(&bucket);
    let object_key = ObjectKey::new(&key);

    match state.metadata.delete_object(&bucket_id, &object_key) {
        Ok(()) => {
            info!(key = %key, "DELETE object success");
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
async fn create_bucket(State(state): State<AppState>, Path(bucket): Path<String>) -> Response {
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

/// GET /{bucket}?list-type=2&prefix=... — list objects in a bucket.
///
/// Supports `prefix` query parameter. Returns an S3-compatible
/// `ListBucketResult` XML response.
///
/// # Errors
///
/// Returns `404` if the bucket does not exist.
async fn list_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
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
async fn delete_bucket(State(state): State<AppState>, Path(bucket): Path<String>) -> Response {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Builds an S3-compatible XML error response from a server error.
fn s3_error_response(err: &Error, bucket: &str, key: &str) -> Response {
    let resource = if key.is_empty() { bucket.to_string() } else { format!("{}/{}", bucket, key) };

    let status = err.s3_status();
    let code = err.s3_code();
    let message = err.s3_message();
    let request_id = uuid_for_error();
    let body = s3_xml::s3_error_xml(code, &message, &resource, &request_id);

    error!(
        status = status.as_u16(),
        code = code,
        message = %message,
        "S3 error response"
    );

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, header_val("application/xml"));
    (status, headers, Body::from(body)).into_response()
}

/// Generates a short request ID for error XML bodies.
fn uuid_for_error() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("{:016x}", ts % 0xFFFFFFFFFFFFFFFFu128)
}

/// Infers a MIME content type from the file extension.
fn infer_content_type(mime_map: &MimeMap, key: &str) -> String {
    mime_map.guess(key)
}

// ---------------------------------------------------------------------------
// MIME type map
// ---------------------------------------------------------------------------

/// A map from file extensions to MIME types.
///
/// Used to set the `Content-Type` header on GET and HEAD responses.
/// The default configuration covers common web and media types.
#[derive(Debug)]
pub struct MimeMap {
    /// Extension → MIME type mapping (extension without dot).
    map: HashMap<String, String>,
}

impl MimeMap {
    /// Creates a new `MimeMap` with default common MIME types.
    pub fn new() -> Self {
        let mut map = HashMap::new();
        // Text
        map.insert("html".into(), "text/html".into());
        map.insert("htm".into(), "text/html".into());
        map.insert("css".into(), "text/css".into());
        map.insert("js".into(), "application/javascript".into());
        map.insert("txt".into(), "text/plain".into());
        map.insert("csv".into(), "text/csv".into());
        map.insert("xml".into(), "application/xml".into());
        map.insert("json".into(), "application/json".into());
        // Images
        map.insert("jpg".into(), "image/jpeg".into());
        map.insert("jpeg".into(), "image/jpeg".into());
        map.insert("png".into(), "image/png".into());
        map.insert("gif".into(), "image/gif".into());
        map.insert("svg".into(), "image/svg+xml".into());
        map.insert("webp".into(), "image/webp".into());
        // Audio/Video
        map.insert("mp3".into(), "audio/mpeg".into());
        map.insert("mp4".into(), "video/mp4".into());
        map.insert("webm".into(), "video/webm".into());
        // Documents
        map.insert("pdf".into(), "application/pdf".into());
        map.insert("zip".into(), "application/zip".into());
        map.insert("gz".into(), "application/gzip".into());
        map.insert("tar".into(), "application/x-tar".into());
        // Binary
        map.insert("wasm".into(), "application/wasm".into());
        Self { map }
    }

    /// Guesses the MIME type from a key (file name or path).
    ///
    /// Returns `"application/octet-stream"` if the extension
    /// is not recognized.
    pub fn guess(&self, key: &str) -> String {
        if let Some(dot_pos) = key.rfind('.') {
            let ext = &key[(dot_pos + 1)..];
            self.map.get(ext).cloned().unwrap_or_else(|| "application/octet-stream".into())
        } else {
            "application/octet-stream".into()
        }
    }
}

impl Default for MimeMap {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{
        BucketId, GossipConfig, HlcClock, Incarnation, NodeId, NodeState, ObjectMetadata,
        RingConfig, RpcConfig,
    };
    use oceanfs_membership::Membership;
    use oceanfs_network::ConnectionPool;
    use oceanfs_routing::{Ring, RingCache};

    use super::*;
    use crate::{
        bucket_config::BucketConfigStore,
        metadata_ops::{MetadataError, MetadataOps},
        read_coordinator::ReadCoordinator,
        write_coordinator::WriteCoordinator,
    };

    // --- Mock MetadataOps ---

    struct MockMetadata {
        objects: parking_lot::RwLock<HashMap<(String, String), ObjectMetadata>>,
        tombstones: parking_lot::RwLock<HashMap<(String, String), bool>>,
    }

    impl MockMetadata {
        fn new() -> Self {
            Self {
                objects: parking_lot::RwLock::new(HashMap::new()),
                tombstones: parking_lot::RwLock::new(HashMap::new()),
            }
        }
    }

    impl MetadataOps for MockMetadata {
        fn get_object(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
        ) -> Result<Option<ObjectMetadata>, MetadataError> {
            Ok(self.objects.read().get(&(bucket.as_str().into(), key.as_str().into())).cloned())
        }

        fn delete_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<(), MetadataError> {
            // S3 DELETE is idempotent: always succeed, even if the
            // object doesn't exist (write a tombstone).
            let k = (bucket.as_str().into(), key.as_str().into());
            self.objects.write().remove(&k);
            self.tombstones.write().insert(k, true);
            Ok(())
        }

        fn list_objects(
            &self,
            _bucket: &BucketId,
            prefix: &str,
        ) -> Result<Vec<ObjectMetadata>, MetadataError> {
            let objs: Vec<_> = self
                .objects
                .read()
                .iter()
                .filter(|(_, v)| v.object_key.as_str().starts_with(prefix))
                .map(|(_, v)| v.clone())
                .collect();
            Ok(objs)
        }
    }

    // --- Test helpers ---

    fn make_app_state() -> AppState {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        ring.add_node(NodeId::new("n1"));
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            addr,
            GossipConfig::default(),
            ring_cache.clone(),
        ));
        membership.upsert_node(NodeId::new("n1"), NodeState::Alive, Incarnation::new(1), addr);
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let hlc_clock = Arc::new(HlcClock::new());

        let write = Arc::new(WriteCoordinator::new(
            ring_cache.clone(),
            membership.clone(),
            pool,
            NodeId::new("n1"),
            hlc_clock,
        ));
        let read = Arc::new(ReadCoordinator::new(ring_cache, NodeId::new("n1"), None));
        let metadata: Arc<dyn MetadataOps> = Arc::new(MockMetadata::new());
        let buckets = Arc::new(BucketConfigStore::new());

        AppState { write, read, metadata, buckets, mime_types: Arc::new(MimeMap::new()) }
    }

    // --- MIME Map tests ---

    #[test]
    fn mime_map_guess_known_extension() {
        let map = MimeMap::new();
        assert_eq!(map.guess("photo.jpg"), "image/jpeg");
        assert_eq!(map.guess("page.html"), "text/html");
        assert_eq!(map.guess("data.json"), "application/json");
    }

    #[test]
    fn mime_map_guess_unknown_returns_octet_stream() {
        let map = MimeMap::new();
        assert_eq!(map.guess("file.xyz"), "application/octet-stream");
    }

    #[test]
    fn mime_map_guess_no_extension_returns_octet_stream() {
        let map = MimeMap::new();
        assert_eq!(map.guess("README"), "application/octet-stream");
    }

    // --- Object handler tests ---

    #[tokio::test]
    async fn put_object_returns_200() {
        let state = make_app_state();
        state.buckets.put("test-bucket".into(), crate::bucket_config::BucketPolicy::default());

        let response = put_object(
            State(state),
            Path(("test-bucket".into(), "file.txt".into())),
            Bytes::from_static(b"hello world"),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn put_object_sets_etag_header() {
        let state = make_app_state();
        state.buckets.put("test-bucket".into(), crate::bucket_config::BucketPolicy::default());

        let response = put_object(
            State(state),
            Path(("test-bucket".into(), "file.txt".into())),
            Bytes::from_static(b"test data"),
        )
        .await;

        let headers = response.headers();
        assert!(headers.contains_key(header::ETAG), "ETag header must be set");
    }

    #[tokio::test]
    async fn get_object_returns_data() {
        let state = make_app_state();
        state.buckets.put("test-bucket".into(), crate::bucket_config::BucketPolicy::default());

        let response =
            get_object(State(state), Path(("test-bucket".into(), "any.txt".into()))).await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn head_object_returns_ok() {
        let state = make_app_state();
        state.buckets.put("test-bucket".into(), crate::bucket_config::BucketPolicy::default());

        let response =
            head_object(State(state), Path(("test-bucket".into(), "meta.txt".into()))).await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn head_object_has_no_body() {
        let state = make_app_state();
        state.buckets.put("test-bucket".into(), crate::bucket_config::BucketPolicy::default());

        let response =
            head_object(State(state), Path(("test-bucket".into(), "small.txt".into()))).await;

        let headers = response.headers();
        assert!(headers.contains_key(header::CONTENT_LENGTH));
        assert!(headers.contains_key(header::ETAG));
    }

    #[tokio::test]
    async fn delete_object_returns_204() {
        let state = make_app_state();

        let response =
            delete_object(State(state), Path(("test-bucket".into(), "delete-me.txt".into()))).await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn delete_nonexistent_object_also_returns_204() {
        // S3 DELETE is idempotent — always returns 204.
        let state = make_app_state();

        let response =
            delete_object(State(state), Path(("test-bucket".into(), "never-existed.txt".into())))
                .await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    // --- Bucket handler tests ---

    #[tokio::test]
    async fn create_bucket_returns_200() {
        let state = make_app_state();
        let response = create_bucket(State(state), Path("photos".into())).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn delete_bucket_returns_no_content() {
        let state = make_app_state();
        let _ = create_bucket(State(state.clone()), Path("temp".into())).await;
        let response = delete_bucket(State(state), Path("temp".into())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn delete_nonexistent_bucket_returns_404() {
        let state = make_app_state();
        let response = delete_bucket(State(state), Path("ghost".into())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_objects_returns_xml() {
        let state = make_app_state();
        state.buckets.put("test-bucket".into(), crate::bucket_config::BucketPolicy::default());

        let response = list_objects(
            State(state),
            Path("test-bucket".into()),
            Query(HashMap::from([("prefix".into(), "".into())])),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(content_type.to_str().unwrap().contains("xml"));
    }
}
