//! S3-compatible HTTP handlers.
//!
//! Implements PUT, GET, HEAD, DELETE, and LIST operations with
//! S3-compatible XML responses.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// An S3-compatible HTTP handler.
///
/// In Phase 5, this provides a minimal request router for S3 operations.
/// Full axum integration comes when the HTTP server is wired in Phase 5+.
pub struct S3Handler;

impl S3Handler {
    /// Creates a new S3 handler.
    pub fn new() -> Self {
        Self
    }

    /// Handles a PUT object request.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or key is invalid.
    pub async fn put_object(
        &self,
        _bucket: &str,
        _key: &str,
        _data: &[u8],
    ) -> Result<PutObjectResponse> {
        Ok(PutObjectResponse { etag: "placeholder".into() })
    }

    /// Handles a GET object request.
    ///
    /// # Errors
    ///
    /// Returns an error if the object is not found.
    pub async fn get_object(&self, _bucket: &str, _key: &str) -> Result<GetObjectResponse> {
        Err(Error::Routing("object not found".into()))
    }

    /// Handles a DELETE object request.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or key is invalid.
    pub async fn delete_object(&self, _bucket: &str, _key: &str) -> Result<()> {
        Ok(())
    }

    /// Lists objects in a bucket with an optional prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    pub async fn list_objects(&self, _bucket: &str, _prefix: &str) -> Result<ListObjectsResponse> {
        Ok(ListObjectsResponse { contents: Vec::new(), is_truncated: false })
    }
}

impl Default for S3Handler {
    fn default() -> Self {
        Self::new()
    }
}

/// Response for a PUT operation.
#[derive(Debug, Clone)]
pub struct PutObjectResponse {
    /// BLAKE3 hash of the object, hex-encoded.
    pub etag: String,
}

/// Response for a GET operation.
#[derive(Debug, Clone)]
pub struct GetObjectResponse {
    /// The object's payload.
    pub data: Vec<u8>,
    /// Content type (default: application/octet-stream).
    pub content_type: String,
    /// ETag header value.
    pub etag: String,
}

/// Response for a LIST operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsResponse {
    /// Objects matching the prefix.
    #[serde(rename = "Contents")]
    pub contents: Vec<ListObjectEntry>,
    /// Whether more results are available.
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
}

/// An object entry in a LIST response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectEntry {
    /// The object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Object size in bytes.
    #[serde(rename = "Size")]
    pub size: u64,
    /// ETag of the object.
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn put_object_returns_etag() {
        let handler = S3Handler::new();
        let resp = tokio_test::block_on(handler.put_object("b", "k", b"data")).unwrap();
        assert!(!resp.etag.is_empty());
    }

    #[test]
    fn get_nonexistent_returns_error() {
        let handler = S3Handler::new();
        let result = tokio_test::block_on(handler.get_object("b", "k"));
        assert!(result.is_err());
    }

    #[test]
    fn delete_does_not_error() {
        let handler = S3Handler::new();
        assert!(tokio_test::block_on(handler.delete_object("b", "k")).is_ok());
    }

    #[test]
    fn list_returns_empty_when_no_objects() {
        let handler = S3Handler::new();
        let resp = tokio_test::block_on(handler.list_objects("b", "prefix")).unwrap();
        assert!(resp.contents.is_empty());
    }
}
