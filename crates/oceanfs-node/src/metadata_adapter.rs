//! Adapter bridging `oceanfs_storage::MetadataStore` → `oceanfs_server::MetadataOps`.
//!
//! Lives in `oceanfs-node` (the composition root) because it depends on both
//! `oceanfs-storage` (concrete store) and `oceanfs-server` (trait).
//! Per architecture.md §2.1, traits live in the consuming crate; the
//! adapter lives in the only crate allowed to import both.

use std::sync::Arc;

use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata};
use oceanfs_server::metadata_ops::{MetadataError, MetadataOps};

/// Bridges `oceanfs_storage::MetadataStore` to `oceanfs_server::MetadataOps`.
///
/// Wraps the concrete RocksDB-backed metadata store and translates
/// storage errors to the server crate's error type via explicit
/// `.map_err()`.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use oceanfs_core::MetadataConfig;
/// use oceanfs_storage::MetadataStore;
/// use oceanfs_node::MetadataStoreAdapter;
///
/// let config = MetadataConfig::default();
/// let store = MetadataStore::open(&config).unwrap();
/// let adapter = MetadataStoreAdapter::new(Arc::new(store));
/// ```
pub struct MetadataStoreAdapter {
    store: Arc<oceanfs_storage::MetadataStore>,
}

impl MetadataStoreAdapter {
    /// Creates a new adapter wrapping the given concrete metadata store.
    pub fn new(store: Arc<oceanfs_storage::MetadataStore>) -> Self {
        Self { store }
    }
}

impl MetadataOps for MetadataStoreAdapter {
    fn get_object(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectMetadata>, MetadataError> {
        self.store.get_object(bucket, key).map_err(|e| MetadataError::Internal(format!("{e}")))
    }

    fn delete_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<(), MetadataError> {
        self.store.delete_object(bucket, key).map_err(|e| MetadataError::Internal(format!("{e}")))
    }

    fn list_objects(
        &self,
        bucket: &BucketId,
        prefix: &str,
    ) -> Result<Vec<ObjectMetadata>, MetadataError> {
        let results = self.store.list_objects(bucket, prefix);
        results
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Internal(format!("{e}")))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Test helper: creates a temporary MetadataStore.
    fn create_test_store() -> Arc<oceanfs_storage::MetadataStore> {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config = oceanfs_core::MetadataConfig {
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        Arc::new(oceanfs_storage::MetadataStore::open(&config).expect("open"))
    }

    #[test]
    fn adapter_get_object_delegates_correctly() {
        let store = create_test_store();
        let adapter = MetadataStoreAdapter::new(store);
        let bucket = BucketId::new("test-bucket");
        let key = ObjectKey::new("test-key");
        let result = adapter.get_object(&bucket, &key);
        // Object doesn't exist yet.
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn adapter_delete_object_delegates_correctly() {
        let store = create_test_store();
        let adapter = MetadataStoreAdapter::new(store);
        let bucket = BucketId::new("test-bucket");
        let key = ObjectKey::new("test-key");
        // Deleting a nonexistent object is OK (idempotent delete).
        let result = adapter.delete_object(&bucket, &key);
        assert!(result.is_ok());
    }

    #[test]
    fn adapter_list_objects_delegates_correctly() {
        let store = create_test_store();
        let adapter = MetadataStoreAdapter::new(store);
        let bucket = BucketId::new("test-bucket");
        let result = adapter.list_objects(&bucket, "");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
