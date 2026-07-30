//! Bucket-level configuration and per-bucket policy.

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

/// Per-bucket configuration policy.
#[derive(Debug, Clone)]
pub struct BucketPolicy {
    /// Write quorum size.
    pub write_quorum: u8,
    /// Read quorum size.
    pub read_quorum: u8,
    /// Total replicas.
    pub total_replicas: u8,
    /// Inline threshold in bytes.
    pub inline_threshold_bytes: u64,
}

impl Default for BucketPolicy {
    fn default() -> Self {
        Self { write_quorum: 2, read_quorum: 1, total_replicas: 3, inline_threshold_bytes: 4096 }
    }
}

/// A store for per-bucket configuration policies.
pub struct BucketConfigStore {
    policies: RwLock<HashMap<String, Arc<BucketPolicy>>>,
}

impl BucketConfigStore {
    /// Creates a new empty config store.
    pub fn new() -> Self {
        Self { policies: RwLock::new(HashMap::new()) }
    }

    /// Creates or updates a bucket policy.
    pub fn put(&self, bucket: String, policy: BucketPolicy) {
        self.policies.write().insert(bucket, Arc::new(policy));
    }

    /// Retrieves a bucket policy, or returns the default if not found.
    pub fn get(&self, bucket: &str) -> Arc<BucketPolicy> {
        self.policies
            .read()
            .get(bucket)
            .cloned()
            .unwrap_or_else(|| Arc::new(BucketPolicy::default()))
    }

    /// Deletes a bucket policy.
    pub fn delete(&self, bucket: &str) -> bool {
        self.policies.write().remove(bucket).is_some()
    }

    /// Lists all configured bucket names.
    pub fn list(&self) -> Vec<String> {
        self.policies.read().keys().cloned().collect()
    }
}

impl Default for BucketConfigStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get_policy() {
        let store = BucketConfigStore::new();
        store.put("my-bucket".into(), BucketPolicy { write_quorum: 3, ..Default::default() });
        let policy = store.get("my-bucket");
        assert_eq!(policy.write_quorum, 3);
    }

    #[test]
    fn get_missing_returns_default() {
        let store = BucketConfigStore::new();
        let policy = store.get("nonexistent");
        assert_eq!(policy.write_quorum, 2); // default
    }

    #[test]
    fn delete_removes_policy() {
        let store = BucketConfigStore::new();
        store.put("temp".into(), BucketPolicy::default());
        assert!(store.delete("temp"));
        assert!(!store.delete("temp"));
    }
}
