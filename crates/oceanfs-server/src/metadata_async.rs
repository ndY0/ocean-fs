//! Async adapter for blocking metadata operations.
//!
//! RocksDB-backed metadata operations are synchronous (blocking) and
//! are invoked directly on tokio worker threads from async handlers.
//! Under load these block runtime workers on RocksDB mutexes/IO,
//! serializing the runtime. This module provides an explicit async
//! boundary: an [`AsyncMetadataOps`] adapter that wraps the sync
//! [`MetadataOps`] trait in `tokio::task::spawn_blocking` plus a
//! bounded semaphore (perf §8.3, §8.5) as a single concurrency knob.
//!
//! The [`MetadataOps`] trait itself is unchanged — the sync trait
//! remains for non-hot paths (durability workers, GC, tests).
//!
//! The seal worker's per-seal `put_segment` is deliberately NOT routed
//! through this adapter: it is handled by the seal pipeline's batched
//! metadata writer (flush coordinator) which persists segment metadata
//! in one RocksDB `WriteBatch` per drain cycle
//! (performance-optimization/seal-pipeline-batching).

use std::sync::Arc;

use oceanfs_core::{BucketId, Hlc, ObjectKey, ObjectMetadata};
use tokio::sync::Semaphore;

use crate::metadata_ops::{MetadataError, MetadataOps, Result};

/// Bounds concurrent blocking metadata operations (perf §8.5).
///
/// `spawn_blocking` has its own hazard: an unbounded blocking pool
/// (default 512 threads) can create unbounded thread churn under a
/// metadata burst. The semaphore is the single knob that bounds it.
const DEFAULT_MAX_CONCURRENT_METADATA_OPS: usize = 16;

/// Async wrapper around the sync [`MetadataOps`] trait.
///
/// Every method acquires a semaphore permit (bounded concurrency) and
/// runs the blocking RocksDB call inside `tokio::task::spawn_blocking`,
/// so no runtime worker thread is ever blocked on a metadata op.
///
/// # Examples
///
/// ```ignore
/// // Wired at the composition root:
/// let sync: Arc<dyn MetadataOps> = Arc::new(MetadataStoreAdapter::new(store));
/// let async_ops = Arc::new(AsyncMetadataOps::new(sync));
/// let meta = async_ops.get_object(&bucket, &key).await?;
/// ```
pub struct AsyncMetadataOps {
    inner: Arc<dyn MetadataOps>,
    semaphore: Arc<Semaphore>,
}

impl AsyncMetadataOps {
    /// Wraps a sync [`MetadataOps`] implementation with a bounded
    /// blocking-pool boundary.
    pub fn new(inner: Arc<dyn MetadataOps>) -> Self {
        Self { inner, semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_METADATA_OPS)) }
    }

    /// Wraps a sync [`MetadataOps`] implementation with an explicit
    /// concurrency bound (overrides the default 16).
    pub fn with_max_concurrency(inner: Arc<dyn MetadataOps>, max_concurrent: usize) -> Self {
        Self { inner, semaphore: Arc::new(Semaphore::new(max_concurrent)) }
    }

    /// Wraps a storage-api
    /// [`MetadataStore`](oceanfs_storage_api::MetadataStore)
    /// implementation (the coordinator's trait) with the same async
    /// boundary.
    ///
    /// The write coordinator holds `Arc<dyn MetadataStore>` (the
    /// storage-api trait consumed by durability workers and tests),
    /// not the server's `MetadataOps` trait; this constructor bridges
    /// the two so the Inline-tier write path gets the same
    /// spawn_blocking + bounded-semaphore treatment as the handler and
    /// read paths.
    pub fn from_storage(store: Arc<dyn oceanfs_storage_api::MetadataStore>) -> Self {
        Self::new(Arc::new(StorageOpsAdapter { store }))
    }

    /// Retrieves object metadata by bucket and key.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable or the
    /// underlying storage operation fails.
    pub async fn get_object(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectMetadata>> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MetadataError::Internal("metadata semaphore closed".into()))?;
        let inner = Arc::clone(&self.inner);
        let bucket = bucket.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            inner.get_object(&bucket, &key)
        })
        .await
        .map_err(|e| MetadataError::Internal(format!("metadata task failed: {e}")))?
    }

    /// Soft-deletes an object by writing a tombstone entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable or the
    /// tombstone write fails.
    pub async fn delete_object(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> Result<()> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MetadataError::Internal("metadata semaphore closed".into()))?;
        let inner = Arc::clone(&self.inner);
        let bucket = bucket.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            inner.delete_object(&bucket, &key, hlc)
        })
        .await
        .map_err(|e| MetadataError::Internal(format!("metadata task failed: {e}")))?
    }

    /// Stores object metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable or the
    /// underlying storage operation fails.
    pub async fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> Result<()> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MetadataError::Internal("metadata semaphore closed".into()))?;
        let inner = Arc::clone(&self.inner);
        let bucket = bucket.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            inner.put_object(&bucket, meta)
        })
        .await
        .map_err(|e| MetadataError::Internal(format!("metadata task failed: {e}")))?
    }

    /// Lists objects in a bucket matching the given prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable or the
    /// iteration over keys fails.
    pub async fn list_objects(
        &self,
        bucket: &BucketId,
        prefix: &str,
    ) -> Result<Vec<ObjectMetadata>> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MetadataError::Internal("metadata semaphore closed".into()))?;
        let inner = Arc::clone(&self.inner);
        let bucket = bucket.clone();
        let prefix = prefix.to_string();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            inner.list_objects(&bucket, &prefix)
        })
        .await
        .map_err(|e| MetadataError::Internal(format!("metadata task failed: {e}")))?
    }
}

/// Bridges the storage-api [`MetadataStore`] trait to the server's
/// [`MetadataOps`] trait so [`AsyncMetadataOps`] can wrap either.
///
/// This mirrors `oceanfs-node`'s `MetadataStoreAdapter` for the write
/// coordinator, which receives the storage-api trait object from the
/// composition root. Methods not present on the storage trait surface
/// used by the coordinator (list variants are routed through the
/// storage trait's equivalents).
struct StorageOpsAdapter {
    store: Arc<dyn oceanfs_storage_api::MetadataStore>,
}

impl MetadataOps for StorageOpsAdapter {
    fn get_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<Option<ObjectMetadata>> {
        self.store
            .get_object_metadata(bucket, key)
            .map_err(|e| MetadataError::Internal(format!("{e}")))
    }

    fn delete_object(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> Result<()> {
        self.store
            .delete_object(bucket, key, hlc)
            .map_err(|e| MetadataError::Internal(format!("{e}")))
    }

    fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> Result<()> {
        self.store.put_object(bucket, meta).map_err(|e| MetadataError::Internal(format!("{e}")))
    }

    fn list_objects(&self, bucket: &BucketId, prefix: &str) -> Result<Vec<ObjectMetadata>> {
        self.store
            .list_objects(bucket, prefix)
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Internal(format!("{e}")))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Records the thread each op ran on to pin the spawn_blocking
    /// boundary (ops must run on the blocking pool, never on the
    /// runtime worker).
    #[derive(Default)]
    struct ThreadRecordingOps {
        threads: std::sync::Mutex<Vec<u64>>,
    }

    impl ThreadRecordingOps {
        fn record(&self) {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::thread::current().id().hash(&mut hasher);
            self.threads.lock().unwrap().push(hasher.finish());
        }
    }

    impl MetadataOps for ThreadRecordingOps {
        fn get_object(
            &self,
            _bucket: &BucketId,
            _key: &ObjectKey,
        ) -> Result<Option<ObjectMetadata>> {
            self.record();
            Ok(None)
        }
        fn delete_object(&self, _b: &BucketId, _k: &ObjectKey, _h: Hlc) -> Result<()> {
            self.record();
            Ok(())
        }
        fn put_object(&self, _b: &BucketId, _m: ObjectMetadata) -> Result<()> {
            self.record();
            Ok(())
        }
        fn list_objects(&self, _b: &BucketId, _p: &str) -> Result<Vec<ObjectMetadata>> {
            self.record();
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn ops_run_on_the_blocking_pool_not_the_runtime_worker() {
        use std::hash::{Hash, Hasher};

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        let test_thread = hasher.finish();

        let recorded = Arc::new(ThreadRecordingOps::default());
        let adapter = AsyncMetadataOps::new(recorded.clone());

        adapter.get_object(&BucketId::new("b"), &ObjectKey::new("k")).await.unwrap();
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("k"),
            size: 0,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        adapter.put_object(&BucketId::new("b"), meta).await.unwrap();
        adapter
            .delete_object(&BucketId::new("b"), &ObjectKey::new("k"), Hlc::zero())
            .await
            .unwrap();
        adapter.list_objects(&BucketId::new("b"), "").await.unwrap();

        let threads = recorded.threads.lock().unwrap();
        assert_eq!(threads.len(), 4, "all four ops must have run");
        for t in threads.iter() {
            assert_ne!(*t, test_thread, "metadata ops must run on the blocking pool");
        }
    }

    #[test]
    fn ops_run_off_a_single_worker_runtime() {
        // DoD test (metadata-io-off-async-workers): spawn a runtime
        // with ONE worker thread, run N concurrent ops through the
        // adapter, and they complete. If the blocking calls ran inline
        // on the runtime worker, the single worker would be stuck in
        // the first op and the rest would never make progress.
        use std::hash::{Hash, Hasher};

        let recorded = Arc::new(ThreadRecordingOps::default());
        let adapter = Arc::new(AsyncMetadataOps::new(recorded.clone()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("single-worker runtime");

        rt.block_on(async {
            let mut handles = Vec::new();
            for _ in 0..8 {
                let adapter = Arc::clone(&adapter);
                handles.push(tokio::spawn(async move {
                    adapter.get_object(&BucketId::new("b"), &ObjectKey::new("k")).await.unwrap();
                }));
            }
            for h in handles {
                tokio::time::timeout(std::time::Duration::from_secs(5), h)
                    .await
                    .expect("ops must complete on a single-worker runtime")
                    .unwrap();
            }
        });

        let threads = recorded.threads.lock().unwrap();
        assert_eq!(threads.len(), 8, "all 8 ops must have run");
        // Sanity: the ops must not have run on the single runtime
        // worker (spawn_blocking threads differ from the runtime's).
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        for t in threads.iter() {
            assert_ne!(*t, hasher.finish(), "blocking ops must not run on the runtime worker");
        }
    }

    #[tokio::test]
    async fn bounded_semaphore_limits_concurrent_ops() {
        // With max_concurrency = 1, two concurrent ops must serialize:
        // the second one waits for the first to finish. The in-flight
        // counter lives INSIDE the recorded op (which runs under the
        // permit), so overlap would show as peak > 1.
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        #[derive(Default)]
        struct OverlapOps {
            in_flight: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }
        impl MetadataOps for OverlapOps {
            fn get_object(
                &self,
                _bucket: &BucketId,
                _key: &ObjectKey,
            ) -> Result<Option<ObjectMetadata>> {
                let n = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(n, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(20));
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(None)
            }
            fn delete_object(&self, _b: &BucketId, _k: &ObjectKey, _h: Hlc) -> Result<()> {
                Ok(())
            }
            fn put_object(&self, _b: &BucketId, _m: ObjectMetadata) -> Result<()> {
                Ok(())
            }
            fn list_objects(&self, _b: &BucketId, _p: &str) -> Result<Vec<ObjectMetadata>> {
                Ok(vec![])
            }
        }

        let recorded = Arc::new(OverlapOps { in_flight: in_flight.clone(), peak: peak.clone() });
        let adapter = Arc::new(AsyncMetadataOps::with_max_concurrency(recorded, 1));

        let mk = |adapter: Arc<AsyncMetadataOps>| async move {
            adapter.get_object(&BucketId::new("b"), &ObjectKey::new("k")).await.unwrap();
        };

        let h1 = tokio::spawn(mk(Arc::clone(&adapter)));
        let h2 = tokio::spawn(mk(Arc::clone(&adapter)));
        h1.await.unwrap();
        h2.await.unwrap();

        assert_eq!(
            peak.load(Ordering::SeqCst),
            1,
            "with 1 permit, concurrent ops must never overlap"
        );
    }
}
