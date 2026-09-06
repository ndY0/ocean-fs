//! Prefetch engine — speculative cache warming.
//!
//! After LIST or GET operations, prefetches metadata for adjacent keys
//! to warm the caches before the client requests them. Bounded queue
//! and semaphore prevent overwhelming the system. Entirely best-effort:
//! prefetch failures are silent.

use std::sync::Arc;

use oceanfs_core::{BucketId, ObjectKey};
use oceanfs_storage_api::MetadataStore;
use parking_lot::Mutex;
use tokio::{
    sync::{mpsc, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{l1_object::ObjectCache, l2_metadata::MetadataCache};

/// Configuration for the prefetch engine.
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Whether prefetching is enabled.
    pub enabled: bool,
    /// Number of objects to prefetch after a LIST response.
    pub after_list: usize,
    /// Number of adjacent objects to prefetch after a GET response.
    pub after_get: usize,
    /// Maximum number of concurrent prefetch operations.
    pub max_concurrency: usize,
    /// Capacity of the prefetch work queue.
    pub queue_capacity: usize,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            after_list: 16,
            after_get: 4,
            max_concurrency: 8,
            queue_capacity: 256,
        }
    }
}

/// A single prefetch task: look up this key and warm the cache.
struct PrefetchTask {
    bucket: BucketId,
    key: ObjectKey,
}

/// Orchestrates speculative cache warming.
///
/// Spawns a background worker that dequeues prefetch tasks and populates
/// the L2 metadata cache (and optionally the L1 object cache for inline blobs).
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_cache::{PrefetchConfig, PrefetchEngine, MetadataCache, MetadataCacheConfig};
/// use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, Tombstone};
/// use oceanfs_storage_api::{BatchOp, MetadataStore};
///
/// struct MockStore;
/// impl MetadataStore for MockStore {
///     fn list_object_keys(
///         &self,
///         _bucket: &BucketId,
///     ) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
///         Ok(vec![])
///     }
///     fn get_object_metadata(
///         &self,
///         _bucket: &BucketId,
///         _key: &ObjectKey,
///     ) -> std::io::Result<Option<ObjectMetadata>> {
///         Ok(None)
///     }
///     fn list_objects(
///         &self,
///         _bucket: &BucketId,
///         _prefix: &str,
///     ) -> Vec<std::io::Result<ObjectMetadata>> {
///         vec![]
///     }
///     fn list_tombstones(
///         &self,
///         _bucket: &BucketId,
///     ) -> Vec<std::io::Result<(ObjectKey, Tombstone)>> {
///         vec![]
///     }
///     fn delete_tombstone(&self, _bucket: &BucketId, _key: &ObjectKey) -> std::io::Result<()> {
///         Ok(())
///     }
///     fn put_object(&self, _bucket: &BucketId, _meta: ObjectMetadata) -> std::io::Result<()> {
///         Ok(())
///     }
///     fn delete_object(
///         &self,
///         _bucket: &BucketId,
///         _key: &ObjectKey,
///         _hlc: oceanfs_core::Hlc,
///     ) -> std::io::Result<()> {
///         Ok(())
///     }
///     fn batch_write(&self, _ops: Vec<BatchOp>) -> std::io::Result<()> {
///         Ok(())
///     }
/// }
///
/// let metadata_cache = Arc::new(MetadataCache::new(
///     MetadataCacheConfig::default(),
///     Box::new(oceanfs_cache::eviction::TtlLruPolicy::new(
///         oceanfs_cache::eviction::TtlLruConfig::default(),
///     )),
/// ));
/// let store: Arc<dyn MetadataStore> = Arc::new(MockStore);
///
/// let engine = PrefetchEngine::new(
///     PrefetchConfig { enabled: true, ..Default::default() },
///     metadata_cache,
///     None,
///     store,
/// );
/// // The engine is created; in a real application with a tokio runtime,
/// // the background worker will process queued prefetch tasks.
/// let keys = [ObjectKey::new("a"), ObjectKey::new("b")];
/// engine.after_list(BucketId::new("b"), &keys, 0);
/// ```
pub struct PrefetchEngine {
    config: PrefetchConfig,
    /// Sender for the bounded work queue. Dropping this shuts down the worker.
    sender: mpsc::Sender<PrefetchTask>,
    /// Metadata store for adjacent-key discovery (M8).
    metadata: Arc<dyn MetadataStore>,
    /// Shutdown signal for the background worker ([`Self::shutdown`]).
    shutdown: CancellationToken,
    /// The background worker's join handle, when a tokio runtime was
    /// active at construction. `shutdown` awaits it so the worker's
    /// store clone is dropped before a same-directory store reopen.
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl PrefetchEngine {
    /// Creates a new prefetch engine and starts the background worker.
    ///
    /// The worker runs until [`Self::shutdown`] is called or `self` is
    /// dropped. If no tokio runtime is available (e.g., in synchronous
    /// contexts), the worker is not spawned and prefetch tasks are
    /// silently dropped.
    pub fn new(
        config: PrefetchConfig,
        metadata_cache: Arc<MetadataCache>,
        object_cache: Option<Arc<ObjectCache>>,
        metadata: Arc<dyn MetadataStore>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let metadata_for_engine = Arc::clone(&metadata);
        let shutdown = CancellationToken::new();

        // Spawn the worker only if a tokio runtime is active. Its join
        // handle is stored so `shutdown` can await the worker's exit
        // (which drops the worker's `Arc<dyn MetadataStore>` clone).
        let worker_join = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let worker = PrefetchWorker {
                config: config.clone(),
                receiver,
                metadata_cache,
                object_cache,
                metadata,
            };
            let worker_shutdown = shutdown.clone();
            Some(handle.spawn(worker.run(worker_shutdown)))
        } else {
            // If no runtime, the worker is not spawned; tasks are
            // silently dropped.
            None
        };

        Self {
            config,
            sender,
            metadata: metadata_for_engine,
            shutdown,
            worker: Mutex::new(worker_join),
        }
    }

    /// Stops the background worker and waits for it to exit.
    ///
    /// Cancels the worker's shutdown token and awaits its join handle
    /// (bounded by a short timeout), draining any in-flight prefetch
    /// tasks. After this returns, the worker no longer holds the
    /// metadata-store clone, so callers that drop the engine can safely
    /// reopen a same-directory store. Idempotent; returns promptly when
    /// the worker was never spawned (no tokio runtime at construction).
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use oceanfs_cache::{PrefetchConfig, PrefetchEngine, MetadataCache, MetadataCacheConfig};
    /// # use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, Tombstone};
    /// # use oceanfs_storage_api::{BatchOp, MetadataStore};
    /// # struct MockStore;
    /// # impl MetadataStore for MockStore {
    /// #     fn list_object_keys(&self, _b: &BucketId) -> std::io::Result<Vec<(BucketId, ObjectKey)>> { Ok(vec![]) }
    /// #     fn get_object_metadata(&self, _b: &BucketId, _k: &ObjectKey) -> std::io::Result<Option<ObjectMetadata>> { Ok(None) }
    /// #     fn list_objects(&self, _b: &BucketId, _p: &str) -> Vec<std::io::Result<ObjectMetadata>> { vec![] }
    /// #     fn list_tombstones(&self, _b: &BucketId) -> Vec<std::io::Result<(ObjectKey, Tombstone)>> { vec![] }
    /// #     fn delete_tombstone(&self, _b: &BucketId, _k: &ObjectKey) -> std::io::Result<()> { Ok(()) }
    /// #     fn put_object(&self, _b: &BucketId, _m: ObjectMetadata) -> std::io::Result<()> { Ok(()) }
    /// #     fn delete_object(&self, _b: &BucketId, _k: &ObjectKey, _h: oceanfs_core::Hlc) -> std::io::Result<()> { Ok(()) }
    /// #     fn batch_write(&self, _ops: Vec<BatchOp>) -> std::io::Result<()> { Ok(()) }
    /// # }
    /// # async fn example() {
    /// # let metadata_cache = Arc::new(MetadataCache::new(
    /// #     MetadataCacheConfig::default(),
    /// #     Box::new(oceanfs_cache::eviction::TtlLruPolicy::new(oceanfs_cache::eviction::TtlLruConfig::default())),
    /// # ));
    /// # let store: Arc<dyn MetadataStore> = Arc::new(MockStore);
    /// let engine = PrefetchEngine::new(
    ///     PrefetchConfig { enabled: true, ..Default::default() },
    ///     metadata_cache,
    ///     None,
    ///     store,
    /// );
    /// engine.shutdown().await;
    /// # }
    /// ```
    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        let join = self.worker.lock().take();
        if let Some(join) = join {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), join).await;
        }
    }

    /// Enqueues prefetch tasks for keys after the given cursor.
    ///
    /// Called after a LIST response. Prefetches up to `after_list` keys
    /// starting from `cursor`. If the queue is full, excess tasks are
    /// silently dropped.
    pub fn after_list(&self, bucket: BucketId, keys: &[ObjectKey], cursor: usize) {
        if !self.config.enabled {
            return;
        }

        let end = (cursor + self.config.after_list).min(keys.len());
        for key in &keys[cursor..end] {
            let task = PrefetchTask { bucket: bucket.clone(), key: key.clone() };
            // Best-effort: if queue is full, drop.
            let _ = self.sender.try_send(task);
        }
    }

    /// Enqueues prefetch tasks for keys adjacent to the given key.
    ///
    /// Called after a GET response. Prefetches up to `after_get` subsequent
    /// keys in the provided list. This is a best-effort hint.
    ///
    /// For automatic adjacent-key discovery (M8), query the metadata store
    /// to find nearby keys via range scan.
    pub fn after_get(&self, bucket: BucketId, _key: &ObjectKey, adjacent_keys: &[ObjectKey]) {
        if !self.config.enabled {
            return;
        }

        let count = self.config.after_get.min(adjacent_keys.len());
        for k in adjacent_keys.iter().take(count) {
            let task = PrefetchTask { bucket: bucket.clone(), key: k.clone() };
            let _ = self.sender.try_send(task);
        }
    }

    /// Discovers keys adjacent to the given key via the metadata store
    /// and enqueues them for prefetch (M8).
    ///
    /// Queries `list_object_keys` for the bucket, sorts the keys
    /// lexicographically, finds the position of `key`, and prefetches
    /// up to `after_get` keys following it. Best-effort: failures are
    /// silent and do not affect the response.
    pub fn discover_and_prefetch_adjacent(&self, bucket: &BucketId, key: &ObjectKey) {
        if !self.config.enabled || self.config.after_get == 0 {
            return;
        }

        // List all keys in the bucket.
        let mut keys = match self.metadata.list_object_keys(bucket) {
            Ok(keys) => keys.into_iter().map(|(_, k)| k).collect::<Vec<_>>(),
            Err(_) => return,
        };
        // Sort lexicographically for consistent adjacency.
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        // Find the position of the current key and select subsequent keys.
        let pos = keys.iter().position(|k| k == key).unwrap_or(keys.len());
        let adjacent: Vec<ObjectKey> =
            keys[pos.saturating_add(1)..].iter().take(self.config.after_get).cloned().collect();

        for k in &adjacent {
            let task = PrefetchTask { bucket: bucket.clone(), key: k.clone() };
            let _ = self.sender.try_send(task);
        }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &PrefetchConfig {
        &self.config
    }
}

/// Background worker that processes the prefetch queue.
struct PrefetchWorker {
    config: PrefetchConfig,
    receiver: mpsc::Receiver<PrefetchTask>,
    metadata_cache: Arc<MetadataCache>,
    object_cache: Option<Arc<ObjectCache>>,
    metadata: Arc<dyn MetadataStore>,
}

impl PrefetchWorker {
    /// Runs the worker loop. Exits when the shutdown token fires or the
    /// sender is dropped, then drains the in-flight prefetch tasks
    /// before returning so no store clone outlives the worker.
    async fn run(mut self, shutdown: CancellationToken) {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrency));
        let mut in_flight = tokio::task::JoinSet::new();

        loop {
            let task = tokio::select! {
                _ = shutdown.cancelled() => break,
                task = self.receiver.recv() => match task {
                    Some(task) => task,
                    None => break, // sender dropped
                },
            };
            let permit = tokio::select! {
                _ = shutdown.cancelled() => break,
                permit = semaphore.clone().acquire_owned() => permit,
            };
            let meta_cache = self.metadata_cache.clone();
            let obj_cache = self.object_cache.clone();
            let store = self.metadata.clone();
            in_flight.spawn(async move {
                let _permit = permit;
                run_prefetch(store, meta_cache, obj_cache, task).await;
            });
        }

        // Drain the in-flight prefetch tasks so their metadata-store
        // clones are released before the worker returns.
        while in_flight.join_next().await.is_some() {}
    }
}

/// Executes one prefetch task: look up the metadata and warm the caches.
async fn run_prefetch(
    store: Arc<dyn MetadataStore>,
    meta_cache: Arc<MetadataCache>,
    obj_cache: Option<Arc<ObjectCache>>,
    task: PrefetchTask,
) {
    // Look up metadata from the backing store.
    match store.get_object_metadata(&task.bucket, &task.key) {
        Ok(Some(meta)) => {
            // Warm the L2 metadata cache.
            meta_cache.put(task.bucket.clone(), task.key.clone(), meta.clone());

            // Optionally warm the L1 object cache with inline data.
            if let (Some(ref obj), Some(inline_data)) = (&obj_cache, &meta.inline_data) {
                obj.put(task.bucket.clone(), task.key.clone(), inline_data.clone());
            }
        }
        Ok(None) => {
            // Key not found — nothing to warm.
        }
        Err(_) => {
            // Store error — silent (best-effort).
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use oceanfs_core::{Hlc, ObjectMetadata, Tombstone};

    use super::*;

    /// A mock metadata store that records lookups.
    struct MockStore {
        lookup_count: AtomicUsize,
        /// Pre-defined metadata to return for specific keys.
        entries: Vec<(BucketId, ObjectKey, ObjectMetadata)>,
    }

    impl MockStore {
        fn new(entries: Vec<(BucketId, ObjectKey, ObjectMetadata)>) -> Self {
            Self { lookup_count: AtomicUsize::new(0), entries }
        }
    }

    impl MetadataStore for MockStore {
        fn list_object_keys(
            &self,
            _bucket: &BucketId,
        ) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
            Ok(self.entries.iter().map(|(b, k, _)| (b.clone(), k.clone())).collect())
        }

        fn get_object_metadata(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
        ) -> std::io::Result<Option<ObjectMetadata>> {
            self.lookup_count.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .entries
                .iter()
                .find(|(b, k, _)| b == bucket && k == key)
                .map(|(_, _, m)| m.clone()))
        }

        fn list_objects(
            &self,
            _bucket: &BucketId,
            _prefix: &str,
        ) -> Vec<std::io::Result<ObjectMetadata>> {
            self.entries.iter().map(|(_, _, m)| Ok(m.clone())).collect()
        }

        fn list_tombstones(
            &self,
            _bucket: &BucketId,
        ) -> Vec<std::io::Result<(ObjectKey, Tombstone)>> {
            vec![]
        }

        fn delete_tombstone(&self, _bucket: &BucketId, _key: &ObjectKey) -> std::io::Result<()> {
            Ok(())
        }

        fn put_object(&self, _bucket: &BucketId, _meta: ObjectMetadata) -> std::io::Result<()> {
            Ok(())
        }

        fn delete_object(
            &self,
            _bucket: &BucketId,
            _key: &ObjectKey,
            _hlc: oceanfs_core::Hlc,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn batch_write(&self, _ops: Vec<oceanfs_storage_api::BatchOp>) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn make_meta(key: &str, inline: Option<&[u8]>) -> ObjectMetadata {
        ObjectMetadata {
            object_key: ObjectKey::new(key),
            size: 100,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: inline.map(bytes::Bytes::copy_from_slice),
            created_at: 0,
            hlc: Hlc::zero(),
        }
    }

    #[test]
    fn disabled_by_default() {
        let config = PrefetchConfig::default();
        assert!(!config.enabled);
    }

    #[test]
    fn can_be_enabled() {
        let config = PrefetchConfig { enabled: true, ..Default::default() };
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn after_list_enqueues_tasks() {
        let metadata_cache = Arc::new(MetadataCache::new(
            crate::l2_metadata::MetadataCacheConfig::default(),
            Box::new(crate::eviction::TtlLruPolicy::new(crate::eviction::TtlLruConfig::default())),
        ));
        let entries = vec![
            (BucketId::new("b"), ObjectKey::new("k1"), make_meta("k1", None)),
            (BucketId::new("b"), ObjectKey::new("k2"), make_meta("k2", None)),
        ];
        let store = Arc::new(MockStore::new(entries));
        let engine = PrefetchEngine::new(
            PrefetchConfig {
                enabled: true,
                after_list: 2,
                max_concurrency: 4,
                queue_capacity: 16,
                ..Default::default()
            },
            metadata_cache.clone(),
            None,
            store.clone() as Arc<dyn MetadataStore>,
        );

        let keys = [ObjectKey::new("k1"), ObjectKey::new("k2")];
        engine.after_list(BucketId::new("b"), &keys, 0);

        // Give the worker time to process.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Metadata cache should now be warm.
        assert!(metadata_cache.get(&BucketId::new("b"), &ObjectKey::new("k1")).is_some());
        assert!(metadata_cache.get(&BucketId::new("b"), &ObjectKey::new("k2")).is_some());
    }

    #[tokio::test]
    async fn after_get_prefetches_adjacent_keys() {
        let metadata_cache = Arc::new(MetadataCache::new(
            crate::l2_metadata::MetadataCacheConfig::default(),
            Box::new(crate::eviction::TtlLruPolicy::new(crate::eviction::TtlLruConfig::default())),
        ));
        let entries = vec![
            (BucketId::new("b"), ObjectKey::new("k1"), make_meta("k1", None)),
            (BucketId::new("b"), ObjectKey::new("k2"), make_meta("k2", None)),
        ];
        let store = Arc::new(MockStore::new(entries));
        let engine = PrefetchEngine::new(
            PrefetchConfig {
                enabled: true,
                after_get: 2,
                max_concurrency: 4,
                queue_capacity: 16,
                ..Default::default()
            },
            metadata_cache.clone(),
            None,
            store.clone() as Arc<dyn MetadataStore>,
        );

        let adjacent = [ObjectKey::new("k1"), ObjectKey::new("k2")];
        engine.after_get(BucketId::new("b"), &ObjectKey::new("k0"), &adjacent);

        // Give the worker time.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert!(metadata_cache.get(&BucketId::new("b"), &ObjectKey::new("k1")).is_some());
    }

    #[tokio::test]
    async fn disabled_engine_is_noop() {
        let metadata_cache = Arc::new(MetadataCache::new(
            crate::l2_metadata::MetadataCacheConfig::default(),
            Box::new(crate::eviction::TtlLruPolicy::new(crate::eviction::TtlLruConfig::default())),
        ));
        let store = Arc::new(MockStore::new(vec![]));
        let engine = PrefetchEngine::new(
            PrefetchConfig { enabled: false, ..Default::default() },
            metadata_cache.clone(),
            None,
            store.clone() as Arc<dyn MetadataStore>,
        );

        engine.after_list(BucketId::new("b"), &[ObjectKey::new("k")], 0);

        // No prefetch should have happened.
        assert!(metadata_cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[tokio::test]
    async fn inline_blob_warms_object_cache() {
        let obj_cache = Arc::new(ObjectCache::new(
            crate::l1_object::ObjectCacheConfig::default(),
            Box::new(crate::eviction::GdsfPolicy::new(crate::eviction::GdsfConfig::default())),
        ));
        let metadata_cache = Arc::new(MetadataCache::new(
            crate::l2_metadata::MetadataCacheConfig::default(),
            Box::new(crate::eviction::TtlLruPolicy::new(crate::eviction::TtlLruConfig::default())),
        ));
        let entries = vec![(
            BucketId::new("b"),
            ObjectKey::new("inline-key"),
            make_meta("inline-key", Some(b"inline-data")),
        )];
        let store = Arc::new(MockStore::new(entries));
        let engine = PrefetchEngine::new(
            PrefetchConfig {
                enabled: true,
                after_list: 1,
                max_concurrency: 4,
                queue_capacity: 16,
                ..Default::default()
            },
            metadata_cache,
            Some(obj_cache.clone()),
            store.clone() as Arc<dyn MetadataStore>,
        );

        engine.after_list(BucketId::new("b"), &[ObjectKey::new("inline-key")], 0);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Object cache should contain the inline blob.
        assert_eq!(
            obj_cache.get(&BucketId::new("b"), &ObjectKey::new("inline-key")),
            Some(bytes::Bytes::from_static(b"inline-data"))
        );
    }

    #[tokio::test]
    async fn queue_full_silently_drops() {
        let metadata_cache = Arc::new(MetadataCache::new(
            crate::l2_metadata::MetadataCacheConfig::default(),
            Box::new(crate::eviction::TtlLruPolicy::new(crate::eviction::TtlLruConfig::default())),
        ));
        let store = Arc::new(MockStore::new(vec![]));
        let engine = PrefetchEngine::new(
            PrefetchConfig {
                enabled: true,
                after_list: 100,
                max_concurrency: 1,
                queue_capacity: 1, // Very small queue.
                ..Default::default()
            },
            metadata_cache,
            None,
            store.clone() as Arc<dyn MetadataStore>,
        );

        // Try to enqueue many tasks.
        let mut keys = Vec::new();
        for i in 0..100 {
            keys.push(ObjectKey::new(format!("k{}", i)));
        }
        // This should not panic, even though the queue is tiny.
        engine.after_list(BucketId::new("b"), &keys, 0);
    }
}
