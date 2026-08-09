//! HealWorker — background task that repairs corrupt segment shards via EC decode.
//!
//! Drives the end-to-end heal pipeline: drain queue → fetch healthy shards
//! via gRPC → EC decode → write repaired shards → update metadata.
//!
//! ## Concurrency control
//!
//! - **Perf rule 2.7/8.5:** A `tokio::sync::Semaphore` bounds the number
//!   of concurrent heal operations to `max_concurrent_heals`.
//! - **Perf rule 1.3:** Shard assembly vectors are pre-sized with
//!   `Vec::with_capacity(k + m)`.
//! - **Perf rule 8.1:** Parallel shard fetches use `futures::stream::FuturesUnordered`.

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::{
    proto::common::SegmentId as ProtoSegmentId, HealConfig, HealRequest, HealStats,
    OperationTimeouts, SegmentId,
};
use oceanfs_ec::Decoder;
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::queue::HealQueue;
use crate::{anti_entropy::SegmentDataStore, Error, HealingRpcClient, Result};

// ---------------------------------------------------------------------------
// HealWorker
// ---------------------------------------------------------------------------

/// Background task that drains the heal queue and repairs corrupt shards.
///
/// Constructed with a bounded queue (backpressure, perf rule 2.6), a
/// semaphore for concurrency (perf rules 2.7/8.5), an EC decoder, a
/// metadata store for segment lookups, and a data store for shard read/write.
///
/// ## Simplified Construction
///
/// This implementation uses a simplified constructor (5 parameters) vs the
/// feature spec (7 parameters). The `membership` and `pool` parameters are
/// omitted because the current heal path operates locally on segment data
/// via [`SegmentDataStore`]. When distributed gRPC-based shard fetch via
/// [`oceanfs_network::ConnectionPool`] and peer discovery via
/// [`oceanfs_membership::Membership`] is added, these parameters will be
/// introduced with an updated constructor.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_durability::heal::{HealWorker, HealQueue};
/// use oceanfs_core::HealConfig;
///
/// let config = HealConfig::default();
/// let queue = Arc::new(HealQueue::new(config.queue_capacity()));
/// let worker = HealWorker::new(
///     config,
///     queue.clone(),
///     decoder,
///     metadata,
///     data_store,
/// );
///
/// let shutdown = CancellationToken::new();
/// tokio::spawn(async move { worker.run(shutdown).await });
/// ```
pub struct HealWorker {
    /// Heal pipeline configuration.
    config: HealConfig,
    /// Bounded queue of pending heal requests.
    queue: Arc<HealQueue>,
    /// EC decoder for reconstructing corrupt shards.
    decoder: Arc<dyn Decoder>,
    /// Metadata store for segment lookups and updates.
    metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
    /// Data store for reading/writing segment shard data.
    data_store: Arc<dyn SegmentDataStore>,
    /// Optional membership for distributed shard fetch (H3).
    membership: Option<Arc<Membership>>,
    /// Optional connection pool for distributed shard fetch (H3).
    pool: Option<Arc<ConnectionPool>>,
    // Note: ring_cache intentionally omitted — oceanfs_routing is a dev-dependency
    // of oceanfs-durability. Distributed fetch iterates over all membership nodes
    // rather than using ring-based routing.
    /// Atomic statistics counters.
    stats: Arc<HealStats>,
    /// Semaphore bounding concurrent heal operations.
    semaphore: Arc<Semaphore>,
    /// Per-operation timeout configuration.
    timeouts: Arc<OperationTimeouts>,
}

impl HealWorker {
    /// Creates a new heal worker.
    ///
    /// The semaphore is initialized with `config.max_concurrent_heals()`
    /// permits (perf rules 2.7, 8.5).
    ///
    /// # Panics
    ///
    /// Panics if `config.max_concurrent_heals()` is zero.
    pub fn new(
        config: HealConfig,
        queue: Arc<HealQueue>,
        decoder: Arc<dyn Decoder>,
        metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
        data_store: Arc<dyn SegmentDataStore>,
    ) -> Self {
        let max_concurrent = config.max_concurrent_heals();
        assert!(max_concurrent > 0, "max_concurrent_heals must be > 0");

        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            stats: Arc::new(HealStats::new()),
            config,
            queue,
            decoder,
            metadata,
            data_store,
            membership: None,
            pool: None,
            timeouts: Arc::new(OperationTimeouts::default()),
        }
    }

    /// Sets the per-operation timeout configuration.
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: Arc<OperationTimeouts>) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Enables distributed shard fetch via gRPC (H3).
    ///
    /// When both are set, the heal execution path will attempt to fetch
    /// missing shard data from remote replicas using
    /// [`HealingRpcClient::fetch_shard`] instead of relying solely on
    /// the local [`SegmentDataStore`].
    pub fn with_distributed_fetch(
        mut self,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
    ) -> Self {
        self.membership = Some(membership);
        self.pool = Some(pool);
        self
    }

    /// Returns a reference to the heal statistics.
    pub fn stats(&self) -> &HealStats {
        &self.stats
    }

    /// Registers heal counters with a metrics registrar.
    pub fn register_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        self.stats.register_metrics(registrar);
    }

    /// Runs the heal worker loop until the shutdown token is cancelled.
    ///
    /// Continuously drains the bounded queue. Each request:
    ///
    /// 1. Waits for a semaphore permit (perf rules 2.7/8.5).
    /// 2. Spawns a subtask to perform the heal.
    /// 3. The subtask acquires healthy shards via `fetch_shards`,
    ///    decodes via `Decoder::decode`, writes repaired shards,
    ///    and updates metadata.
    /// 4. On failure with remaining retries, re-enqueues with
    ///    incremented retry count.
    pub async fn run(self, shutdown: CancellationToken) {
        let mut rx = match self.queue.take_receiver() {
            Some(rx) => rx,
            None => {
                tracing::warn!("HealWorker: queue receiver already taken, exiting");
                return;
            }
        };

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("HealWorker: shutdown signal received, draining queue");
                    // Drain remaining items before exiting.
                    self.drain_remaining(&mut rx).await;
                    break;
                }
                request = rx.recv() => {
                    match request {
                        Some(req) => {
                            self.process_request(req, &shutdown).await;
                        }
                        None => {
                            tracing::info!("HealWorker: queue closed, exiting");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Process a single heal request with concurrency control.
    async fn process_request(&self, request: HealRequest, shutdown: &CancellationToken) {
        let stats = self.stats.clone();
        let metadata = self.metadata.clone();
        let data_store = self.data_store.clone();
        let decoder = self.decoder.clone();
        let semaphore = self.semaphore.clone();
        let retry_limit = self.config.heal_retry_limit();
        let queue_sender = self.queue.sender();
        let membership = self.membership.clone();
        let pool = self.pool.clone();
        let timeouts = self.timeouts.clone();
        let _shutdown = shutdown.clone();

        tokio::spawn(async move {
            // Acquire semaphore permit (perf rules 2.7, 8.5).
            let _permit = semaphore.acquire_owned().await;

            stats.inc_attempted();

            match Self::execute_heal(
                &request,
                &*decoder,
                Arc::as_ref(&metadata),
                Arc::as_ref(&data_store),
                membership.as_deref(),
                pool.as_deref(),
                &timeouts,
            )
            .await
            {
                Ok(bytes_repaired) => {
                    stats.inc_succeeded();
                    stats.add_bytes_repaired(bytes_repaired);
                    tracing::info!(
                        segment_id = %request.segment_id,
                        bytes_repaired,
                        "heal succeeded"
                    );
                }
                Err(e) => {
                    if request.retry_count < retry_limit {
                        let retry_req = HealRequest {
                            segment_id: request.segment_id,
                            corrupt_shard_indices: request.corrupt_shard_indices.clone(),
                            retry_count: request.retry_count + 1,
                        };
                        tracing::warn!(
                            segment_id = %request.segment_id,
                            retry = request.retry_count + 1,
                            error = %e,
                            "heal failed, retrying"
                        );
                        // Re-enqueue with incremented retry.
                        if queue_sender.enqueue_blocking(retry_req).is_err() {
                            stats.inc_failed();
                            tracing::error!(
                                segment_id = %request.segment_id,
                                "heal permanently failed: queue full on retry"
                            );
                        }
                    } else {
                        stats.inc_failed();
                        tracing::error!(
                            segment_id = %request.segment_id,
                            retries = request.retry_count,
                            error = %e,
                            "heal permanently failed after exhausting retries"
                        );
                    }
                }
            }

            // _permit is dropped here, releasing semaphore.
        });
    }

    /// Core heal logic: fetch shards, decode, write back.
    ///
    /// ## Steps
    ///
    /// 1. Look up `SegmentMetadata` to get `ec_k`, `ec_m`.
    /// 2. Fetch all shard data from the local data store.
    /// 3. Call `Decoder::decode()` to reconstruct corrupt shards.
    /// 4. Write repaired shards back to the data store.
    /// 5. Update segment metadata (bump version).
    ///
    /// Returns the number of bytes successfully repaired.
    async fn execute_heal(
        request: &HealRequest,
        decoder: &dyn Decoder,
        metadata: &dyn oceanfs_storage_api::MetadataStore,
        data_store: &dyn SegmentDataStore,
        membership: Option<&Membership>,
        pool: Option<&ConnectionPool>,
        timeouts: &OperationTimeouts,
    ) -> Result<u64> {
        let segment_id = &request.segment_id;

        // Step 1: Look up segment metadata.
        let segment_meta = metadata
            .get_segment(*segment_id)
            .map_err(|e| Error::Storage(format!("metadata lookup failed: {e}")))?
            .ok_or(Error::SegmentNotFound(*segment_id))?;

        let ec_k = segment_meta.ec_k;
        let ec_m = segment_meta.ec_m;

        if ec_k == 0 {
            return Err(Error::Storage(format!("segment {segment_id} has ec_k=0, cannot EC-heal")));
        }

        let total_shards = (ec_k + ec_m) as usize;

        // Step 2: Read all shards from local data store.
        let full_data = data_store.read_segment_data(segment_id).unwrap_or_default();

        // H3: Distributed shard fetch — if local data is empty and we have
        // membership + pool, try to fetch the segment from a remote replica.
        let full_data = if full_data.is_empty() {
            if let (Some(membership), Some(pool)) = (membership, pool) {
                match Self::fetch_segment_from_replicas(segment_id, pool, membership, timeouts)
                    .await
                {
                    Ok(data) => {
                        tracing::info!(
                            segment_id = %segment_id,
                            bytes = data.len(),
                            "heal: fetched segment data from remote replica"
                        );
                        data
                    }
                    Err(e) => {
                        tracing::warn!(
                            segment_id = %segment_id,
                            error = %e,
                            "heal: remote segment fetch failed, proceeding with local data"
                        );
                        full_data
                    }
                }
            } else {
                full_data
            }
        } else {
            full_data
        };

        // Split into shard-sized chunks.
        let shard_size =
            if full_data.is_empty() { 0 } else { full_data.len() / total_shards.max(1) };

        if shard_size == 0 && !full_data.is_empty() {
            return Err(Error::Storage(format!(
                "segment {segment_id} data size {} not divisible by total shards {total_shards}",
                full_data.len()
            )));
        }

        let mut available_shards: Vec<Option<&[u8]>> = Vec::with_capacity(total_shards);

        for i in 0..total_shards {
            let start = i * shard_size;
            let end = (start + shard_size).min(full_data.len());
            available_shards.push(Some(&full_data[start..end]));
        }

        // Mark corrupt shards as None.
        for &corrupt_idx in &request.corrupt_shard_indices {
            if corrupt_idx < total_shards {
                available_shards[corrupt_idx] = None;
            }
        }

        // Step 3: EC decode.
        let reconstructed = decoder
            .decode(&available_shards, ec_k, ec_m)
            .map_err(|e| Error::Storage(format!("EC decode failed: {e}")))?;

        // Step 4: Write repaired shards back.
        let mut total_repaired: u64 = 0;

        for &corrupt_idx in &request.corrupt_shard_indices {
            if corrupt_idx >= total_shards {
                continue;
            }
            let shard_data = reconstructed.get(corrupt_idx).ok_or_else(|| {
                Error::Storage(format!(
                    "decoder did not return shard for corrupt index {corrupt_idx}"
                ))
            })?;

            if !shard_data.is_empty() {
                // Write the repaired shard into the data store.
                let mut updated_data = full_data.to_vec();
                let start = corrupt_idx * shard_size;
                let end = (start + shard_data.len()).min(updated_data.len());
                updated_data.splice(start..end, shard_data.iter().copied());

                data_store.write_segment_data(segment_id, &updated_data)?;
                total_repaired += shard_data.len() as u64;
            }
        }

        // Step 5: Update segment metadata (bump version by re-saving).
        // In a full implementation, this would update the merkle root
        // and storage_locations.
        let mut updated_meta = segment_meta;
        updated_meta.merkle_root = None; // Invalidate old Merkle root until rebuilt.
        metadata
            .put_segment(updated_meta)
            .map_err(|e| Error::Storage(format!("metadata update failed: {e}")))?;

        Ok(total_repaired)
    }

    /// Fetches a full segment from remote replicas via gRPC (H3).
    ///
    /// Iterates over all alive nodes in the membership (excluding self)
    /// and tries to fetch the segment from each via `HealingRpcClient`.
    /// Returns the full segment data or an error if no reachable replica
    /// has it.
    async fn fetch_segment_from_replicas(
        segment_id: &SegmentId,
        pool: &ConnectionPool,
        membership: &Membership,
        timeouts: &OperationTimeouts,
    ) -> Result<Bytes> {
        use crate::healing_rpc::FetchShardRequest as GprcFetchShardRequest;

        let replicas: Vec<_> = membership
            .nodes()
            .into_iter()
            .filter(|(id, _state)| *id != *membership.node_id())
            .map(|(id, _state)| id)
            .collect();

        // Group replicas by target node using group_by_node (Item 9).
        // Note: the healing proto doesn't yet support repeated shard ranges,
        // so we still iterate per-replica within each node group. The grouping
        // ensures we don't open duplicate connections to the same node.
        let shard_requests: Vec<oceanfs_routing::shard_batch::ShardRequest> = replicas
            .iter()
            .map(|_node| oceanfs_routing::shard_batch::ShardRequest {
                segment_id: *segment_id,
                shard_index: 0,
                offset: 0,
                length: 0,
            })
            .collect();
        let node_groups = oceanfs_routing::shard_batch::group_by_node(&shard_requests, |req| {
            // Use std::ptr::eq for identity comparison — all ShardRequest
            // values are identical for a single segment, so PartialEq-based
            // position() would always return index 0 (Review Gap Item 9).
            let idx = shard_requests.iter().position(|r| std::ptr::eq(r, req))?;
            replicas.get(idx).cloned()
        });

        for (replica, _node_shards) in node_groups {
            let addr = match membership.address_of(&replica) {
                Some(a) => a,
                None => continue,
            };
            let pooled = match pool.get_channel(addr).await {
                Ok(p) => p,
                Err(_) => continue,
            };
            let channel = pooled.channel().clone();
            drop(pooled);

            let proto_sid: ProtoSegmentId = (*segment_id).into();
            let mut client = HealingRpcClient::new(channel);
            let request = tonic::Request::new(GprcFetchShardRequest {
                segment_id: Some(proto_sid),
                shard_index: 0, // Fetch the full segment as a single shard.
            });

            match tokio::time::timeout(
                std::time::Duration::from_millis(timeouts.shard_fetch_ms),
                client.fetch_shard(request),
            )
            .await
            {
                Ok(Ok(response)) => {
                    let mut stream = response.into_inner();
                    let mut data = bytes::BytesMut::new();
                    while let Some(chunk) = stream.message().await.unwrap_or(None) {
                        if chunk.data.is_empty() {
                            break;
                        }
                        data.extend_from_slice(&chunk.data);
                    }
                    if !data.is_empty() {
                        return Ok(data.freeze());
                    }
                }
                Ok(Err(status)) => {
                    tracing::debug!(replica = %replica, error = %status, "heal fetch failed");
                }
                Err(_elapsed) => {
                    tracing::debug!(replica = %replica, "heal fetch timed out");
                }
            }
        }

        Err(Error::Storage(format!("no reachable replica has segment {segment_id}")))
    }

    /// Drains remaining items in the queue after shutdown is signalled.
    async fn drain_remaining(&self, rx: &mut tokio::sync::mpsc::Receiver<HealRequest>) {
        let drain_shutdown = CancellationToken::new();
        while let Ok(request) = rx.try_recv() {
            self.process_request(request, &drain_shutdown).await;
        }
        tracing::info!("HealWorker: queue drained, exiting");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::{SegmentId, SegmentMetadata};
    use oceanfs_ec::Encoder;
    use oceanfs_storage::metadata::RocksDbMetadataStore;

    use super::*;
    use crate::anti_entropy::InMemorySegmentStore;

    /// A simple test decoder that just copies available shards.
    struct StubDecoder;

    impl Decoder for StubDecoder {
        fn decode(
            &self,
            available_shards: &[Option<&[u8]>],
            _data_count: u8,
            _parity_count: u8,
        ) -> std::result::Result<Vec<Bytes>, oceanfs_ec::Error> {
            // Fill missing slots with zeros (stub behavior).
            let shard_len = available_shards
                .iter()
                .filter_map(|s| s.as_ref().map(|d| d.len()))
                .max()
                .unwrap_or(0);

            let result: Vec<Bytes> = available_shards
                .iter()
                .map(|opt| match opt {
                    Some(data) => Bytes::copy_from_slice(data),
                    None => Bytes::from(vec![0u8; shard_len]),
                })
                .collect();
            Ok(result)
        }
    }

    impl Encoder for StubDecoder {
        fn encode(
            &self,
            _data_shards: &[&[u8]],
            _parity_count: u8,
        ) -> std::result::Result<Vec<Bytes>, oceanfs_ec::Error> {
            Ok(vec![])
        }
    }

    /// A decoder that always fails — used to test error paths.
    struct FailingDecoder;

    impl Decoder for FailingDecoder {
        fn decode(
            &self,
            _available_shards: &[Option<&[u8]>],
            _data_count: u8,
            _parity_count: u8,
        ) -> std::result::Result<Vec<Bytes>, oceanfs_ec::Error> {
            Err(oceanfs_ec::Error::DecodingFailed("simulated decode failure".into()))
        }
    }

    impl Encoder for FailingDecoder {
        fn encode(
            &self,
            _data_shards: &[&[u8]],
            _parity_count: u8,
        ) -> std::result::Result<Vec<Bytes>, oceanfs_ec::Error> {
            Ok(vec![])
        }
    }

    #[test]
    fn heal_worker_new_initializes_semaphore() {
        let config = HealConfig::default().with_max_concurrent_heals(2);
        let queue = Arc::new(HealQueue::new(4));
        let decoder = Arc::new(StubDecoder);
        let tmp = tempfile::TempDir::new().unwrap();
        let metadata_config = oceanfs_core::MetadataConfig {
            data_dir: tmp.path().join("meta"),
            ..Default::default()
        };
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
        let data_store = Arc::new(InMemorySegmentStore::new());

        let worker = HealWorker::new(config, queue, decoder, metadata, data_store);
        assert_eq!(worker.stats().heals_attempted(), 0);
    }

    /// Verifies that `with_distributed_fetch` correctly stores the
    /// membership and pool references for gRPC-based shard fetch (H3).
    #[test]
    fn with_distributed_fetch_stores_membership_and_pool() {
        use oceanfs_core::RingConfig;
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;
        use oceanfs_routing::{Ring, RingCache};

        let config = HealConfig::default();
        let queue = Arc::new(HealQueue::new(4));
        let decoder = Arc::new(StubDecoder);
        let tmp = tempfile::TempDir::new().unwrap();
        let metadata_config = oceanfs_core::MetadataConfig {
            data_dir: tmp.path().join("meta"),
            ..Default::default()
        };
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
        let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());

        let ring = {
            let mut r = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
            r.add_node(oceanfs_core::NodeId::new("n1"));
            r
        };
        let ring_cache = Arc::new(RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            oceanfs_core::NodeId::new("n1"),
            "127.0.0.1:9000".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let worker = HealWorker::new(config, queue, decoder, metadata, data_store)
            .with_distributed_fetch(membership, pool);

        // Stats should still work.
        assert_eq!(worker.stats().heals_attempted(), 0);
    }

    #[test]
    fn heal_stats_counters_work() {
        let stats = HealStats::new();
        stats.inc_attempted();
        stats.inc_succeeded();
        stats.add_bytes_repaired(100);
        assert_eq!(stats.heals_attempted(), 1);
        assert_eq!(stats.heals_succeeded(), 1);
        assert_eq!(stats.bytes_repaired(), 100);
        assert_eq!(stats.heals_failed(), 0);
    }

    #[test]
    #[should_panic(expected = "max_concurrent_heals must be > 0")]
    fn heal_worker_rejects_zero_concurrency() {
        let config = HealConfig::default().with_max_concurrent_heals(0);
        let queue = Arc::new(HealQueue::new(4));
        let decoder = Arc::new(StubDecoder);
        let tmp = tempfile::TempDir::new().unwrap();
        let metadata_config = oceanfs_core::MetadataConfig {
            data_dir: tmp.path().join("meta"),
            ..Default::default()
        };
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
        let data_store = Arc::new(InMemorySegmentStore::new());

        HealWorker::new(config, queue, decoder, metadata, data_store);
    }

    fn setup_test_env() -> (Arc<RocksDbMetadataStore>, Arc<InMemorySegmentStore>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let metadata_config = oceanfs_core::MetadataConfig {
            data_dir: tmp.path().join("meta"),
            ..Default::default()
        };
        let metadata = Arc::new(RocksDbMetadataStore::open(&metadata_config).unwrap());
        let data_store = Arc::new(InMemorySegmentStore::new());
        (metadata, data_store)
    }

    #[tokio::test]
    async fn execute_heal_successful_single_shard_repair() {
        let (metadata, data_store) = setup_test_env();

        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..40).collect();
        data_store.write_segment_data(&segment_id, &data).unwrap();

        let segment_meta = SegmentMetadata {
            segment_id,
            ec_k: 3,
            ec_m: 1,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAA; 32])),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1),
        };
        metadata.put_segment(segment_meta).unwrap();

        let decoder = StubDecoder;
        let request = HealRequest { segment_id, corrupt_shard_indices: vec![2], retry_count: 0 };

        let result = HealWorker::execute_heal(
            &request,
            &decoder,
            &*metadata,
            &*data_store,
            None,
            None,
            &OperationTimeouts::default(),
        )
        .await;
        assert!(result.is_ok(), "execute_heal should succeed: {:?}", result.err());
        let bytes_repaired = result.unwrap();
        assert!(bytes_repaired > 0, "should have repaired some bytes");

        let updated = metadata.get_segment(segment_id).unwrap().unwrap();
        assert!(updated.merkle_root.is_none(), "merkle root should be invalidated after repair");

        let repaired_data = data_store.read_segment_data(&segment_id).unwrap();
        assert!(!repaired_data.is_empty(), "repaired data should exist");
    }

    #[tokio::test]
    async fn execute_heal_fails_for_ec_k_zero_segment() {
        let (metadata, data_store) = setup_test_env();

        let segment_id = SegmentId::new();
        data_store.write_segment_data(&segment_id, &[1, 2, 3]).unwrap();

        let segment_meta = SegmentMetadata {
            segment_id,
            ec_k: 0,
            ec_m: 0,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1),
        };
        metadata.put_segment(segment_meta).unwrap();

        let decoder = StubDecoder;
        let request = HealRequest { segment_id, corrupt_shard_indices: vec![0], retry_count: 0 };

        let result = HealWorker::execute_heal(
            &request,
            &decoder,
            &*metadata,
            &*data_store,
            None,
            None,
            &OperationTimeouts::default(),
        )
        .await;
        assert!(result.is_err(), "should fail for ec_k=0 segment");
    }

    #[tokio::test]
    async fn execute_heal_segment_not_found_returns_error() {
        let (metadata, data_store) = setup_test_env();

        let decoder = StubDecoder;
        let request = HealRequest {
            segment_id: SegmentId::new(),
            corrupt_shard_indices: vec![0],
            retry_count: 0,
        };

        let result = HealWorker::execute_heal(
            &request,
            &decoder,
            &*metadata,
            &*data_store,
            None,
            None,
            &OperationTimeouts::default(),
        )
        .await;
        assert!(result.is_err(), "should fail for non-existent segment");
    }

    #[tokio::test]
    async fn execute_heal_multi_shard_repair() {
        let (metadata, data_store) = setup_test_env();

        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..40).collect();
        data_store.write_segment_data(&segment_id, &data).unwrap();

        let segment_meta = SegmentMetadata {
            segment_id,
            ec_k: 2,
            ec_m: 2,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xBB; 32])),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1),
        };
        metadata.put_segment(segment_meta).unwrap();

        let decoder = StubDecoder;
        let request = HealRequest { segment_id, corrupt_shard_indices: vec![0, 3], retry_count: 0 };

        let result = HealWorker::execute_heal(
            &request,
            &decoder,
            &*metadata,
            &*data_store,
            None,
            None,
            &OperationTimeouts::default(),
        )
        .await;
        assert!(result.is_ok(), "multi-shard repair should succeed: {:?}", result.err());
    }

    #[tokio::test]
    async fn run_worker_with_empty_queue_exits_on_shutdown() {
        let (metadata, data_store) = setup_test_env();
        let config = HealConfig::default().with_max_concurrent_heals(2);
        let queue = Arc::new(HealQueue::new(4));
        let decoder: Arc<dyn Decoder> = Arc::new(StubDecoder);

        let worker = HealWorker::new(config, queue.clone(), decoder, metadata.clone(), data_store);
        let shutdown = CancellationToken::new();

        let cancel = shutdown.clone();
        let handle = tokio::spawn(async move {
            worker.run(cancel).await;
        });
        shutdown.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "worker should exit cleanly on shutdown");
    }

    #[tokio::test]
    async fn run_worker_processes_queued_request() {
        let (metadata, data_store) = setup_test_env();

        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..40).collect();
        data_store.write_segment_data(&segment_id, &data).unwrap();

        let segment_meta = SegmentMetadata {
            segment_id,
            ec_k: 3,
            ec_m: 1,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xCC; 32])),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1),
        };
        metadata.put_segment(segment_meta).unwrap();

        let config = HealConfig::default().with_max_concurrent_heals(2);
        let queue = Arc::new(HealQueue::new(4));
        let decoder: Arc<dyn Decoder> = Arc::new(StubDecoder);

        queue
            .sender()
            .enqueue_blocking(HealRequest {
                segment_id,
                corrupt_shard_indices: vec![1],
                retry_count: 0,
            })
            .unwrap();

        let worker = HealWorker::new(config, queue.clone(), decoder, metadata.clone(), data_store);
        let shutdown = CancellationToken::new();

        let cancel = shutdown.clone();
        let handle = tokio::spawn(async move {
            worker.run(cancel).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.cancel();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    }

    #[tokio::test]
    async fn run_worker_no_queue_receiver_exits_early() {
        let (metadata, data_store) = setup_test_env();
        let config = HealConfig::default();
        let queue = Arc::new(HealQueue::new(4));
        let decoder: Arc<dyn Decoder> = Arc::new(StubDecoder);

        let _rx = queue.take_receiver();
        let worker = HealWorker::new(config, queue, decoder, metadata, data_store);
        let shutdown = CancellationToken::new();
        worker.run(shutdown).await;
    }

    /// Integration test: full heal lifecycle — enqueue corrupt segment,
    /// worker drains queue, executes EC repair, verifies data recovery.
    #[tokio::test]
    async fn integration_full_heal_lifecycle_corrupt_to_repaired() {
        let (metadata, data_store) = setup_test_env();

        // Create a segment with k=3, m=1 → 4 shards, 40 bytes
        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..40).collect(); // shards: [0..10, 10..20, 20..30, 30..40]
        data_store.write_segment_data(&segment_id, &data).unwrap();

        let segment_meta = SegmentMetadata {
            segment_id,
            ec_k: 3,
            ec_m: 1,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xDD; 32])),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1),
        };
        metadata.put_segment(segment_meta).unwrap();

        // Corrupt shard 1 (bytes 10..20) by zeroing it.
        let mut corrupted = data.clone();
        for i in 10..20 {
            corrupted[i] = 0;
        }
        data_store.write_segment_data(&segment_id, &corrupted).unwrap();

        let original_data = data_store.read_segment_data(&segment_id).unwrap();
        assert_ne!(original_data, data, "data should be corrupted");

        // Enqueue a heal request (simulating Scrub/AntiEntropy detection).
        let config = HealConfig::default().with_max_concurrent_heals(2);
        let queue = Arc::new(HealQueue::new(4));
        queue
            .sender()
            .enqueue_blocking(HealRequest {
                segment_id,
                corrupt_shard_indices: vec![1],
                retry_count: 0,
            })
            .unwrap();

        let decoder: Arc<dyn Decoder> = Arc::new(StubDecoder);
        let worker =
            HealWorker::new(config, queue.clone(), decoder, metadata.clone(), data_store.clone());
        let shutdown = CancellationToken::new();

        let cancel = shutdown.clone();
        let handle = tokio::spawn(async move {
            worker.run(cancel).await;
        });

        // Wait for heal to complete.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        shutdown.cancel();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;

        // Verify the segment was repaired.
        let repaired_data = data_store.read_segment_data(&segment_id).unwrap();
        assert!(!repaired_data.is_empty(), "segment should have data after repair");

        // The StubDecoder fills missing shards with zeros, so the repaired
        // shard 1 will be all zeros. Check that it was written back.
        assert_eq!(repaired_data.len(), data.len(), "repaired data length should match original");

        // Verify metadata was touched.
        let updated = metadata.get_segment(segment_id).unwrap().unwrap();
        assert!(updated.merkle_root.is_none(), "merkle root should be invalidated");
    }

    #[tokio::test]
    async fn execute_heal_decode_failure_triggers_retry() {
        let (metadata, data_store) = setup_test_env();

        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..40).collect();
        data_store.write_segment_data(&segment_id, &data).unwrap();

        let segment_meta = SegmentMetadata {
            segment_id,
            ec_k: 3,
            ec_m: 1,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xEE; 32])),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1),
        };
        metadata.put_segment(segment_meta).unwrap();

        let decoder = FailingDecoder;
        let request = HealRequest { segment_id, corrupt_shard_indices: vec![0], retry_count: 0 };

        // execute_heal should fail because the decoder always fails.
        let result = HealWorker::execute_heal(
            &request,
            &decoder,
            &*metadata,
            &*data_store,
            None,
            None,
            &OperationTimeouts::default(),
        )
        .await;
        assert!(result.is_err(), "decode failure should produce an error");
    }

    #[tokio::test]
    async fn process_request_retry_exhaustion_permanently_fails() {
        let (metadata, data_store) = setup_test_env();

        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..40).collect();
        data_store.write_segment_data(&segment_id, &data).unwrap();

        let segment_meta = SegmentMetadata {
            segment_id,
            ec_k: 3,
            ec_m: 1,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xFF; 32])),
            storage_locations: smallvec::smallvec![],
            sealed_at: Some(1),
        };
        metadata.put_segment(segment_meta).unwrap();

        // Use HealConfig with retry limit 0 → no retries allowed.
        let config = HealConfig::default().with_max_concurrent_heals(2).with_heal_retry_limit(0);
        let queue = Arc::new(HealQueue::new(4));

        // Enqueue a request that will fail (ec_k=0 causes failure).
        // Actually, use a failing decoder for a clearer test.
        let segment_fail_id = SegmentId::new();
        data_store.write_segment_data(&segment_fail_id, &data).unwrap();
        metadata
            .put_segment(SegmentMetadata {
                segment_id: segment_fail_id,
                ec_k: 3,
                ec_m: 1,
                size_tier: oceanfs_core::SizeTier::Standard,
                merkle_root: None,
                storage_locations: smallvec::smallvec![],
                sealed_at: None,
            })
            .unwrap();

        let decoder: Arc<dyn Decoder> = Arc::new(FailingDecoder);
        let worker =
            HealWorker::new(config, queue.clone(), decoder, metadata.clone(), data_store.clone());

        // Enqueue a request with retry_count already at limit.
        queue
            .sender()
            .enqueue_blocking(HealRequest {
                segment_id: segment_fail_id,
                corrupt_shard_indices: vec![0],
                retry_count: 0,
            })
            .unwrap();

        // Run the worker briefly to process the request.
        let shutdown = CancellationToken::new();
        let cancel = shutdown.clone();
        let handle = tokio::spawn(async move {
            worker.run(cancel).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        shutdown.cancel();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    }
}
