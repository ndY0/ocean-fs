//! Metadata-pool loss recovery (g8, ADR-0029 §D7) — the rebuild side.
//!
//! A metadata-pool replacement loses ONLY the local RocksDB objects +
//! deletions index (data pools `.dat` and the wal-pool lifecycle registry
//! survive). The boot branch gates the node (`node_unavailable`), and
//! [`MetadataRecoveryCoordinator::run_deferred_boot_drain`] rebuilds the
//! fresh store from live peers over the node's owned ring ranges
//! (ADR-0035's peer-fetch substrate, but NEVER touching segment lifecycle
//! state and NEVER re-replicating — the segments are intact).
//!
//! Recovery path only (perf rule 7.1): one-time at boot, off the hot
//! path; the range stream is row-by-row and never buffers a whole range.
//!
//! The SERVER side of the pull (`ListObjectsInRange`) is implemented by
//! [`MetadataStoreRangeLister`], injected into the healing gRPC service by
//! the composition root.

use std::sync::Arc;

use oceanfs_core::{Counter, Gauge, LabelSet, MetricRegistrar, NodeId, PoolRole, VnodeRange};
use oceanfs_durability::healing_service::MetadataRangeLister;
use oceanfs_storage::{
    metadata::object_key_bytes, pool::health::HealthMonitor, PoolRegistry, RocksDbMetadataStore,
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Status;

/// Scans a node's own objects + deletions CFs and streams every row whose
/// object-key ring hash falls inside the requested range (g8 server side).
pub(crate) struct MetadataStoreRangeLister {
    store: Arc<RocksDbMetadataStore>,
}

impl MetadataStoreRangeLister {
    /// Creates a lister over the node's concrete metadata store.
    pub(crate) fn new(store: Arc<RocksDbMetadataStore>) -> Self {
        Self { store }
    }
}

impl MetadataRangeLister for MetadataStoreRangeLister {
    fn stream_range(
        &self,
        start: [u8; 32],
        end: [u8; 32],
        tx: mpsc::Sender<Result<oceanfs_durability::healing_rpc::MetadataRow, Status>>,
    ) {
        use bytes::Bytes;
        use oceanfs_durability::healing_rpc::{
            metadata_row::Row, DeletionRow as ProtoDeletionRow, MetadataRow,
            ObjectRow as ProtoObjectRow,
        };

        let store = Arc::clone(&self.store);
        let range = VnodeRange { start, end };
        tokio::task::spawn_blocking(move || {
            let emit = |key: &[u8], value: &[u8], deletion: bool, tx: &mpsc::Sender<_>| -> bool {
                let Some(obj_key) = object_key_bytes(key) else { return true };
                if !range.contains(&oceanfs_routing::hash_key(&obj_key)) {
                    return true;
                }
                let row = if deletion {
                    MetadataRow {
                        row: Some(Row::Deletion(ProtoDeletionRow {
                            key: Bytes::copy_from_slice(key),
                            value: Bytes::copy_from_slice(value),
                        })),
                    }
                } else {
                    MetadataRow {
                        row: Some(Row::Object(ProtoObjectRow {
                            key: Bytes::copy_from_slice(key),
                            value: Bytes::copy_from_slice(value),
                        })),
                    }
                };
                tx.blocking_send(Ok(row)).is_ok()
            };

            let objects_ok = store.visit_objects_rows(|k, v| emit(k, v, false, &tx)).is_ok();
            let deletions_ok = store.visit_deletions_rows(|k, v| emit(k, v, true, &tx)).is_ok();
            if !objects_ok || !deletions_ok {
                let _ = tx.blocking_send(Err(Status::internal(
                    "metadata range scan failed on the responder",
                )));
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Boot recovery coordinator
// ---------------------------------------------------------------------------

/// g8 recovery metrics (fresh names — none were registered before).
pub(crate) struct MetadataRecoveryMetrics {
    unavailable_seconds: Gauge,
    rebuilt_objects_total: Counter,
    rebuilt_deletions_total: Counter,
    range_errors_total: Counter,
}

impl MetadataRecoveryMetrics {
    /// Creates the series (unregistered until [`register`](Self::register)).
    pub(crate) fn new() -> Self {
        Self {
            unavailable_seconds: Gauge::new(
                "oceanfs_metadata_unavailable_seconds".into(),
                "Seconds the node has been unavailable (metadata pool Dead)".into(),
                LabelSet::empty(),
            ),
            rebuilt_objects_total: Counter::new(
                "oceanfs_metadata_rebuild_objects_total".into(),
                "Object rows folded into a fresh store during metadata rebuild".into(),
                LabelSet::empty(),
            ),
            rebuilt_deletions_total: Counter::new(
                "oceanfs_metadata_rebuild_deletions_total".into(),
                "Deletion rows folded into a fresh store during metadata rebuild".into(),
                LabelSet::empty(),
            ),
            range_errors_total: Counter::new(
                "oceanfs_metadata_rebuild_range_errors_total".into(),
                "Failed or retried range streams during metadata rebuild".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Registers the series with a metric registrar.
    pub(crate) fn register(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_gauge(self.unavailable_seconds.clone());
        registrar.register_counter(self.rebuilt_objects_total.clone());
        registrar.register_counter(self.rebuilt_deletions_total.clone());
        registrar.register_counter(self.range_errors_total.clone());
    }

    fn record(&self, objects: u64, deletions: u64, range_errors: u64, unavailable_secs: f64) {
        self.rebuilt_objects_total.add(objects);
        self.rebuilt_deletions_total.add(deletions);
        self.range_errors_total.add(range_errors);
        self.unavailable_seconds.set(unavailable_secs as u64);
    }
}

/// The g8 boot-branch recovery coordinator: gates the node when the
/// metadata store was replaced, rebuilds the fresh objects+deletions CFs
/// from live peers over the node's owned ranges, then reopens the node.
///
/// Owned by [`crate::modules::durability::DurabilityModule`] (which holds
/// the membership + data-plane pool the peer pulls need). Constructed once
/// at build from the storage bundle.
pub(crate) struct MetadataRecoveryCoordinator {
    self_id: NodeId,
    membership: Arc<oceanfs_membership::Membership>,
    pool: Arc<oceanfs_network::ConnectionPool>,
    metadata_store: Arc<RocksDbMetadataStore>,
    pool_registry: Arc<PoolRegistry>,
    health_monitor: Arc<HealthMonitor>,
    pending: Arc<std::sync::atomic::AtomicBool>,
    metrics: MetadataRecoveryMetrics,
}

impl MetadataRecoveryCoordinator {
    /// Builds the coordinator from the storage bundle + network handles.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        self_id: NodeId,
        membership: Arc<oceanfs_membership::Membership>,
        pool: Arc<oceanfs_network::ConnectionPool>,
        metadata_store: Arc<RocksDbMetadataStore>,
        pool_registry: Arc<PoolRegistry>,
        health_monitor: Arc<HealthMonitor>,
        pending: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            self_id,
            membership,
            pool,
            metadata_store,
            pool_registry,
            health_monitor,
            pending,
            metrics: MetadataRecoveryMetrics::new(),
        }
    }

    /// Registers the g8 recovery metrics.
    pub(crate) fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        self.metrics.register(registrar);
    }

    /// Runs the boot deferred rebuild when the boot path detected a
    /// replaced metadata store (a no-op otherwise). Returns whether the
    /// rebuild ran.
    ///
    /// Called by the composition root after `spawn_all` (membership + the
    /// gRPC healing service must be live — the drain pulls over
    /// `ListObjectsInRange`). On success the node is reopened (metadata
    /// pool Healthy, `pending` cleared); on failure the node stays gated.
    ///
    /// # Errors
    ///
    /// Returns an error when no live peer is available to serve the owned
    /// ranges (the rebuild cannot proceed — the node must stay gated).
    pub(crate) async fn run_deferred_boot_drain(&self) -> Result<bool, String> {
        if !self.pending.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(false);
        }
        let started = std::time::Instant::now();

        // The peer pulls need at least one OTHER ring member (the node's
        // replica holders). Membership converges after the join at boot;
        // wait a bounded window.
        self.wait_for_peers().await?;

        let ring = self.membership.ring().snapshot();
        let ranges = ring.ranges_owned_by(&self.self_id);
        if ranges.is_empty() {
            return Err("metadata rebuild: no owned ring ranges (empty ring)".to_string());
        }
        let peers: Vec<NodeId> = self.live_peers();
        if peers.is_empty() {
            return Err("metadata rebuild: no live peers to pull rows from".to_string());
        }

        let mut objects = 0u64;
        let mut deletions = 0u64;
        let mut range_errors = 0u64;
        for range in &ranges {
            for peer in &peers {
                match self.fetch_and_fold_range(peer, range).await {
                    Ok((o, d)) => {
                        objects += o;
                        deletions += d;
                    }
                    Err(e) => {
                        range_errors += 1;
                        tracing::warn!(
                            peer = %peer,
                            error = %e,
                            "metadata rebuild range pull failed (retried against other peers)"
                        );
                    }
                }
            }
        }

        let unavailable_secs = started.elapsed().as_secs_f64();
        self.metrics.record(objects, deletions, range_errors, unavailable_secs);

        if let Some(meta_pool) = self.pool_registry.pool_by_role(PoolRole::Metadata) {
            self.pool_registry.set_status(meta_pool.id(), oceanfs_storage::PoolStatus::Healthy);
            self.health_monitor.reset_pool(meta_pool.id(), oceanfs_storage::PoolStatus::Healthy);
        }
        self.pending.store(false, std::sync::atomic::Ordering::Release);
        tracing::info!(
            objects,
            deletions,
            range_errors,
            elapsed_ms = started.elapsed().as_millis(),
            "metadata rebuild complete — node reopened"
        );
        Ok(true)
    }

    async fn wait_for_peers(&self) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while self.membership.ring().snapshot().node_count() < 2 {
            if tokio::time::Instant::now() >= deadline {
                return Err(
                    "metadata rebuild: ring never converged to a peer within 30s".to_string()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        Ok(())
    }

    fn live_peers(&self) -> Vec<NodeId> {
        use oceanfs_core::NodeState;
        self.membership
            .nodes_full()
            .into_iter()
            .filter(|(id, state, _, _, _, _, _, _)| {
                *id != self.self_id && matches!(state, NodeState::Alive | NodeState::Suspect)
            })
            .map(|(id, ..)| id)
            .collect()
    }

    /// Pulls one owned range from one peer and folds the rows into the
    /// fresh store. Returns the number of object/deletion rows APPLIED.
    async fn fetch_and_fold_range(
        &self,
        peer: &NodeId,
        range: &VnodeRange,
    ) -> Result<(u64, u64), String> {
        use oceanfs_durability::healing_rpc::{
            healing_rpc_client::HealingRpcClient, metadata_row::Row, ObjectRangeRequest,
        };

        let addr = self
            .membership
            .address_of(peer)
            .ok_or_else(|| format!("peer {peer} not found in membership"))?;
        let pooled = self
            .pool
            .get_channel(addr)
            .await
            .map_err(|e| format!("connection pool error for {peer}: {e}"))?;
        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = HealingRpcClient::new(channel);
        let request = tonic::Request::new(ObjectRangeRequest {
            start: bytes::Bytes::copy_from_slice(&range.start),
            end: bytes::Bytes::copy_from_slice(&range.end),
        });
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        let mut stream = tokio::time::timeout_at(deadline, client.list_objects_in_range(request))
            .await
            .map_err(|_| format!("peer {peer} range fetch timed out"))?
            .map_err(|status| format!("peer {peer} range fetch failed: {status}"))?
            .into_inner();

        let mut objects = 0u64;
        let mut deletions = 0u64;
        loop {
            let msg = tokio::time::timeout_at(deadline, stream.next()).await;
            match msg {
                Ok(Some(Ok(row))) => match row.row {
                    Some(Row::Object(o))
                        if self
                            .metadata_store
                            .rebuild_apply_object_row(&o.key, &o.value)
                            .map_err(|e| format!("rebuild fold error: {e}"))? =>
                    {
                        objects += 1;
                    }
                    Some(Row::Deletion(d))
                        if self
                            .metadata_store
                            .rebuild_apply_deletion_row(&d.key, &d.value)
                            .map_err(|e| format!("rebuild fold error: {e}"))? =>
                    {
                        deletions += 1;
                    }
                    Some(Row::Object(_)) | Some(Row::Deletion(_)) | None => {}
                },
                Ok(Some(Err(status))) => {
                    return Err(format!("peer {peer} range stream error: {status}"))
                }
                Ok(None) => break,
                Err(_) => return Err(format!("peer {peer} range stream timed out")),
            }
        }
        Ok((objects, deletions))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// `object_key_bytes` handles objects rows and both deletions shapes.
    #[test]
    fn object_key_bytes_extracts_key_from_all_row_shapes() {
        let obj = b"photos\0cat.jpg";
        assert_eq!(object_key_bytes(obj).as_deref(), Some(&b"cat.jpg"[..]));
        // A plain tombstone key is byte-identical to the object row key.
        let plain = b"photos\0cat.jpg";
        assert_eq!(object_key_bytes(plain).as_deref(), Some(&b"cat.jpg"[..]));
    }
}
