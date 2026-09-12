//! Tier-1 `metadata_sync` worker (ADR-0038 D4; ae2 / S2).
//!
//! One node-local worker owned by `oceanfs-durability`. Each cycle it
//! pulls every alive peer's journal past its durable watermark, filters
//! the entries to keys this node **currently** co-owns, coalesces the
//! latest entry per key, point-fetches the keys' current state from
//! manifest-healthy co-holders, and applies it through the store's
//! HLC-LWW guards. It **never pushes**, never scans the keyspace, and its
//! per-cycle entry/byte budgets bound the work.
//!
//! Gaps (trimmed ranges, foreign epochs) increment
//! `oceanfs_metadata_sync_gap_detected_total` and enqueue a bootstrap
//! through [`BootstrapEnqueuer`] — the S3 consumer; S2 only records the
//! signal.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use oceanfs_core::{
    BucketId, Counter, Gauge, Hlc, LabelSet, MetadataSyncConfig, MetricRegistrar, NodeId,
    NodeState, ObjectKey, ObjectMetadata, SharedMetricRegistrar, Tombstone,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::hash_key;
use oceanfs_storage::{
    metadata::{journal::JournalEntry, split_object_row_key},
    JournalOp, MetadataJournal, RocksDbMetadataStore, SyncApplyOutcome,
};
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::{Mutex, Semaphore};
use tokio_stream::StreamExt;
use tonic::Request;

use crate::{
    error::{Error, Result},
    healing_rpc::{metadata_row, FetchJournalRequest, MetadataRow, MetadataRowsRequest},
    metadata_sync::watermark::WatermarkStore,
    scheduler::{DurabilityBudget, DurabilityTask, KeyspaceWindow},
    HealingRpcClient,
};

/// Why a triggered bootstrap was requested.
///
/// # Examples
///
/// ```
/// use oceanfs_durability::BootstrapReason;
///
/// let reason = BootstrapReason::Gap;
/// assert!(matches!(reason, BootstrapReason::Gap));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BootstrapReason {
    /// The peer's watermark fell below its journal's replayable floor.
    Gap,
    /// The peer's journal epoch changed with prior progress.
    EpochChange,
    /// A hard journal cap forced a trim past the peer's watermark.
    CapTrim,
}

/// Sink for triggered bootstrap requests (the S3 consumer).
///
/// # Examples
///
/// ```ignore
/// struct LoggingSink;
/// impl BootstrapEnqueuer for LoggingSink {
///     fn enqueue(&self, peer: &NodeId, reason: BootstrapReason) {
///         tracing::info!(%peer, ?reason, "bootstrap requested");
///     }
/// }
/// ```
///
/// S2 records the signal; the implementation that actually walks owned
/// ranges lands with S3. The node wires a stub (or nothing) in S2.
pub trait BootstrapEnqueuer: Send + Sync {
    /// Enqueues a triggered, range-bounded bootstrap for the range the
    /// node owns from `peer`'s plane.
    fn enqueue(&self, peer: &NodeId, reason: BootstrapReason);
}

/// The `oceanfs_metadata_sync_*` metric handles (ADR-0038 D9 S2 subset).
///
/// # Examples
///
/// ```ignore
/// let metrics = MetadataSyncMetrics::new();
/// metrics.register(&registry); // only when the worker is enabled
/// ```
#[derive(Clone)]
pub struct MetadataSyncMetrics {
    /// `oceanfs_metadata_sync_entries_consumed_total`.
    pub entries_consumed_total: Counter,
    /// `oceanfs_metadata_sync_keys_skipped_total{reason="not_owner"}`.
    pub keys_skipped_not_owner: Counter,
    /// `oceanfs_metadata_sync_keys_skipped_total{reason="already_current"}`.
    pub keys_skipped_already_current: Counter,
    /// `oceanfs_metadata_sync_rows_pulled_total`.
    pub rows_pulled_total: Counter,
    /// `oceanfs_metadata_sync_rows_applied_total`.
    pub rows_applied_total: Counter,
    /// `oceanfs_metadata_sync_rows_rejected_total{reason="lww"}`.
    pub rows_rejected_lww: Counter,
    /// `oceanfs_metadata_sync_rows_rejected_total{reason="tombstone"}`.
    pub rows_rejected_tombstone: Counter,
    /// `oceanfs_metadata_sync_rows_rejected_total{reason="hash_mismatch"}`.
    pub rows_rejected_hash_mismatch: Counter,
    /// `oceanfs_metadata_sync_gap_detected_total`.
    pub gap_detected_total: Counter,
    lag_gauges: Arc<DashMap<NodeId, Gauge>>,
    registrar: Arc<ParkingMutex<Option<SharedMetricRegistrar>>>,
}

impl MetadataSyncMetrics {
    /// Creates the metric handles (unregistered until
    /// [`Self::register`]).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let metrics = MetadataSyncMetrics::new();
    /// metrics.register(&registry);
    /// ```
    pub fn new() -> Self {
        let reason = |value: &str| LabelSet::new(&[("reason", value)]);
        Self {
            entries_consumed_total: Counter::new(
                "oceanfs_metadata_sync_entries_consumed_total".into(),
                "Metadata journal entries consumed from peers".into(),
                LabelSet::empty(),
            ),
            keys_skipped_not_owner: Counter::new(
                "oceanfs_metadata_sync_keys_skipped_total".into(),
                "Metadata sync keys skipped by reason".into(),
                reason("not_owner"),
            ),
            keys_skipped_already_current: Counter::new(
                "oceanfs_metadata_sync_keys_skipped_total".into(),
                "Metadata sync keys skipped by reason".into(),
                reason("already_current"),
            ),
            rows_pulled_total: Counter::new(
                "oceanfs_metadata_sync_rows_pulled_total".into(),
                "Current-state metadata rows pulled by the sync worker".into(),
                LabelSet::empty(),
            ),
            rows_applied_total: Counter::new(
                "oceanfs_metadata_sync_rows_applied_total".into(),
                "Metadata rows applied through the LWW guard".into(),
                LabelSet::empty(),
            ),
            rows_rejected_lww: Counter::new(
                "oceanfs_metadata_sync_rows_rejected_total".into(),
                "Metadata rows rejected by the sync worker, by reason".into(),
                reason("lww"),
            ),
            rows_rejected_tombstone: Counter::new(
                "oceanfs_metadata_sync_rows_rejected_total".into(),
                "Metadata rows rejected by the sync worker, by reason".into(),
                reason("tombstone"),
            ),
            rows_rejected_hash_mismatch: Counter::new(
                "oceanfs_metadata_sync_rows_rejected_total".into(),
                "Metadata rows rejected by the sync worker, by reason".into(),
                reason("hash_mismatch"),
            ),
            gap_detected_total: Counter::new(
                "oceanfs_metadata_sync_gap_detected_total".into(),
                "Journal gaps forcing a triggered bootstrap".into(),
                LabelSet::empty(),
            ),
            lag_gauges: Arc::new(DashMap::new()),
            registrar: Arc::new(ParkingMutex::new(None)),
        }
    }

    /// Stores the shared registrar used to lazily register per-peer lag
    /// gauges (called by the composition root when enabled).
    /// # Examples
    ///
    /// ```ignore
    /// metrics.set_registrar(registrar.clone());
    /// ```
    pub fn set_registrar(&self, registrar: SharedMetricRegistrar) {
        *self.registrar.lock() = Some(registrar);
    }

    /// Registers the static series (and any lag gauges created so far).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// MetadataSyncMetrics::new().register(&registry);
    /// ```
    pub fn register(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.entries_consumed_total.clone());
        registrar.register_counter(self.keys_skipped_not_owner.clone());
        registrar.register_counter(self.keys_skipped_already_current.clone());
        registrar.register_counter(self.rows_pulled_total.clone());
        registrar.register_counter(self.rows_applied_total.clone());
        registrar.register_counter(self.rows_rejected_lww.clone());
        registrar.register_counter(self.rows_rejected_tombstone.clone());
        registrar.register_counter(self.rows_rejected_hash_mismatch.clone());
        registrar.register_counter(self.gap_detected_total.clone());
        for gauge in self.lag_gauges.iter() {
            registrar.register_gauge(gauge.value().clone());
        }
    }

    fn set_lag(&self, peer: &NodeId, seconds: u64) {
        if let Some(gauge) = self.lag_gauges.get(peer) {
            gauge.set(seconds);
            return;
        }
        let gauge = Gauge::new(
            "oceanfs_metadata_sync_watermark_lag_seconds".into(),
            "Age of the oldest unconsumed journal entry per peer".into(),
            LabelSet::new(&[("peer", peer.as_str())]),
        );
        gauge.set(seconds);
        if let Some(registrar) = self.registrar.lock().as_ref() {
            registrar.register_gauge(gauge.clone());
        }
        self.lag_gauges.insert(peer.clone(), gauge);
    }
}

impl Default for MetadataSyncMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-cycle entry/byte budget shared across peers.
struct CycleBudget {
    entries_left: u64,
    bytes_left: u64,
}

impl CycleBudget {
    fn new(entries: u64, bytes: u64) -> Self {
        Self { entries_left: entries, bytes_left: bytes }
    }

    fn exhausted(&self) -> bool {
        self.entries_left == 0 || self.bytes_left == 0
    }

    fn consume_entries(&mut self, count: u64) {
        self.entries_left = self.entries_left.saturating_sub(count);
    }

    fn consume_bytes(&mut self, bytes: u64) {
        self.bytes_left = self.bytes_left.saturating_sub(bytes);
    }
}

/// What a point-fetched row did on apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppliedRow {
    Applied,
    AlreadyCurrent,
    LocalLww,
    /// A pulled delete lost to a newer local live row.
    LocalTombstone,
    HashMismatch,
}

/// The Tier-1 metadata sync worker.
///
/// # Examples
///
/// ```ignore
/// let worker = MetadataSync::new(
///     node_id, &config, membership, pool, timeout, store, journal, watermarks,
/// )?;
/// scheduler.register(Arc::new(worker) as Arc<dyn DurabilityTask>);
/// worker.run_cycle_once().await?;
/// ```
pub struct MetadataSync {
    node_id: NodeId,
    interval: Duration,
    max_entries_per_cycle: u64,
    max_bytes_per_cycle: u64,
    journal_max_bytes: u64,
    journal_max_age: Duration,
    membership: Arc<Membership>,
    pool: Arc<ConnectionPool>,
    rpc_timeout: Duration,
    metadata: Arc<RocksDbMetadataStore>,
    journal: Arc<MetadataJournal>,
    watermarks: Arc<WatermarkStore>,
    bootstrap: Option<Arc<dyn BootstrapEnqueuer>>,
    metrics: MetadataSyncMetrics,
    inflight: Arc<Semaphore>,
    cycle_lock: Mutex<()>,
}

impl std::fmt::Debug for MetadataSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataSync")
            .field("node_id", &self.node_id)
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

impl MetadataSync {
    /// Builds the worker from the `[metadata_sync]` configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when `max_inflight_pulls` is zero (validated at
    /// config load, defended here too).
    #[allow(clippy::too_many_arguments)] // composition-root wiring constructor
    /// # Examples
    ///
    /// ```ignore
    /// let worker = MetadataSync::new(
    ///     node_id, &config, membership, pool, timeout, store, journal, watermarks,
    /// )?;
    /// ```
    pub fn new(
        node_id: NodeId,
        config: &MetadataSyncConfig,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
        rpc_timeout: Duration,
        metadata: Arc<RocksDbMetadataStore>,
        journal: Arc<MetadataJournal>,
        watermarks: Arc<WatermarkStore>,
    ) -> Result<Self> {
        if config.max_inflight_pulls == 0 {
            return Err(Error::InvalidConfig(
                "metadata_sync.max_inflight_pulls must be at least 1".into(),
            ));
        }
        Ok(Self {
            node_id,
            interval: Duration::from_secs(config.interval_sec.max(1)),
            max_entries_per_cycle: config.max_entries_per_cycle,
            max_bytes_per_cycle: config.max_bytes_per_cycle,
            journal_max_bytes: config.journal_max_bytes,
            journal_max_age: Duration::from_secs(config.journal_max_age_secs),
            membership,
            pool,
            rpc_timeout,
            metadata,
            journal,
            watermarks,
            bootstrap: None,
            metrics: MetadataSyncMetrics::new(),
            inflight: Arc::new(Semaphore::new(config.max_inflight_pulls)),
            cycle_lock: Mutex::new(()),
        })
    }

    /// Installs the bootstrap enqueuer (S3 seam).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let worker = worker.with_bootstrap(Arc::new(MyBootstrapSink));
    /// ```
    #[must_use]
    pub fn with_bootstrap(mut self, bootstrap: Arc<dyn BootstrapEnqueuer>) -> Self {
        self.bootstrap = Some(bootstrap);
        self
    }

    /// Stores the shared registrar used for the dynamic per-peer lag
    /// gauges.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let worker = worker.with_metric_registrar(registrar.clone());
    /// ```
    #[must_use]
    pub fn with_metric_registrar(self, registrar: SharedMetricRegistrar) -> Self {
        self.metrics.set_registrar(registrar);
        self
    }

    /// Returns the metric handles.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// worker.metrics().entries_consumed_total.get();
    /// ```
    pub fn metrics(&self) -> &MetadataSyncMetrics {
        &self.metrics
    }

    /// Registers the `oceanfs_metadata_sync_*` series.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// worker.register_metrics(&*metrics_registry);
    /// ```
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        self.metrics.register(registrar);
    }

    /// Runs one serialized cycle (no budget acquisition). The scheduler
    /// already holds the Tier-1 permit; the membership-event path calls
    /// [`Self::run_event_cycle`] instead.
    ///
    /// Returns the number of peers contacted.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let contacted = worker.run_cycle_once().await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when retention enforcement fails.
    pub async fn run_cycle_once(&self) -> Result<usize> {
        let _guard = self.cycle_lock.lock().await;
        self.cycle_locked().await
    }

    /// The cycle body (caller holds the cycle lock).
    async fn cycle_locked(&self) -> Result<usize> {
        let ring = self.membership.ring().snapshot();
        let peers = self.pull_candidates();
        let mut budget = CycleBudget::new(self.max_entries_per_cycle, self.max_bytes_per_cycle);
        let mut contacted = 0usize;
        for peer in peers {
            if budget.exhausted() {
                break;
            }
            if let Err(error) = self.pull_peer(&peer, &ring, &mut budget).await {
                tracing::debug!(peer = %peer, %error, "metadata_sync: peer pull failed");
            }
            contacted += 1;
        }
        self.trim(&ring)?;
        if let Err(error) = self.watermarks.flush() {
            tracing::warn!(%error, "metadata_sync: watermark flush failed");
        }
        Ok(contacted)
    }

    /// Runs one event-triggered cycle (peer became Alive), acquiring its
    /// own Tier-1 permit so the scheduler's budget accounting still
    /// holds. If a cycle is already in flight the wake is skipped (the
    /// periodic cycle covers it) — event storms never queue.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// worker.run_event_cycle(&budget).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when retention enforcement fails.
    pub async fn run_event_cycle(&self, budget: &DurabilityBudget) -> Result<usize> {
        let _permit = budget.acquire_housekeeping().await;
        // Never queue event cycles behind a running one: if a cycle is
        // already in flight the periodic/scheduler cycle covers the wake,
        // so skip instead of serializing an event storm.
        let Ok(_guard) = self.cycle_lock.try_lock() else {
            return Ok(0);
        };
        self.cycle_locked().await
    }

    fn pull_candidates(&self) -> Vec<NodeId> {
        self.membership
            .nodes()
            .into_iter()
            .filter(|(id, state)| {
                *id != self.node_id && matches!(state, NodeState::Alive | NodeState::Suspect)
            })
            .map(|(id, _)| id)
            .collect()
    }

    /// One peer's pull: FetchJournal → ownership/local filter → coalesce →
    /// point fetch → apply → watermark advance.
    async fn pull_peer(
        &self,
        peer: &NodeId,
        ring: &oceanfs_routing::Ring,
        budget: &mut CycleBudget,
    ) -> Result<()> {
        let watermark = self.watermarks.get(peer);
        let request = FetchJournalRequest {
            epoch: watermark.epoch.map(|epoch| Bytes::copy_from_slice(&epoch)).unwrap_or_default(),
            from_seq: watermark.consumed,
            max_entries: budget.entries_left.min(u64::from(u32::MAX)) as u32,
            max_bytes: budget.bytes_left,
            requester_id: Bytes::copy_from_slice(self.node_id.as_str().as_bytes()),
        };
        let Some(address) = self.membership.address_of(peer) else {
            return Ok(());
        };
        let pooled = match self.pool.get_channel(address).await {
            Ok(pooled) => pooled,
            Err(error) => {
                tracing::debug!(peer = %peer, %error, "metadata_sync: no channel");
                return Ok(());
            }
        };
        let mut client = HealingRpcClient::new(pooled.channel().clone());
        let response = match tokio::time::timeout(
            self.rpc_timeout,
            client.fetch_journal(Request::new(request)),
        )
        .await
        {
            Err(_) => return Ok(()),
            Ok(Err(status)) => {
                if status.code() == tonic::Code::Unavailable {
                    // Mixed kill-switch mesh: a disabled peer is
                    // skipped and must not pin the trim frontier.
                    self.watermarks.mark_non_consuming(peer);
                }
                return Ok(());
            }
            Ok(Ok(response)) => response.into_inner(),
        };
        self.watermarks.mark_consuming(peer);

        let Some(epoch) = decode_epoch(&response.epoch) else {
            return Ok(());
        };
        if response.acknowledged_seq > 0
            && decode_epoch(&response.acknowledged_epoch) == Some(*self.journal.epoch().as_bytes())
        {
            // Accept the responder's view of our journal only when it
            // names our CURRENT epoch; a stale-epoch ack is meaningless.
            self.watermarks.set_acked(
                peer,
                *self.journal.epoch().as_bytes(),
                response.acknowledged_seq,
            );
        }

        // Gap: a foreign epoch with prior progress.
        if watermark.epoch != Some(epoch) && watermark.consumed > 0 {
            self.metrics.gap_detected_total.inc();
            self.enqueue_bootstrap(peer, BootstrapReason::EpochChange);
            self.watermarks.set_consumed(peer, epoch, 0);
            return Ok(());
        }
        // Gap: the requested position fell below the replayable floor.
        if watermark.consumed < response.oldest_seq {
            self.metrics.gap_detected_total.inc();
            self.enqueue_bootstrap(peer, BootstrapReason::Gap);
            self.watermarks.set_consumed(peer, epoch, response.oldest_seq);
            return Ok(());
        }
        if watermark.epoch != Some(epoch) {
            self.watermarks.set_consumed(peer, epoch, watermark.consumed);
        }

        let next_seq = response.next_seq;
        let wire_entries = response.entries;
        let entry_count = wire_entries.len() as u64;
        budget.consume_entries(entry_count);
        self.metrics.entries_consumed_total.add(entry_count);

        // Watermark-lag gauge: age of the oldest unconsumed entry as
        // observed on this pull (records carry HLCs, not append times).
        if let Some(first) = wire_entries.first().and_then(|entry| entry.hlc.as_ref()) {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let lag = now_ms.saturating_sub(first.wall_time) / 1000;
            self.metrics.set_lag(peer, lag);
        } else {
            self.metrics.set_lag(peer, 0);
        }

        let entries: Vec<JournalEntry> =
            wire_entries.into_iter().filter_map(decode_entry).collect();
        let mut to_fetch: Vec<FetchKey> = Vec::new();
        for entry in coalesce_latest(entries) {
            let Some((bucket, key)) = split_object_row_key(&entry.key) else {
                continue;
            };
            let holders = ring.lookup(&hash_key(key.as_bytes()));
            if !holders.contains(&self.node_id) {
                self.metrics.keys_skipped_not_owner.inc();
                continue;
            }
            let bucket_id = BucketId::new(bucket);
            let object_key = ObjectKey::new(key);
            let local_row = self.metadata.get_object(&bucket_id, &object_key).ok().flatten();
            let local_tombstone =
                self.metadata.get_tombstone(&bucket_id, &object_key).ok().flatten();
            if local_skip_is_current(&entry, local_row.as_ref(), local_tombstone.as_ref()) {
                self.metrics.keys_skipped_already_current.inc();
                continue;
            }
            to_fetch.push(FetchKey {
                full_key: entry.key.clone(),
                holders: holders.into_iter().filter(|h| *h != self.node_id).collect(),
                seq: entry.seq,
            });
        }

        let unresolved_min = if to_fetch.is_empty() {
            None
        } else {
            self.point_fetch_and_apply(&to_fetch, budget).await
        };
        // The watermark may only advance past entries whose current state
        // was actually resolved (applied, already current, locally newer,
        // or a definitive holder response). An entry whose point fetch
        // never got an answer (transport failure, no eligible holder,
        // budget exhaustion) keeps the watermark behind it: the next
        // cycle retries it rather than dropping the change silently.
        let advance_to = advance_watermark(watermark.consumed, next_seq, unresolved_min);
        self.watermarks.set_consumed(peer, epoch, advance_to);
        Ok(())
    }

    /// Point-fetches the keys' current state from up to three eligible
    /// co-holders and applies each returned row through the store's
    /// HLC-LWW guards.
    ///
    /// Returns the smallest sequence of a key that was **not answered**
    /// by any holder (the watermark must not advance past it), or `None`
    /// when every key was resolved.
    async fn point_fetch_and_apply(
        &self,
        keys: &[FetchKey],
        budget: &mut CycleBudget,
    ) -> Option<u64> {
        let mut pending: Vec<usize> = (0..keys.len()).collect();
        // A key is resolved once any holder returned a definitive
        // response for it (even an absent row — J2 treats a key absent
        // everywhere as a consumed no-op). A transport failure leaves it
        // unresolved so the change is retried, never dropped.
        let mut answered: Vec<bool> = vec![false; keys.len()];
        for attempt in 0..3 {
            if pending.is_empty() || budget.exhausted() {
                break;
            }
            let mut groups: HashMap<NodeId, Vec<usize>> = HashMap::new();
            for &index in &pending {
                let eligible: Vec<NodeId> = keys[index]
                    .holders
                    .iter()
                    .filter(|holder| self.holder_eligible(holder))
                    .cloned()
                    .collect();
                // Try a DIFFERENT holder per attempt: retrying the same
                // holder adds no information (an absent answer is
                // definitive) and double-fetches under a tight budget.
                let Some(holder) = eligible.get(attempt).cloned() else {
                    continue;
                };
                groups.entry(holder).or_default().push(index);
            }
            if groups.is_empty() {
                break;
            }

            let mut joinset = tokio::task::JoinSet::new();
            for (holder, indexes) in groups {
                let request_keys: Vec<Bytes> =
                    indexes.iter().map(|&index| keys[index].full_key.clone()).collect();
                let Ok(permit) = self.inflight.clone().acquire_owned().await else {
                    break;
                };
                let pool = Arc::clone(&self.pool);
                let membership = Arc::clone(&self.membership);
                let timeout = self.rpc_timeout;
                let max_bytes = budget.bytes_left;
                joinset.spawn(async move {
                    let _permit = permit;
                    let result = fetch_rows_raw(
                        &pool,
                        &membership,
                        timeout,
                        &holder,
                        request_keys,
                        max_bytes,
                    )
                    .await;
                    (indexes, result)
                });
            }

            let mut returned: HashMap<Bytes, MetadataRow> = HashMap::new();
            while let Some(joined) = joinset.join_next().await {
                let Ok((indexes, Ok((rows, truncated)))) = joined else { continue };
                if !truncated {
                    // A complete response answers every requested key
                    // (an absent row is a definitive answer — J2).
                    for &index in &indexes {
                        answered[index] = true;
                    }
                } else {
                    // A budget-truncated stream answered only the rows it
                    // actually delivered; the rest stay unresolved so the
                    // watermark never advances past them.
                    for row in &rows {
                        if let Some(key) = row_key(row) {
                            for &index in &indexes {
                                if keys[index].full_key == key {
                                    answered[index] = true;
                                }
                            }
                        }
                    }
                }
                for row in rows {
                    let Some(key) = row_key(&row) else { continue };
                    let record_bytes = match row.row.as_ref() {
                        Some(metadata_row::Row::Object(object)) => {
                            object.key.len() + object.value.len() + 16
                        }
                        Some(metadata_row::Row::Deletion(deletion)) => {
                            deletion.key.len() + deletion.value.len() + 16
                        }
                        None => 0,
                    };
                    budget.consume_bytes(record_bytes as u64);
                    returned.insert(key, row);
                }
            }
            if returned.is_empty() {
                continue;
            }

            let mut applied_keys = Vec::new();
            for &index in &pending {
                let Some(row) = returned.get(&keys[index].full_key).cloned() else {
                    continue;
                };
                self.metrics.rows_pulled_total.inc();
                match self.apply_row(&row) {
                    AppliedRow::Applied => self.metrics.rows_applied_total.inc(),
                    AppliedRow::AlreadyCurrent => {}
                    AppliedRow::LocalLww => self.metrics.rows_rejected_lww.inc(),
                    AppliedRow::LocalTombstone => self.metrics.rows_rejected_tombstone.inc(),
                    AppliedRow::HashMismatch => self.metrics.rows_rejected_hash_mismatch.inc(),
                }
                applied_keys.push(index);
            }
            pending.retain(|index| !applied_keys.contains(index));
        }

        keys.iter().enumerate().filter(|(index, _)| !answered[*index]).map(|(_, key)| key.seq).min()
    }

    /// Applies one fetched row through the store's LWW guards.
    fn apply_row(&self, row: &MetadataRow) -> AppliedRow {
        match row.row.as_ref() {
            Some(metadata_row::Row::Object(object)) => {
                let Some((bucket, key)) = split_object_row_key(&object.key) else {
                    return AppliedRow::HashMismatch;
                };
                let Ok(meta) = bincode::deserialize::<ObjectMetadata>(&object.value) else {
                    return AppliedRow::HashMismatch;
                };
                if meta.object_key.as_str() != key {
                    return AppliedRow::HashMismatch;
                }
                match self.metadata.sync_apply_object(&BucketId::new(bucket), meta) {
                    Ok(SyncApplyOutcome::Applied) => AppliedRow::Applied,
                    Ok(SyncApplyOutcome::AlreadyCurrent) => AppliedRow::AlreadyCurrent,
                    Ok(SyncApplyOutcome::LocalWins) => AppliedRow::LocalLww,
                    Ok(_) => AppliedRow::AlreadyCurrent,
                    Err(error) => {
                        tracing::warn!(%error, "metadata_sync: object apply failed");
                        AppliedRow::HashMismatch
                    }
                }
            }
            Some(metadata_row::Row::Deletion(deletion)) => {
                let Some((bucket, key)) = split_object_row_key(&deletion.key) else {
                    return AppliedRow::HashMismatch;
                };
                let Ok(tombstone) = bincode::deserialize::<Tombstone>(&deletion.value) else {
                    return AppliedRow::HashMismatch;
                };
                match self.metadata.sync_apply_tombstone(
                    &BucketId::new(bucket),
                    &ObjectKey::new(key),
                    tombstone,
                ) {
                    Ok(SyncApplyOutcome::Applied) => AppliedRow::Applied,
                    Ok(SyncApplyOutcome::AlreadyCurrent) => AppliedRow::AlreadyCurrent,
                    Ok(SyncApplyOutcome::LocalWins) => AppliedRow::LocalTombstone,
                    Ok(_) => AppliedRow::AlreadyCurrent,
                    Err(error) => {
                        tracing::warn!(%error, "metadata_sync: tombstone apply failed");
                        AppliedRow::HashMismatch
                    }
                }
            }
            None => AppliedRow::HashMismatch,
        }
    }

    /// Whether a peer is an eligible co-holder: alive/suspect and, when a
    /// manifest is known, able to accept writes (the same predicate the
    /// write path uses; a manifest miss stays eligible).
    fn holder_eligible(&self, holder: &NodeId) -> bool {
        match self.membership.state_of(holder) {
            Some(NodeState::Alive | NodeState::Suspect) => {}
            _ => return false,
        }
        match self.membership.manifest_of(holder) {
            Some(manifest) => manifest_can_accept_writes(&manifest),
            None => true,
        }
    }

    /// Journal retention: trim below the minimum ack over retained
    /// (Alive/Suspect/Dead) peers; the hard caps override and open a gap.
    fn trim(&self, _ring: &oceanfs_routing::Ring) -> Result<()> {
        let retained: Vec<NodeId> = self
            .membership
            .nodes()
            .into_iter()
            .filter(|(id, state)| {
                *id != self.node_id
                    && matches!(state, NodeState::Alive | NodeState::Suspect | NodeState::Dead)
            })
            .map(|(id, _)| id)
            .collect();
        let frontier = self
            .watermarks
            .frontier(&retained, *self.journal.epoch().as_bytes())
            .unwrap_or_else(|| self.journal.last_seq());
        let report = self.journal.enforce_retention(
            frontier,
            self.journal_max_bytes,
            self.journal_max_age,
        )?;
        if report.cap_forced {
            self.metrics.gap_detected_total.inc();
            for peer in &retained {
                // Non-consuming (disabled) peers are excluded from the
                // frontier by construction and cannot be "passed" by a
                // cap trim — no signal for them.
                if !self.watermarks.is_consuming(peer) {
                    continue;
                }
                let acked = self.watermarks.get(peer);
                let current = acked.acked_epoch == Some(*self.journal.epoch().as_bytes());
                if !current || acked.acked < report.base_seq {
                    self.enqueue_bootstrap(peer, BootstrapReason::CapTrim);
                }
            }
        }
        Ok(())
    }

    fn enqueue_bootstrap(&self, peer: &NodeId, reason: BootstrapReason) {
        if let Some(bootstrap) = &self.bootstrap {
            bootstrap.enqueue(peer, reason);
        }
    }
}

#[async_trait]
impl DurabilityTask for MetadataSync {
    fn name(&self) -> &'static str {
        "metadata_sync"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn keyspace_fraction(&self) -> f64 {
        1.0
    }

    async fn run_cycle(&self, window: KeyspaceWindow) -> Result<u64> {
        match window {
            KeyspaceWindow::Full => {}
            KeyspaceWindow::Shard { .. } => {
                return Err(Error::Internal(
                    "metadata_sync operates on the journal, not keyspace shards".into(),
                ))
            }
        }
        Ok(self.run_cycle_once().await? as u64)
    }
}

struct FetchKey {
    full_key: Bytes,
    holders: Vec<NodeId>,
    /// The journal entry's sequence (the watermark may only advance past
    /// keys that were actually resolved).
    seq: u64,
}

/// Fetches current-state rows from one holder. Free function so it can be
/// spawned without borrowing the worker (`max_inflight_pulls`).
async fn fetch_rows_raw(
    pool: &ConnectionPool,
    membership: &Membership,
    timeout: Duration,
    holder: &NodeId,
    keys: Vec<Bytes>,
    max_bytes: u64,
) -> std::result::Result<(Vec<MetadataRow>, bool), String> {
    let address = membership.address_of(holder).ok_or_else(|| "no address".to_string())?;
    let pooled = pool.get_channel(address).await.map_err(|error| error.to_string())?;
    let mut client = HealingRpcClient::new(pooled.channel().clone());
    let request = Request::new(MetadataRowsRequest { keys });
    let deadline = tokio::time::Instant::now() + timeout;
    let response = tokio::time::timeout_at(deadline, client.fetch_metadata_rows(request))
        .await
        .map_err(|_| "metadata rows fetch timed out".to_string())?
        .map_err(|status| status.to_string())?;
    // Bounded stream: never buffer more than the caller's remaining
    // cycle budget plus one row (perf 4.4 bounded streaming), and never
    // wait past the RPC deadline while draining it.
    let cap = max_bytes.max(64 * 1024);
    let mut stream = response.into_inner();
    let mut rows = Vec::new();
    let mut received: u64 = 0;
    let mut truncated = false;
    loop {
        let item = match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_) => return Err("metadata rows stream timed out".to_string()),
            Ok(None) => break,
            Ok(Some(item)) => item.map_err(|status| status.to_string())?,
        };
        received = received.saturating_add(row_bytes(&item) as u64);
        rows.push(item);
        if received >= cap {
            truncated = true;
            break;
        }
    }
    Ok((rows, truncated))
}

fn row_bytes(row: &MetadataRow) -> usize {
    match row.row.as_ref() {
        Some(metadata_row::Row::Object(object)) => object.key.len() + object.value.len() + 16,
        Some(metadata_row::Row::Deletion(deletion)) => {
            deletion.key.len() + deletion.value.len() + 16
        }
        None => 0,
    }
}

fn row_key(row: &MetadataRow) -> Option<Bytes> {
    match row.row.as_ref() {
        Some(metadata_row::Row::Object(object)) => Some(object.key.clone()),
        Some(metadata_row::Row::Deletion(deletion)) => Some(deletion.key.clone()),
        None => None,
    }
}

fn decode_epoch(bytes: &Bytes) -> Option<[u8; 16]> {
    if bytes.len() != 16 {
        return None;
    }
    let mut epoch = [0u8; 16];
    epoch.copy_from_slice(bytes);
    Some(epoch)
}

fn decode_entry(entry: crate::healing_rpc::JournalEntry) -> Option<JournalEntry> {
    let op = JournalOp::from_u8(entry.op as u8)?;
    let hlc = entry.hlc.as_ref()?;
    Some(JournalEntry {
        seq: entry.seq,
        op,
        key: entry.key,
        hlc: Hlc::new(hlc.wall_time, hlc.logical),
    })
}

/// Keeps only the latest entry per key (ascending input by sequence).
fn coalesce_latest(entries: Vec<JournalEntry>) -> Vec<JournalEntry> {
    let mut latest: BTreeMap<Bytes, JournalEntry> = BTreeMap::new();
    for entry in entries {
        match latest.get(&entry.key) {
            Some(existing) if existing.seq >= entry.seq => {}
            _ => {
                latest.insert(entry.key.clone(), entry);
            }
        }
    }
    latest.into_values().collect()
}

/// Computes the next `consumed` position: the full `next_seq` when every
/// entry resolved, otherwise just below the earliest unresolved entry
/// (never backwards).
fn advance_watermark(current: u64, next_seq: u64, unresolved_min: Option<u64>) -> u64 {
    match unresolved_min {
        Some(seq) => seq.saturating_sub(1).max(current),
        None => next_seq.max(current),
    }
}

/// Whether the local state is already strictly newer than the entry (no
/// fetch needed). Equal versions still fetch so the symmetric identity
/// tie-break can run.
fn local_skip_is_current(
    entry: &JournalEntry,
    local_row: Option<&ObjectMetadata>,
    local_tombstone: Option<&Tombstone>,
) -> bool {
    let version = match entry.op {
        JournalOp::Put => local_row.map(|row| row.hlc),
        JournalOp::Delete => local_tombstone.map(|tombstone| tombstone.hlc),
        _ => None,
    };
    version.map(|version| version > entry.hlc).unwrap_or(false)
}

/// The same writability predicate the node's write path uses for a
/// manifest (duplicated here because the node crate is above this one in
/// the dependency graph).
fn manifest_can_accept_writes(manifest: &oceanfs_membership::manifest::NodeManifest) -> bool {
    !manifest.node_unavailable()
        && !manifest.pools().iter().any(|pool| pool.write_degraded())
        && manifest.pools().iter().any(|pool| pool.role() == "data" && pool.status() == "healthy")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(seq: u64, key: &[u8], hlc: Hlc) -> JournalEntry {
        JournalEntry { seq, op: JournalOp::Put, key: Bytes::copy_from_slice(key), hlc }
    }

    #[test]
    fn coalesce_keeps_the_latest_entry_per_key() {
        let entries = vec![
            entry(1, b"a", Hlc::new(1, 0)),
            entry(2, b"b", Hlc::new(2, 0)),
            entry(3, b"a", Hlc::new(3, 0)),
        ];
        let coalesced = coalesce_latest(entries);
        assert_eq!(coalesced.len(), 2);
        assert_eq!(coalesced[0].key.as_ref(), b"a");
        assert_eq!(coalesced[0].seq, 3);
        assert_eq!(coalesced[1].key.as_ref(), b"b");
    }

    #[test]
    fn local_skip_only_when_the_local_version_is_strictly_newer() {
        let mut row = ObjectMetadata {
            object_key: ObjectKey::new("k"),
            size: 1,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 0,
            hlc: Hlc::new(10, 0),
        };
        let put = entry(1, b"b\0k", Hlc::new(10, 0));
        assert!(!local_skip_is_current(&put, Some(&row), None), "equal versions fetch");
        row.hlc = Hlc::new(11, 0);
        assert!(local_skip_is_current(&put, Some(&row), None));
        row.hlc = Hlc::new(9, 0);
        assert!(!local_skip_is_current(&put, Some(&row), None));
        assert!(!local_skip_is_current(&put, None, None), "absent state always fetches");
    }

    #[test]
    fn cycle_budget_saturates_to_zero() {
        let mut budget = CycleBudget::new(2, 10);
        budget.consume_entries(5);
        budget.consume_bytes(100);
        assert!(budget.exhausted());
        budget.consume_entries(1);
        assert_eq!(budget.entries_left, 0);
    }

    #[test]
    fn decode_epoch_rejects_wrong_lengths() {
        assert!(decode_epoch(&Bytes::from_static(b"short")).is_none());
        assert_eq!(decode_epoch(&Bytes::from_static(&[9u8; 16])), Some([9u8; 16]));
    }

    #[test]
    fn watermark_advance_stops_below_the_first_unresolved_entry() {
        // Everything resolved: advance to the responder's next_seq.
        assert_eq!(advance_watermark(3, 9, None), 9);
        // One unresolved entry at seq 7: stop at 6.
        assert_eq!(advance_watermark(3, 9, Some(7)), 6);
        // Never moves backwards (unresolved seq below the current).
        assert_eq!(advance_watermark(5, 9, Some(4)), 5);
        // The first entry is unresolved: stay put.
        assert_eq!(advance_watermark(0, 9, Some(1)), 0);
    }

    /// Gap 4: a byte-cap trim past a lagging peer's watermark enqueues a
    /// `CapTrim` bootstrap (the journal layer is unit-tested separately).
    #[test]
    fn cap_trim_enqueues_a_bootstrap_for_the_lagging_peer() {
        use oceanfs_core::{GossipConfig, MetadataConfig, RingConfig};
        use oceanfs_routing::{Ring, RingCache};
        use oceanfs_storage::MetadataJournal;

        struct Recording {
            calls: std::sync::Mutex<Vec<(NodeId, BootstrapReason)>>,
        }
        impl BootstrapEnqueuer for Recording {
            fn enqueue(&self, peer: &NodeId, reason: BootstrapReason) {
                self.calls.lock().unwrap().push((peer.clone(), reason));
            }
        }

        let meta_dir = tempfile::tempdir().unwrap();
        let journal_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: meta_dir.path().to_path_buf(),
                block_cache_size: 4 * 1024 * 1024,
                memtable_size: 4 * 1024 * 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        let journal =
            Arc::new(MetadataJournal::open(journal_dir.path(), &NodeId::new("node-a")).unwrap());
        // 16 384 × 4 KiB records (> 64 MiB) rotate the active segment.
        let keys: Vec<Vec<u8>> = (0..16_384u64)
            .map(|index| {
                let mut key = format!("bucket\0{index:060}").into_bytes();
                key.resize(4096, b'x');
                key
            })
            .collect();
        let items: Vec<(JournalOp, &[u8], Hlc)> =
            keys.iter().map(|key| (JournalOp::Put, key.as_slice(), Hlc::new(1, 0))).collect();
        journal.append_batch(&items).unwrap();
        assert!(
            journal.on_disk_bytes() > oceanfs_storage::metadata::journal::SEGMENT_TARGET_BYTES,
            "the batch rotated a segment"
        );

        let ring_cache = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
        let membership = Arc::new(Membership::new(
            NodeId::new("node-a"),
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            GossipConfig::default(),
            ring_cache,
        ));
        membership.upsert_node(
            NodeId::new("node-a"),
            NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            None,
        );
        membership.upsert_node(
            NodeId::new("node-b"),
            NodeState::Dead,
            oceanfs_core::Incarnation::new(1),
            None,
        );

        let watermarks =
            Arc::new(WatermarkStore::open(&meta_dir.path().join("watermarks")).unwrap());
        let mut config = MetadataSyncConfig::default();
        config.enabled = true;
        config.journal_max_bytes = 1; // the cap bites immediately
        let recording = Arc::new(Recording { calls: std::sync::Mutex::new(Vec::new()) });
        let sync = MetadataSync::new(
            NodeId::new("node-a"),
            &config,
            membership,
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            Duration::from_secs(1),
            Arc::clone(&store),
            Arc::clone(&journal),
            Arc::clone(&watermarks),
        )
        .unwrap()
        .with_bootstrap(Arc::clone(&recording) as Arc<dyn BootstrapEnqueuer>);

        let ring = sync.membership.ring().snapshot();
        sync.trim(&ring).unwrap();
        let calls = recording.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "one lagging peer was signalled");
        assert_eq!(calls[0].0, NodeId::new("node-b"));
        assert_eq!(calls[0].1, BootstrapReason::CapTrim);
        drop(calls);
        assert!(journal.oldest_seq() > 0, "the cap advanced the floor");
    }
}
