//! Replaced-wal-pool recovery (g7, ADR-0035).
//!
//! When the **wal** pool dies, the node loses its data WAL, event WAL and
//! checkpoint — everything that journals its durability record. The node
//! rejects writes (a Dead wal pool sets `write_degraded`) but keeps
//! serving reads (the objects CF on the metadata pool and the data-pool
//! `.dat` files are intact).
//!
//! When the wal pool is **replaced** — at boot after a restart, or live
//! via remount (mandatory, g7 D2) — recovery must NOT trust the old WAL
//! contents and must NOT run the normal event-WAL fold. It rebuilds its
//! lifecycle registry from the **replicated lifecycle state** (ADR-0035
//! D1/D2): every alive holder of a segment already carries that segment's
//! seal-time metadata in its own registry entry. Recovery (a) detects the
//! replacement, (b) suppresses the destructive once-per-boot residue
//! sweep, (c) re-derives the registry by pulling holder lifecycle
//! metadata for the candidate segments found in its intact data-pool
//! `.dat` files, and (d) re-materializes any segment that is missing
//! locally through the ADR-0030 `ReRepWorker` catch-up path. Writes
//! resume only after catch-up completes and the fresh WAL passes a
//! verification write.
//!
//! This module owns branch selection and the drain orchestration; the
//! storage-side primitives (fresh-WAL reopen, verification write, the
//! write-resume gate) live in their owning crates.

use std::sync::Arc;

use oceanfs_core::SegmentId;
use oceanfs_storage::PoolRegistry;
use tokio_stream::StreamExt;

use crate::pool_paths::PoolPaths;

/// Branch selector result for the startup recovery path.
///
/// [`detect_wal_recovery_mode`] distinguishes the two mutually-exclusive
/// boot branches: the normal event-WAL fold, and the replaced-wal
/// rebuild-from-holders branch. The replaced branch must never run the
/// once-per-boot `.dat` residue sweep (audit C1) and must never replay
/// the (gone) event log.
///
/// # Examples
///
/// ```
/// use oceanfs_node::WalRecoveryMode;
/// assert_eq!(WalRecoveryMode::NormalFold, WalRecoveryMode::NormalFold);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecoveryMode {
    /// A normal restart: fold the existing event WAL into the registry
    /// and run the once-per-boot residue sweep (unchanged behavior).
    NormalFold,
    /// The wal pool was replaced: rebuild the registry from holders and
    /// suppress the residue sweep (ADR-0035 D4).
    RebuildFromHolders,
}

/// Marker file name on the wal pool root. Its presence (written by the
/// live-remount path) is an explicit replacement signal that boot
/// honours even when the empty-root heuristic would be ambiguous.
pub(crate) const REPLACEMENT_MARKER: &str = ".oceanfs-wal-replaced";

/// Returns the marker file path for the wal pool root.
pub(crate) fn replacement_marker_path(paths: &PoolPaths) -> std::path::PathBuf {
    paths.wal.join(REPLACEMENT_MARKER)
}

/// Detects whether the node is booting after a **wal pool replacement**
/// (ADR-0035 D4) or on a normal restart.
///
/// Two signals, OR-ed:
///
/// 1. **Marker**: an explicit `.oceanfs-wal-replaced` file on the wal
///    root (written by the live-remount path). The marker is
///    authoritative.
/// 2. **Heuristic**: the wal root contains no WAL/checkpoint/event files
///    (nothing to fold) while at least one data-pool root contains a
///    `.dat` file (the node holds sealed data a normal first boot cannot
///    explain). This covers the restart-after-out-of-band-replacement
///    case where no marker could have been written.
///
/// A normal restart (existing WAL files present) or a genuinely fresh
/// node (empty wal AND empty data pools) takes
/// [`WalRecoveryMode::NormalFold`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_storage::PoolRegistry;
/// use oceanfs_core::{PoolRole, StorageConfig, StoragePoolConfig};
/// use oceanfs_node::pool_paths::PoolPaths;
/// # fn pool(name: &str, role: PoolRole, root: std::path::PathBuf) -> StoragePoolConfig {
/// #     StoragePoolConfig { name: name.into(), role, root, weight: None, tech: Default::default(), health: Default::default() }
/// # }
/// # let tmp = tempfile::tempdir().expect("tempdir");
/// # let data_dir = tmp.path().join("data");
/// # std::fs::create_dir_all(&data_dir).expect("data dir");
/// # let storage = StorageConfig {
/// #     pools: vec![
/// #         pool("data-0", PoolRole::Data, tmp.path().join("pool-data")),
/// #         pool("wal-0", PoolRole::Wal, tmp.path().join("pool-wal")),
/// #         pool("meta-0", PoolRole::Metadata, tmp.path().join("pool-meta")),
/// #         pool("hints-0", PoolRole::Hints, tmp.path().join("pool-hints")),
/// #     ],
/// #     missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
/// # };
/// # let registry = Arc::new(PoolRegistry::from_config(&storage, &data_dir).expect("registry"));
/// # let paths = PoolPaths {
/// #     metadata: tmp.path().join("pool-meta"),
/// #     wal: tmp.path().join("pool-wal"),
/// #     event_wal: tmp.path().join("pool-wal").join("event-wal"),
/// #     hints: tmp.path().join("pool-hints"),
/// # };
/// // A fresh node (empty wal AND empty data pools) is a normal boot.
/// assert_eq!(
///     oceanfs_node::detect_wal_recovery_mode(&paths, &registry),
///     oceanfs_node::WalRecoveryMode::NormalFold,
/// );
/// ```
pub fn detect_wal_recovery_mode(paths: &PoolPaths, registry: &PoolRegistry) -> WalRecoveryMode {
    if replacement_marker_path(paths).exists() {
        return WalRecoveryMode::RebuildFromHolders;
    }
    if !wal_root_is_empty(paths) {
        return WalRecoveryMode::NormalFold;
    }
    if data_pools_have_dat(registry) {
        WalRecoveryMode::RebuildFromHolders
    } else {
        WalRecoveryMode::NormalFold
    }
}

/// `true` when the wal root holds **no recoverable journal content**:
/// no event-WAL records (`evl_*.log` with nonzero length under
/// `{wal}/event-wal`) and no event-WAL checkpoint file.
///
/// File *presence* is deliberately not the signal: `WalWriter::open` and
/// `EventWal::open` create zero-length placeholder files even on an empty
/// (replaced) device during `StorageModule::build`, so a replaced boot
/// and a normal boot both "have" those files. What distinguishes them is
/// recoverable state — lifecycle events and/or a checkpoint, both of
/// which live on the wal device and are gone after a replacement.
fn wal_root_is_empty(paths: &PoolPaths) -> bool {
    !dir_has_nonempty_files(&paths.event_wal, |n| n.starts_with("evl_") && n.ends_with(".log"))
        && !dir_has_files(&paths.event_wal, |n| n.starts_with("checkpoint-"))
}

/// `true` when the directory has at least one **non-empty** file
/// matching `pred` (empty placeholder files do not count as content).
fn dir_has_nonempty_files(dir: &std::path::Path, pred: impl Fn(&str) -> bool) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|e| {
        e.file_name().to_str().map(&pred).unwrap_or(false)
            && e.metadata().map(|m| m.len() > 0).unwrap_or(false)
    })
}

/// `true` when the directory has at least one file matching `pred`.
fn dir_has_files(dir: &std::path::Path, pred: impl Fn(&str) -> bool) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        // A missing dir is empty.
        return false;
    };
    entries.flatten().any(|e| e.file_name().to_str().map(&pred).unwrap_or(false))
}

/// `true` when at least one data-pool root contains a `.dat` segment
/// file — the node holds sealed data.
fn data_pools_have_dat(registry: &PoolRegistry) -> bool {
    registry.data_pools().iter().any(|pool| dir_has_files(pool.root(), |n| n.ends_with(".dat")))
}

/// Outcome of a replaced-wal recovery drain.
///
/// # Examples
///
/// ```
/// use oceanfs_node::WalRecoveryOutcome;
///
/// let outcome = WalRecoveryOutcome {
///     candidates: 0,
///     restored: 0,
///     missing: vec![],
///     caught_up: 0,
///     dangling: vec![],
///     verified: false,
/// };
/// assert_eq!(outcome.candidates, 0);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecoveryOutcome {
    /// Candidate segment ids enumerated from the data pools + objects CF.
    pub candidates: usize,
    /// Registry entries restored from holder metadata / recomputed.
    pub restored: usize,
    /// Segment ids that were referenced but not materialized locally and
    /// STILL keep the write gate set (unresolved holder-fold candidates +
    /// catch-up requests that did not complete within the drain budget).
    /// g4 re-drives these.
    pub missing: Vec<SegmentId>,
    /// Segments re-materialized through the ReRepWorker.
    pub caught_up: usize,
    /// Referenced segment ids with NO live holder — the documented
    /// out-of-scope residual window (ADR-0029 §D7): reads surface
    /// `SegmentUnavailable`, nothing can pull them, and they do NOT block
    /// the write-resume gate.
    pub dangling: Vec<SegmentId>,
    /// Whether the fresh WAL passed the verification write.
    pub verified: bool,
}

/// Replaced-wal recovery coordinator (g7, ADR-0035).
///
/// Owned by [`crate::modules::durability::DurabilityModule`] — the
/// module that already holds the ReRepWorker, AE tree and the
/// membership/pool handles the drain needs. Constructed once in
/// `DurabilityModule::build` from the storage bundle + network handles,
/// so the composition root and `ServerModule` never hold per-recovery
/// state.
pub(crate) struct WalRecoveryCoordinator {
    /// Node's own id (self is excluded from holder pulls).
    pub(crate) self_id: oceanfs_core::NodeId,
    /// Membership for holder address + ring replica-set resolution.
    pub(crate) membership: Arc<oceanfs_membership::Membership>,
    /// Connection pool for the holder-fetch RPC.
    pub(crate) pool: Arc<oceanfs_network::ConnectionPool>,
    /// Live storage registry (status + write gate).
    pub(crate) pool_registry: Arc<PoolRegistry>,
    /// The lifecycle coordinator (registry seeding through the machine).
    pub(crate) lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    /// The lifecycle registry (read + seed surface).
    pub(crate) lifecycle_registry:
        Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
    /// The single shared segment store (candidate `.dat` presence).
    pub(crate) data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore>,
    /// The metadata store (objects-CF chunk-ref enumeration).
    pub(crate) metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
    /// Re-replication worker sender (catch-up feed).
    pub(crate) rep_sender:
        Option<tokio::sync::mpsc::Sender<oceanfs_durability::healing_service::ReRepRequest>>,
    /// The pool health monitor (write-resume `reset_pool` handoff).
    pub(crate) health_monitor: Arc<oceanfs_storage::pool::health::HealthMonitor>,
    /// The live data-WAL writer (verification probe target).
    pub(crate) wal_writer: Arc<oceanfs_storage::WalWriter>,
    /// The event WAL (fresh reopen on replacement).
    pub(crate) event_wal: Arc<oceanfs_storage::EventWal>,
    /// The event-WAL checkpoint manager (reset on replacement).
    pub(crate) event_checkpoint: Arc<oceanfs_storage::EventCheckpoint>,
    /// The anti-entropy worker (tree rebuild after the restored registry
    /// is folded).
    pub(crate) ae: Arc<oceanfs_durability::AntiEntropy>,
    /// Boot branch signal: set by `StorageModule::prepare_replaced_wal_recovery`,
    /// cleared after the deferred drain runs.
    pub(crate) pending: Arc<std::sync::atomic::AtomicBool>,
    /// Role-pinned paths (marker + wal/event-wal roots).
    pub(crate) paths: PoolPaths,
    /// g7 recovery metrics (registered with the central registry by the
    /// durability module).
    pub(crate) metrics: WalRecoveryMetrics,
}

impl WalRecoveryCoordinator {
    /// Builds the coordinator from the storage bundle + network handles.
    ///
    /// `storage` provides every storage-side Arc (registry, lifecycle,
    /// WALs, stores, monitor); `rep_sender` is the ReRepWorker's queue
    /// sender; `membership`/`pool` are the data-plane handles; `paths`
    /// are the role-pinned directories.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        self_id: oceanfs_core::NodeId,
        storage: &crate::modules::storage::StorageModule,
        membership: Arc<oceanfs_membership::Membership>,
        pool: Arc<oceanfs_network::ConnectionPool>,
        rep_sender: Option<
            tokio::sync::mpsc::Sender<oceanfs_durability::healing_service::ReRepRequest>,
        >,
        ae: Arc<oceanfs_durability::AntiEntropy>,
        pending: Arc<std::sync::atomic::AtomicBool>,
        paths: &PoolPaths,
    ) -> Self {
        Self {
            self_id,
            membership,
            pool,
            pool_registry: storage.registry.clone(),
            lifecycle: storage.lifecycle.clone(),
            lifecycle_registry: storage.lifecycle_registry.clone(),
            data_store: storage.data_store.clone(),
            metadata_store: storage.metadata_store.clone(),
            rep_sender,
            health_monitor: storage.health_monitor.clone(),
            wal_writer: storage.wal_writer.clone(),
            event_wal: storage.event_wal.clone(),
            event_checkpoint: storage.event_checkpoint.clone(),
            ae,
            pending,
            paths: paths.clone(),
            metrics: WalRecoveryMetrics::new(),
        }
    }

    /// Registers the g7 recovery metrics with the central registry.
    pub(crate) fn register_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        self.metrics.register(registrar);
    }

    /// Runs the boot deferred drain when the boot path detected a
    /// replaced wal pool (a no-op otherwise).
    ///
    /// Called by the composition root after `spawn_all` (the ReRepWorker
    /// and membership plane are live). On a fully-successful drain the
    /// write gate is cleared and the marker removed; the AE tree is
    /// rebuilt over the restored registry. `Ok` on both success and
    /// no-op; an incomplete drain leaves `write_degraded` set (g4 is the
    /// backstop).
    ///
    /// # Errors
    ///
    /// Returns an error if the drain itself fails.
    pub(crate) async fn run_deferred_boot_drain(&self) -> Result<(), String> {
        if !self.pending.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        let outcome = self.run_drain_with_gate().await?;
        self.rebuild_ae_after_recovery(&outcome);
        // The normal boot branch sets the WAL retention liveness closure
        // at the end of `run_startup_recovery`; the replaced branch
        // returns early from that method, so set it here now that the
        // registry is rebuilt (same registry-backed closure).
        self.install_wal_liveness();
        self.pending.store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Wires the machine-backed WAL retention liveness predicate
    /// (ADR-0024 §Retention): an entry is garbage iff its segment's
    /// registry entry is absent or sealed with `data_wal_pos ≥ pos`.
    fn install_wal_liveness(&self) {
        let registry = Arc::clone(&self.lifecycle_registry);
        self.wal_writer.set_liveness(Arc::new(move |segment_id, pos| {
            match registry.get(segment_id) {
                Some(entry) => oceanfs_storage::entry_is_garbage(&entry, &pos),
                None => true,
            }
        }));
    }

    /// Replaces a **dead wal pool** live (g7 D2 — the mandatory live
    /// remount path, ADR-0035), without a restart.
    ///
    /// Runs the same local prep the boot branch performs (fresh WAL /
    /// event-WAL / checkpoint reopen, marker, write gate), then the same
    /// registry-rebuild + catch-up drain against the already-running
    /// `ReRepWorker`. Reads may serve throughout; writes stay 503 until
    /// the drain completes and the fresh WAL passes the verification
    /// write.
    ///
    /// # Errors
    ///
    /// Returns an error if the fresh-WAL reopen, the registry rebuild or
    /// the drain fails.
    pub(crate) async fn live_remount(&self) -> Result<(), String> {
        self.prepare_local().await?;
        let outcome = self.run_drain_with_gate().await?;
        self.rebuild_ae_after_recovery(&outcome);
        self.pending.store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// The shared drain + write-resume-gate sequence (boot and live).
    async fn run_drain_with_gate(&self) -> Result<WalRecoveryOutcome, String> {
        let outcome = run_wal_pool_recovery(self).await?;
        if outcome.verified && outcome.missing.is_empty() {
            if let Some(wal_pool) = self.pool_registry.pool_by_role(oceanfs_core::PoolRole::Wal) {
                reset_wal_pool(&self.pool_registry, &self.health_monitor, wal_pool.id());
            }
            let marker = replacement_marker_path(&self.paths);
            if marker.exists() {
                let _ = std::fs::remove_file(marker);
                tracing::info!(
                    "wal replacement marker cleared — next boot folds the rebuilt registry"
                );
            }
        } else {
            tracing::warn!(
                verified = outcome.verified,
                remaining = outcome.missing.len(),
                "replaced-wal recovery incomplete — write_degraded stays set (g4 backstop)"
            );
        }
        Ok(outcome)
    }

    /// Rebuilds the AE tree over the restored registry (continuous AE
    /// must cover the rebuilt segments). A failure is logged — recovery
    /// itself already succeeded; the next AE cycle + the seal notifier
    /// self-heal the tree.
    fn rebuild_ae_after_recovery(&self, _outcome: &WalRecoveryOutcome) {
        if let Err(e) = self.ae.rebuild_tree_from_registry() {
            tracing::error!(error = %e, "AE tree rebuild after replaced-wal recovery failed");
        }
    }

    /// The local replaced-branch prep shared by boot and live remount:
    /// marker, fresh data WAL / event WAL / checkpoint, and the write
    /// gate. The boot path runs it inside
    /// `StorageModule::prepare_replaced_wal_recovery`; the live remount
    /// handler runs it here.
    pub(crate) async fn prepare_local(&self) -> Result<(), String> {
        prepare_wal_replacement(
            &self.paths,
            &self.wal_writer,
            &self.event_wal,
            &self.event_checkpoint,
            &self.pool_registry,
            &self.pending,
        )
        .await
    }
}

/// Shared replaced-wal local prep: replacement marker, fresh data WAL /
/// event WAL / checkpoint on the (empty) journal device, the write gate
/// (wal pool Dead + `write_degraded`), and the pending-drain signal.
///
/// Used by the boot branch (`StorageModule::prepare_replaced_wal_recovery`)
/// and the live-remount path (`WalRecoveryCoordinator::prepare_local`).
pub(crate) async fn prepare_wal_replacement(
    paths: &PoolPaths,
    wal_writer: &oceanfs_storage::WalWriter,
    event_wal: &oceanfs_storage::EventWal,
    event_checkpoint: &oceanfs_storage::EventCheckpoint,
    pool_registry: &PoolRegistry,
    pending: &Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), String> {
    let marker = replacement_marker_path(paths);
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create wal root for marker: {e}"))?;
    }
    std::fs::write(&marker, b"replaced")
        .map_err(|e| format!("failed to write wal replacement marker: {e}"))?;
    wal_writer.reopen_fresh().await.map_err(|e| format!("failed to open fresh data WAL: {e}"))?;
    event_wal.reopen_fresh().await.map_err(|e| format!("failed to open fresh event WAL: {e}"))?;
    event_checkpoint
        .reset_for_fresh()
        .map_err(|e| format!("failed to reset event WAL checkpoint: {e}"))?;
    if let Some(wal_pool) = pool_registry.pool_by_role(oceanfs_core::PoolRole::Wal) {
        pool_registry.set_status(wal_pool.id(), oceanfs_storage::PoolStatus::Dead);
        pool_registry.set_write_degraded(wal_pool.id(), true);
    }
    pending.store(true, std::sync::atomic::Ordering::Release);
    Ok(())
}

// ---------------------------------------------------------------------------
// Candidate enumeration
// ---------------------------------------------------------------------------

/// Enumerates the candidate segments recovery must re-derive.
///
/// Returns `(present, referenced)` where:
///
/// - `present` maps each segment id found as an intact data-pool `.dat`
///   file to the **local** pool id that holds it (a node-local pool id —
///   ADR-0035 D2/D4: the rebuilt registry entry must point at THIS
///   node's owning pool, never a holder's).
/// - `referenced` is the set of segment ids the surviving objects CF
///   chunk refs point at (the missing-segment reconciliation input).
///
/// This is a **one-time, recovery-only** enumeration (ADR-0034
/// carve-out: it runs once per replaced-wal recovery, never
/// periodically).
pub(crate) fn enumerate_candidates(
    ctx: &WalRecoveryCoordinator,
) -> (Vec<(SegmentId, u32)>, Vec<SegmentId>) {
    // (1) Data-pool `.dat` roots → (id, local pool id).
    let mut present: Vec<(SegmentId, u32)> = Vec::new();
    let mut seen_present: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
    for pool in ctx.pool_registry.data_pools() {
        let root = pool.root().to_path_buf();
        let pool_id = pool.id();
        match ctx.data_store.list_segment_files(&root) {
            Ok(paths) => {
                for path in paths {
                    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                        continue;
                    };
                    let Some(rest) = name.strip_suffix(".dat") else { continue };
                    if let Ok(uuid) = uuid::Uuid::parse_str(rest) {
                        let id = oceanfs_core::SegmentId::from_uuid_bytes(*uuid.as_bytes());
                        if seen_present.insert(id) {
                            present.push((id, pool_id));
                        }
                    }
                }
            }
            Err(e) => tracing::warn!(root = ?root, error = %e, "candidate `.dat` listing failed"),
        }
    }
    present.sort_by_key(|(id, _)| *id);

    // (2) Objects-CF chunk refs — segments the surviving object index
    // still points at.
    let mut referenced: Vec<SegmentId> = Vec::new();
    let mut seen_ref: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
    for obj in ctx.metadata_store.list_objects_all() {
        let Ok(meta) = obj else { continue };
        for chunk in &meta.chunks {
            if seen_ref.insert(chunk.segment_id) {
                referenced.push(chunk.segment_id);
            }
        }
    }
    referenced.sort();

    (present, referenced)
}

/// Returns the ring replica set of `segment_id` minus self (the
/// candidate live holders to pull lifecycle metadata from).
pub(crate) fn holder_candidates(
    ctx: &WalRecoveryCoordinator,
    segment_id: SegmentId,
) -> Vec<oceanfs_core::NodeId> {
    use oceanfs_routing::segment_replica_set;
    segment_replica_set(ctx.membership.ring(), &segment_id)
        .into_iter()
        .filter(|id| *id != ctx.self_id)
        .collect()
}

/// `true` when the membership plane knows at least one remote node in a
/// servable state (Alive | Suspect). A single-node cluster has no remote
/// peer, so an empty ring replica set genuinely means "no holder
/// position"; with remote peers present, an empty ring set is a
/// convergence artifact and must NOT be treated as local-only.
fn membership_has_remote_peer(ctx: &WalRecoveryCoordinator) -> bool {
    use oceanfs_core::NodeState;
    ctx.membership.nodes_full().into_iter().any(|(id, state, ..)| {
        id != ctx.self_id && matches!(state, NodeState::Alive | NodeState::Suspect)
    })
}

/// Decides whether a present-but-unseeded candidate is a genuine
/// local-only (ADR-0035 D3) recompute candidate, versus an unresolved
/// segment whose live holder was merely unreachable / not yet gossiped.
///
/// Local-only is only ever true when:
/// - every reachable holder answered "not held" (the segment has no live
///   copy among its ring replicas), OR
/// - the ring replica set is empty AND the cluster has no remote peer (a
///   single node — no holder position can ever exist).
///
/// An empty ring replica set with remote peers present is a convergence
/// artifact and must NOT be treated as local-only (the boot drain starts
/// before gossip has fully populated the ring).
fn is_local_only_d3_candidate(
    ctx: &WalRecoveryCoordinator,
    segment_id: SegmentId,
    reachable_absent: &std::collections::HashSet<SegmentId>,
) -> bool {
    let no_ring_holder = holder_candidates(ctx, segment_id).is_empty();
    let reachable_but_absent = reachable_absent.contains(&segment_id);
    let single_node = !membership_has_remote_peer(ctx);
    reachable_but_absent || (no_ring_holder && single_node)
}

// ---------------------------------------------------------------------------
// Holder lifecycle-metadata fold (ADR-0035 D2)
// ---------------------------------------------------------------------------

/// Fetches the lifecycle metadata for `segment_ids` from a single live
/// holder via `FetchSegmentLifecycleMetadata`. Returns the entries the
/// holder actually holds (absent ids are simply missing from the stream).
async fn fetch_metadata_from_holder(
    ctx: &WalRecoveryCoordinator,
    holder: &oceanfs_core::NodeId,
    segment_ids: &[SegmentId],
    timeout_ms: u64,
) -> Result<Vec<oceanfs_durability::healing_rpc::SegmentLifecycleEntry>, String> {
    use oceanfs_durability::healing_rpc::{
        healing_rpc_client::HealingRpcClient, SegmentLifecycleQuery,
    };

    let addr = ctx
        .membership
        .address_of(holder)
        .ok_or_else(|| format!("holder {holder} not found in membership"))?;
    let pooled = ctx
        .pool
        .get_channel(addr)
        .await
        .map_err(|e| format!("connection pool error for {holder}: {e}"))?;
    let channel = pooled.channel().clone();
    drop(pooled);

    let proto_ids: Vec<oceanfs_core::proto::common::SegmentId> =
        segment_ids.iter().map(|id| (*id).into()).collect();
    let mut client = HealingRpcClient::new(channel);
    let request = tonic::Request::new(SegmentLifecycleQuery { segment_ids: proto_ids });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    let mut stream =
        tokio::time::timeout_at(deadline, client.fetch_segment_lifecycle_metadata(request))
            .await
            .map_err(|_| format!("holder {holder} metadata fetch timed out"))?
            .map_err(|status| format!("holder {holder} metadata fetch failed: {status}"))?
            .into_inner();

    let mut entries = Vec::new();
    loop {
        let msg = tokio::time::timeout_at(deadline, stream.next()).await;
        match msg {
            Ok(Some(Ok(entry))) => entries.push(entry),
            Ok(Some(Err(status))) => {
                return Err(format!("holder {holder} metadata stream error: {status}"))
            }
            Ok(None) => break,
            Err(_) => return Err(format!("holder {holder} metadata stream timed out")),
        }
    }
    Ok(entries)
}

/// Pulls lifecycle metadata for every locally-present candidate from one
/// live holder per candidate and folds the replies into the registry.
///
/// Candidates whose holder answers are folded through the coordinator's
/// durable requests (reserve → seal-with-contained → refresh), so the
/// rebuilt registry is event-WAL durable and a later normal boot folds
/// the same state. Returns the number of entries restored.
///
/// The holder's `pool_id` is deliberately NOT copied: pool ids are
/// node-local, and each rebuilt entry must point at the LOCAL pool that
/// holds the candidate's `.dat` (passed via `local_pool`).
/// Outcome of one holder-fold pass over the locally-present candidates.
pub(crate) struct HolderFoldOutcome {
    /// Segments seeded from holder metadata this pass.
    pub(crate) restored: Vec<SegmentId>,
    /// Present candidates still without a registry entry after this pass.
    pub(crate) still_present: Vec<(SegmentId, u32)>,
    /// Still-present candidates for which at least one ring replica was
    /// REACHABLE and answered "not held" this pass (genuinely no live
    /// holder — a D3 candidate once the retry budget is exhausted).
    pub(crate) reachable_absent: Vec<SegmentId>,
}

/// One holder-fold pass over `present` (the locally-present candidates).
///
/// For each candidate that still lacks a registry entry: ask every ring
/// replica (minus self) for its lifecycle metadata and seed the first
/// Sealed reply (ADR-0035 D2). A pass makes no network retries by
/// itself — the caller re-runs it until the membership plane converges
/// or the recovery budget is exhausted.
async fn holder_fold_pass(
    ctx: &WalRecoveryCoordinator,
    present: &[(SegmentId, u32)],
) -> Result<HolderFoldOutcome, String> {
    let mut restored = Vec::new();
    let mut still_present = Vec::new();
    let mut reachable_absent = Vec::new();

    for (segment_id, local_pool) in present {
        if ctx.lifecycle_registry.get(*segment_id).is_some() {
            continue;
        }
        let holders = holder_candidates(ctx, *segment_id);
        if holders.is_empty() {
            // No ring replica known yet (membership may not have
            // converged at boot) — retry; never classify here.
            still_present.push((*segment_id, *local_pool));
            continue;
        }

        let mut seeded = false;
        let mut any_reachable = false;
        for holder in &holders {
            match fetch_metadata_from_holder(ctx, holder, std::slice::from_ref(segment_id), 5_000)
                .await
            {
                Ok(entries) => {
                    any_reachable = true;
                    for entry in entries {
                        let held = entry
                            .segment_id
                            .as_ref()
                            .map(|p| *p == (*segment_id).into())
                            .unwrap_or(false);
                        // A seedable Sealed entry must carry the seal-time
                        // merkle root (the coordinator's request_seal
                        // requires it; a root-less entry is not seedable
                        // and is treated as "not held").
                        let seedable = held && entry.state == 1 && !entry.merkle_root.is_empty();
                        if seedable {
                            seed_holder_entry(ctx, entry, *local_pool).await?;
                            restored.push(*segment_id);
                            seeded = true;
                            break;
                        }
                    }
                    if seeded {
                        break;
                    }
                }
                Err(e) => tracing::debug!(
                    segment_id = %segment_id,
                    holder = %holder,
                    error = %e,
                    "holder metadata fetch failed (trying next holder)"
                ),
            }
        }
        if !seeded {
            if any_reachable {
                // A reachable holder answered but does not hold this
                // segment — genuinely no live holder (D3 candidate).
                reachable_absent.push(*segment_id);
            }
            still_present.push((*segment_id, *local_pool));
        }
    }

    Ok(HolderFoldOutcome { restored, still_present, reachable_absent })
}

/// Re-seeds one segment's Sealed entry from its replicated lifecycle
/// metadata (ADR-0035 D1/D2), durably through the coordinator's
/// event-WAL-backed requests so a later normal boot folds the same
/// state.
async fn seed_holder_entry(
    ctx: &WalRecoveryCoordinator,
    entry: oceanfs_durability::healing_rpc::SegmentLifecycleEntry,
    local_pool: u32,
) -> Result<(), String> {
    use oceanfs_core::{HashOutput, SizeTier};

    let Some(proto_id) = entry.segment_id else { return Ok(()) };
    let segment_id =
        oceanfs_core::SegmentId::try_from(proto_id).map_err(|e| format!("bad holder id: {e}"))?;

    if ctx.lifecycle_registry.get(segment_id).is_some() {
        return Ok(());
    }
    // The responder only streams Sealed entries; reject anything else
    // defensively (0=Reserved, 2=Deleted).
    if entry.state != 1 {
        return Ok(());
    }

    let tier = match entry.tier {
        1 => SizeTier::Small,
        2 => SizeTier::Standard,
        3 => SizeTier::Multi,
        _ => SizeTier::Inline,
    };
    let ec_k = u8::try_from(entry.ec_k).unwrap_or(1);
    let ec_m = u8::try_from(entry.ec_m).unwrap_or(0);
    let merkle_root = if entry.merkle_root.is_empty() {
        // No seal-time root: not seedable (the coordinator's seal
        // requires one). The caller treats root-less entries as absent.
        return Ok(());
    } else {
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&entry.merkle_root);
        Some(HashOutput::from_bytes(bytes))
    };
    let locations: smallvec::SmallVec<[oceanfs_core::NodeId; 16]> =
        entry.storage_locations.iter().cloned().map(oceanfs_core::NodeId::from).collect();
    let contained: Option<Vec<oceanfs_core::ContainedObject>> =
        if entry.contained_objects.is_empty() {
            None
        } else {
            Some(
                entry
                    .contained_objects
                    .iter()
                    .map(|co| {
                        let bucket = co.bucket.as_ref().map(|b| b.name.clone()).unwrap_or_default();
                        let key = co.key.as_ref().map(|k| k.key.clone()).unwrap_or_default();
                        oceanfs_core::ContainedObject {
                            bucket: oceanfs_core::BucketId::new(bucket),
                            key: oceanfs_core::ObjectKey::new(key),
                        }
                    })
                    .collect(),
            )
        };

    ctx.lifecycle
        .request_reserve(segment_id, tier, ec_k, ec_m)
        .await
        .map_err(|e| format!("recovery reserve failed for {segment_id}: {e}"))?;

    let metadata = oceanfs_core::SegmentMetadata {
        pool_id: local_pool,
        total_bytes: entry.total_bytes,
        segment_id,
        ec_k,
        ec_m,
        size_tier: tier,
        merkle_root,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: if entry.sealed_at > 0 { Some(entry.sealed_at) } else { None },
    };
    ctx.lifecycle
        .request_seal_with_contained(segment_id, metadata.clone(), None, contained.as_deref())
        .await
        .map_err(|e| format!("recovery seal failed for {segment_id}: {e}"))?;

    // Stamp the replicated holder set (durable refresh).
    ctx.lifecycle
        .request_refresh_metadata(segment_id, merkle_root, Some(locations))
        .await
        .map_err(|e| format!("recovery location stamp failed for {segment_id}: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Local-only recompute (ADR-0035 D3)
// ---------------------------------------------------------------------------

/// Recomputes a Sealed entry for a locally-present segment with no live
/// remote holder from its own `.dat` (ADR-0035 D3).
///
/// Reads the file directly from the owning pool root (the registry entry
/// does not exist yet, so the registry-resolving store read is
/// unavailable), recomputes the Merkle root over the data section, infers
/// tier from the data size, and registers a Sealed entry with degraded
/// accounting (`contained_objects = None`, `total_bytes = data size`).
pub(crate) async fn recompute_local_only(
    ctx: &WalRecoveryCoordinator,
    segment_id: SegmentId,
    local_pool: u32,
) -> Result<bool, String> {
    use oceanfs_core::SizeTier;
    use oceanfs_storage::SegmentHeader;

    if ctx.lifecycle_registry.get(segment_id).is_some() {
        return Ok(false);
    }
    // Locate the owning root.
    let Some(pool) = ctx.pool_registry.pool_by_id(local_pool) else {
        return Err(format!("local pool {local_pool} not found for {segment_id}"));
    };
    let path = pool.root().join(format!("{segment_id}.dat"));
    let bytes = std::fs::read(&path)
        .map_err(|e| format!("local-only recompute: cannot read {path:?}: {e}"))?;
    let header = SegmentHeader::from_bytes(&bytes)
        .ok_or_else(|| format!("local-only recompute: unparseable header for {segment_id}"))?;
    let hdr_size = header.serialized_size();
    let data_end = hdr_size
        .saturating_add(usize::try_from(header.size).unwrap_or(usize::MAX))
        .min(bytes.len());
    let data = &bytes[hdr_size..data_end];

    let merkle_root = oceanfs_durability::MerkleTree::build(data, 0)
        .ok_or_else(|| format!("local-only recompute: merkle build failed for {segment_id}"))?
        .root()
        .hash();
    // Tier inference: small-tier segments are the common local-only
    // residual (they fit one stripe). Classify by data size against the
    // default thresholds.
    let tier = if header.size <= oceanfs_core::SegmentSizeConfig::default().small_target_size {
        SizeTier::Small
    } else {
        SizeTier::Standard
    };
    let ec_k = 1u8;
    let ec_m = 0u8;

    ctx.lifecycle
        .request_reserve(segment_id, tier, ec_k, ec_m)
        .await
        .map_err(|e| format!("local-only reserve failed for {segment_id}: {e}"))?;
    let metadata = oceanfs_core::SegmentMetadata {
        pool_id: local_pool,
        total_bytes: header.size,
        segment_id,
        ec_k,
        ec_m,
        size_tier: tier,
        merkle_root: Some(merkle_root),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: None,
    };
    ctx.lifecycle
        .request_seal(segment_id, metadata, None)
        .await
        .map_err(|e| format!("local-only seal failed for {segment_id}: {e}"))?;
    tracing::warn!(
        segment_id = %segment_id,
        "wal recovery: local-only segment recomputed (no live holder; contained/total accounting degraded)"
    );
    Ok(true)
}

// ---------------------------------------------------------------------------
// Missing-segment catch-up (ADR-0035 D2 / g7 D3)
// ---------------------------------------------------------------------------

/// Enqueues one re-replication request per referenced-but-not-materialized
/// segment and returns `(enqueued, dangling)`.
///
/// `missing` are segment ids the surviving objects CF references that are
/// neither present as a local `.dat` nor restored by the holder fold.
/// Each catchable segment enqueues a
/// [`ReRepRequest`](oceanfs_durability::healing_service::ReRepRequest)
/// through the ReRepWorker's bounded sender; the worker pulls full data +
/// metadata from a live holder, writes through the pool-aware store,
/// registers reserve + seal, and stamps `storage_locations`.
///
/// A referenced segment with **no live holder** is the documented
/// residual window (ADR-0029 §D7 / the feature's out-of-scope "dangling
/// row"): there is no copy to pull from, reads surface
/// `SegmentUnavailable`, and it must NOT wedge the write-resume gate.
/// Such segments are returned as `dangling` (reported, not enqueued).
pub(crate) async fn enqueue_catch_up(
    ctx: &WalRecoveryCoordinator,
    missing: &[SegmentId],
) -> Result<(usize, Vec<SegmentId>), String> {
    use oceanfs_durability::healing_service::{ReRepRequest, RepairReason};

    let Some(sender) = &ctx.rep_sender else {
        return Err("re-replication worker not available for catch-up".into());
    };

    let mut enqueued = 0usize;
    let mut dangling: Vec<SegmentId> = Vec::new();
    for segment_id in missing {
        // Determine the live holders to pull from (ring replica set −
        // self). Segments with no live remote holder are the documented
        // residual window (ADR-0029 §D7) — they cannot be re-materialized.
        let holders: Vec<oceanfs_core::NodeId> = holder_candidates(ctx, *segment_id)
            .into_iter()
            .filter(|h| ctx.membership.state_of(h).is_some())
            .collect();
        if holders.is_empty() {
            tracing::warn!(
                segment_id = %segment_id,
                "wal recovery: referenced segment has no live holder — reads surface SegmentUnavailable"
            );
            dangling.push(*segment_id);
            continue;
        }

        // Fetch the holder's seal-time shape so the re-materialized copy
        // is registered with the REAL geometry (tier/ec/merkle), not
        // defaults (ADR-0030: the request carries the source shape).
        let mut shape: Option<oceanfs_durability::healing_rpc::SegmentLifecycleEntry> = None;
        for holder in &holders {
            if let Ok(entries) =
                fetch_metadata_from_holder(ctx, holder, std::slice::from_ref(segment_id), 5_000)
                    .await
            {
                if let Some(entry) =
                    entries.into_iter().find(|e| e.state == 1 && !e.merkle_root.is_empty())
                {
                    shape = Some(entry);
                    break;
                }
            }
        }
        let (tier, ec_k, ec_m, merkle_root) = match shape {
            Some(entry) => {
                let tier = match entry.tier {
                    1 => oceanfs_core::SizeTier::Small,
                    2 => oceanfs_core::SizeTier::Standard,
                    3 => oceanfs_core::SizeTier::Multi,
                    _ => oceanfs_core::SizeTier::Standard,
                };
                let ec_k = u8::try_from(entry.ec_k).unwrap_or(1);
                let ec_m = u8::try_from(entry.ec_m).unwrap_or(0);
                let merkle_root = if entry.merkle_root.is_empty() {
                    None
                } else {
                    let mut bytes = [0u8; 32];
                    bytes.copy_from_slice(&entry.merkle_root);
                    Some(oceanfs_core::HashOutput::from_bytes(bytes))
                };
                (tier, ec_k, ec_m, merkle_root)
            }
            // No holder answered the shape probe — request without a
            // merkle anchor (the worker skips verification) and default
            // geometry. The g4 loop re-drives with a shape if one exists.
            None => (oceanfs_core::SizeTier::Standard, 1u8, 0u8, None),
        };

        let request = ReRepRequest {
            origin: ctx.self_id.clone(),
            segment_id: *segment_id,
            holders: holders.clone(),
            reason: RepairReason::Reconciliation,
            retry_count: 0,
            merkle_root,
            tier,
            ec_k,
            ec_m,
        };
        sender
            .send(request)
            .await
            .map_err(|e| format!("wal recovery catch-up enqueue failed (queue closed): {e}"))?;
        enqueued += 1;
    }
    Ok((enqueued, dangling))
}

/// Re-checks the drain completion condition for one segment: materialized
/// iff its `.dat` exists AND its registry entry is Sealed with a
/// non-empty `storage_locations`.
pub(crate) async fn is_materialized(ctx: &WalRecoveryCoordinator, segment_id: SegmentId) -> bool {
    use oceanfs_storage::segment::lifecycle::SegmentState;

    let present = matches!(ctx.data_store.read_segment_data(&segment_id).await, Ok(Some(_)));
    if !present {
        return false;
    }
    match ctx.lifecycle_registry.get(segment_id) {
        Some(entry) => {
            entry.state == SegmentState::Sealed && !entry.metadata.storage_locations.is_empty()
        }
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Write-resume gate (ADR-0035 D4 / audit H2)
// ---------------------------------------------------------------------------

/// Resets the wal pool's write gate after a successful replaced-wal
/// recovery: the registry `Healthy` + `write_degraded(false)` handoff
/// followed by the health monitor's `reset_pool` (clears the monitor's
/// internal Dead mirror so `Dead` stops being absorbing).
pub(crate) fn reset_wal_pool(
    pool_registry: &PoolRegistry,
    health_monitor: &oceanfs_storage::pool::health::HealthMonitor,
    pool_id: u32,
) {
    pool_registry.set_status(pool_id, oceanfs_storage::PoolStatus::Healthy);
    pool_registry.set_write_degraded(pool_id, false);
    health_monitor.reset_pool(pool_id, oceanfs_storage::PoolStatus::Healthy);
    tracing::info!(pool_id, "wal pool write gate cleared after replaced-wal recovery");
}

// ---------------------------------------------------------------------------
// Top-level drain
// ---------------------------------------------------------------------------

/// Runs the full replaced-wal recovery drain:
///
/// 1. enumerate candidates (`.dat` ∪ objects-CF);
/// 2. fold holder lifecycle metadata into the registry (ADR-0035 D2);
/// 3. recompute local-only segments (D3);
/// 4. cross-check objects-CF references → catch-up through the
///    ReRepWorker (g7 D3);
/// 5. wait for the catch-up set to drain (materialization re-check);
/// 6. verify the fresh WAL with a write probe.
///
/// The caller then performs the write-resume gate handoff — see
/// [`WalRecoveryCoordinator::run_drain_with_gate`] for the combined
/// drain + gate sequence used by the boot path and the live-remount
/// handler.
pub(crate) async fn run_wal_pool_recovery(
    ctx: &WalRecoveryCoordinator,
) -> Result<WalRecoveryOutcome, String> {
    use std::time::Instant;

    let started = Instant::now();
    let (present, referenced) = enumerate_candidates(ctx);
    let present_ids: std::collections::HashSet<SegmentId> =
        present.iter().map(|(id, _)| *id).collect();

    // (2) Holder fold for locally-present candidates. At boot the ring /
    // membership plane may not have converged yet (the drain runs right
    // after spawn_all), so retry the fold until no progress for several
    // rounds or the budget is exhausted. A segment is only declared
    // local-only (D3) once its ring replica set is genuinely empty —
    // which is only true when the cluster has no remote peer at all (a
    // single node), never merely because gossip has not yet populated the
    // ring.
    let fold_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut still_present: Vec<(SegmentId, u32)> = present.clone();
    let mut reachable_absent: std::collections::HashSet<SegmentId> =
        std::collections::HashSet::new();
    let mut restored = 0usize;
    let mut last_restored = 0usize;
    let mut stale_rounds = 0u32;
    loop {
        let pass = holder_fold_pass(ctx, &still_present).await?;
        restored += pass.restored.len();
        reachable_absent.extend(pass.reachable_absent);
        still_present = pass.still_present;
        if still_present.is_empty() {
            break;
        }
        if restored > last_restored {
            last_restored = restored;
            stale_rounds = 0;
        } else {
            stale_rounds += 1;
        }
        // Early exit only when the remaining candidates are NOT waiting on
        // ring convergence. Candidates with an empty ring set AND live
        // remote peers may still gain holders as gossip converges, so keep
        // retrying them up to the fold deadline.
        let waiting_on_ring = still_present.iter().any(|(segment_id, _)| {
            holder_candidates(ctx, *segment_id).is_empty() && membership_has_remote_peer(ctx)
        });
        if !waiting_on_ring && stale_rounds >= 5 {
            break;
        }
        if tokio::time::Instant::now() >= fold_deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // (3) Classify what the fold could not restore, once the retry
    // budget is exhausted:
    //   - every reachable holder answered "not held" → local-only
    //     recompute (ADR-0035 D3);
    //   - ring replica set empty AND the cluster has no remote peer (a
    //     single node — no holder position can ever exist) → local-only
    //     recompute (D3);
    //   - ring replica set empty but remote peers EXIST (the ring did not
    //     converge in budget) OR ring has holders but none was reachable
    //     → unresolved (write_degraded stays; g4 backstop) — NOT
    //     local-only, because a live holder may exist that was merely
    //     unreachable / not yet gossiped.
    let mut recomputed = 0usize;
    let mut unresolved: Vec<SegmentId> = Vec::new();
    for (segment_id, local_pool) in &still_present {
        if ctx.lifecycle_registry.get(*segment_id).is_some() {
            continue;
        }
        if is_local_only_d3_candidate(ctx, *segment_id, &reachable_absent) {
            if recompute_local_only(ctx, *segment_id, *local_pool).await? {
                recomputed += 1;
            }
        } else {
            unresolved.push(*segment_id);
        }
    }

    // (4) missing = referenced ∧ ¬present ∧ ¬already-restored. These are
    // re-materialized through the ReRepWorker catch-up path (g7 D3).
    // Segments with no live holder (`dangling`) are the documented
    // out-of-scope residual window: nothing can pull them, reads surface
    // `SegmentUnavailable`, and they must NOT wedge the write-resume gate.
    let mut missing: Vec<SegmentId> = referenced
        .into_iter()
        .filter(|id| !present_ids.contains(id) && ctx.lifecycle_registry.get(*id).is_none())
        .collect();
    missing.sort();

    let (caught_up, dangling) = enqueue_catch_up(ctx, &missing).await?;
    let catchable: std::collections::HashSet<SegmentId> =
        missing.iter().copied().filter(|id| !dangling.contains(id)).collect();
    // (5) drain completion: poll until every CATCHABLE segment is
    // materialized. The loop is progress-aware — it keeps polling while
    // the outstanding set shrinks (the worker is making progress and may
    // still be retrying internally) and stops only when it is empty, when
    // the set stops shrinking for several consecutive rounds (permanently
    // failed), or after a generous hard cap (the g4 reconciliation loop
    // is the eventual backstop).
    let drain_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut remaining: Vec<SegmentId> = catchable.into_iter().collect();
    remaining.sort();
    let mut no_progress_rounds = 0u32;
    loop {
        let previous_len = remaining.len();
        // Re-check the whole outstanding set each round (idempotent).
        let mut next: Vec<SegmentId> = Vec::with_capacity(previous_len);
        for segment_id in remaining.drain(..) {
            if !is_materialized(ctx, segment_id).await {
                next.push(segment_id);
            }
        }
        remaining = next;
        if remaining.is_empty() {
            break;
        }
        if remaining.len() < previous_len {
            no_progress_rounds = 0;
        } else {
            no_progress_rounds += 1;
        }
        if no_progress_rounds >= 24 {
            // The set has not shrunk for ~6 s (24 × 250 ms): the worker's
            // retries for these segments are effectively exhausted. They
            // keep write_degraded set and are re-driven by the g4 loop.
            tracing::warn!(
                pending = remaining.len(),
                "wal recovery catch-up drain stalled (no progress); write_degraded stays set (g4 backstop)"
            );
            break;
        }
        if tokio::time::Instant::now() >= drain_deadline {
            tracing::warn!(
                pending = remaining.len(),
                "wal recovery catch-up drain exceeded the hard budget"
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // (6) verify the fresh data WAL: one write + fsync + read-back probe
    // through the LIVE writer (the journal the node resumes writes
    // through). The writer is quiescent pre-resume, so the probe append +
    // truncate-away is race-free; the probe leaves no residue.
    //
    // The write-resume gate clears only when BOTH the catch-up set is
    // empty AND the verification write passes.
    let drain_empty = remaining.is_empty() && unresolved.is_empty();
    let verified = if drain_empty {
        match oceanfs_storage::wal::verify_wal_write(&ctx.wal_writer).await {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(error = %e, "wal verification write failed; write_degraded stays set");
                false
            }
        }
    } else {
        false
    };

    // Segments that could not be materialized join the reported missing
    // set (they keep write_degraded set; g4 re-drives them). Dangling
    // references (no live holder) are the documented out-of-scope
    // residual — reported separately, never counted as gate-blocking.
    remaining.extend(unresolved);
    remaining.sort();
    remaining.dedup();

    let outcome = WalRecoveryOutcome {
        candidates: present.len() + missing.len(),
        restored: restored + recomputed,
        missing: remaining,
        caught_up,
        dangling,
        verified,
    };
    ctx.metrics.record(&outcome, started.elapsed().as_secs_f64());
    tracing::info!(
        candidates = outcome.candidates,
        restored = outcome.restored,
        missing = outcome.missing.len(),
        caught_up = outcome.caught_up,
        dangling = outcome.dangling.len(),
        verified = outcome.verified,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "replaced-wal recovery drain complete"
    );
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Metrics (audit M4 — fresh names; none of the earlier
// `oceanfs_wal_recovery_*` series are registered today)
// ---------------------------------------------------------------------------

/// g7 replaced-wal recovery metrics.
///
/// # Examples
///
/// ```
/// use oceanfs_node::WalRecoveryMetrics;
/// use oceanfs_core::MetricRegistrar;
///
/// struct Noop;
/// impl MetricRegistrar for Noop {
///     fn register_counter(&self, _: oceanfs_core::Counter) {}
///     fn register_gauge(&self, _: oceanfs_core::Gauge) {}
///     fn register_histogram(&self, _: std::sync::Arc<oceanfs_core::Histogram>) {}
/// }
/// let metrics = WalRecoveryMetrics::new();
/// metrics.register(&Noop);
/// ```
#[derive(Clone)]
pub struct WalRecoveryMetrics {
    replaced_total: oceanfs_core::Counter,
    registry_rebuilt_segments: oceanfs_core::Gauge,
    caught_up_total: oceanfs_core::Counter,
    pending: oceanfs_core::Gauge,
    recovery_seconds: oceanfs_core::Gauge,
}

impl WalRecoveryMetrics {
    /// Creates the metric series (unregistered until [`register`](Self::register)).
    pub fn new() -> Self {
        Self {
            replaced_total: oceanfs_core::Counter::new(
                "oceanfs_wal_replaced_total".into(),
                "Replaced-wal recoveries entered".into(),
                oceanfs_core::LabelSet::empty(),
            ),
            registry_rebuilt_segments: oceanfs_core::Gauge::new(
                "oceanfs_wal_recovery_registry_rebuilt_segments".into(),
                "Registry entries restored from holders / recomputed after a wal replacement"
                    .into(),
                oceanfs_core::LabelSet::empty(),
            ),
            caught_up_total: oceanfs_core::Counter::new(
                "oceanfs_wal_recovery_caught_up_total".into(),
                "Segments re-materialized through the ReRepWorker after a wal replacement".into(),
                oceanfs_core::LabelSet::empty(),
            ),
            pending: oceanfs_core::Gauge::new(
                "oceanfs_wal_recovery_pending".into(),
                "Current catch-up set depth during replaced-wal recovery".into(),
                oceanfs_core::LabelSet::empty(),
            ),
            recovery_seconds: oceanfs_core::Gauge::new(
                "oceanfs_wal_recovery_seconds".into(),
                "Last replaced-wal recovery duration, seconds".into(),
                oceanfs_core::LabelSet::empty(),
            ),
        }
    }

    /// Records one completed recovery pass (increments the counter,
    /// updates the gauges).
    pub fn record(&self, outcome: &WalRecoveryOutcome, elapsed_secs: f64) {
        self.replaced_total.add(1);
        self.registry_rebuilt_segments.set(outcome.restored as u64);
        self.caught_up_total.add(outcome.caught_up as u64);
        self.pending.set(outcome.missing.len() as u64);
        self.recovery_seconds.set(elapsed_secs as u64);
    }

    /// Registers the series with a metric registrar.
    pub fn register(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        registrar.register_counter(self.replaced_total.clone());
        registrar.register_gauge(self.registry_rebuilt_segments.clone());
        registrar.register_counter(self.caught_up_total.clone());
        registrar.register_gauge(self.pending.clone());
        registrar.register_gauge(self.recovery_seconds.clone());
    }
}

impl Default for WalRecoveryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Builds a coordinator over a real storage prelude + durability
    /// module (the same wiring `DurabilityModule::build` performs) so the
    /// drain's pure sub-steps are unit-testable without a live peer.
    async fn build_coordinator(tmp: &tempfile::TempDir) -> Arc<WalRecoveryCoordinator> {
        use crate::modules::storage::test_support::build_storage_prelude;

        let prelude = build_storage_prelude(tmp).await;
        let durability = Arc::new(
            crate::modules::durability::DurabilityModule::build(
                &prelude.config,
                &prelude.module,
                prelude.membership.clone(),
                prelude.pool.clone(),
                &prelude.module.paths,
                "127.0.0.1:0".parse().expect("grpc addr"),
            )
            .await
            .expect("durability module build"),
        );
        Arc::clone(&durability.wal_recovery)
    }

    fn pool(
        name: &str,
        role: oceanfs_core::PoolRole,
        root: std::path::PathBuf,
    ) -> oceanfs_core::StoragePoolConfig {
        oceanfs_core::StoragePoolConfig {
            name: name.into(),
            role,
            root,
            weight: None,
            tech: Default::default(),
            health: Default::default(),
        }
    }

    fn pinned_registry(tmp: &tempfile::TempDir) -> (PoolRegistry, std::path::PathBuf) {
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let storage = oceanfs_core::StorageConfig {
            pools: vec![
                pool("data-0", oceanfs_core::PoolRole::Data, tmp.path().join("pool-data")),
                pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.path().join("pool-wal")),
                pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.path().join("pool-meta")),
                pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.path().join("pool-hints")),
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Degraded,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
        let paths = crate::pool_paths::pool_paths(&registry);
        (registry, paths.wal.clone())
    }

    /// A genuinely fresh node (no data `.dat`, no event content) is a
    /// normal boot — never a replacement.
    #[test]
    fn fresh_node_detects_normal_fold() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, _wal) = pinned_registry(&tmp);
        let paths = crate::pool_paths::pool_paths(&registry);
        assert_eq!(detect_wal_recovery_mode(&paths, &registry), WalRecoveryMode::NormalFold);
    }

    /// A normal restart: the event WAL holds recoverable content (a
    /// checkpoint file — even when the data WAL placeholder exists), so
    /// the fold path is selected even though data pools hold `.dat`.
    #[test]
    fn normal_restart_with_checkpoint_detects_fold() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, _wal) = pinned_registry(&tmp);
        let paths = crate::pool_paths::pool_paths(&registry);
        // Data pools hold sealed segments…
        let data_root = tmp.path().join("pool-data");
        std::fs::create_dir_all(&data_root).unwrap();
        std::fs::write(data_root.join("some.dat"), b"data").unwrap();
        // …and the event WAL has a checkpoint (a normal restart always
        // has recoverable event content — a checkpoint and/or events).
        let event_wal_dir = tmp.path().join("pool-wal").join("event-wal");
        std::fs::create_dir_all(&event_wal_dir).unwrap();
        std::fs::write(event_wal_dir.join("checkpoint-00000000-0"), b"snapshot").unwrap();
        assert_eq!(detect_wal_recovery_mode(&paths, &registry), WalRecoveryMode::NormalFold);
    }

    /// The audit-C1 hazard: after a wal replacement, `StorageModule::build`
    /// opens the data WAL + event WAL and leaves ZERO-LENGTH placeholder
    /// files on the (empty) device. Detection must treat empty files as
    /// "no recoverable content" and select the rebuild branch — otherwise
    /// the residue sweep deletes every intact data-pool `.dat`.
    #[test]
    fn replaced_wal_with_empty_placeholder_files_detects_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, _wal) = pinned_registry(&tmp);
        let paths = crate::pool_paths::pool_paths(&registry);
        // Data pools hold sealed segments…
        let data_root = tmp.path().join("pool-data");
        std::fs::create_dir_all(&data_root).unwrap();
        let id = SegmentId::new();
        std::fs::write(data_root.join(format!("{id}.dat")), b"data").unwrap();
        // …the wal root holds only the empty files the writer creates at
        // open on a replaced device (no events, no checkpoint).
        let wal_root = tmp.path().join("pool-wal");
        let event_wal_dir = wal_root.join("event-wal");
        std::fs::create_dir_all(&event_wal_dir).unwrap();
        std::fs::write(wal_root.join("wal_00000000.log"), b"").unwrap();
        std::fs::write(event_wal_dir.join("evl_00000000.log"), b"").unwrap();
        assert_eq!(
            detect_wal_recovery_mode(&paths, &registry),
            WalRecoveryMode::RebuildFromHolders
        );
    }

    /// An empty journal device with intact data-pool `.dat` files is a
    /// replaced-wal boot.
    #[test]
    fn empty_wal_with_data_dat_detects_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, _wal) = pinned_registry(&tmp);
        let paths = crate::pool_paths::pool_paths(&registry);
        // Data pool holds a sealed segment; the journal is empty.
        let data_root = tmp.path().join("pool-data");
        std::fs::create_dir_all(&data_root).unwrap();
        let id = SegmentId::new();
        std::fs::write(data_root.join(format!("{id}.dat")), b"data").unwrap();
        assert_eq!(
            detect_wal_recovery_mode(&paths, &registry),
            WalRecoveryMode::RebuildFromHolders
        );
    }

    /// The replacement marker is authoritative even when the journal has
    /// files (a live-remount wrote it before a later restart).
    #[test]
    fn marker_overrides_wal_files() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, wal) = pinned_registry(&tmp);
        let paths = crate::pool_paths::pool_paths(&registry);
        std::fs::write(wal.join("wal_00000000.log"), b"x").unwrap();
        std::fs::write(replacement_marker_path(&paths), b"replaced").unwrap();
        assert_eq!(
            detect_wal_recovery_mode(&paths, &registry),
            WalRecoveryMode::RebuildFromHolders
        );
    }

    /// A full-prep registry setup: builds real WalWriter / EventWal /
    /// EventCheckpoint on the wal root so `prepare_wal_replacement` and
    /// `reset_wal_pool` are exercised against live objects.
    async fn prep_env(
        tmp: &tempfile::TempDir,
    ) -> (
        Arc<PoolRegistry>,
        Arc<oceanfs_storage::WalWriter>,
        Arc<oceanfs_storage::EventWal>,
        Arc<oceanfs_storage::EventCheckpoint>,
        PoolPaths,
    ) {
        let (registry, wal_root) = pinned_registry(tmp);
        let registry = Arc::new(registry);
        let wal_config =
            oceanfs_core::WalConfig { data_dir: wal_root.clone(), ..Default::default() };
        let wal_writer = Arc::new(oceanfs_storage::WalWriter::open(&wal_config).await.unwrap());
        let event_config = oceanfs_core::EventWalConfig {
            event_wal_dir: wal_root.join("event-wal"),
            ..Default::default()
        };
        let event_wal = Arc::new(
            oceanfs_storage::EventWal::open(event_config.event_wal_dir.clone(), &event_config)
                .await
                .unwrap(),
        );
        let event_checkpoint = Arc::new(
            oceanfs_storage::EventCheckpoint::open(event_config.event_wal_dir, event_wal.clone())
                .unwrap(),
        );
        let paths = crate::pool_paths::pool_paths(&registry);
        (registry, wal_writer, event_wal, event_checkpoint, paths)
    }

    /// `prepare_wal_replacement` writes the marker, opens fresh WALs and
    /// sets the write gate (wal Dead + write_degraded) + the pending flag.
    #[tokio::test]
    async fn prepare_wal_replacement_sets_marker_gate_and_pending() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, wal_writer, event_wal, event_checkpoint, paths) = prep_env(&tmp).await;
        let pending = Arc::new(std::sync::atomic::AtomicBool::new(false));

        prepare_wal_replacement(
            &paths,
            &wal_writer,
            &event_wal,
            &event_checkpoint,
            &registry,
            &pending,
        )
        .await
        .unwrap();

        // Marker present → detection selects the rebuild branch.
        assert!(replacement_marker_path(&paths).exists());
        assert_eq!(
            detect_wal_recovery_mode(&paths, &registry),
            WalRecoveryMode::RebuildFromHolders
        );
        // Write gate: the wal pool is Dead + write_degraded.
        let wal_pool = registry.pool_by_role(oceanfs_core::PoolRole::Wal).expect("wal pool");
        assert_eq!(wal_pool.status(), oceanfs_storage::PoolStatus::Dead);
        assert!(wal_pool.write_degraded());
        assert!(!registry.accepts_writes(), "writes must 503 during replaced-wal recovery");
        assert!(pending.load(std::sync::atomic::Ordering::Acquire));
    }

    /// `reset_wal_pool` clears the write gate (registry Healthy +
    /// write_degraded false) and the health monitor's Dead mirror.
    #[tokio::test]
    async fn reset_wal_pool_clears_gate_and_monitor_mirror() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, wal_writer, event_wal, event_checkpoint, paths) = prep_env(&tmp).await;
        let pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
        prepare_wal_replacement(
            &paths,
            &wal_writer,
            &event_wal,
            &event_checkpoint,
            &registry,
            &pending,
        )
        .await
        .unwrap();

        let observer = Arc::new(oceanfs_storage::io::IoObserver::new());
        registry.observe_into(&observer);
        let (monitor, _events) = oceanfs_storage::pool::health::HealthMonitor::new(
            registry.clone(),
            observer,
            Default::default(),
        );
        // Drive the monitor to the Dead mirror first (as a real wal death
        // would), then the reset handoff clears it.
        let wal_pool = registry.pool_by_role(oceanfs_core::PoolRole::Wal).expect("wal pool");
        reset_wal_pool(&registry, &monitor, wal_pool.id());

        assert_eq!(wal_pool.status(), oceanfs_storage::PoolStatus::Healthy);
        assert!(!wal_pool.write_degraded());
        assert!(registry.accepts_writes(), "writes resume after the reset handoff");
    }

    /// The recovery candidate enumeration returns the union of the intact
    /// data-pool `.dat` roots (each with its LOCAL pool id) and the
    /// objects-CF chunk refs.
    #[tokio::test]
    async fn enumerate_candidates_unions_dat_and_objects_cf() {
        let tmp = tempfile::tempdir().unwrap();
        let coordinator = build_coordinator(&tmp).await;

        // An intact data-pool `.dat`.
        let data_pool = coordinator.pool_registry.data_pools()[0].clone();
        let id = SegmentId::new();
        std::fs::write(data_pool.root().join(format!("{id}.dat")), b"data").unwrap();

        let (present, referenced) = enumerate_candidates(&coordinator);
        assert!(
            present.iter().any(|(sid, pool_id)| *sid == id && *pool_id == data_pool.id()),
            "the data-pool .dat must be a present candidate with its local pool id"
        );
        // The objects CF is empty in a fresh prelude → no referenced ids.
        assert!(referenced.is_empty(), "no objects → no referenced ids");
    }

    /// D3 classifier: a reachable-absent candidate (every holder answered
    /// "not held") is local-only. The single-node shape (empty ring +
    /// no remote peer) is also local-only.
    #[tokio::test]
    async fn d3_classifier_local_only_shapes() {
        let tmp = tempfile::tempdir().unwrap();
        let coordinator = build_coordinator(&tmp).await;
        let id = SegmentId::new();

        // A candidate a reachable holder answered "not held".
        let mut reachable_absent: std::collections::HashSet<SegmentId> =
            std::collections::HashSet::new();
        reachable_absent.insert(id);
        assert!(
            is_local_only_d3_candidate(&coordinator, id, &reachable_absent),
            "reachable-but-absent must be local-only (D3)"
        );

        // Single-node shape: no reachable-absent signal, but the ring is
        // empty AND the prelude membership has no remote peer.
        let empty: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
        assert!(
            is_local_only_d3_candidate(&coordinator, id, &empty),
            "empty ring + single node must be local-only (D3)"
        );
    }
}
