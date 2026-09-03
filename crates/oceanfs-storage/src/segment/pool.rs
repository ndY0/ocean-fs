//! Active segment pool with pipeline parallelism.
//!
//! A pool of N active segments decouples append latency from seal-time I/O.
//! While one segment is being sealed (written to disk, metadata persisted),
//! the next segment in the pool accepts writes. Combined with per-core segment
//! sharding, this eliminates write blocking during seal cycles.
//!
//! ## Pool States
//!
//! Each slot transitions through a lifecycle:
//! ```text
//! Appending → Sealing (frozen data retained in slot) → Appending
//! ```
//!
//! State and segment live under ONE lock per slot ([`SlotState`]), so the
//! pair can never drift apart: a slot that reports `Appending` always has
//! its buffer, the append + fill-check + freeze transition is a single
//! critical section, and a `Sealing` slot keeps its frozen data until the
//! replacement is installed (ADR-0021 read window). The three TOCTOU
//! windows that had to be patched with retries in the two-lock design are
//! un-representable here (segment-pool-slot-state-machine).
//!
//! Per performance guideline §2.5 (sharded segment buffer), §2.6 (bounded
//! channels), and §2.7 (semaphore-bounded concurrency).
//!
//! # LOCK ORDER
//!
//! `current_index → slot_state → registry shard` (fill path): the frozen
//! buffer is attached to the lifecycle registry entry in the same
//! critical section as the slot freeze, so the read window is continuous
//! (lifecycle-read-path). The registry shard is never held while
//! acquiring a slot lock — `read_source` releases before the caller's
//! slot scan, and the coordinator's transitions never touch slots.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use oceanfs_core::{CodecConfig, PoolConfig, SegmentId, SizeTier};
use parking_lot::{Condvar, Mutex};
use tokio::sync::{mpsc, Semaphore};

use crate::{
    buffer_pool::BufferPool,
    error::{Error, Result},
    segment::{
        buffer::{ActiveSegment, SealedSegment},
        lifecycle::{SegmentLifecycleRegistry, SegmentReadSource},
    },
};

/// Upper bound on how long an append may wait while the pool is silent.
/// The budget is refreshed on every re-activation signal (a busy pool
/// never fails a write, no matter how often the waiting thread loses the
/// race for the fresh slot), and waiters self-heal stranded slots before
/// waiting — so the terminal error below is only reachable when segment
/// creation itself keeps failing.
const SLOT_ACTIVATION_WAIT: std::time::Duration = std::time::Duration::from_millis(10);

/// Each condvar wait sleeps at most this long before re-scanning the
/// slots. Waiting the whole budget in one shot would let a single lost
/// wakeup consume the entire budget and leave exactly one re-scan at the
/// deadline — which under continuous churn often lands in another transit
/// window. Short slices give the re-scan many chances within the budget.
const SLOT_ACTIVATION_WAIT_SLICE: std::time::Duration = std::time::Duration::from_millis(1);

/// Upper bound on the seal pipeline's in-flight set (frozen segments
/// between fill and durable seal) — ADR-0021's bound:
/// `active_pool_size × shard_count × num_tiers ≤ 16` segments ≈ 1 GB
/// worst case. While the registry holds this many frozen buffers, slots
/// are not re-armed: appends wait on slot re-activation (backpressure),
/// the frozen data stays readable via the registry entry, and the seal
/// queue drains. Seals completing below the cap re-open the slots.
const IN_FLIGHT_CAP: usize = 16;

/// A work item sent to the seal worker when a segment is filled.
///
/// Contains the segment's identity, its data (a shared `Bytes` handle
/// into the original buffer — recycled via `release_buffer` after the
/// seal completes), the storage tier, and the EC parameters the seal
/// worker uses to compute and persist per-segment parity at seal time
/// (so EC recovery can repair corrupt data shards).
pub struct SealingWork {
    /// The unique identifier of the segment to seal.
    pub segment_id: SegmentId,
    /// The segment data bytes, shared with the sealing-data set and the
    /// (now replaced) slot's frozen copy.
    pub segment_data: Bytes,
    /// The storage tier of the segment (Small or Standard).
    pub tier: SizeTier,
    /// Number of EC data shards (k). 0 if EC is not used.
    pub ec_k: u8,
    /// Number of EC parity shards (m). 0 if EC is not used.
    pub ec_m: u8,
    /// EC strip size in bytes (the shard size). 0 when EC is not used.
    /// The seal worker uses (k, m, strip) to compute and persist the
    /// segment's parity at seal time on the blocking pool.
    pub strip_size_bytes: usize,
    /// The encoder used for the seal-time EC parity encode. The node
    /// wires the AccelDispatcher so seal encodes are observable through
    /// the accel metrics; `None` falls back to the plain Cauchy encoder.
    pub ec_encoder: Option<std::sync::Arc<dyn oceanfs_ec::Encoder>>,
}

impl std::fmt::Debug for SealingWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealingWork")
            .field("segment_id", &self.segment_id)
            .field("tier", &self.tier)
            .field("ec_k", &self.ec_k)
            .field("ec_m", &self.ec_m)
            .field("strip_size_bytes", &self.strip_size_bytes)
            .field("ec_encoder", &self.ec_encoder.as_ref().map(|_| "<encoder>"))
            .finish_non_exhaustive()
    }
}

/// The state of a pool slot throughout its lifecycle.
///
/// State and segment are unified: each variant carries exactly what the
/// slot holds, so "the slot is Appending" and "the slot has an appendable
/// segment" are the same fact — enforced by the type, not by retries.
pub(crate) enum SlotState {
    /// Slot holds a segment that is actively accepting writes.
    Appending(ActiveSegment),
    /// The slot's segment has been filled and frozen; the data is being
    /// handed to the seal worker while the slot re-arms itself.
    ///
    /// The frozen `Bytes` (and its `SegmentId`) remain in the slot until a
    /// replacement is installed, so the segment stays reachable via
    /// `try_read` for the whole transit — a preempted filler can never
    /// strand a data-less slot (ADR-0021 read window).
    Sealing(SegmentId, Bytes),
    /// No segment. Never produced by the production paths (construction
    /// arms slots directly); kept so the state space is total and
    /// `install_replacement` can also serve future idle slots.
    Idle,
}

/// Result of a successful per-slot append attempt.
struct AppendOutcome {
    /// The segment the data was appended to.
    segment_id: SegmentId,
    /// Offset of the append within the segment.
    offset: u64,
    /// Length of the appended data.
    length: u32,
    /// When the append filled the segment: the sealed payload to hand to
    /// the seal queue (collected outside the slot lock).
    sealed: Option<SealedSegment>,
}

/// A single slot in the segment pool, holding one active segment and its state.
///
/// ONE lock guards state and segment together: a slot that reports
/// [`SlotState::Appending`] always has its buffer, and the fill transition
/// (append → fill-check → freeze → `Sealing`) is a single critical
/// section. The previous two-lock split let state and segment drift apart,
/// creating three TOCTOU windows that had to be patched reactively with
/// retries (segment-pool-slot-state-machine).
pub(crate) struct PoolSlot {
    state: Mutex<SlotState>,
}

impl PoolSlot {
    /// Creates a new pool slot with a fresh active segment.
    fn new(
        tier: SizeTier,
        config: &oceanfs_core::SegmentSizeConfig,
        pool: &BufferPool,
    ) -> Result<Self> {
        let segment = ActiveSegment::new(tier, config, pool)?;
        Ok(Self { state: Mutex::new(SlotState::Appending(segment)) })
    }

    /// Creates a new pool slot in Idle state (no segment).
    #[allow(dead_code)]
    fn new_idle() -> Self {
        Self { state: Mutex::new(SlotState::Idle) }
    }

    /// Returns `true` if the slot is accepting writes.
    fn is_appending(&self) -> bool {
        matches!(*self.state.lock(), SlotState::Appending(_))
    }

    /// Returns `true` if the slot has no appendable segment and may accept
    /// a replacement (Sealing transit or Idle).
    fn needs_segment(&self) -> bool {
        matches!(*self.state.lock(), SlotState::Sealing(..) | SlotState::Idle)
    }

    /// Atomically moves the slot's segment from `Appending` to `Sealing`,
    /// freezing its data in the slot and returning the sealed payload.
    ///
    /// The transition is a single lock acquisition: the frozen `Bytes`
    /// lands in the slot in the same critical section that removes the
    /// segment, so the data is never unreachable (ADR-0021). Parity
    /// collection is deferred to [`SealedSegment::collect_parity`] so no
    /// spinning happens under the lock (perf §7.1).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // PoolSlot is pub(crate); examples are in unit tests.
    /// ```
    // Non-test builds exercise this transition inline under the append
    // critical section; the standalone method is used by the tests.
    #[allow(dead_code)]
    fn take_for_sealing(
        &self,
        registry: &SegmentLifecycleRegistry,
        tier: SizeTier,
        ec_k: u8,
        ec_m: u8,
    ) -> Option<SealedSegment> {
        let mut guard = self.state.lock();
        Self::transition_to_sealing(&mut guard, registry, tier, ec_k, ec_m, false)
    }

    /// The `Appending` → `Sealing` transition, given that the slot lock is
    /// already held (single critical section with the appending append).
    ///
    /// The frozen buffer is attached to the lifecycle registry entry in
    /// THIS critical section (slot lock → registry shard write — the
    /// documented fill-path order): the read window is continuous, with
    /// no freeze→attach gap a concurrent GET could fall into.
    fn transition_to_sealing(
        guard: &mut parking_lot::MutexGuard<'_, SlotState>,
        registry: &SegmentLifecycleRegistry,
        tier: SizeTier,
        ec_k: u8,
        ec_m: u8,
        ignore_cap: bool,
    ) -> Option<SealedSegment> {
        // In-flight cap gate (ADR-0021's bound: ≤ 16 frozen segments
        // ≈ 1 GB worst case — the memory-bound DoD test pins this):
        // while the registry holds the cap's worth of frozen buffers,
        // NO new FILL freeze happens — the slot stays `Appending` with
        // its full segment (still readable via the slot scan; the write
        // that filled it is acked, its WAL entry written, and the
        // recovery re-freezes it once the seal queue can accept it).
        // Appends beyond the cap therefore backpressure on full slots
        // instead of growing the in-flight set. The recovery path
        // (`ignore_cap`) bypasses the gate — its frozen segments'
        // data is already ACKED and the queue capacity check guarantees
        // the work is accepted immediately, breaking the count=16 +
        // empty-queue deadlock.
        if !ignore_cap && registry.in_flight_count() >= IN_FLIGHT_CAP {
            return None;
        }
        let current = std::mem::replace(&mut **guard, SlotState::Idle);
        match current {
            SlotState::Appending(segment) => {
                let sealed = segment.seal();
                **guard = SlotState::Sealing(sealed.segment_id, sealed.data.clone());
                // Attach before returning: the registry entry now owns
                // the read window (was `sealing_data`). The slot keeps
                // its own refcounted copy until the replacement is
                // installed (unchanged transit). The attach inserts a
                // registry-only entry when the write path's durable
                // reserve has not landed yet (fill-before-reserve
                // window — the segment id is only known after the
                // append that filled it).
                if !registry.attach_in_flight(
                    sealed.segment_id,
                    tier,
                    ec_k,
                    ec_m,
                    sealed.data.clone(),
                ) {
                    tracing::warn!(
                        segment_id = %sealed.segment_id,
                        "frozen segment holds a higher lifecycle state; in-flight window unavailable"
                    );
                }
                Some(sealed)
            }
            other => {
                **guard = other;
                None
            }
        }
    }

    /// Installs a replacement segment into a slot that is sealing or idle
    /// — a single pointer swap under one lock acquisition.
    ///
    /// The caller builds the replacement **outside** the lock (perf §7.1);
    /// if another thread already installed a segment (the activation
    /// race), this returns `false` and the caller's replacement is
    /// dropped. The swap shrinks the `Sealing` transit from allocation
    /// time to a pointer move.
    fn install_replacement(&self, replacement: ActiveSegment) -> bool {
        let mut guard = self.state.lock();
        match &mut *guard {
            SlotState::Sealing(..) | SlotState::Idle => {
                *guard = SlotState::Appending(replacement);
                true
            }
            SlotState::Appending(_) => false,
        }
    }

    /// Appends to the slot's segment under ONE lock acquisition.
    ///
    /// The hook runs while the slot lock is held and **before** any
    /// fill-triggered seal transition, preserving the `append_with_hook`
    /// ordering guarantee: the seal work item for this segment can never
    /// be observed by the seal worker before the hook recorded its state.
    ///
    /// Returns `Ok(None)` when the slot is not appending — the hook is
    /// left unconsumed so the caller can retry another slot. Returns
    /// `Ok(Some(outcome))` on success, with `outcome.sealed` carrying the
    /// sealed payload when the append filled the segment.
    fn try_append_with_hook<F: FnOnce(SegmentId, u64, u32)>(
        &self,
        data: &[u8],
        hook: &mut Option<F>,
        registry: &SegmentLifecycleRegistry,
        tier: SizeTier,
        ec_k: u8,
        ec_m: u8,
    ) -> Result<Option<AppendOutcome>, Error> {
        let mut guard = self.state.lock();
        let SlotState::Appending(_) = &mut *guard else {
            // Not appending — the caller scans another slot. The hook is
            // untouched (`FnOnce` lives in the caller's `Option`).
            return Ok(None);
        };

        // Scoped borrow keeps the critical section explicit: append, hook,
        // fill-check and the freeze transition all happen under this lock.
        let (segment_id, offset, length, full) = {
            let SlotState::Appending(segment) = &mut *guard else {
                unreachable!("state checked Appending above and the lock is held")
            };
            let (offset, length) = match segment.append(data) {
                Ok(placed) => placed,
                // Defensive: an Appending slot's segment cannot be full —
                // the fill→Sealing transition happens in the same critical
                // section as the appending append, so no concurrent filler
                // can interleave. Kept so a future regression degrades to
                // a retry on the next slot, not a failed write.
                Err(Error::SegmentFull { .. }) => return Ok(None),
                Err(e) => return Err(e),
            };
            let segment_id = segment.id();
            let length_u32 = u32::try_from(length).unwrap_or(u32::MAX);
            // Airtight ordering: the hook runs under the slot lock, before
            // the fill transition below can hand the segment to the seal
            // queue (read-path-integrity-under-load Defect 2).
            if let Some(hook) = hook.take() {
                hook(segment_id, offset, length_u32);
            }
            (segment_id, offset, length_u32, segment.is_full())
        };

        let sealed = if full {
            Self::transition_to_sealing(&mut guard, registry, tier, ec_k, ec_m, false)
        } else {
            None
        };

        Ok(Some(AppendOutcome { segment_id, offset, length, sealed }))
    }
}

/// A pool of active segments for a single tier and shard.
///
/// Manages N concurrent active segments, rotating through them as segments
/// fill up. When the current appending segment is full, it's moved to the
/// sealing state, a new segment is activated from the pool, and the sealed
/// segment is sent to the EC encoding queue.
///
/// # Examples
///
/// ```text
/// // SegmentPool examples are in unit tests.
/// ```
pub struct SegmentPool {
    /// Pool slots, one per active segment.
    slots: Vec<Arc<PoolSlot>>,
    /// The storage tier this pool serves.
    tier: SizeTier,
    /// Segment size configuration for creating new active segments.
    size_config: oceanfs_core::SegmentSizeConfig,
    /// EC codec configuration; when set, the seal worker computes and
    /// persists per-segment parity at seal time.
    ec_config: Option<CodecConfig>,
    /// Encoder for seal-time EC parity (the node wires the
    /// AccelDispatcher; None falls back to the plain Cauchy encoder).
    ec_encoder: Option<std::sync::Arc<dyn oceanfs_ec::Encoder>>,
    /// Index of the current appending slot (round-robin).
    current_index: Mutex<usize>,
    /// Sender for the seal work queue.
    seal_tx: mpsc::Sender<SealingWork>,
    /// Receiver for the seal work queue (held by the pool, drained by a seal worker).
    seal_rx: Mutex<Option<mpsc::Receiver<SealingWork>>>,
    /// Semaphore limiting in-flight seals (enforces bounded concurrency).
    seal_semaphore: Arc<Semaphore>,
    /// Pool configuration.
    #[allow(dead_code)]
    config: PoolConfig,
    /// Reference to the buffer pool for creating new active segments.
    buffer_pool: Arc<BufferPool>,
    /// The lifecycle registry — the machine that owns the read path
    /// resolution and the in-flight read window (ADR-0025 Decision 2).
    /// The fill transition attaches the frozen buffer to the registry
    /// entry; `try_read` resolves through it.
    registry: Arc<SegmentLifecycleRegistry>,
    /// Wake-up signal for appenders waiting on slot re-activation
    /// (bounded backpressure when a concurrent burst fills every slot).
    /// The mutex guards nothing; it only serves as the wait primitive
    /// for the condvar — waiters re-scan the slot states after waking.
    slot_activation: (Mutex<()>, Condvar),
    /// Async wakeup for the non-blocking append path
    /// (`append_with_hook_async`): notified on every slot re-activation
    /// so async waiters re-scan without a fixed fail budget — the
    /// caller's deadline bounds the wait (backpressure propagates up).
    slot_activation_notify: tokio::sync::Notify,
    /// Test-only: when set, slot re-activation fails (simulates a stuck
    /// activation) so the timeout path of the bounded wait is reachable.
    #[cfg(test)]
    fail_activation: std::sync::atomic::AtomicBool,
}

/// How long a replay enqueue may wait for seal-queue space before
/// failing startup. Generous: the seal worker drains concurrently
/// during replay (started before the WAL replay in the node).
const REPLAY_SEAL_ENQUEUE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// How long the idle-seal driver's in-flight retry waits for seal-queue
/// space before deferring to the next tick (10 ms — a draining queue
/// accepts the work in microseconds; a stalled one is retried next
/// tick, the in-flight window stays readable throughout).
const INFLIGHT_RETRY_ENQUEUE_DEADLINE: std::time::Duration = std::time::Duration::from_millis(10);

/// Copies `[offset, offset+length)` from a byte buffer (the append-mode
/// buffer is `BytesMut`-backed and cannot be shared without freezing);
/// clamped to the buffer end; `None` when the offset is past the end.
fn slice_range(data: &[u8], offset: u64, length: u32) -> Option<Bytes> {
    let start = offset as usize;
    let end = start.saturating_add(length as usize).min(data.len());
    if start < data.len() {
        Some(Bytes::copy_from_slice(&data[start..end]))
    } else {
        None
    }
}

/// Slices `[offset, offset+length)` from a frozen `Bytes` — zero-copy
/// (refcount only); clamped to the buffer end; `None` when the offset
/// is past the end.
fn slice_bytes(data: &Bytes, offset: u64, length: u32) -> Option<Bytes> {
    let start = offset as usize;
    let end = start.saturating_add(length as usize).min(data.len());
    if start < data.len() {
        Some(data.slice(start..end))
    } else {
        None
    }
}

impl SegmentPool {
    /// Creates a new segment pool.
    ///
    /// If `config.ec_streaming_encode` is `true` and `ec_config` is
    /// provided, seal work items carry the EC parameters (k, m, strip) so
    /// the seal worker persists per-segment parity; otherwise segments
    /// are sealed without parity.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer pool cannot provide enough buffers
    /// for the initial active segments.
    pub fn new(
        config: PoolConfig,
        tier: SizeTier,
        size_config: &oceanfs_core::SegmentSizeConfig,
        buffer_pool: Arc<BufferPool>,
        ec_config: Option<CodecConfig>,
        ec_encoder: Option<std::sync::Arc<dyn oceanfs_ec::Encoder>>,
        registry: Arc<SegmentLifecycleRegistry>,
    ) -> Result<Self> {
        let (seal_tx, seal_rx) = mpsc::channel(config.encode_queue_capacity);

        // Resolve EC config: only carry EC parameters when the flag is
        // set AND a codec is provided.
        let actual_ec = if config.ec_streaming_encode { ec_config } else { None };

        // Create pool_size appending slots.
        let mut slots = Vec::with_capacity(config.active_pool_size);
        for _ in 0..config.active_pool_size {
            let slot = PoolSlot::new(tier, size_config, buffer_pool.as_ref())?;
            slots.push(Arc::new(slot));
        }

        let seal_semaphore = Arc::new(Semaphore::new(config.max_inflight_encodes));

        Ok(Self {
            slots,
            tier,
            size_config: size_config.clone(),
            ec_config: actual_ec,
            ec_encoder,
            current_index: Mutex::new(0),
            seal_tx,
            seal_rx: Mutex::new(Some(seal_rx)),
            seal_semaphore,
            registry,
            config,
            buffer_pool: Arc::clone(&buffer_pool),
            slot_activation: (Mutex::new(()), Condvar::new()),
            slot_activation_notify: tokio::sync::Notify::new(),
            #[cfg(test)]
            fail_activation: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Appends replayed WAL data to the segment with the given id.
    ///
    /// WAL replay runs single-threaded at startup, before the server
    /// accepts writes, so this uses a plain critical section per slot
    /// (no backpressure machinery):
    ///
    /// 1. If a slot already holds an appending segment with
    ///    `segment_id`, append there — WAL entries for one segment are
    ///    contiguous, so replay order reconstructs the original offsets.
    /// 2. Otherwise, claim the first slot whose fresh (empty) segment
    ///    has never been written and replace it with a segment carrying
    ///    the original id.
    ///
    /// Rebuilding under the **original** id is what makes object
    /// metadata (committed before the crash and referencing that id)
    /// readable again — the pool fallback reader finds the rebuilt
    /// segment, so no object data is lost.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when no slot can host the
    /// segment (more live segments than pool slots) and
    /// [`Error::WriteBackpressureTimeout`] when the seal queue cannot
    /// accept the filled segment within the deadline.
    pub async fn append_replayed(&self, segment_id: SegmentId, data: &[u8]) -> Result<()> {
        // Pass 1: append to a slot already rebuilding this segment. WAL
        // entries for one segment are contiguous, so appending in replay
        // order reconstructs the original offsets.
        for slot in &self.slots {
            // The slot lock is scoped to this block: the async seal
            // handoff below must never run while holding it.
            let sealed = {
                let mut guard = slot.state.lock();
                let SlotState::Appending(segment) = &mut *guard else {
                    continue;
                };
                if segment.id() != segment_id {
                    continue;
                }
                let full = {
                    let (_offset, _length) = match segment.append(data) {
                        Ok(placed) => placed,
                        Err(Error::SegmentFull { .. }) => {
                            // The fill→Sealing transition happens in the
                            // same critical section as the appending
                            // append, so a full Appending segment is
                            // unreachable here.
                            return Err(Error::InvalidConfig(
                                "replayed append hit a full appending segment".into(),
                            ));
                        }
                        Err(e) => return Err(e),
                    };
                    segment.is_full()
                };
                if full {
                    // The segment filled — freeze it through the shared
                    // transition (which attaches the frozen buffer to
                    // the lifecycle registry entry under this lock).
                    // `seal` consumes the segment, so the id check above
                    // guarantees `Appending` here.
                    let (ec_k, ec_m, _strip) = self.ec_params();
                    PoolSlot::transition_to_sealing(
                        &mut guard,
                        &self.registry,
                        self.tier,
                        ec_k,
                        ec_m,
                        false,
                    )
                } else {
                    None
                }
            };
            // [review][algorithmic][critical]
            // same remarks regarding the use of a callback + signaling (oneshot or Notify), to avoid using arbitrary timers
            // [end]
            // Await queue space — a dropped seal work item would orphan
            // the segment's data (the WAL is truncated after replay).
            self.finish_seal_handoff_async(
                sealed,
                std::time::Instant::now() + REPLAY_SEAL_ENQUEUE_DEADLINE,
            )
            .await?;
            return Ok(());
        }

        // Pass 2: no slot is rebuilding this segment yet. Claim the first
        // slot whose fresh (empty) segment has never been written, and
        // replace it with a segment carrying the original id. Replay runs
        // single-threaded at startup before the server accepts writes, so
        // the plain critical section below cannot race other appenders.
        //
        // Self-heal first: a crash can leave MORE in-flight segments than
        // the pool has slots (16 workers × 16 MiB bodies fills many
        // segments per interval). Sealing slots hold frozen data that has
        // already been handed off; installing their replacement recycles
        // them, so replay of arbitrarily many segments always progresses.
        self.try_activate_slot();
        for slot in &self.slots {
            let sealed = {
                let mut guard = slot.state.lock();
                let SlotState::Appending(segment) = &mut *guard else {
                    continue;
                };
                if !segment.is_empty() {
                    continue;
                }
                let mut replacement = ActiveSegment::new_with_id(
                    segment_id,
                    self.tier,
                    &self.size_config,
                    self.buffer_pool.as_ref(),
                )?;
                let (_offset, _length) = replacement.append(data)?;
                let full = replacement.is_full();
                if full {
                    let sealed = replacement.seal();
                    *guard = SlotState::Sealing(sealed.segment_id, sealed.data.clone());
                    // Attach the frozen buffer to the machine entry in
                    // this critical section (same discipline as the
                    // write-path fill — the window is continuous).
                    let (ec_k, ec_m, _strip) = self.ec_params();
                    if !self.registry.attach_in_flight(
                        sealed.segment_id,
                        self.tier,
                        ec_k,
                        ec_m,
                        sealed.data.clone(),
                    ) {
                        tracing::warn!(
                            segment_id = %sealed.segment_id,
                            "replayed frozen segment holds a higher state; window unavailable"
                        );
                    }
                    Some(sealed)
                } else {
                    *guard = SlotState::Appending(replacement);
                    None
                }
            };
            self.finish_seal_handoff_async(
                sealed,
                std::time::Instant::now() + REPLAY_SEAL_ENQUEUE_DEADLINE,
            )
            .await?;
            return Ok(());
        }

        // Every slot is occupied by a distinct non-empty segment: the
        // WAL holds more live segments than the pool has slots. This is
        // impossible for a crash-recovery replay of a pool with the same
        // slot count — each slot held at most one unsealed segment.
        {
            let states: Vec<String> = self
                .slots
                .iter()
                .map(|s| {
                    let g = s.state.lock();
                    match &*g {
                        SlotState::Appending(seg) => {
                            format!("Appending({}B)", seg.data().len())
                        }
                        SlotState::Sealing(id, d) => {
                            format!("Sealing({}, {}B)", id, d.len())
                        }
                        SlotState::Idle => "Idle".into(),
                    }
                })
                .collect();
            tracing::error!(
                tier = ?self.tier,
                slot_count = self.slots.len(),
                states = ?states,
                "no pool slot available to rebuild replayed segment"
            );
            Err(Error::InvalidConfig("no pool slot available to rebuild replayed segment".into()))
        }
    }

    // [review][cleanup][high]
    // this function is only ever used in tests, if not used, should be discarded and pruned for any test usage.
    // carefull, the inner function is also a single use
    // [end]
    /// Appends data to the current active segment.
    ///
    /// If the current segment is full after the append, it triggers a
    /// rotation to the next idle slot.
    ///
    /// This is a synchronous operation because the pool uses `parking_lot::Mutex`
    /// internally — lock hold times are microsecond-scale so blocking the
    /// calling thread is negligible. Callers on the tokio runtime should
    /// use `tokio::task::spawn_blocking` if wrapping is desired. When a
    /// concurrent burst fills every slot at once, the call may additionally
    /// block for up to `SLOT_ACTIVATION_WAIT` until a slot is re-activated
    /// (normally microseconds); waiters self-heal stranded slots, so the
    /// append only fails with [`Error::InvalidConfig`] when segment creation
    /// itself keeps failing.
    ///
    /// # Returns
    ///
    /// `(segment_id, offset, length)` identifying the written data.
    ///
    /// # Errors
    ///
    /// Returns an error if no segment is available for writing or if
    /// the underlying append operation fails.
    pub fn append(&self, data: &[u8]) -> Result<(SegmentId, u64, u32)> {
        self.append_with_hook(data, |_, _, _| {})
    }

    /// Appends data and invokes `hook(segment_id, offset, length)` after
    /// the append but **before** any fill-triggered seal enqueue.
    ///
    /// The hook runs under the slot lock, which makes the ordering
    /// airtight: a seal worker running on another thread cannot observe
    /// the seal work item before the hook has recorded its state (e.g.
    /// the write coordinator's blob index entry). The slot state machine
    /// guarantees this structurally — append, hook, fill-check and the
    /// `Appending → Sealing` transition are one critical section
    /// (segment-pool-slot-state-machine).
    ///
    /// # Errors
    ///
    /// Same error conditions as [`append`](Self::append).
    pub fn append_with_hook<F: FnOnce(SegmentId, u64, u32)>(
        &self,
        data: &[u8],
        hook: F,
    ) -> Result<(SegmentId, u64, u32)> {
        self.append_to_next_available_with_hook(data, hook)
    }

    /// Returns the number of slots in Appending state.
    ///
    /// A live "pipeline is producing" signal: slots churn between
    /// Appending and Sealing as segments fill, so the count oscillates
    /// under load and collapses only if the pool machinery stalls.
    pub fn active_count(&self) -> usize {
        self.slots.iter().filter(|slot| slot.is_appending()).count()
    }

    // [review][cleanup][medium]
    // remove dead code if never used. if only used in test, use a conditionnal test directive
    // [end]
    /// Returns the number of pool slots.
    #[allow(dead_code)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Reads a chunk from an active (unsealed) segment in this pool.
    ///
    /// Resolves "where do I read this segment?" through the lifecycle
    /// registry in one lookup (ADR-0025 Decision 2): the registry's
    /// in-flight buffer (`InFlight` — was the `sealing_data` side-map)
    /// or an active slot buffer (`ActiveSlot` — the slot scan, bounded
    /// by `active_pool_size × num_tiers`). `Sealed`/`Missing` → `None`,
    /// the caller's fall-through (`DiskSegmentReader` / replica).
    ///
    /// The slot scan serves only append-mode slots — `Sealing` slots
    /// no longer hold the read window (the registry entry does,
    /// attached in the same critical section as the freeze). A segment
    /// that froze between the resolution and the scan is re-resolved
    /// once: the registry now carries its in-flight data.
    ///
    /// Returns `None` if no segment in this pool matches the id.
    /// This is a fast, synchronous operation — only a memcpy under the
    /// slot mutex, same lock used by `append`.
    pub fn try_read(&self, segment_id: SegmentId, offset: u64, length: u32) -> Option<Bytes> {
        match self.registry.read_source(segment_id) {
            // The registry's in-flight buffer is immutable frozen data —
            // serve it as a zero-copy `Bytes` slice (no copy, perf §1.1).
            SegmentReadSource::InFlight(data) => slice_bytes(&data, offset, length),
            SegmentReadSource::ActiveSlot => {
                for slot in self.slots.iter() {
                    let guard = slot.state.lock();
                    if let SlotState::Appending(segment) = &*guard {
                        if segment.id() == segment_id {
                            let data = segment.data();
                            return slice_range(data, offset, length);
                        }
                    }
                }
                // The segment froze between the resolution and the scan
                // (a concurrent fill attached its buffer to the registry
                // entry): re-resolve once — the in-flight window is
                // continuous.
                match self.registry.read_source(segment_id) {
                    SegmentReadSource::InFlight(data) => slice_bytes(&data, offset, length),
                    _ => None,
                }
            }
            SegmentReadSource::Sealed | SegmentReadSource::Missing => None,
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Appends data to the next available appending slot (round-robin).
    ///
    /// When every slot is transiently unavailable — a concurrent burst can
    /// fill all slots at once, leaving each in the `Sealing` transit while
    /// its replacement segment is allocated — this method self-heals
    /// stranded slots, then waits (bounded by `SLOT_ACTIVATION_WAIT`) for
    /// a slot re-activation instead of failing the write. Re-activation is
    /// performed synchronously by the filling thread right after the seal
    /// enqueue, so the wait is normally microseconds; the terminal error is
    /// only reachable when segment creation itself keeps failing.
    fn append_to_next_available_with_hook<F: FnOnce(SegmentId, u64, u32)>(
        &self,
        data: &[u8],
        hook: F,
    ) -> Result<(SegmentId, u64, u32)> {
        // The hook is `FnOnce`: wrap it so a failed scan pass doesn't
        // consume it before the successful append.
        let mut hook = Some(hook);
        let mut deadline = std::time::Instant::now() + SLOT_ACTIVATION_WAIT;
        loop {
            if let Some(outcome) = self.try_append_single_pass(data, &mut hook)? {
                // Sync hand-off: `try_send`, drop-on-full as a safety
                // valve (the sync path cannot await; the production
                // async path guarantees the enqueue instead).
                self.finish_seal_handoff(outcome.sealed);
                return Ok((outcome.segment_id, outcome.offset, outcome.length));
            }

            // Every slot is unavailable. Recover full-but-unfrozen slots
            // first (their fill was gated by the in-flight cap — the cap
            // may have dropped since, and the full segments must resume
            // the seal pipeline or the pool stalls), then self-heal: if a
            // concurrent filler was descheduled between freezing its
            // segment and installing the replacement, a waiter installs it
            // — a Sealing slot (whose frozen data is already in the slot
            // and the registry entry) can never block the pool
            // indefinitely.
            self.recover_full_slots_sync();
            self.try_activate_slot();

            // Wait for a re-activation signal in short slices, re-scanning
            // between them: a lost wakeup costs at most one slice, never
            // the whole budget.
            let (lock, cvar) = &self.slot_activation;
            let mut guard = lock.lock();
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                drop(guard);
                break;
            }
            // Spurious wakeups are harmless: the loop re-scans and
            // re-enters the wait with the remaining budget.
            let outcome = cvar.wait_for(&mut guard, remaining.min(SLOT_ACTIVATION_WAIT_SLICE));
            drop(guard);
            if !outcome.timed_out() {
                // An activation occurred: the pool is making progress and
                // the waiter simply lost the race for the fresh slot. The
                // budget measures silence (a genuinely stuck pool), not
                // losing streaks — refresh it so a busy-but-fair pool
                // never fails writes.
                deadline = std::time::Instant::now() + SLOT_ACTIVATION_WAIT;
            }
        }
        Err(Error::InvalidConfig("no appending segment available in pool".into()))
    }

    // [review][architecture][criticial]
    // this approach should be the standard for any call site trying to append data to the pool,
    // the backpressure avoid using any kind of arbitrary timer
    // [end]
    /// Appends data with asynchronous backpressure: never fails on a
    /// transiently exhausted pool, never blocks a runtime worker, and
    /// **never drops a seal work item** — the filled segment's enqueue
    /// awaits queue space (bounded by `timeout`), so a write that
    /// returns `Ok` is guaranteed to be enqueued for sealing before the
    /// caller records its WAL entry (no orphaned acknowledged writes).
    ///
    /// Scans the slots once per iteration; when every slot is in the
    /// `Sealing` transit, self-heals stranded slots and awaits a
    /// re-activation notification (tokio) instead of failing. The wait
    /// is bounded by `timeout` — on expiry the write is rejected with
    /// [`Error::WriteBackpressureTimeout`], which callers propagate as a
    /// retryable `503 SlowDown` (nothing was recorded; the client may
    /// safely retry). This is the production write path: the fixed
    /// 10 ms budget of the synchronous [`append_with_hook`](Self::append_with_hook)
    /// is replaced by the caller's deadline, so transient scheduling
    /// jitter or slow activation allocations queue instead of failing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WriteBackpressureTimeout`] when no slot
    /// re-activated — or no seal-queue space freed — within `timeout`.
    /// Returns the underlying append error otherwise.
    pub async fn append_with_hook_async<F: FnOnce(SegmentId, u64, u32)>(
        &self,
        data: &[u8],
        hook: F,
        timeout: std::time::Duration,
    ) -> Result<(SegmentId, u64, u32, Option<SealedSegment>)> {
        let mut hook = Some(hook);
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(outcome) = self.try_append_single_pass(data, &mut hook)? {
                // The seal hand-off is DELIBERATELY not enqueued here:
                // the caller (the write path) appends the segment's data
                // WAL entry and records its position AFTER this returns,
                // and enqueues the payload via
                // [`Self::enqueue_seal_handoff`]. That ordering is the
                // retention contract (ADR-0024 §Retention): the seal
                // worker must never observe the work item before the
                // position record, or the `SealEvent` would capture a
                // stale `(0, 0)` and pin the segment's WAL files forever.
                // An enqueue failure is surfaced by the caller's enqueue
                // call — the write is rejected (never acked), the
                // in-flight read window stays owned by the registry
                // entry, and the idle driver retries the seal.
                return Ok((outcome.segment_id, outcome.offset, outcome.length, outcome.sealed));
            }
            // Recover full-but-unfrozen slots (their fill was gated by
            // the in-flight cap — freeze + hand off now that the cap may
            // have dropped), then self-heal stranded slots (same
            // fallback as the sync path). The `notified()` future is
            // registered BEFORE the self-heal so a notification from a
            // racing installer cannot be lost; when this call itself
            // installs a segment, re-scan immediately instead of waiting
            // on our own notification.
            self.recover_full_slots_async(deadline).await?;
            let notified = self.slot_activation_notify.notified();
            tokio::pin!(notified);
            if self.try_activate_slot() {
                continue;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::WriteBackpressureTimeout);
            }
            // The timeout outcome is intentionally ignored: whether the
            // wait expired or a re-activation fired, the loop re-scans and
            // re-checks the deadline at the top.
            let _ = tokio::time::timeout(remaining, &mut notified).await;
        }
    }

    /// Enqueues a sealed payload returned by `append_with_hook_async`
    /// — the caller-side half of the seal hand-off.
    ///
    /// The write path calls this AFTER its data-WAL append and position
    /// record (the record precedes the enqueue, in the same task), so
    /// the seal worker can never observe the work item before the
    /// segment's final data-WAL position is recorded — the
    /// `SealEvent`'s `data_wal_pos` is then always the final one
    /// (ADR-0024 §Retention; a seal can never capture a stale `(0, 0)`
    /// that would pin the segment's WAL files).
    ///
    /// Mirrors the internal `finish_seal_handoff_async` semantics:
    /// the frozen slot is re-armed and the work item enqueued within
    /// `deadline`; on failure the write is rejected (never acked), the
    /// registry entry keeps the in-flight read window, and the
    /// pending-seal drain retries the seal.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WriteBackpressureTimeout`] when the seal queue
    /// cannot accept the work within `deadline` (or the pool is shut
    /// down).
    pub async fn enqueue_seal_handoff(
        &self,
        sealed: Option<SealedSegment>,
        deadline: std::time::Instant,
    ) -> Result<(), Error> {
        self.finish_seal_handoff_async(sealed, deadline).await
    }

    /// One non-waiting scan pass over all slots, starting at the
    /// round-robin index. Returns `Ok(Some(outcome))` on success (the
    /// caller performs the seal hand-off), `Ok(None)` when every slot
    /// was unavailable.
    fn try_append_single_pass<F: FnOnce(SegmentId, u64, u32)>(
        &self,
        data: &[u8],
        hook: &mut Option<F>,
    ) -> Result<Option<AppendOutcome>, Error> {
        // Round-robin start so a busy pool spreads appends across
        // slots instead of always probing slot 0 first.
        let start = {
            let mut current = self.current_index.lock();
            let idx = *current;
            *current = (*current + 1) % self.slots.len();
            idx
        };

        for offset in 0..self.slots.len() {
            let slot = &self.slots[(start + offset) % self.slots.len()];
            let (ec_k, ec_m, _strip) = self.ec_params();
            match slot.try_append_with_hook(data, hook, &self.registry, self.tier, ec_k, ec_m) {
                Ok(Some(outcome)) => {
                    // The hook already ran under the slot lock, so the
                    // seal worker can never observe the work item
                    // before the hook recorded its state — regardless
                    // of when the caller enqueues it. The caller (write
                    // path) additionally enqueues only AFTER its
                    // data-WAL append + position record, so the seal
                    // worker also can never observe the work item
                    // before the segment's final `data_wal_pos` exists
                    // (ADR-0024 §Retention — no stale `(0, 0)` seals).
                    return Ok(Some(outcome));
                }
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    /// Freezes `Appending` slots holding FULL segments (their fill was
    /// gated by the in-flight cap) and hands the sealed payloads off —
    /// synchronous variant. Called when a pass found no appendable
    /// slot: the cap may have dropped since the fills, and the full
    /// segments must resume the seal pipeline or the pool stalls on
    /// full-but-unfrozen slots.
    fn recover_full_slots_sync(&self) {
        // The recovery is the seal pipeline's continuation for
        // cap-gated fills (their data is ACKED — the WAL entry exists):
        // it must proceed whenever the seal queue can accept the work.
        // A stalled queue (no capacity) defers the freeze — the entries
        // stay readable and the count stays at the cap; a draining
        // queue accepts the recovered seals immediately, breaking the
        // count=16 + empty-queue deadlock. The freeze bypasses the
        // in-flight cap gate (`ignore_cap`).
        if self.seal_tx.capacity() == 0 {
            return;
        }
        let mut sealed: Vec<SealedSegment> = Vec::new();
        let (ec_k, ec_m, _strip) = self.ec_params();
        for slot in &self.slots {
            let payload = {
                let mut guard = slot.state.lock();
                let SlotState::Appending(segment) = &*guard else { continue };
                if !segment.is_full() {
                    continue;
                }
                PoolSlot::transition_to_sealing(
                    &mut guard,
                    &self.registry,
                    self.tier,
                    ec_k,
                    ec_m,
                    true,
                )
            };
            if let Some(payload) = payload {
                sealed.push(payload);
            }
        }
        for payload in sealed {
            self.finish_seal_handoff(Some(payload));
        }
    }

    /// Asynchronous variant of [`recover_full_slots_sync`] — fully
    /// NON-BLOCKING: a `try_send` enqueue that never waits for queue
    /// space, so the recovery cannot consume the caller's write budget.
    /// A full queue defers the seal to the idle driver (the entry stays
    /// readable, `seal_queued` is reset, and the seal is retried, not
    /// skipped).
    async fn recover_full_slots_async(&self, _deadline: std::time::Instant) -> Result<(), Error> {
        // See [`recover_full_slots_sync`]: freeze + re-enqueue only
        // when the seal queue can accept the work (a stalled queue
        // defers — the entries stay readable and the count stays at
        // the cap; a draining queue breaks the cap-deadlock). The
        // freeze bypasses the in-flight cap gate (`ignore_cap`).
        if self.seal_tx.capacity() == 0 {
            return Ok(());
        }
        let mut sealed: Vec<SealedSegment> = Vec::new();
        let (ec_k, ec_m, _strip) = self.ec_params();
        for slot in &self.slots {
            let payload = {
                let mut guard = slot.state.lock();
                let SlotState::Appending(segment) = &*guard else { continue };
                if !segment.is_full() {
                    continue;
                }
                PoolSlot::transition_to_sealing(
                    &mut guard,
                    &self.registry,
                    self.tier,
                    ec_k,
                    ec_m,
                    true,
                )
            };
            if let Some(payload) = payload {
                sealed.push(payload);
            }
        }
        for payload in sealed {
            // The bounded handoff RE-ARMS the frozen slot (the critical
            // part — without it the Sealing slots accumulate and starve
            // the pool) and enqueues the work with a short wait; a full
            // queue defers the seal to the idle driver (the entry stays
            // readable; the seal is retried, not skipped). The wait is
            // capped at [`INFLIGHT_RETRY_ENQUEUE_DEADLINE`] so the
            // recovery cannot consume the caller's write budget.
            let segment_id = payload.segment_id;
            let retry_deadline = std::time::Instant::now() + INFLIGHT_RETRY_ENQUEUE_DEADLINE;
            let _ = self.finish_seal_handoff_async(Some(payload), retry_deadline).await;
            let _ = segment_id;
        }
        Ok(())
    }

    /// Hands a sealed segment to the seal queue and re-arms its slot —
    /// synchronous path (`try_send`, drop-on-full safety valve).
    ///
    /// Runs with no slot lock held: the slot is already `Sealing` and
    /// serves reads from its frozen data. Order of operations matters:
    ///
    /// 1. **Insert the sealing-data entry FIRST** — the seal worker
    ///    removes the entry after sealing, and an insert-after-enqueue
    ///    could race that removal and leak a stale entry that is never
    ///    cleaned up.
    /// 2. **Re-arm the slot** — the `Sealing` transit must stay a
    ///    pointer move; nothing on this path spins or allocates beyond
    ///    the replacement segment's construction (perf §7.1).
    /// 3. Enqueue the seal work item last — the hook already ran under
    ///    the slot lock, so the seal worker can never observe the item
    ///    before the hook recorded its state.
    fn finish_seal_handoff(&self, sealed: Option<SealedSegment>) {
        let Some(payload) = sealed else { return };
        self.try_activate_slot();
        if self.enqueue_seal(payload.segment_id, payload.data, payload.tier) {
            self.registry.mark_seal_queued(payload.segment_id);
        }
    }

    /// Hands a sealed segment to the seal queue and re-arms its slot —
    /// asynchronous path: the enqueue **awaits queue space** (bounded
    /// by `deadline`) so a seal work item is never dropped. A dropped
    /// item would orphan the WAL entry the caller writes after this
    /// returns: the acknowledged data would be unreadable until a
    /// restart replays the WAL. On enqueue failure the write is
    /// rejected (never acked) — but the in-flight read window is owned
    /// by the lifecycle registry entry and stays readable; the
    /// idle-seal driver retries the seal (lifecycle-read-path).
    ///
    /// Ordering: re-arm second (the frozen data was attached to the
    /// registry entry at the freeze, under the slot lock), enqueue
    /// last.
    async fn finish_seal_handoff_async(
        &self,
        sealed: Option<SealedSegment>,
        deadline: std::time::Instant,
    ) -> Result<(), Error> {
        let Some(payload) = sealed else { return Ok(()) };
        let segment_id = payload.segment_id;
        self.try_activate_slot();

        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            // The write is rejected (never acked). The registry entry
            // keeps the in-flight window; the idle driver retries the
            // seal (the seal is delayed, not skipped).
            self.registry.mark_seal_unqueued(segment_id);
            return Err(Error::WriteBackpressureTimeout);
        }

        let (ec_k, ec_m, strip_size_bytes) = self.ec_params();
        let work = SealingWork {
            segment_id,
            segment_data: payload.data,
            tier: payload.tier,
            ec_k,
            ec_m,
            strip_size_bytes,
            ec_encoder: self.ec_encoder.clone(),
        };
        match tokio::time::timeout(remaining, self.seal_tx.send(work)).await {
            Ok(Ok(())) => {
                self.registry.mark_seal_queued(segment_id);
                Ok(())
            }
            // The queue is closed (pool shutdown) or no space freed
            // within the deadline: reject the write — it was never
            // acked — and leave the in-flight window for the idle
            // driver's retry (a full seal queue delays the seal but
            // never removes the read window).
            Ok(Err(_)) | Err(_) => {
                self.registry.mark_seal_unqueued(segment_id);
                tracing::warn!(
                    segment_id = %segment_id,
                    "seal enqueue failed within deadline; write rejected (retryable)"
                );
                Err(Error::WriteBackpressureTimeout)
            }
        }
    }

    /// Runs a periodic sweep that seals **idle** (partially-filled)
    /// segments.
    ///
    /// The fill path seals a segment only when an append makes it full.
    /// A segment that receives its last write short of the target size
    /// would otherwise stay `Appending` (registered-unsealed) forever:
    /// the WAL cleanup protects every file holding its entries (they are
    /// the segment's only durable copy), so the WAL file count grows
    /// without bound — the `wal_not_unbounded` leak observed under
    /// sustained load (~1.5 protected files/min).
    ///
    /// Every `interval`, each slot's segment is sealed if it has been
    /// idle for at least `idle_timeout` and holds data. The seal goes
    /// through the same queue as a fill-triggered seal, so the seal
    /// worker persists the `.dat`, registers the sealed metadata (making
    /// the WAL entries sweepable), and the slot re-arms for new writes.
    ///
    /// Freezes a partial segment for sealing — the deterministic
    /// seal-on-zero trigger's pool-side half (called by the
    /// coordinator's pending-seal drain).
    ///
    /// Finds the slot still holding `segment_id` in `Appending` state
    /// and runs the fill transition (freeze + attach the in-flight
    /// window). `Ok(None)` when the slot moved on (a concurrent fill
    /// already froze it — its work item is queued) or the in-flight
    /// cap gated the freeze (the caller re-notes the segment and
    /// retries on the next drain). The returned payload goes through
    /// the same seal pipeline as a fill: blob index, merkle root,
    /// `.dat`, `SealEvent`.
    ///
    /// # Errors
    ///
    /// Returns the underlying transition error (registry write
    /// failure).
    pub(crate) async fn freeze_partial_for_seal(
        &self,
        segment_id: SegmentId,
    ) -> Result<Option<SealedSegment>, Error> {
        let (ec_k, ec_m, _strip) = self.ec_params();
        for slot in &self.slots {
            let payload = {
                let mut guard = slot.state.lock();
                let SlotState::Appending(segment) = &*guard else { continue };
                if segment.id() != segment_id {
                    continue;
                }
                PoolSlot::transition_to_sealing(
                    &mut guard,
                    &self.registry,
                    self.tier,
                    ec_k,
                    ec_m,
                    false,
                )
            };
            if let Some(payload) = payload {
                return Ok(Some(payload));
            }
        }
        Ok(None)
    }

    /// Seals a rebuilt segment that did not fill during replay (a
    /// partial segment whose WAL entries ended with the crash). The
    /// replay drains queued segments one at a time, sealing each to
    /// free its slot — the pool's configured slot count never bounds
    /// the replay.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when no slot holds the segment
    /// (already sealed — a no-op success for filled segments) and
    /// [`Error::WriteBackpressureTimeout`] when the seal queue cannot
    /// accept the work within the deadline.
    pub async fn seal_replayed_partial(&self, segment_id: SegmentId) -> Result<()> {
        for slot in &self.slots {
            let sealed = {
                let mut guard = slot.state.lock();
                let SlotState::Appending(segment) = &mut *guard else {
                    continue;
                };
                if segment.id() != segment_id {
                    continue;
                }
                // `seal` consumes the segment; freeze it through the
                // shared transition (which attaches the frozen buffer
                // to the lifecycle registry entry under this lock).
                let (ec_k, ec_m, _strip) = self.ec_params();
                PoolSlot::transition_to_sealing(
                    &mut guard,
                    &self.registry,
                    self.tier,
                    ec_k,
                    ec_m,
                    false,
                )
            };
            if let Some(sealed) = sealed {
                self.finish_seal_handoff_async(
                    Some(sealed),
                    std::time::Instant::now() + REPLAY_SEAL_ENQUEUE_DEADLINE,
                )
                .await?;
                return Ok(());
            }
        }
        // No slot holds the segment — it already filled and sealed
        // during the rebuild (or is unknown). A no-op is correct.
        Ok(())
    }

    /// Returns the EC parameters carried by seal work items
    /// `(k, m, strip_size_bytes)`; all zero when EC is not configured.
    pub fn ec_params(&self) -> (u8, u8, usize) {
        self.ec_config
            .as_ref()
            .map(|c| (c.data_shards, c.parity_shards, c.strip_size_bytes))
            .unwrap_or((0, 0, 0))
    }

    /// Enqueues a filled segment for sealing on the bounded work channel.
    ///
    /// Uses `try_send` for non-blocking enqueue. If the channel is full,
    /// the seal is deferred and will be retried later by the pool
    /// rotation logic. This avoids blocking the caller in async contexts.
    /// The production (async) path uses [`finish_seal_handoff_async`]
    /// instead, which never drops an enqueue.
    /// Enqueues a filled segment for sealing on the bounded work
    /// channel. Returns whether the work item was accepted.
    ///
    /// Uses `try_send` for non-blocking enqueue; when the channel is
    /// full, `blocking_send` applies backpressure to the caller instead
    /// of losing the segment (a dropped seal leaves the segment
    /// registered-unsealed forever). The production (async) path uses
    /// [`finish_seal_handoff_async`] instead, which never drops an
    /// enqueue. A `Closed` queue (shutdown) returns `false` — the
    /// registry entry keeps the in-flight window.
    fn enqueue_seal(&self, segment_id: SegmentId, segment_data: Bytes, tier: SizeTier) -> bool {
        let (ec_k, ec_m, strip_size_bytes) = self.ec_params();
        let work = SealingWork {
            segment_id,
            segment_data,
            tier,
            ec_k,
            ec_m,
            strip_size_bytes,
            ec_encoder: self.ec_encoder.clone(),
        };
        match self.seal_tx.try_send(work) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(work)) => {
                // The sync path runs on blocking contexts (never a
                // runtime worker), so blocking_send applies
                // backpressure to the caller instead of losing the
                // segment.
                match self.seal_tx.blocking_send(work) {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(
                            segment_id = %segment_id,
                            error = %e,
                            "seal queue closed; seal work dropped on shutdown"
                        );
                        false
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!(
                    segment_id = %segment_id,
                    "seal queue closed; segment will not be sealed"
                );
                false
            }
        }
    }

    /// Re-enqueues a frozen segment's seal work on behalf of the
    /// idle-seal driver (lifecycle-read-path): the seal queue was at
    /// capacity when the segment froze, so its enqueue was deferred;
    /// the registry entry owns the read window throughout.
    ///
    /// Bounded wait ([`INFLIGHT_RETRY_ENQUEUE_DEADLINE`]) — a draining
    /// queue accepts the work in microseconds; a stalled one defers to
    /// the next tick. Returns whether the work was accepted.
    pub(crate) async fn enqueue_inflight_work(
        &self,
        segment_id: SegmentId,
        tier: SizeTier,
        data: Bytes,
    ) -> bool {
        let (ec_k, ec_m, strip_size_bytes) = self.ec_params();
        let work = SealingWork {
            segment_id,
            segment_data: data,
            tier,
            ec_k,
            ec_m,
            strip_size_bytes,
            ec_encoder: self.ec_encoder.clone(),
        };
        tokio::time::timeout(INFLIGHT_RETRY_ENQUEUE_DEADLINE, self.seal_tx.send(work)).await.is_ok()
    }

    /// Returns the storage tier this pool serves.
    pub(crate) fn storage_tier(&self) -> SizeTier {
        self.tier
    }

    /// Returns the lifecycle registry this pool resolves reads through
    /// (the machine — ADR-0025 Decision 2). Test-only.
    #[cfg(test)]
    pub(crate) fn lifecycle_registry(&self) -> &SegmentLifecycleRegistry {
        &self.registry
    }

    /// Attempts to activate a new segment in a sealing or idle slot.
    ///
    /// The replacement segment is built **outside** any slot lock — the
    /// allocation on miss no longer runs inside a critical section
    /// (perf §7.1) — and installed with a single pointer swap
    /// ([`PoolSlot::install_replacement`]). If two threads race, the
    /// loser's install returns `false` and its freshly built segment is
    /// dropped (the buffer returns to the allocator; the buffer pool
    /// recycles only buffers released after a successful seal).
    ///
    /// Returns `true` when this call installed a replacement (and thus
    /// notified waiters), `false` otherwise. Callers that need to react
    /// to their own install (the async append loop) use the return value
    /// to re-scan immediately instead of waiting for a notification they
    /// may have triggered themselves.
    pub(crate) fn try_activate_slot(&self) -> bool {
        // Quick peek for a slot that can accept a replacement.
        let Some(slot) = self.slots.iter().find(|slot| slot.needs_segment()) else {
            return false;
        };

        #[cfg(test)]
        if self.fail_activation.load(std::sync::atomic::Ordering::Relaxed) {
            // Test seam: pretend activation keeps failing so the
            // bounded-wait timeout path is exercisable.
            return false;
        }

        // Build the replacement outside the slot lock.
        let replacement =
            match ActiveSegment::new(self.tier, &self.size_config, self.buffer_pool.as_ref()) {
                Ok(segment) => segment,
                Err(e) => {
                    tracing::warn!(
                        tier = ?self.tier,
                        error = %e,
                        "failed to create new active segment; slot remains sealing"
                    );
                    return false;
                }
            };

        if slot.install_replacement(replacement) {
            tracing::info!(
                tier = ?self.tier,
                "pool slot re-activated with new active segment"
            );
            // Wake appenders blocked on slot exhaustion: sync waiters on
            // the condvar, async waiters on the notify (bounded
            // backpressure).
            self.slot_activation.1.notify_all();
            self.slot_activation_notify.notify_waiters();
            true
        } else {
            false
        }
    }

    /// Returns the seal receiver channel, if available.
    ///
    /// The seal worker must call this to obtain the receiver for
    /// draining sealed segments. Only one worker should take the
    /// receiver; subsequent calls return `None`.
    pub fn take_seal_rx(&self) -> Option<mpsc::Receiver<SealingWork>> {
        self.seal_rx.lock().take()
    }

    /// Returns a clone of the seal semaphore for worker tasks.
    pub fn seal_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.seal_semaphore)
    }

    /// Returns a segment backing buffer to the shared buffer pool after
    /// a seal completes.
    ///
    /// Called by the seal worker once the sealing-data reference has been
    /// dropped and the work item's `Bytes` is the unique owner of the
    /// original `BytesMut` allocation (recovered via `Bytes::try_into_mut`).
    /// The next segment activation reuses the allocation instead of
    /// malloc'ing a fresh buffer (pool-backpressure-and-buffer-recycling).
    pub fn release_buffer(&self, buf: BytesMut) {
        self.buffer_pool.release(buf);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc as StdArc,
        },
        thread,
    };

    use oceanfs_core::{SegmentMetadata, SegmentSizeConfig};

    use super::*;

    fn test_config() -> (PoolConfig, SegmentSizeConfig) {
        (PoolConfig::default(), SegmentSizeConfig::default())
    }

    fn test_registry() -> Arc<SegmentLifecycleRegistry> {
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()))
    }

    /// Reserves a segment in the pool's registry (the write path's
    /// contract: the machine entry must exist before the first append —
    /// the fill attach and the read resolution both require it).
    fn reserve_segment(pool: &SegmentPool, id: SegmentId) {
        pool.registry
            .reserve(
                id,
                SegmentMetadata {
                    pool_id: 0,
                    segment_id: id,
                    ec_k: 0,
                    ec_m: 0,
                    size_tier: pool.tier,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                },
            )
            .unwrap();
    }

    fn test_pool() -> Arc<BufferPool> {
        Arc::new(BufferPool::new(65536, 32))
    }

    #[test]
    fn pool_creation_has_correct_slot_count() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();
        assert_eq!(pool.slot_count(), 4);
        assert_eq!(pool.active_count(), 4, "all slots start in Appending state");
    }

    #[test]
    fn pool_append_returns_valid_offset_and_length() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();
        let (seg_id, offset, length) = pool.append(b"hello world").unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 11);
        assert_ne!(seg_id, SegmentId::default());
    }

    // ── Replay reconstruction (crash recovery) ─────

    /// Drains the pool's seal queue so replay handoffs (which await
    /// queue space) never block in tests without a real seal worker.
    fn drain_seal_queue(pool: &SegmentPool) {
        if let Some(mut rx) = pool.take_seal_rx() {
            std::thread::spawn(move || while rx.blocking_recv().is_some() {});
        }
    }

    #[tokio::test]
    async fn append_replayed_rebuilds_segment_under_original_id() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();
        drain_seal_queue(&pool);

        let original_id = SegmentId::new();
        // Replay reserves before appending (the write-path contract).
        reserve_segment(&pool, original_id);
        pool.append_replayed(original_id, b"alpha").await.unwrap();
        // A second entry for the SAME segment must append to the same
        // rebuilt segment (contiguous offsets).
        pool.append_replayed(original_id, b"beta").await.unwrap();

        let read = pool.try_read(original_id, 0, 5).expect("original id must resolve");
        assert_eq!(&read[..], b"alpha");
        let read2 = pool.try_read(original_id, 5, 4).expect("second entry readable");
        assert_eq!(&read2[..], b"beta");
    }

    #[tokio::test]
    async fn append_replayed_keeps_distinct_segments_distinct() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();
        drain_seal_queue(&pool);

        let id_a = SegmentId::new();
        let id_b = SegmentId::new();
        // Replay reserves before appending (the write-path contract).
        reserve_segment(&pool, id_a);
        reserve_segment(&pool, id_b);
        pool.append_replayed(id_a, b"aaaa").await.unwrap();
        pool.append_replayed(id_b, b"bbbb").await.unwrap();

        assert_eq!(&pool.try_read(id_a, 0, 4).expect("id_a")[..], b"aaaa");
        assert_eq!(&pool.try_read(id_b, 0, 4).expect("id_b")[..], b"bbbb");
        // The pool must NOT have merged the two segments.
        assert!(pool.try_read(id_a, 4, 4).is_none(), "id_a must not contain id_b's data");
    }

    #[tokio::test]
    async fn append_replayed_errors_when_all_slots_occupied() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();
        drain_seal_queue(&pool);

        // Fill every slot with a distinct replayed segment.
        for i in 0..pool.slot_count() {
            let id = SegmentId::new();
            pool.append_replayed(id, format!("segment-{i}").as_bytes()).await.unwrap();
        }
        // One more distinct segment than slots → error.
        let err = pool.append_replayed(SegmentId::new(), b"overflow").await.expect_err("must fail");
        assert!(matches!(err, Error::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn append_replayed_filled_segment_hands_to_seal_queue() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Small,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        // Fill the segment past its target with one replay append — the
        // fill→Sealing transition must enqueue seal work (same contract
        // as the write path), not silently swallow the data.
        let original_id = SegmentId::new();
        let target = size_cfg.small_target_size;
        let mut payload = vec![0xabu8; target as usize];
        // Replay reserves before appending (the fill attach requires it).
        reserve_segment(&pool, original_id);
        pool.append_replayed(original_id, &payload).await.unwrap();
        let mut rx = pool.take_seal_rx().expect("seal receiver");
        let work = rx.try_recv().expect("filled rebuilt segment must be enqueued for sealing");
        assert_eq!(work.segment_id, original_id, "seal work must carry the original id");
        // The sealing slot still serves reads (ADR-0021 read window).
        payload[0] = 0xcd;
        let read_len = u32::try_from(target).unwrap();
        assert_eq!(pool.try_read(original_id, 0, read_len).unwrap()[0], 0xab);
    }

    #[test]
    fn concurrent_writes_across_slots_do_not_corrupt_data() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = StdArc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool.clone(),
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );

        let write_count = StdArc::new(AtomicUsize::new(0));
        let num_threads = 8;
        let writes_per_thread = 50;

        let mut handles = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let pool = StdArc::clone(&pool);
            let write_count = StdArc::clone(&write_count);
            let handle = thread::spawn(move || {
                for _ in 0..writes_per_thread {
                    let result = pool.append(b"data");
                    assert!(result.is_ok(), "append must not fail under concurrency");
                    write_count.fetch_add(1, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(write_count.load(Ordering::Relaxed), num_threads * writes_per_thread);
    }

    #[test]
    fn encode_queue_is_created() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let rx = pool.take_seal_rx();
        assert!(rx.is_some(), "seal receiver must exist");
    }

    #[test]
    fn encode_semaphore_has_correct_permits() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg.clone(),
            SizeTier::Standard,
            &size_cfg,
            buf_pool.clone(),
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let sem = pool.seal_semaphore();
        // Verify that the semaphore has been created with the expected count.
        // We can't directly inspect Semaphore internals, but we can acquire
        // all permits and then try one more.
        let mut permits = Vec::new();
        for _ in 0..pool_cfg.max_inflight_encodes {
            let permit = sem.try_acquire();
            assert!(permit.is_ok(), "should be able to acquire up to max permits");
            permits.push(permit.unwrap());
        }
        // One more should fail (no permits left).
        assert!(sem.try_acquire().is_err(), "should be exhausted");
    }

    #[test]
    fn custom_pool_size_config() {
        let pool_cfg = PoolConfig { active_pool_size: 8, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 32));
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Small,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();
        assert_eq!(pool.slot_count(), 8);
    }

    #[test]
    fn pool_append_returns_different_segment_ids() {
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let (id1, _, _) = pool.append(b"a").unwrap();
        // The second append may go to a different slot (round-robin).
        let (id2, _, _) = pool.append(b"b").unwrap();
        // Both IDs are valid UUIDs.
        assert_ne!(id1, SegmentId::default());
        assert_ne!(id2, SegmentId::default());
    }

    // ── Pool rotation tests (fill → seal → new segment) ───────────

    #[test]
    fn pool_rotation_fills_segment_and_activates_new_slot() {
        // Use a tiny target size so a single append triggers is_full().
        let pool_cfg = PoolConfig { active_pool_size: 4, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 10,
            small_target_size: 10,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        // We need a tokio runtime because enqueue_encoding calls
        // Handle::current(). Create a minimal runtime for the test.
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let _guard = rt.enter();

        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();
        assert_eq!(pool.active_count(), 4, "all 4 slots start appending");

        // Append data larger than target_size (10 bytes).
        // This fills the first slot's segment, triggering seal+rotation.
        let data = b"hello world, this is longer than 10 bytes";
        let (seg_id, offset, length) = pool.append(data).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, data.len() as u32);
        assert_ne!(seg_id, SegmentId::default());

        // After fill+rotation, active_count should still be 4
        // (the filled slot was replaced with a newly activated one).
        assert_eq!(pool.active_count(), 4, "pool should re-activate after fill");

        // The seal queue should have received the sealed segment.
        let rx = pool.take_seal_rx();
        assert!(rx.is_some(), "seal queue should have entries after fill");

        // Verify subsequent appends succeed (pool is still functional).
        let (seg_id2, offset2, _len2) = pool.append(b"more data").unwrap();
        assert_eq!(offset2, 0);
        assert_ne!(seg_id2, SegmentId::default());
    }

    #[test]
    fn pool_rotation_multiple_fills_all_slots() {
        // Fill all slots sequentially and verify pool remains functional.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 15,
            small_target_size: 15,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let _guard = rt.enter();

        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();
        assert_eq!(pool.active_count(), 2);

        // Fill both slots multiple times. Each 20-byte append overflows
        // target_size=15, triggering rotation.
        for i in 0..10 {
            let data = format!("fill-iteration-{i:02}-padding");
            let result = pool.append(data.as_bytes());
            assert!(result.is_ok(), "append {} must succeed after rotation", i);
        }

        // Pool should still have active slots.
        assert!(pool.active_count() > 0, "pool must have active slots after fill cycles");
    }

    // ── Backpressure test ─────────────────────────────────────────

    #[test]
    fn seal_queue_backpressure_config_is_respected() {
        // Verify that a pool with small encode_queue_capacity can be
        // created and used without panicking, even when the queue fills.
        // Uses a large target size so enqueue_seal is never called
        // (segment never fills). This tests the configuration path.
        let pool_cfg =
            PoolConfig { active_pool_size: 4, encode_queue_capacity: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        // The seal queue should exist with the configured capacity.
        let rx = pool.take_seal_rx();
        assert!(rx.is_some(), "seal queue must exist");

        // Appends should succeed normally (segment won't fill with 4MB target).
        for _ in 0..10 {
            assert!(pool.append(b"data").is_ok());
        }
    }

    #[test]
    fn pool_handles_segment_full_with_seal_queue_not_draining() {
        // Create a pool with tiny target and take the seal receiver.
        // Verify that appends still succeed after the segment fills
        // (enqueue_seal handles full channel gracefully without panic).
        use std::sync::Arc as StdArc;

        let pool_cfg =
            PoolConfig { active_pool_size: 4, encode_queue_capacity: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 20,
            small_target_size: 20,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

        let pool = StdArc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );

        // Drain the queue on a background thread: the enqueue path
        // NEVER drops a seal work item (a dropped seal orphans the
        // segment's data), so a full queue applies backpressure via
        // `blocking_send` — with the receiver taken and never drained
        // the sync path would block forever. The drainer keeps the
        // channel consuming while the appends fill it.
        let _rx = pool.take_seal_rx();
        if let Some(mut rx) = _rx {
            std::thread::spawn(move || while rx.blocking_recv().is_some() {});
        }

        // Execute on the runtime so block_on works for enqueue_seal.
        rt.block_on(async {
            // Fill segments. With encode_queue_capacity=2, after 2 fills
            // the channel is full. Subsequent fills trigger the enqueue_seal
            // try_send failure path but should not panic.
            for i in 0..5 {
                let pool = StdArc::clone(&pool);
                let data = format!("fill-data-{i:02}-enough-bytes-to-overflow");
                let result = tokio::task::spawn_blocking(move || pool.append(data.as_bytes()))
                    .await
                    .unwrap();
                assert!(result.is_ok(), "append {i} must succeed");
            }
        });
    }

    // ── State transition test ─────────────────────────────────────

    #[test]
    fn pool_slot_state_transitions_after_fill() {
        let pool_cfg = PoolConfig { active_pool_size: 4, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 10,
            small_target_size: 10,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();

        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let _guard = rt.enter();

        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        // Before writes: all slots in Appending.
        assert_eq!(pool.active_count(), 4);

        // Fill one slot with a large append.
        pool.append(b"this is more than 10 bytes, should fill").unwrap();

        // After fill: the filled slot transitions to Sealing then gets
        // a new segment via try_activate_slot(). Verify active count
        // remains stable (re-activation happened).
        assert!(pool.active_count() >= 3, "at least 3 slots should still be appending");

        // Subsequent appends across remaining active slots succeed.
        for _ in 0..10 {
            let result = pool.append(b"small");
            assert!(result.is_ok(), "append after rotation must succeed");
        }
        assert!(pool.active_count() > 0, "pool must still have active slots");
    }

    // ── State machine tests (segment-pool-slot-state-machine) ──────

    #[test]
    fn take_for_sealing_freezes_segment_in_slot_and_returns_payload() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let slot = Arc::clone(&pool.slots[0]);
        let (seg_id, _, _) = pool.append(b"payload-data").unwrap();
        reserve_segment(&pool, seg_id);

        let sealed =
            slot.take_for_sealing(&pool.registry, pool.tier, 0, 0).expect("appending slot seals");
        assert_eq!(sealed.segment_id, seg_id);
        assert_eq!(sealed.tier, SizeTier::Standard);
        assert_eq!(&sealed.data[..], b"payload-data");

        // The frozen data was attached to the lifecycle registry entry
        // in the freeze critical section: readable by id while Sealing
        // (the in-flight window; the slot scan serves append-mode only).
        let read = pool.try_read(seg_id, 0, 12).expect("in-flight registry entry serves reads");
        assert_eq!(&read[..], b"payload-data");
        assert_eq!(pool.active_count(), 3, "one slot moved out of Appending");
    }

    #[test]
    fn take_for_sealing_on_parked_or_idle_slot_returns_none() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let slot = Arc::clone(&pool.slots[0]);
        assert!(
            slot.take_for_sealing(&pool.registry, pool.tier, 0, 0).is_some(),
            "first seal succeeds"
        );
        assert!(
            slot.take_for_sealing(&pool.registry, pool.tier, 0, 0).is_none(),
            "second seal is a no-op"
        );
        let idle = PoolSlot::new_idle();
        assert!(
            idle.take_for_sealing(&pool.registry, pool.tier, 0, 0).is_none(),
            "idle slot has nothing to seal"
        );
    }

    #[test]
    fn install_replacement_swaps_sealing_slot_exactly_once() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool.clone(),
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let slot = Arc::clone(&pool.slots[0]);
        slot.take_for_sealing(&pool.registry, pool.tier, 0, 0).expect("park the slot");

        let first = ActiveSegment::new(SizeTier::Standard, &size_cfg, buf_pool.as_ref()).unwrap();
        assert!(slot.install_replacement(first), "sealing slot accepts a replacement");
        assert!(slot.is_appending());

        // A second install loses the race: the replacement is refused.
        let second = ActiveSegment::new(SizeTier::Standard, &size_cfg, buf_pool.as_ref()).unwrap();
        assert!(!slot.install_replacement(second), "already-appending slot refuses installs");
        assert!(slot.is_appending());
    }

    #[test]
    fn install_replacement_accepts_idle_slots() {
        let idle = PoolSlot::new_idle();
        let (_, size_cfg) = test_config();
        let buf_pool = test_pool();
        let replacement =
            ActiveSegment::new(SizeTier::Standard, &size_cfg, buf_pool.as_ref()).unwrap();
        assert!(idle.install_replacement(replacement));
        assert!(idle.is_appending());
    }

    #[test]
    fn append_skips_non_appending_slot_without_consuming_hook() {
        // The hook is FnOnce and must survive a failed slot attempt: park
        // slot 0, then append — the scan must land on another slot and the
        // hook must still fire exactly once.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        pool.slots[0].take_for_sealing(&pool.registry, pool.tier, 0, 0).expect("park slot 0");
        let calls = StdArc::new(AtomicUsize::new(0));
        let calls2 = StdArc::clone(&calls);
        let (seg_id, offset, length) = pool
            .append_with_hook(b"data", move |_, _, _| {
                calls2.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1, "hook fires exactly once");
        assert_eq!(length, 4);
        assert_ne!(seg_id, SegmentId::default());
        assert_eq!(offset, 0);
    }

    #[test]
    fn fill_append_invokes_hook_before_seal_enqueue() {
        // A fill-triggering append must record the hook before the seal
        // work item is observable. The ordering is structural (hook inside
        // the critical section, enqueue after it); this pins the
        // observable contract: the work item's segment id matches the one
        // the hook recorded, and the segment stays readable through the
        // sealing-data set after the slot has been re-armed.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 10,
            small_target_size: 10,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();
        let pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let _guard = rt.enter();

        let recorded = StdArc::new(Mutex::new(Vec::<(SegmentId, u64, u32)>::new()));
        let recorded2 = StdArc::clone(&recorded);
        // The fill's freeze attaches the frozen buffer to the machine
        // entry (inserting a registry-only entry when the write path's
        // durable reserve has not landed yet — the fill-before-reserve
        // window), so the segment stays readable through the seal.
        let (seg_id, _offset, length) = pool
            .append_with_hook(b"this fills the segment", move |sid, off, len| {
                recorded2.lock().push((sid, off, len));
            })
            .unwrap();
        assert_eq!(length, 22);

        let mut rx = pool.take_seal_rx().expect("seal rx");
        let work = rx.blocking_recv().expect("filled segment enqueued");
        assert_eq!(work.segment_id, seg_id);
        let hook_recorded = recorded.lock();
        assert_eq!(work.segment_id, hook_recorded[0].0, "hook ran before enqueue");
        assert_eq!(work.tier, SizeTier::Standard);
        assert_eq!(&work.segment_data[..], b"this fills the segment");
        drop(hook_recorded);

        // The slot was re-armed by finish_seal_handoff; the segment stays
        // readable through the sealing-data set while the work item is in
        // flight (ADR-0021 window).
        assert!(pool.active_count() >= 1, "slot re-armed after fill");
        let read = pool.try_read(seg_id, 0, length).expect("readable during seal window");
        assert_eq!(&read[..], b"this fills the segment");
    }

    // ── Read-path windows + in-flight cap (lifecycle-read-path) ──

    #[tokio::test]
    async fn try_read_serves_append_inflight_and_after_freeze_windows() {
        // The read-after-write window matrix at the pool level: a
        // freshly appended segment resolves via the slot scan
        // (ActiveSlot); once it freezes, the registry's in-flight
        // buffer serves (InFlight), attached in the freeze critical
        // section — the window is continuous across the fill.
        let pool_cfg = PoolConfig { active_pool_size: 1, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 1024,
            small_target_size: 1024,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = test_pool();
        let pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );
        drain_seal_queue(&pool);

        // (a) Append-mode: the data is readable from the active slot.
        let data = b"window matrix payload";
        let (seg_id, offset, length) = pool.append(data).unwrap();
        reserve_segment(&pool, seg_id);
        let chunk = pool.try_read(seg_id, offset, length).expect("append-mode read");
        assert_eq!(&chunk[..], &data[..]);

        // (b) In-flight: the fill freezes the segment and attaches the
        // buffer to the machine entry (the attach self-heals the entry
        // even before the write path's durable reserve lands). The
        // single-slot pool guarantees the second append hits the same
        // segment (round-robin over one slot).
        let fill_data = vec![0xABu8; 2048];
        let (fill_id, fill_offset, _) = pool.append(&fill_data).unwrap();
        assert_eq!(fill_id, seg_id, "same segment until it fills");
        assert_eq!(fill_offset, data.len() as u64, "fill lands right after the first append");
        let chunk = pool.try_read(seg_id, fill_offset, 2048).expect("in-flight read after freeze");
        assert_eq!(&chunk[..], &fill_data[..]);
    }

    #[tokio::test]
    async fn in_flight_set_bounded_and_readable_under_stalled_seal_queue() {
        // The DoD memory-bound test: with the seal queue stalled, the
        // in-flight set never exceeds the ADR-0021 cap (16) and every
        // frozen entry stays readable. A full seal queue delays the
        // seal but never removes the read window.
        let pool_cfg = PoolConfig {
            active_pool_size: 4,
            encode_queue_capacity: 2, // tiny queue → stalls fast
            ..PoolConfig::default()
        };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 1024,
            small_target_size: 1024,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = Arc::new(BufferPool::new(65536, 64));
        let registry =
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
        let pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                Arc::clone(&registry),
            )
            .unwrap(),
        );
        // Take the receiver WITHOUT draining — the seal queue stalls.
        let _rx = pool.take_seal_rx().expect("seal rx");

        // Fill segments until the deferred-seal cap closes the slots.
        // The append never waits on the queue (the hand-off is
        // caller-side); the ENQUEUE waits for queue space within a
        // short deadline — once the queue is full the enqueue fails
        // (write rejected, never acked) and the frozen entries stay in
        // the registry.
        let mut written_ids: Vec<SegmentId> = Vec::new();
        let mut backpressured = 0usize;
        for _ in 0..200 {
            let data = vec![0xCDu8; 2048]; // fills (target 1024)
            match pool
                .append_with_hook_async(&data, |_, _, _| {}, std::time::Duration::from_millis(20))
                .await
            {
                Ok((id, _, _, sealed)) => {
                    // The write path's contract: the durable reserve
                    // lands right after the append, then the caller
                    // enqueues the seal hand-off.
                    reserve_segment(&pool, id);
                    written_ids.push(id);
                    // The stalled queue backpressures the ENQUEUE (the
                    // caller-side half of the hand-off). Note the append
                    // can also time out when the queue is full AND the
                    // in-flight cap gates a fill — both are the same
                    // backpressure signal.
                    match pool
                        .enqueue_seal_handoff(
                            sealed,
                            std::time::Instant::now() + std::time::Duration::from_millis(20),
                        )
                        .await
                    {
                        Ok(()) => {}
                        Err(Error::WriteBackpressureTimeout) => backpressured += 1,
                        Err(e) => panic!("unexpected enqueue error: {e}"),
                    }
                }
                Err(Error::WriteBackpressureTimeout) => backpressured += 1,
                Err(e) => panic!("unexpected append error: {e}"),
            }
        }
        assert!(backpressured > 0, "the stalled queue must backpressure appends");
        assert!(
            registry.in_flight_count() <= IN_FLIGHT_CAP,
            "in-flight set must never exceed the cap: {}",
            registry.in_flight_count()
        );

        // Every acked entry is still readable (the window is owned by
        // the machine, not the channel): the frozen ones resolve via the
        // registry's in-flight buffer, the cap-gated full ones via the
        // slot scan.
        for id in &written_ids {
            let chunk = pool.try_read(*id, 0, 2048).unwrap_or_else(|| {
                panic!("acked segment {id} must stay readable under backpressure")
            });
            assert_eq!(&chunk[..], &[0xCDu8; 2048]);
        }
    }

    // ── Backpressure tests (pool-backpressure-and-buffer-recycling) ──

    /// Parks every slot in `Sealing` with no appendable segment, simulating
    /// the transit window of a concurrent fill burst. The frozen data stays
    /// in the slot (matching the unified state machine: a Sealing slot
    /// never loses its data — ADR-0021 read window).
    fn park_all_slots(pool: &SegmentPool) {
        for slot in pool.slots.iter() {
            slot.take_for_sealing(&pool.registry, pool.tier, 0, 0);
        }
    }

    #[test]
    fn append_waits_for_slot_reactivation() {
        let pool_cfg = PoolConfig { active_pool_size: 4, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 32));
        let pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );

        park_all_slots(&pool);
        assert_eq!(pool.active_count(), 0, "all slots parked");

        let pool2 = Arc::clone(&pool);
        let handle = std::thread::spawn(move || pool2.append(b"hello"));

        // Give the appender time to enter the bounded wait, then
        // re-activate a slot exactly like the fill path does.
        std::thread::sleep(std::time::Duration::from_millis(2));
        pool.try_activate_slot();

        let result = handle.join().unwrap();
        assert!(result.is_ok(), "append must complete after re-activation: {result:?}");
    }

    #[test]
    fn append_returns_error_when_activation_keeps_failing() {
        // The terminal error is only reachable when segment creation keeps
        // failing (real resource exhaustion): with activation disabled and
        // every slot parked, the bounded wait must expire and return the
        // existing InvalidConfig error instead of hanging forever.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 8));
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        park_all_slots(&pool);
        pool.fail_activation.store(true, std::sync::atomic::Ordering::Relaxed);

        let result = pool.append(b"hello");
        assert!(
            matches!(result, Err(Error::InvalidConfig(_))),
            "expected exhaustion error, got {result:?}"
        );
    }

    #[test]
    fn append_self_heals_when_all_slots_are_parked() {
        // With the self-healing wait, a pool whose slots are all in the
        // Sealing-with-no-segment state is re-activated by the appender
        // itself — the write succeeds instead of failing.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 8));
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        park_all_slots(&pool);

        let result = pool.append(b"hello");
        assert!(result.is_ok(), "append must self-heal a parked pool: {result:?}");
        assert_eq!(pool.active_count(), 1, "the appender activated one slot");
    }

    #[test]
    fn concurrent_churn_never_exhausts_slots() {
        // Production-shaped churn: 4 slots, tiny target, every append fills
        // its segment (the worst burst a multi-tier workload produces).
        // Under the bounded backpressure, appends must never fail with
        // slot exhaustion (they may wait briefly).
        let pool_cfg =
            PoolConfig { active_pool_size: 4, encode_queue_capacity: 8, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 1024,
            small_target_size: 1024,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = Arc::new(BufferPool::new(65536, 64));
        let pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );

        // Drain the seal queue so the fillers never back up on it,
        // simulating the seal worker's fold: each drained item is
        // registry-sealed, clearing its in-flight entry (the seal
        // transition's job) so the deferred-seal gate re-opens the
        // slots — exactly what the real seal worker does. The drain
        // holds ONLY the registry (not the pool): `drop(pool)` below
        // must close the seal sender so the drain's `blocking_recv`
        // terminates.
        let drain_registry = Arc::clone(&pool.registry);
        let mut rx = pool.take_seal_rx().expect("seal rx available");
        let drain = std::thread::spawn(move || {
            while let Some(work) = rx.blocking_recv() {
                let meta = SegmentMetadata {
                    pool_id: 0,
                    segment_id: work.segment_id,
                    ec_k: work.ec_k,
                    ec_m: work.ec_m,
                    size_tier: work.tier,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: Some(1),
                };
                let _ = drain_registry.seal(work.segment_id, meta);
            }
        });

        let mut handles = Vec::new();
        for _ in 0..16 {
            let pool = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                let data = vec![0xABu8; 2048]; // > target: every append fills
                let mut failures = 0usize;
                for _ in 0..200 {
                    if pool.append(&data).is_err() {
                        failures += 1;
                    }
                }
                failures
            }));
        }
        let mut failures = 0usize;
        for h in handles {
            failures += h.join().unwrap();
        }
        // The bounded wait may legitimately expire under OS-scheduling
        // turbulence in this 8×-adversarial configuration (16 threads ×
        // 4 slots, every append fills) — the in-flight cap's recovery
        // windows add a few expiries (~0.3%: the freeze gate + the
        // full-slot recovery defer re-arms while the seal pipeline
        // catches up). Regression mutations (removing the self-heal,
        // the budget refresh, or the recovery) fail essentially every
        // append; scheduling noise stays far below this 1% bound.
        assert!(
            failures <= 32,
            "slot exhaustion too frequent: {failures} of {} appends failed",
            16 * 200
        );
        drop(pool); // close the seal sender
        drain.join().unwrap();
    }

    // ── Async append tests (write-path backpressure propagation) ──

    #[tokio::test]
    async fn append_async_waits_for_reactivation_then_succeeds() {
        // The async path must never fail on a transiently exhausted
        // pool: park every slot, start an async append with a generous
        // deadline, then re-activate a slot — the append completes.
        let pool_cfg = PoolConfig { active_pool_size: 4, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 32));
        let pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );

        park_all_slots(&pool);
        assert_eq!(pool.active_count(), 0, "all slots parked");

        let pool2 = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            pool2
                .append_with_hook_async(b"hello", |_, _, _| {}, std::time::Duration::from_secs(5))
                .await
        });

        // Give the appender time to enter the async wait, then re-activate
        // a slot exactly like the fill path does.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        pool.try_activate_slot();

        let result = handle.await.expect("task must not panic");
        assert!(result.is_ok(), "async append must complete after re-activation: {result:?}");
    }

    #[tokio::test]
    async fn append_async_returns_backpressure_timeout_when_activation_keeps_failing() {
        // With activation disabled and every slot parked, the async
        // append must wait out its deadline and return the dedicated
        // backpressure error — never a hang, never the sync path's
        // InvalidConfig.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 8));
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        park_all_slots(&pool);
        pool.fail_activation.store(true, std::sync::atomic::Ordering::Relaxed);

        let result = pool
            .append_with_hook_async(b"hello", |_, _, _| {}, std::time::Duration::from_millis(50))
            .await;
        assert!(
            matches!(result, Err(Error::WriteBackpressureTimeout)),
            "expected backpressure timeout, got {result:?}"
        );
    }

    #[tokio::test]
    async fn append_async_self_heals_when_all_slots_are_parked() {
        // The async path keeps the self-heal: a pool whose slots are all
        // in the Sealing transit is re-activated by the waiter itself.
        let pool_cfg = PoolConfig { active_pool_size: 2, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig::default();
        let buf_pool = Arc::new(BufferPool::new(65536, 8));
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        park_all_slots(&pool);

        let result = pool
            .append_with_hook_async(b"hello", |_, _, _| {}, std::time::Duration::from_secs(5))
            .await;
        assert!(result.is_ok(), "async append must self-heal a parked pool: {result:?}");
        assert_eq!(pool.active_count(), 1, "the appender activated one slot");
    }

    #[tokio::test]
    async fn append_async_waits_for_seal_queue_space_instead_of_dropping() {
        // The production path must never drop a seal work item: with a
        // capacity-1 queue already full, the second fill's enqueue must
        // await queue space and succeed once the worker drains.
        let pool_cfg =
            PoolConfig { active_pool_size: 1, encode_queue_capacity: 1, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 16,
            small_target_size: 16,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = Arc::new(BufferPool::new(65536, 8));
        let pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );

        let mut rx = pool.take_seal_rx().expect("seal rx");
        let deadline = || std::time::Instant::now() + std::time::Duration::from_secs(5);

        // First fill; the caller-side enqueue fills the capacity-1 queue
        // (now full).
        let (_, _, _, sealed) = pool
            .append_with_hook_async(
                b"0123456789abcdef",
                |_, _, _| {},
                std::time::Duration::from_secs(5),
            )
            .await
            .expect("first fill succeeds");
        pool.enqueue_seal_handoff(sealed, deadline()).await.expect("first enqueue succeeds");

        // Second fill's enqueue must await queue space instead of
        // dropping.
        let pool2 = Arc::clone(&pool);
        let second = tokio::spawn(async move {
            let (_, _, _, sealed) = pool2
                .append_with_hook_async(
                    b"0123456789abcdef",
                    |_, _, _| {},
                    std::time::Duration::from_secs(5),
                )
                .await
                .expect("second fill succeeds");
            pool2.enqueue_seal_handoff(sealed, deadline()).await
        });

        // Let the second enqueue enter the wait, then drain. The
        // recv is timeout-bounded so a regression (e.g. re-introducing
        // try_send drop-on-full) fails fast instead of hanging.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("first work item must arrive")
            .expect("channel open");
        let second_item = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("second work item must arrive after queue space frees")
            .expect("channel open");

        assert_eq!(&first.segment_data[..], b"0123456789abcdef");
        assert_eq!(&second_item.segment_data[..], b"0123456789abcdef");
        let result = second.await.expect("task must not panic");
        assert!(result.is_ok(), "second enqueue must succeed after queue space frees: {result:?}");
    }

    #[tokio::test]
    async fn append_async_rejects_write_when_seal_queue_never_drains() {
        // With the queue permanently full, the async append's enqueue
        // must time out and REJECT the write (never acked, retryable) —
        // never silently drop the item (which would orphan the caller's
        // WAL entry) and never hang.
        let pool_cfg =
            PoolConfig { active_pool_size: 1, encode_queue_capacity: 1, ..PoolConfig::default() };
        let size_cfg = SegmentSizeConfig {
            default_target_size: 16,
            small_target_size: 16,
            ..SegmentSizeConfig::default()
        };
        let buf_pool = Arc::new(BufferPool::new(65536, 8));
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        // Hold the receiver and never drain — the queue stays full.
        let _rx = pool.take_seal_rx();

        let (_, _, _, sealed) = pool
            .append_with_hook_async(
                b"0123456789abcdef",
                |_, _, _| {},
                std::time::Duration::from_secs(5),
            )
            .await
            .expect("first fill succeeds");
        pool.enqueue_seal_handoff(
            sealed,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .expect("first enqueue fills the queue");

        let (_, _, _, sealed) = pool
            .append_with_hook_async(
                b"0123456789abcdef",
                |_, _, _| {},
                std::time::Duration::from_secs(5),
            )
            .await
            .expect("second fill succeeds");
        let result = pool
            .enqueue_seal_handoff(
                sealed,
                std::time::Instant::now() + std::time::Duration::from_millis(50),
            )
            .await;
        assert!(
            matches!(result, Err(Error::WriteBackpressureTimeout)),
            "expected backpressure timeout, got {result:?}"
        );
    }

    // ── try_read tests ───────────────────────────────────────────
    #[test]
    fn try_read_returns_data_after_append() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let data = b"hello world, this is a test segment read";
        let (seg_id, offset, length) = pool.append(data).unwrap();
        // The read resolution goes through the machine — reserve the
        // entry (the write path's contract) before reading.
        reserve_segment(&pool, seg_id);

        // Data should be readable immediately from the active segment.
        let chunk = pool.try_read(seg_id, offset, length).expect("try_read should find segment");
        assert_eq!(chunk.len(), length as usize);
        assert_eq!(&chunk[..], &data[..length as usize]);
    }

    #[test]
    fn try_read_returns_none_for_unknown_segment() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        // A segment id that was never appended.
        let unknown_id = SegmentId::new();
        let result = pool.try_read(unknown_id, 0, 10);
        assert!(result.is_none(), "try_read must return None for unknown segment");
    }

    #[test]
    fn try_read_respects_offset_and_length() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let data = b"abcdefghijklmnopqrstuvwxyz";
        let (seg_id, offset, length) = pool.append(data).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 26);
        // The read resolution goes through the machine.
        reserve_segment(&pool, seg_id);

        // Read a sub-range: bytes 5..10 = "fghij"
        let chunk = pool.try_read(seg_id, 5, 5).expect("sub-range read");
        assert_eq!(&chunk[..], b"fghij");

        // Read from non-zero offset to end.
        let chunk = pool.try_read(seg_id, 20, 10).expect("tail read");
        assert_eq!(&chunk[..], b"uvwxyz");
    }

    #[test]
    fn try_read_clamped_at_buffer_end() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = SegmentPool::new(
            pool_cfg,
            SizeTier::Standard,
            &size_cfg,
            buf_pool,
            None,
            None,
            test_registry(),
        )
        .unwrap();

        let data = b"short";
        let (seg_id, offset, _length) = pool.append(data).unwrap();
        assert_eq!(offset, 0);
        // The read resolution goes through the machine.
        reserve_segment(&pool, seg_id);

        // Request more bytes than written — should be clamped.
        let chunk = pool.try_read(seg_id, 0, 100).expect("clamped read");
        assert_eq!(chunk.len(), 5);
        assert_eq!(&chunk[..], b"short");
    }

    #[tokio::test]
    async fn freeze_partial_for_seal_preserves_read_window_and_rearms_slot() {
        let (pool_cfg, size_cfg) = test_config();
        let buf_pool = test_pool();
        let pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_cfg,
                buf_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );
        // Drain the seal queue so the async hand-off never blocks.
        drain_seal_queue(&pool);

        // Append a small blob — the segment is far from full. Reserve
        // first (the machine entry must exist for the read resolution).
        let (seg_id, offset, length) = pool.append(b"partial").unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 7);
        reserve_segment(&pool, seg_id);

        // The seal-on-zero trigger's pool half: freeze the partial
        // segment and hand the payload to the seal queue.
        let payload =
            pool.freeze_partial_for_seal(seg_id).await.expect("freeze").expect("slot found");
        pool.enqueue_seal_handoff(
            Some(payload),
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .expect("enqueue");

        // The sealed segment's data remains readable during the seal
        // window (read-after-write gap preserved)...
        assert_eq!(&pool.try_read(seg_id, 0, 7).expect("read during seal window")[..], b"partial");

        // ...but the segment itself is no longer active: the slot was
        // re-armed with a FRESH segment, so the next append must land in
        // a different segment id (the old one is sealed + enqueued).
        let (new_seg_id, new_offset, _) = pool.append(b"more").unwrap();
        assert_eq!(new_offset, 0, "fresh segment starts at offset 0");
        assert_ne!(new_seg_id, seg_id, "old segment must be sealed, new append uses a new one");
    }
}
