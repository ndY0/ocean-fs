//! Bounded heal queue for EC repair requests.
//!
//! Implements perf rule 2.6: uses a bounded `tokio::sync::mpsc` channel
//! to enforce backpressure when the heal pipeline is saturated.

use oceanfs_core::{HealRequest, SegmentId};
use oceanfs_storage::{Error, Result};
use tokio::sync::mpsc;

/// A global sender for submitting heal requests without direct queue access.
///
/// Initialized via [`init_global_queue`] during startup. Callers use
/// [`enqueue_heal`] which delegates to this singleton.
static GLOBAL_HEAL_SENDER: std::sync::OnceLock<HealQueueSender> = std::sync::OnceLock::new();

// ---------------------------------------------------------------------------
// HealQueueSender
// ---------------------------------------------------------------------------

/// The sending half of a bounded heal queue.
///
/// Acquired from [`HealQueue::sender`]. Cloneable — multiple callers
/// (Scrub, Anti-Entropy) can share one sender concurrently.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_durability::heal::HealQueue;
///
/// let queue = HealQueue::new(64);
/// let sender = queue.sender();
/// sender.enqueue(request).await?;
/// ```
pub struct HealQueueSender {
    tx: mpsc::Sender<HealRequest>,
}

impl Clone for HealQueueSender {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl HealQueueSender {
    /// Sends a heal request into the bounded queue.
    ///
    /// Uses `try_send` for immediate backpressure feedback (perf rule 2.6).
    /// If the queue is full, returns `Error::HealQueueFull` immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue is at capacity or the receiver
    /// has been dropped (i.e. the heal worker has shut down).
    pub fn enqueue(&self, request: HealRequest) -> Result<()> {
        self.tx.try_send(request).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::HealQueueFull,
            mpsc::error::TrySendError::Closed(_) => Error::Heal("heal queue closed".into()),
        })
    }
}

// ---------------------------------------------------------------------------
// HealQueue
// ---------------------------------------------------------------------------

/// A bounded multi-producer, single-consumer queue for heal requests.
///
/// Accepts [`HealRequest`] items from corruption detectors (Scrub,
/// Anti-Entropy) via [`HealQueueSender`], and delivers them to the
/// [`super::HealWorker`] for EC-based repair.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_durability::heal::HealQueue;
///
/// let queue = HealQueue::new(128);
/// let sender = queue.sender();
/// ```
pub struct HealQueue {
    /// The receiving half of the bounded channel.
    rx: parking_lot::Mutex<Option<mpsc::Receiver<HealRequest>>>,
    /// Cloneable sender for enqueueing requests.
    tx: HealQueueSender,
}

impl HealQueue {
    /// Creates a new bounded heal queue with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero (debug only — zero capacity makes
    /// the queue unusable).
    pub fn new(capacity: usize) -> Self {
        // SAFETY: capacity > 0 is a required invariant. A zero-capacity
        // channel serves no purpose and indicates a misconfiguration.
        assert!(capacity > 0, "HealQueue capacity must be > 0");

        let (tx, rx) = mpsc::channel(capacity);
        Self { rx: parking_lot::Mutex::new(Some(rx)), tx: HealQueueSender { tx } }
    }

    /// Returns a cloneable sender for enqueueing heal requests.
    pub fn sender(&self) -> HealQueueSender {
        self.tx.clone()
    }

    /// Takes ownership of the receiver.
    ///
    /// This should only be called once by the [`super::HealWorker`] at
    /// startup. Subsequent calls return `None`.
    pub(crate) fn take_receiver(&self) -> Option<mpsc::Receiver<HealRequest>> {
        self.rx.lock().take()
    }
}

// ---------------------------------------------------------------------------
// Global enqueue convenience
// ---------------------------------------------------------------------------

/// Initializes the global heal queue sender singleton.
///
/// Called during node startup to wire the [`HealQueueSender`] into the
/// static global, enabling [`enqueue_heal`] to work without explicit
/// dependency injection.
///
/// If the global sender is already initialized (e.g., from a prior test),
/// the call is silently ignored (idempotent).
pub fn init_global_queue(sender: HealQueueSender) {
    GLOBAL_HEAL_SENDER.get_or_init(|| sender);
}

/// Submits a heal request for the given segment and corrupt shard indices.
///
/// This is the primary interface for Scrub and Anti-Entropy to trigger
/// EC-based healing. The request is enqueued into the global bounded
/// channel and processed asynchronously by the [`super::HealWorker`].
///
/// Requires [`init_global_queue`] to have been called during startup.
///
/// # Errors
///
/// Returns an error if the heal queue is full, closed, or not yet
/// initialized.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_durability::heal::enqueue_heal;
///
/// enqueue_heal(segment_id, vec![2, 3])?;
/// ```
pub fn enqueue_heal(segment_id: SegmentId, corrupt_shard_indices: Vec<usize>) -> Result<()> {
    let sender = GLOBAL_HEAL_SENDER.get().ok_or_else(|| {
        Error::Heal(
            "global heal queue not initialized — call init_global_queue during startup".into(),
        )
    })?;

    let request = HealRequest { segment_id, corrupt_shard_indices, retry_count: 0 };

    // enqueue_heal delegates to enqueue_blocking which uses try_send
    // for immediate backpressure (perf rule 2.6).
    // The call site in scrub/anti-entropy is synchronous.
    sender.enqueue_blocking(request)
}

impl HealQueueSender {
    /// Synchronous variant of [`enqueue`] for use from non-async contexts.
    ///
    /// Uses `try_send` directly, returning immediately on backpressure.
    pub(crate) fn enqueue_blocking(&self, request: HealRequest) -> Result<()> {
        self.tx.try_send(request).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::HealQueueFull,
            mpsc::error::TrySendError::Closed(_) => Error::Heal("heal queue closed".into()),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn queue_new_with_capacity() {
        let queue = HealQueue::new(8);
        let sender = queue.sender();
        // Verify sender is cloneable
        let _sender2 = sender.clone();
        assert!(queue.take_receiver().is_some());
    }

    #[test]
    #[should_panic(expected = "HealQueue capacity must be > 0")]
    fn queue_new_with_zero_capacity_panics() {
        HealQueue::new(0);
    }

    #[test]
    fn queue_single_send_receive() {
        let queue = HealQueue::new(4);
        let sender = queue.sender();
        let request = HealRequest {
            segment_id: SegmentId::new(),
            corrupt_shard_indices: vec![0],
            retry_count: 0,
        };

        // Send via blocking enqueue
        sender.enqueue_blocking(request.clone()).unwrap();

        // Receive
        let mut rx = queue.take_receiver().unwrap();
        let received = rx.try_recv().unwrap();
        assert_eq!(received.segment_id, request.segment_id);
        assert_eq!(received.corrupt_shard_indices, request.corrupt_shard_indices);
    }

    #[test]
    fn queue_bounded_backpressure_returns_error() {
        let queue = HealQueue::new(1);
        let sender = queue.sender();
        let request = HealRequest {
            segment_id: SegmentId::new(),
            corrupt_shard_indices: vec![],
            retry_count: 0,
        };

        // First send should succeed
        sender.enqueue_blocking(request.clone()).unwrap();

        // Second send should fail (queue full)
        let result = sender.enqueue_blocking(request);
        assert!(matches!(result, Err(Error::HealQueueFull)));
    }

    #[test]
    fn global_enqueue_heal_requires_init() {
        // Without init, enqueue_heal should fail
        let result = enqueue_heal(SegmentId::new(), vec![1]);
        assert!(result.is_err());
    }

    #[test]
    fn global_enqueue_heal_after_init_succeeds() {
        let queue = HealQueue::new(4);
        init_global_queue(queue.sender());
        let result = enqueue_heal(SegmentId::new(), vec![2]);
        assert!(result.is_ok());
    }

    #[test]
    fn sender_clone_produces_working_copy() {
        let queue = HealQueue::new(4);
        let s1 = queue.sender();
        let s2 = s1.clone();

        let req = HealRequest {
            segment_id: SegmentId::new(),
            corrupt_shard_indices: vec![3],
            retry_count: 0,
        };
        s2.enqueue_blocking(req).unwrap();

        let mut rx = queue.take_receiver().unwrap();
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn take_receiver_returns_none_on_second_call() {
        let queue = HealQueue::new(4);
        assert!(queue.take_receiver().is_some());
        assert!(queue.take_receiver().is_none());
    }
}
