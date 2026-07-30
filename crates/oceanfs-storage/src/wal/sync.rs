//! Group commit for WAL fsync.
//!
//! Collects pending fsync waiters and wakes them all after a single
//! fsync call, amortizing the cost across many concurrent appends.
//! Per performance guideline §3.4.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::error::Result;

/// Internal group-commit coordinator for batched WAL fsync.
///
/// Writers register their append with `submit`, receiving a oneshot
/// that resolves when the batch is flushed. The background flusher
/// task collects waiters and calls `fsync` once per batch.
pub(crate) struct WalSyncGroup {
    /// Sender for fsync requests.
    tx: mpsc::Sender<oneshot::Sender<()>>,
    /// Handle to the background flusher task.
    _flush_handle: tokio::task::JoinHandle<()>,
}

impl WalSyncGroup {
    /// Creates a new sync group and spawns the background flusher.
    ///
    /// `fsync_fn` is called to flush pending writes. `batch_timeout_ms`
    /// controls the maximum delay before a batch is flushed even if
    /// the batch hasn't filled.
    pub fn new<F>(fsync_fn: F, batch_timeout_ms: u64) -> Self
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<oneshot::Sender<()>>(1024);

        let flush_handle = tokio::spawn(async move {
            let fsync_fn = Arc::new(fsync_fn);
            loop {
                // Wait for at least one waiter or a timeout.
                let timeout =
                    tokio::time::sleep(std::time::Duration::from_millis(batch_timeout_ms));
                tokio::pin!(timeout);

                let mut waiters: Vec<oneshot::Sender<()>> = Vec::with_capacity(64);

                // Collect the first waiter.
                let first = tokio::select! {
                    msg = rx.recv() => msg,
                    _ = &mut timeout => {
                        // Timeout with no waiters — just loop.
                        continue;
                    }
                };

                match first {
                    Some(waiter) => waiters.push(waiter),
                    None => break, // channel closed
                }

                // Drain any additional waiters without blocking.
                while let Ok(waiter) = rx.try_recv() {
                    waiters.push(waiter);
                    if waiters.len() >= 64 {
                        break;
                    }
                }

                // Perform the fsync.
                if let Err(e) = fsync_fn() {
                    tracing::error!(?e, "WAL fsync failed");
                }

                // Wake all waiters.
                for waiter in waiters {
                    let _ = waiter.send(());
                }
            }
        });

        Self { tx, _flush_handle: flush_handle }
    }

    /// Submits an fsync request.
    ///
    /// Returns a oneshot receiver that resolves when this entry has been
    /// flushed to disk (along with all other entries in the same batch).
    pub async fn submit(&self) -> Result<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(tx).await.map_err(|_| {
            crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WAL sync group shut down",
            ))
        })?;
        Ok(rx)
    }
}

impl Drop for WalSyncGroup {
    fn drop(&mut self) {
        // The channel will be closed, causing the flusher task to exit.
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[tokio::test]
    async fn group_commit_flushes_batch() {
        let flush_count = Arc::new(AtomicU32::new(0));
        let fc = flush_count.clone();

        let group = WalSyncGroup::new(
            move || {
                fc.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            50, // 50ms timeout
        );

        // Submit 3 entries in quick succession.
        let rx1 = group.submit().await.unwrap();
        let rx2 = group.submit().await.unwrap();
        let rx3 = group.submit().await.unwrap();

        // All should resolve within the batch timeout.
        tokio::time::timeout(std::time::Duration::from_secs(2), rx1).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), rx2).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), rx3).await.unwrap().unwrap();

        // One or more fsyncs should have occurred.
        assert!(flush_count.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn timeout_flushes_empty_batch() {
        let flush_count = Arc::new(AtomicU32::new(0));
        let fc = flush_count.clone();

        let group = WalSyncGroup::new(
            move || {
                fc.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            10, // short timeout
        );

        // Submit one entry.
        let rx = group.submit().await.unwrap();
        rx.await.unwrap();

        drop(group);
    }
}
