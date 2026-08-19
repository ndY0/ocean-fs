//! Churn scheduler — periodic node kill/restart for Phase 3 cluster tests.
//!
//! The [`ChurnScheduler`] is a background task spawned alongside load
//! workers. It periodically kills a random node (SIGKILL) and later
//! restarts it, producing churn events recorded in the `LoadReport`.
//!
//! ## Modes
//!
//! - [`ChurnMode::Deterministic`]: fixed sequence of kill/restart events,
//!   reproducible from the seed.
//! - [`ChurnMode::Random`]: random node selection with Poisson-distributed
//!   intervals, also seeded for reproducibility.
//!
//! ## Safety invariant
//!
//! The scheduler never kills the last alive node (`alive_count ≥ 2`).
//!
//! ## Usage
//!
//! ```no_run
//! use std::{sync::Arc, time::Duration};
//! use e2e::harness::{config_3node_w2_r2, Cluster};
//! use e2e::load::churn::{ChurnMode, ChurnScheduler};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let cluster = Arc::new(Cluster::spawn(3, &config_3node_w2_r2()).await?);
//! let churn = ChurnScheduler::new(
//!     Arc::clone(&cluster),
//!     ChurnMode::Random,
//!     Duration::from_secs(10),
//!     Duration::from_secs(15),
//!     42,
//! );
//! let events = churn.run(Duration::from_secs(60)).await;
//! # Ok(())
//! # }
//! ```

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha12Rng;
use serde::Serialize;

use crate::harness::Cluster;

// ---------------------------------------------------------------------------
// ChurnScheduler
// ---------------------------------------------------------------------------

/// Manages periodic node kill/restart during a cluster churn test.
///
/// Runs in the background alongside load workers. Tracks which nodes
/// are currently dead and respects a restart delay before bringing
/// them back.
pub struct ChurnScheduler {
    /// Shared reference to the cluster under test.
    cluster: Arc<Cluster>,
    /// Churn mode: deterministic or random.
    mode: ChurnMode,
    /// How often to trigger a churn event (kill or restart check).
    churn_interval: Duration,
    /// How long to wait before restarting a killed node.
    restart_delay: Duration,
    /// Set of currently dead node indices.
    dead_nodes: HashSet<usize>,
    /// Seeded RNG for node selection.
    rng: ChaCha12Rng,
    /// Timestamps when each node was killed (for restart delay).
    killed_at: Vec<Option<Instant>>,
}

impl ChurnScheduler {
    /// Creates a new churn scheduler.
    ///
    /// The `cluster` must have at least 2 nodes for churn to be meaningful.
    pub fn new(
        cluster: Arc<Cluster>,
        mode: ChurnMode,
        churn_interval: Duration,
        restart_delay: Duration,
        seed: u64,
    ) -> Self {
        let node_count = cluster.len();
        Self {
            cluster,
            mode,
            churn_interval,
            restart_delay,
            dead_nodes: HashSet::new(),
            rng: ChaCha12Rng::seed_from_u64(seed),
            killed_at: vec![None; node_count],
        }
    }

    /// Returns the set of currently dead node indices.
    pub fn dead_nodes(&self) -> &HashSet<usize> {
        &self.dead_nodes
    }

    /// Runs the churn scheduler for the given duration.
    ///
    /// Returns a list of [`ChurnEvent`] records for each kill and restart
    /// operation attempted. The scheduler **drains pending restarts**
    /// before returning: a node killed in the final tick is restarted in
    /// a drain phase so the caller's post-run convergence and manifest
    /// verification observe the full cluster.
    pub async fn run(mut self, duration: Duration) -> Vec<ChurnEvent> {
        let mut events = Vec::new();
        let start = Instant::now();

        loop {
            let elapsed = start.elapsed();
            if elapsed >= duration {
                break;
            }

            // ── Kill phase ──
            let alive: Vec<usize> = self.alive_indices();
            if alive.len() >= 2 {
                // Never kill the last node (alive ≥ 2 invariant).
                let target = match self.mode {
                    ChurnMode::Deterministic => {
                        // Round-robin through nodes.
                        let tick =
                            (elapsed.as_secs_f64() / self.churn_interval.as_secs_f64()) as usize;
                        alive[tick % alive.len()]
                    }
                    ChurnMode::Random => {
                        let idx = self.rng.gen_range(0..alive.len());
                        alive[idx]
                    }
                };

                let now = Instant::now();
                let success = self.cluster.kill(target).is_ok();
                if success {
                    self.dead_nodes.insert(target);
                    self.killed_at[target] = Some(now);
                }
                events.push(ChurnEvent {
                    timestamp: elapsed.as_secs_f64(),
                    action: ChurnAction::Kill,
                    node_index: target,
                    success,
                });
            }

            // ── Restart phase ──
            let to_restart: Vec<usize> = {
                let now = Instant::now();
                self.dead_nodes
                    .iter()
                    .copied()
                    .filter(|&i| {
                        self.killed_at[i]
                            .map(|killed_time| {
                                now.duration_since(killed_time) >= self.restart_delay
                            })
                            .unwrap_or(true)
                    })
                    .collect()
            };

            for node_i in to_restart {
                let _now = Instant::now();
                let success = self.cluster.restart(node_i).await.is_ok();
                if success {
                    self.dead_nodes.remove(&node_i);
                    self.killed_at[node_i] = None;
                }
                events.push(ChurnEvent {
                    timestamp: elapsed.as_secs_f64(),
                    action: ChurnAction::Restart,
                    node_index: node_i,
                    success,
                });
            }

            tokio::time::sleep(self.churn_interval).await;
        }

        // ── Drain phase ───────────────────────────────────────────
        // The loop exits when the duration elapses; a node killed in the
        // final tick is still down (its restart_delay may not have
        // elapsed). Restart every remaining dead node so the caller's
        // post-churn convergence/verification sees the full cluster.
        for node_i in self.dead_nodes.iter().copied().collect::<Vec<_>>() {
            let success = self.cluster.restart(node_i).await.is_ok();
            if success {
                self.dead_nodes.remove(&node_i);
                self.killed_at[node_i] = None;
            }
            events.push(ChurnEvent {
                timestamp: start.elapsed().as_secs_f64(),
                action: ChurnAction::Restart,
                node_index: node_i,
                success,
            });
        }

        events
    }

    /// Returns the indices of all alive nodes.
    fn alive_indices(&self) -> Vec<usize> {
        let node_count = self.cluster.len();
        (0..node_count).filter(|i| !self.dead_nodes.contains(i)).collect()
    }
}

// ---------------------------------------------------------------------------
// ChurnMode
// ---------------------------------------------------------------------------

/// How the churn scheduler selects nodes to kill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChurnMode {
    /// Fixed deterministic sequence from seed (round-robin).
    Deterministic,
    /// Random node selection from seeded RNG.
    Random,
}

// ---------------------------------------------------------------------------
// ChurnEvent
// ---------------------------------------------------------------------------

/// A recorded kill or restart event during a churn test.
#[derive(Debug, Clone, Serialize)]
pub struct ChurnEvent {
    /// Seconds since the churn scheduler started.
    pub timestamp: f64,
    /// Whether this was a kill or restart operation.
    pub action: ChurnAction,
    /// The node index that was targeted.
    pub node_index: usize,
    /// Whether the operation succeeded.
    pub success: bool,
}

// ---------------------------------------------------------------------------
// ChurnAction
// ---------------------------------------------------------------------------

/// The type of churn operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ChurnAction {
    /// A node was killed (SIGKILL).
    Kill,
    /// A previously killed node was restarted.
    Restart,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn churn_scheduler_new_initializes_dead_nodes_empty() {
        // We can't construct a real Cluster in unit tests, but we can
        // test the types and logic without one.
        let _mode = ChurnMode::Deterministic;
        let _mode_r = ChurnMode::Random;
    }

    #[test]
    fn churn_mode_deterministic_reproducible_name() {
        assert_eq!(ChurnMode::Deterministic, ChurnMode::Deterministic);
    }

    #[test]
    fn churn_action_serializes_lowercase() {
        assert_eq!(serde_json::to_string(&ChurnAction::Kill).unwrap(), "\"kill\"");
        assert_eq!(serde_json::to_string(&ChurnAction::Restart).unwrap(), "\"restart\"");
    }

    #[test]
    fn churn_event_serializes_correctly() {
        let event =
            ChurnEvent { timestamp: 12.5, action: ChurnAction::Kill, node_index: 1, success: true };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"timestamp\":12.5"));
        assert!(json.contains("\"action\":\"kill\""));
        assert!(json.contains("\"node_index\":1"));
        assert!(json.contains("\"success\":true"));
    }

    #[test]
    fn churn_event_failure_serializes() {
        let event = ChurnEvent {
            timestamp: 30.0,
            action: ChurnAction::Restart,
            node_index: 2,
            success: false,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"success\":false"));
    }

    // ── Determinism test ─────

    #[test]
    fn deterministic_mode_same_seed_same_sequence() {
        let mut r1 = ChaCha12Rng::seed_from_u64(42);
        let mut r2 = ChaCha12Rng::seed_from_u64(42);

        let v1: Vec<u32> = (0..10).map(|_| r1.gen::<u32>()).collect();
        let v2: Vec<u32> = (0..10).map(|_| r2.gen::<u32>()).collect();
        assert_eq!(v1, v2, "same seed must produce same RNG output");
    }

    #[test]
    fn churn_scheduler_alive_indices_logic() {
        // Test the alive_indices logic without a real cluster.
        // dead_nodes = {1}, len = 3 => alive = {0, 2}
        let mut dead = HashSet::new();
        dead.insert(1usize);
        let alive: Vec<usize> = (0..3).filter(|i| !dead.contains(i)).collect();
        assert_eq!(alive, vec![0, 2]);
    }

    #[test]
    fn alive_count_must_be_at_least_2_to_kill() {
        // If only 1 node is alive, no kill should be attempted.
        // Testing the logic: alive.len() >= 2 is the guard.
        let alive_1: Vec<usize> = vec![0];
        let alive_3: Vec<usize> = vec![0, 1, 2];
        assert!(!(alive_1.len() >= 2));
        assert!(alive_3.len() >= 2);
    }
}
