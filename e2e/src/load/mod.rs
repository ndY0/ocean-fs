//! Load test harness — orchestrator, worker framework, and statistics.
//!
//! This module provides the engine for every Phase 1-4 load test:
//!
//! - [`Manifest`]: PUT recording and post-run verification
//! - [`LoadScenario`]: test configuration (concurrency, duration, operation mix, etc.)
//! - [`Worker`]: single load-generator task
//! - [`Orchestrator`]: spawns workers, collects aggregate stats
//! - [`AggregateStats`]: merged statistics with p50/p99 percentiles
//! - [`MetricsSnapshot`]: scrapes `/admin/metrics` and computes counter deltas
//! - [`LoadReport`]: JSON output, assertions, and Prometheus textfile
//! - [`FailureInjectionRecord`]: records of injected failures during degraded-mode tests
//! - [`ChurnScheduler`]: periodic node kill/restart for cluster churn tests

pub mod churn;
pub mod degrade;
pub mod generator;
pub mod manifest;
pub mod metrics;
pub mod report;

// Re-export public types from submodules.
pub use churn::{ChurnAction, ChurnEvent, ChurnMode, ChurnScheduler};
pub use degrade::FailureInjectionRecord;
pub use generator::{
    AggregateStats, BlobSizeDist, KeySpace, LoadScenario, OpWeight, Operation, Orchestrator,
    Worker, WorkerStats,
};
pub use manifest::{Manifest, ManifestSummary, Mismatch};
pub use metrics::{parse_prometheus_text, MetricsSnapshot};
pub use report::{assert_that, AssertionResult, FailureDetail, LoadReport, ReportResult};
