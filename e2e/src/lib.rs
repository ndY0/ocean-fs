//! End-to-end smoke tests for OceanFS.
//!
//! This crate spawns the OceanFS release binary as a child process,
//! exercises it via HTTP, and asserts behavior programmatically.
//!
//! ## Architecture
//!
//! - [`harness::NodeProcess`]: spawns the binary, waits for health,
//!   provides HTTP helpers.
//! - `tests/`: one test file per subsystem, each spawning its own
//!   node process with a fresh temp directory and config.
//!
//! ## Usage
//!
//! ```bash
//! cargo test -p e2e
//! ```
//!
//! Tests are independent and can run in parallel. Each test picks
//! unique OS-assigned ports to avoid conflicts.

#![forbid(unsafe_code)]
#![deny(clippy::wildcard_imports, clippy::undocumented_unsafe_blocks, missing_docs)]
// E2E tests naturally use unwrap/expect for assertions; these are
// acceptable in test harness code per coding.md §9.2.1.
#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod harness;
pub mod load;
pub mod remote;
