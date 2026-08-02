# OceanFS — Coding Standards

**Version:** 0.2.0 — Draft
**Date:** 2026-07-30

---

## Philosophy

No arbitrary limits on file length or function length. Coupling and
internal coherence are the judges of when something belongs together,
not a line count. A 600-line module implementing a single,
well-boundaried type is fine. A 20-line file with three unrelated
types is not.

Every rule below is designed to make the codebase navigable,
consistent, and safe — not to enforce an aesthetic.

---

## 1. Visibility

### 1.1 Default Visibility Is `pub(crate)`

All items within a crate default to `pub(crate)`. Only items listed
in the crate's `lib.rs` facade get `pub`.

```rust
// Correct
pub(crate) struct ActiveSegment { ... }
pub struct SegmentHandle { ... }       // re-exported in lib.rs

// Wrong — leaks internal type to dependents
pub struct ActiveSegment { ... }
```

### 1.2 `pub` Only on Facade Exports

`pub` visibility is reserved for types, functions, and traits that
are part of the crate's public API — the things re-exported from
`lib.rs`.

**Enforcement:** Audit with `grep -r "^pub " src/ | grep -v lib.rs`.
Any `pub` outside `lib.rs` is suspect.

### 1.3 `pub(super)` for Sibling Access

When a module needs to expose items to its parent module (but not the
crate at large), use `pub(super)`. This is more granular than
`pub(crate)` and documents the narrower intent.

```rust
// src/segment/index.rs
pub(super) struct SegmentIndex { ... }  // visible to src/segment/mod.rs only
```

### 1.4 No `pub` Fields on Structs

Struct fields are always private. Access is through constructors,
getters, or the builder pattern.

```rust
// Correct
pub struct SegmentHandle {
    id: SegmentId,
    node_ids: Vec<NodeId>,
}
impl SegmentHandle {
    pub fn id(&self) -> SegmentId { self.id }
    pub fn node_ids(&self) -> &[NodeId] { &self.node_ids }
}

// Wrong
pub struct SegmentHandle {
    pub id: SegmentId,
    pub node_ids: Vec<NodeId>,
}
```

**Exception:** `pub(crate)` fields are acceptable inside a crate when
the fields are set by different modules within the same crate. This
is still visible to the whole crate; prefer `pub(super)` if the
access pattern is narrower.

### 1.5 `#[non_exhaustive]` on Public Enums

All `pub` enums are annotated with `#[non_exhaustive]`. This allows
adding variants without a semver-breaking change.

```rust
#[non_exhaustive]
pub enum CodecType {
    CauchyRs,
    StandardRs,
    Lrc,
    Clay,
}
```

---

## 2. Imports

### 2.1 Four-Group Import Order

```
// 1. std
use std::collections::HashMap;
use std::sync::Arc;

// 2. External crates
use bytes::Bytes;
use tokio::sync::RwLock;

// 3. Crate-internal (crate::)
use oceanfs_core::{BucketId, ObjectKey};

// 4. super / self
use super::segment::SegmentHandle;
use self::index::SegmentIndex;
```

**Enforcement:** `rustfmt` handles this automatically when configured:
```toml
# rustfmt.toml
group_imports = "StdExternalCrate"
```

### 2.2 No Wildcard Imports

`use foo::*` is forbidden except in:
- `lib.rs` facade re-exports (`pub use segment::*;`)
- Prelude modules (`pub mod prelude { pub use ...; }`)

**Enforcement:** Clippy lint: `clippy::wildcard_imports` (denied).

### 2.3 Merge Same-Crate Imports

```rust
// Correct
use oceanfs_core::{BucketId, Config, ObjectKey};

// Wrong
use oceanfs_core::BucketId;
use oceanfs_core::Config;
use oceanfs_core::ObjectKey;
```

**Enforcement:** `rustfmt` handles this.

### 2.4 Re-export Types Used in Public API

If a crate's public API exposes a type from `oceanfs-core`, re-export
it so dependents don't need to add `oceanfs-core` to their `Cargo.toml`.

```rust
// oceanfs-storage/src/lib.rs
pub use oceanfs_core::SegmentId;  // re-export

// Dependent now writes:
// use oceanfs_storage::SegmentId;
// instead of needing oceanfs_core in their Cargo.toml to use SegmentId
```

---

## 3. Error Handling

### 3.1 Each Crate Has Its Own Error Enum

Every crate defines `src/error.rs` with a `pub enum Error { ... }`.
This is the only error type the crate returns.

```rust
// oceanfs-storage/src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("segment {0} not found")]
    SegmentNotFound(SegmentId),

    #[error("WAL I/O error: {0}")]
    WalIo(#[source] std::io::Error),

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch {
        expected: HashOutput,
        actual: HashOutput,
    },

    #[error("invalid config: {0}")]
    InvalidConfig(String),
}
```

### 3.2 Error Variants Group by Cause

Variants are grouped by root cause, not by API method:

- I/O errors: `WalIo`, `SegmentRead`, `SegmentWrite`
- Input validation: `InvalidConfig`, `InvalidKey`, `InvalidSize`
- Data integrity: `ChecksumMismatch`, `MerkleMismatch`
- Not found: `SegmentNotFound`, `ObjectNotFound`
- Internal: `EncodingError`, `DecodingError`

**Wrong:**
```rust
// DON'T group by method — this creates explosion of variants
enum Error {
    AppendFailed(#[source] io::Error),
    SealFailed(#[source] io::Error),
    ReadFailed(#[source] io::Error),
    DeleteFailed(#[source] io::Error),
}
```

### 3.3 `#[from]` Internally, `.map_err()` at Boundaries

Within a crate, use `#[from]` for automatic error conversion:

```rust
// oceanfs-storage/src/error.rs
pub enum Error {
    #[error("WAL I/O error: {0}")]
    WalIo(#[from] std::io::Error),       // automatic conversion from io::Error
}
```

At crate boundaries (in dependent crates), use explicit `.map_err()`
to wrap errors into the caller's error type:

```rust
// oceanfs-server/src/write_coordinator.rs
let handle = self.segment_store
    .append(&key, data)
    .await
    .map_err(|e| ServerError::StorageWrite(format!("{e}")))?;
```

**Rationale:** `#[from]` is convenient internally but creates an
implicit coupling — changing the error type of a dependency silently
changes your error type. At crate boundaries, make the mapping
explicit.

### 3.4 Only `std::io::Error` Crosses Crate Boundaries Raw

The only external error type propagated through multiple crate layers
without wrapping is `std::io::Error`. All other external errors are
wrapped into the crate's own error enum.

```rust
// Allowed: io::Error surfaces in storage::Error
pub enum StorageError {
    Io(#[from] std::io::Error),
}

// Disallowed: a third-party error type appears in the public API
pub enum StorageError {
    RocksDb(#[from] rocksdb::Error),  // leaks dependency type
}
```

### 3.5 All Errors Implement Standard Traits

Every crate's `Error` type implements:

```rust
impl std::error::Error for Error {}
// Send + Sync + 'static is automatic for enums with Send + Sync variants
// but verify: CI checks with static_assertions::assert_impl_all!
```

**Enforcement:** CI assertion:
```rust
#[cfg(test)]
mod assertions {
    use static_assertions::assert_impl_all;
    use super::Error;
    assert_impl_all!(Error: std::error::Error, Send, Sync);
}
```

---

## 4. Testing

### 4.1 Unit Tests Colocated

```rust
// src/segment.rs
impl SegmentHandle {
    pub fn id(&self) -> SegmentId { self.id }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_handle_id_returns_assigned_id() {
        let handle = SegmentHandle::new(SegmentId::new(), vec![]);
        assert_eq!(handle.id(), SegmentId::new());
    }
}
```

Tests live in the same file as the code they test. This makes tests
discoverable and keeps them in sync with the implementation.

### 4.2 Integration Tests at Crate Boundaries

`tests/` at the crate root exercises exactly one crate's public API.

```
oceanfs-storage/tests/
  segment_lifecycle.rs
  wal_recovery.rs
  metadata_crud.rs
```

Each test file is a self-contained scenario with its own setup and
teardown.

### 4.3 Property-Based Tests for Mathematical Code

EC encode/decode, hashing, and any code with a round-trip property
uses `proptest` for exhaustive edge-case testing.

```rust
proptest! {
    #[test]
    fn ec_roundtrip_does_not_corrupt_data(
        data in prop::collection::vec(any::<u8>(), 1..65536),
        k in 1u8..16, m in 1u8..8,
    ) {
        let encoded = encode(&data, k, m)?;
        let decoded = decode(&encoded, k, m)?;
        prop_assert_eq!(data, decoded);
    }
}
```

### 4.4 One Behavior per Test, Named Conventionally

```
fn {method_or_constructor}_{condition}_{expected_outcome}()
```

Examples:
```rust
fn segment_seal_when_full_marks_sealed()
fn segment_seal_when_empty_reuses_buffer()
fn wal_append_when_disk_full_returns_error()
fn cache_get_when_ttl_expired_returns_miss()
```

### 4.5 Unsafe Code Requires a Safety Test

Every `unsafe` block must have at least one unit test that exercises
the safety invariant. If the invariant can't be tested (e.g., it
depends on an external hardware guarantee), the code is too clever
and must be restructured.

```rust
// SAFETY: The slice is aligned to 64 bytes and its length is a multiple of 64.
unsafe { process_aligned_shards(&shards) }

#[test]
fn process_aligned_shards_panics_on_unaligned_input() {
    let result = std::panic::catch_unwind(|| {
        process_aligned_shards(&[0u8; 63]); // not multiple of 64
    });
    assert!(result.is_err());
}
```

### 4.6 Test Philosophy

Coverage is not a gate. Write tests for correctness, not to satisfy
a numeric threshold. The project values well-tested critical paths
over blanket coverage percentages.

---

## 5. Documentation

### 5.1 All `pub` Items Have Doc Comments with Examples

```rust
/// Writes a blob to the given bucket and key.
///
/// The blob is appended to an active segment. If the segment fills,
/// it is sealed, EC-encoded asynchronously, and distributed.
///
/// # Examples
///
/// ```
/// # use oceanfs_storage::SegmentStore;
/// # async fn example(store: impl SegmentStore) -> Result<()> {
/// let handle = store.append(&key, blob_data).await?;
/// # Ok(())
/// # }
/// ```
pub async fn append(&self, key: &Key, data: Bytes) -> Result<SegmentHandle> {
    // ...
}
```

**Enforcement:** `#![deny(missing_docs)]` in each crate's `lib.rs`.

### 5.2 Module-Level Documentation

Every `lib.rs` and every `mod.rs` / module root file begins with a
`//!` comment explaining the module's responsibility:

```rust
//! Segment storage engine.
//!
//! Manages the lifecycle of segments: buffering writes in active
//! segments, sealing them when full, encoding via erasure coding,
//! and distributing shards across the cluster.
//!
//! ## Architecture
//!
//! The segment engine has three main components:
//! - `ActiveSegment`: in-memory buffer accepting appends
//! - `SegmentStore`: manages sealed segments on disk
//! - `SegmentIndex`: B-tree index for blob lookup within a segment
```

### 5.3 Inline Comments Explain "Why", Not "What"

```rust
// Correct: explains the non-obvious constraint
// We use BLAKE3 here (not xxHash) because the segment
// checksum must be cryptographically collision-resistant
// for the anti-entropy Merkle exchange.
let hash = blake3::hash(&data);

// Wrong: restates the code
// Hash the data using BLAKE3
let hash = blake3::hash(&data);
```

### 5.4 Architecture Decision Records

Every significant design decision is documented as an ADR in
`docs/adr/`. See `docs/adr/0000-template.md` for the format.

Current ADRs expected:
| ADR | Topic |
|---|---|
| 0001 | Segment packing vs per-object EC |
| 0002 | SWIM + consistent hashing vs Raft per shard |
| 0003 | Cauchy RS vs standard RS vs Clay codes |
| 0004 | Tiered segment sizing (inline / small / standard) |
| 0005 | Trait-in-consuming-crate pattern |
| 0006 | GPU acceleration tier model |

---

## 6. Naming Conventions

### 6.1 Crates

`oceanfs-{subsystem}` in `kebab-case`:

```
oceanfs-core, oceanfs-hash, oceanfs-ec, oceanfs-accel,
oceanfs-storage, oceanfs-routing, oceanfs-membership,
oceanfs-network, oceanfs-cache, oceanfs-server,
oceanfs-node, oceanfs (binary)
```

### 6.2 Types, Traits, Functions

| Element | Convention | Examples |
|---|---|---|
| Struct | `PascalCase` noun | `SegmentHandle`, `RingCache`, `ConnectionPool` |
| Enum | `PascalCase` noun | `CodecType`, `NodeState`, `CacheResult` |
| Trait | `PascalCase` verb-noun or adjective | `SegmentStore`, `Encodable`, `MetadataStore` |
| Function | `snake_case` verb_noun | `seal_segment`, `encode_stripe`, `fetch_shard` |
| Method | `snake_case` verb_noun | `segment.handle()`, `ring.lookup()` |
| Constant | `SCREAMING_SNAKE_CASE` | `DEFAULT_STRIP_SIZE`, `MAX_BLOBS_PER_SEGMENT` |
| Module | `snake_case` | `segment_index`, `buffer_pool`, `connection_pool` |
| Error type | `Error` (per crate) | `oceanfs_storage::Error` |
| Config struct | `{Type}Config` or `{Type}Options` | `NodeConfig`, `BucketOptions`, `AccelConfig` |
| Builder | `{Type}Builder` | `SegmentBuilder`, `EncodingConfigBuilder` |

### 6.3 Permitted Abbreviations

Only these abbreviations are allowed in identifiers:

| Abbreviation | Meaning |
|---|---|
| `ec` | Erasure Coding |
| `wal` | Write-Ahead Log |
| `dht` | Distributed Hash Table |
| `hlc` | Hybrid Logical Clock |
| `gc` | Garbage Collection |
| `io` | Input/Output |
| `rpc` | Remote Procedure Call |
| `gpu` | Graphics Processing Unit |
| `simd` | Single Instruction Multiple Data |
| `lru` | Least Recently Used |

All other terms are spelled out in full. No `seg`, `enc`, `cfg`,
`buf`, `addr`, `msg`, `ctx`, `req`, `resp` — spell them out.

### 6.4 Generic Parameters

Single uppercase letter for simple cases; descriptive name for
complex bounds:

```rust
// Correct: simple
fn encode<D: AsRef<[u8]>>(data: D) -> Result<EncodedData>

// Correct: complex
fn decode<StripeIter, ErrorHandler>(
    stripes: StripeIter,
    on_error: ErrorHandler,
) -> Result<Vec<u8>>
where
    StripeIter: IntoIterator<Item = Stripe>,
    ErrorHandler: Fn(DecodeError) -> ControlFlow,
```

---

## 7. Unsafe Code

### 7.1 Gate Unsafe to Three Crates

`#![forbid(unsafe_code)]` in all crates except:
- `oceanfs-accel` (GPU FFI, SIMD intrinsics)
- `oceanfs-hash` (if manual BLAKE3 implementation)
- `oceanfs-ec` (SIMD-accelerated GF arithmetic)

**Enforcement:** CI fails if `#![forbid(unsafe_code)]` is absent from
a non-permitted crate.

### 7.2 `// SAFETY:` Comments

Every `unsafe { ... }` block must be immediately preceded by a
`// SAFETY:` comment citing the invariant that makes it sound.

```rust
// SAFETY: `shards` is a valid reference to a `[Stripe; 16]` array.
// The caller guarantees that `shards.len() == 16` and the pointer
// is aligned to `align_of::<Stripe>()`.
let stripes = unsafe { &*(shards.as_ptr() as *const [Stripe; 16]) };
```

**Enforcement:** Clippy lint: `clippy::undocumented_unsafe_blocks` (denied).

---

## 8. Dependencies

### 8.1 Minimize External Dependencies

Before adding a dependency, answer:
1. Is this functionality available in `std`? Use `std`.
2. Can we implement it in <100 lines? Implement it.
3. Is there a widely-adopted, actively-maintained crate? Use it.
4. Is it behind a feature flag? Feature-gate it.

### 8.2 Audit Dependency Tree

CI runs `cargo-deny` to check:
- License compatibility (no GPL in a permissive project)
- Known vulnerabilities (RUSTSEC advisories)
- Duplicate dependencies (multiple versions of the same crate)
- Banned crates (explicitly disallowed)

### 8.3 Feature-Flag Optional Dependencies

All optional dependencies are feature-gated:

```toml
[features]
cuda = ["dep:cudarc"]
isa-l = ["dep:isal-rs"]
```

The crate compiles and passes tests with `--no-default-features`.

---

## 9. Code Style Automation

### 9.1 Required Tools

| Tool | Configuration | CI Check |
|---|---|---|
| `rustfmt` | `rustfmt.toml` (workspace) | `cargo fmt --check` |
| `clippy` | `clippy.toml` (workspace) | `cargo clippy -- -D warnings` |
| `cargo-deny` | `deny.toml` (workspace) | `cargo deny check` |

| `cargo-miri` | — | `cargo miri test` (unsafe crates) |

### 9.2 Clippy Configuration

```toml
# clippy.toml (workspace root)
disallowed-types = [
    "std::sync::Mutex",
    "std::sync::RwLock",
]
```

```rust
// Each crate's lib.rs
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::missing_safety_doc,
    missing_docs,
    unsafe_code,               // except in permitted crates
)]
```

#### 9.2.1 Production Code vs Test Code

The `clippy::unwrap_used` and `clippy::expect_used` lints target **production
code only** (`src/` excluding `#[cfg(test)]` modules). Test code naturally
uses `.unwrap()` and `.expect()` for assertions — these are acceptable and
do not block feature completeness.

For feature Definition of Done, the relevant check is:

```
cargo clippy --lib -- -D warnings    # production code only
```

The `--all-targets` flag includes test code and will produce `unwrap_used` /
`expect_used` warnings in every crate's `#[cfg(test)]` modules and integration
tests. These are structural codebase hygiene issues tracked separately, not
feature-completeness gates.

Test-specific lint exceptions:
- `#[cfg(test)]` modules: `.unwrap()` and `.expect()` are permitted
- Integration tests (`tests/`): same exemption
- If a test's logic requires `expect()` for correctness, add
  `#[allow(clippy::expect_used)]` on that function

### 9.3 Pre-Commit Checks

A `scripts/ci-checks.sh` runs the full CI pipeline locally before
pushing:

```bash
#!/bin/bash
set -euo pipefail

echo "==> fmt"
cargo fmt --all -- --check

echo "==> clippy"
cargo clippy --all-targets --all-features -- -D warnings

echo "==> build"
cargo build --all-targets --all-features

echo "==> test"
cargo test --all-targets --all-features

echo "==> docs"
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```
