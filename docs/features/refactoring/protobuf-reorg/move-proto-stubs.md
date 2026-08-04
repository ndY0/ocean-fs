---
feature: "Move Protobuf Service Stubs to Owning Crates"
epic: "protobuf-reorg"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: type-system-cleanup
    reason: Proto-generated types reference oceanfs-core types
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Move Protobuf Service Stubs to Owning Crates

## Summary

`crates/oceanfs-network/src/generated/` currently contains generated
protobuf service stubs for **all** OceanFS services: cache, gossip,
healing, scrub, storage, plus common types. Architecture §2.4 states
that service definitions belong to the crate that implements them:

> Messages are shared; services belong to the crate that implements them.

Only `oceanfs.common.rs` and `oceanfs.gossip.rs` (plus `membership.rs`)
should remain in `oceanfs-network`. The cache, healing, scrub, and
storage service stubs should move to the crates that implement those
services. Additionally, audit finding M12 notes that no `.proto` source
files exist under `crates/` — the generated code lacks provenance.
This feature moves the generated stubs and resolves the proto source
location question.

## Scope

### In Scope

- Move generated protobuf service stubs from `oceanfs-network/src/generated/`
  to their owning crates:
  - `oceanfs.cache.rs` → `oceanfs-cache/src/generated/oceanfs.cache.rs`
  - `oceanfs.healing.rs` → `oceanfs-storage/src/generated/oceanfs.healing.rs`
  - `oceanfs.scrub.rs` → `oceanfs-storage/src/generated/oceanfs.scrub.rs`
  - `oceanfs.storage.rs` → `oceanfs-storage/src/generated/oceanfs.storage.rs`
- Keep in `oceanfs-network/src/generated/`:
  - `oceanfs.common.rs` — shared message types (belongs to core conceptually,
    but network is the lowest-level RPC crate; acceptable per §2.4
    "Messages in Core")
  - `oceanfs.gossip.rs` — gossip service (implemented by membership crate,
    but network layer owns the gRPC client/server stub; keep if network
    genuinely implements the gossip transport)
  - `oceanfs.membership.rs` — membership messages (shared types used by
    gossip service)
  - `oceanfs.segment.rs` — segment message types (shared data, keep in
    network or move to core — evaluate during implementation)
- Update all imports in consuming code to reference the new crate-local
  `generated` module paths
- Resolve M12: **Add `.proto` source files** to the expected locations
  (`oceanfs-core/proto/`, `oceanfs-storage/proto/`, etc.) so that generated
  code has canonical provenance. Extract from existing workspace-level
  `proto/` directory if it exists, or create the proto sources alongside
  the generated code.

### Out of Scope

- Regenerating protobuf code from `.proto` files — this is a pure file
  move of the already-generated Rust code
- Changes to protobuf message definitions or service interfaces
- Moving `oceanfs.common.rs` to `oceanfs-core` — that is a separate
  evaluation (the current placement in network is acceptable per §2.4:
  network provides the gRPC transport types)
- Adding protobuf code generation to the build pipeline — this feature
  only moves existing generated files

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-network` | Delete `src/generated/oceanfs.cache.rs`, `oceanfs.healing.rs`, `oceanfs.scrub.rs`, `oceanfs.storage.rs`. Update `src/generated/mod.rs` to remove `pub mod` declarations. Keep `oceanfs.common.rs`, `oceanfs.gossip.rs`, `oceanfs.membership.rs`, `oceanfs.segment.rs`. |
| `oceanfs-cache` | New directory `src/generated/` with `oceanfs.cache.rs` and `mod.rs`. Add `#[path = "generated/mod.rs"] mod generated;` or inline declarations in `lib.rs`. |
| `oceanfs-storage` | New files in `src/generated/`: `oceanfs.healing.rs`, `oceanfs.scrub.rs`, `oceanfs.storage.rs` plus `mod.rs`. Update `lib.rs` to declare the generated module. |
| `oceanfs-server` | May import storage/healing service stubs — update imports from `oceanfs_network::generated::oceanfs::storage` to `oceanfs_storage::generated::oceanfs::storage`. |
| `oceanfs-node` | May import service stubs — update imports accordingly. |

## Interface (Public API)

Each target crate gains a `generated` module that re-exports the moved
service stubs:

```rust
// oceanfs-storage/src/lib.rs
pub mod generated;

// oceanfs-storage/src/generated/mod.rs
pub mod oceanfs {
    pub mod storage {
        include!("oceanfs.storage.rs");
    }
    pub mod healing {
        include!("oceanfs.healing.rs");
    }
    pub mod scrub {
        include!("oceanfs.scrub.rs");
    }
}
```

**Alternative (simpler):** Since the generated files already contain the
module hierarchy (`pub mod oceanfs { pub mod storage { ... } }`), each
crate can `include!` them directly or use `#[path]` attributes. The
implementer should choose the approach that minimizes diff and doesn't
require editing generated code.

## Migration Plan

### Step 1: Create target directories

```
mkdir -p oceanfs-storage/src/generated
mkdir -p oceanfs-cache/src/generated
```

### Step 2: Move files

```bash
# To oceanfs-storage
mv oceanfs-network/src/generated/oceanfs.storage.rs oceanfs-storage/src/generated/
mv oceanfs-network/src/generated/oceanfs.healing.rs oceanfs-storage/src/generated/
mv oceanfs-network/src/generated/oceanfs.scrub.rs oceanfs-storage/src/generated/

# To oceanfs-cache
mv oceanfs-network/src/generated/oceanfs.cache.rs oceanfs-cache/src/generated/
```

### Step 3: Create mod.rs in each target

For `oceanfs-storage/src/generated/mod.rs`:
```rust
//! Generated protobuf service stubs for storage-layer services.
//!
//! Sources: workspace-level `proto/` directory.
//! Regenerate with: `./scripts/gen-proto.sh`

pub mod oceanfs {
    pub mod storage {
        include!("oceanfs.storage.rs");
    }
    pub mod healing {
        include!("oceanfs.healing.rs");
    }
    pub mod scrub {
        include!("oceanfs.scrub.rs");
    }
}
```

For `oceanfs-cache/src/generated/mod.rs`:
```rust
//! Generated protobuf service stubs for cache-layer services.

pub mod oceanfs {
    pub mod cache {
        include!("oceanfs.cache.rs");
    }
}
```

### Step 4: Add module declarations to lib.rs

```rust
// oceanfs-storage/src/lib.rs
pub mod generated;

// oceanfs-cache/src/lib.rs
pub mod generated;
```

### Step 5: Update oceanfs-network

Remove the moved files from `oceanfs-network/src/generated/mod.rs`:
```diff
- pub mod oceanfs {
-     pub mod cache { include!("oceanfs.cache.rs"); }
-     pub mod healing { include!("oceanfs.healing.rs"); }
-     pub mod scrub { include!("oceanfs.scrub.rs"); }
-     pub mod storage { include!("oceanfs.storage.rs"); }
- }
```

Keep: `oceanfs.common.rs`, `oceanfs.gossip.rs`, `oceanfs.membership.rs`,
`oceanfs.segment.rs`.

### Step 6: Update all imports

Search for and update all references:
```bash
grep -rn "oceanfs_network.*storage\|oceanfs_network.*healing\|oceanfs_network.*scrub\|oceanfs_network.*cache" crates/
```

Change patterns:
- `oceanfs_network::generated::oceanfs::storage::*` →
  `oceanfs_storage::generated::oceanfs::storage::*`
- `oceanfs_network::generated::oceanfs::cache::*` →
  `oceanfs_cache::generated::oceanfs::cache::*`
- etc.

### Step 7: Resolve M12 — Add `.proto` source files

Create `.proto` source files in expected locations per architecture §2.4:
- `oceanfs-core/proto/common.proto`, `segment.proto`, `membership.proto`
- `oceanfs-storage/proto/storage.proto`, `healing.proto`, `scrub.proto`
- `oceanfs-cache/proto/cache.proto`

Extract the `.proto` definitions from the existing generated code or from
the workspace-level `proto/` directory if it exists.

### Step 8: Build and test

```bash
cargo build --workspace --all-targets
cargo test --workspace
```

## Data Flow

Unchanged. The generated code is identical; only the crate that owns the
module changes. Service stubs are compiled into their owning crate and
consumed by `oceanfs-server` (or `oceanfs-node`) through that crate's
public API.

## Definition of Done

- [ ] **Code:** `cargo build --workspace --all-targets` succeeds with no
  import errors
- [ ] **Tests:** `cargo test --workspace` passes
- [ ] **Files moved:** `oceanfs.cache.rs` in `oceanfs-cache/src/generated/`;
  `oceanfs.storage.rs`, `oceanfs.healing.rs`, `oceanfs.scrub.rs` in
  `oceanfs-storage/src/generated/`; originals removed from
  `oceanfs-network/src/generated/`
- [ ] **Imports:** No remaining references to moved modules through
  `oceanfs_network::generated::oceanfs::{cache,healing,scrub,storage}`
- [ ] **Docs:** Architecture §2.4 verified to match reality. `.proto` source
  files present in expected crate locations.
- [ ] **ADR:** Not required (this is an implementation of existing §2.4)
- [ ] **Perf:** Not applicable (compile-time module re-organization)
- [ ] **Integration:** gRPC integration tests in `oceanfs-node/tests/`
  continue passing (they exercise the full RPC stack)
