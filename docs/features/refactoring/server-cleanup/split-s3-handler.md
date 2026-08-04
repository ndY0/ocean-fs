---
feature: "Split S3 Handler File"
epic: "server-cleanup"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: type-system-cleanup
    reason: S3 handler imports shared types from oceanfs-core; split-core-types must complete first
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Split S3 Handler File

## Summary

Split `crates/oceanfs-server/src/s3_handler.rs` (1,252 lines) into a module
directory with separate files for each responsibility. Currently the file
mixes axum handler functions, the `AppState` struct, response type construction,
the `MimeMap` type, and test-only mocks. This violates the one-type-per-file
guideline (§3.3) and makes the module harder to navigate. The split preserves
all public API through re-exports from `s3_handler/mod.rs`.

## Scope

### In Scope

- Create `crates/oceanfs-server/src/s3_handler/` directory with `mod.rs`
  as the re-export facade
- Move axum handler functions (`put_object`, `get_object`, `head_object`,
  `delete_object`, `create_bucket`, `delete_bucket`, `list_objects`) plus
  internal helpers (`header_val`, `s3_error_response`) into
  `s3_handler/handlers.rs`
- Move `AppState` struct (currently `pub(crate)`) into
  `s3_handler/app_state.rs`
- Move `MimeMap` type into `s3_handler/mime_map.rs`
- Move `MockMetadata` out of the `#[cfg(test)]` module into a new
  `crates/oceanfs-server/src/test_util.rs` or an existing test-support
  location, re-exported for use in `s3_handler` tests and other crate tests
- Update `s3_handler/mod.rs` to re-export `S3Handler`, `MimeMap`, and any
  other types currently re-exported from `lib.rs`
- Update `oceanfs-server/src/lib.rs` imports to reflect new module path

### Out of Scope

- Changing handler logic — pure mechanical split
- Moving `S3Handler` configuration to a separate file — the struct is
  well-sized and cohesive
- Response type extraction: the audit (H4) mentions `PutObjectResponse`,
  `GetObjectResponse`, etc., but the handlers return `axum::response::Response`
  directly rather than named response structs. The response construction logic
  is inline within each handler — no separate response types exist to extract
- Any changes to `s3_xml.rs` — it is already well-separated (L3 in the audit)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | Replace `src/s3_handler.rs` with `src/s3_handler/` directory containing `mod.rs`, `handlers.rs`, `app_state.rs`, `mime_map.rs`. Add `src/test_util.rs` for `MockMetadata`. |

## Interface (Public API)

- `pub struct S3Handler` — unchanged; re-exported from `s3_handler/mod.rs`
  via `pub use handlers::S3Handler;`
- `pub struct MimeMap` — unchanged; re-exported from `s3_handler/mod.rs`
  via `pub use mime_map::MimeMap;`
- `pub(crate) struct AppState` — unchanged visibility; defined in
  `s3_handler/app_state.rs`, re-exported via `pub(crate) use app_state::AppState;`

No public API breakage. The `S3Handler` re-export from `oceanfs-server/src/lib.rs`
(line 55: `pub use s3_handler::S3Handler;`) remains valid since `mod.rs` re-exports
the same symbol.

## Data Flow

The data flow is unchanged by this refactor. The handlers receive HTTP requests
and delegate to coordinators as before:

```
HTTP Request → axum router
  → s3_handler::handlers::put_object (or get_object, head_object, ...)
    → AppState::write_coordinator.put() / read_coordinator.get()
      → Response (with headers from MimeMap)
```

## Implementation Plan

1. Create `src/s3_handler/` directory
2. Extract `MimeMap` into `s3_handler/mime_map.rs` (simplest, zero deps beyond `std`)
3. Extract `AppState` into `s3_handler/app_state.rs`
4. Move handler functions into `s3_handler/handlers.rs`
5. Create `s3_handler/mod.rs` with appropriate `mod` and re-export declarations
6. Move `MockMetadata` to `src/test_util.rs`
7. Update `src/lib.rs`: change `mod s3_handler;` to `pub mod s3_handler;`
   (the `pub mod` is needed because `s3_handler/mod.rs` is the module root)
8. Update imports in `handlers.rs` — use `use super::app_state::AppState;`,
   `use super::mime_map::MimeMap;`
9. Add `mod test_util;` to `lib.rs` (or gate behind `#[cfg(test)]`)
10. Run `cargo build --all-targets -p oceanfs-server` and fix any import errors
11. Run `cargo test -p oceanfs-server` — all existing tests must pass

## Definition of Done

- [ ] **Code:** `cargo build --all-targets -p oceanfs-server` succeeds
- [ ] **Tests:** `cargo test -p oceanfs-server` passes; all existing S3 handler
  tests pass from their new file locations
- [ ] **Docs:** `#![deny(missing_docs)]` passes; every `pub` item retains its
  doc comment through the move
- [ ] **ADR:** Not required (pure mechanical refactor, no architectural change)
- [ ] **Perf:** Not applicable (no algorithmic change; import paths resolved at
  compile time)
- [ ] **Integration:** `oceanfs-node` integration tests exercise the S3 API
  end-to-end and must continue passing
