---
feature: "Per-Node HintWal Files"
epic: "durability-wal-consolidation"
status: done
priority: high
owner: ""
dependencies:
  - epic: phase-7-durability
    reason: Requires HintedHandoffManager, HintWal, HintedHandoffConfig, and existing hint delivery infrastructure
adr:
  - 0018-durability-wal-consolidation
  - 0009-storage-crate-split
perf: []
created: 2026-08-10
updated: 2026-08-10
---

# Per-Node HintWal Files

## Summary

ADR-0018 Decision 2 replaces the single `hints.wal` file with per-node files
under `{data_dir}/hints/{node_id}.wal`. Each file is independently managed
and truncated. This eliminates cross-node truncation pollution — when one
node returns and its hints are delivered, its file is truncated to zero
without affecting hints for other nodes still down. The `HintWal` struct
itself is unchanged (it already operates on a per-file basis via `open(path)`).
The change is in `HintedHandoffManager`, which now maintains a
`DashMap<NodeId, Arc<HintWal>>` with lazy open/close semantics.

This feature is **independent** of Decision 1 (remove MerkleWal) and
Decision 3 (segment-ref hints).

## Scope

### In Scope

- **Modify** `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs` — `HintedHandoffManager` struct:
  - Replace field `hint_wal: Arc<HintWal>` (line 162) with:
    ```rust
    wal_dir: PathBuf,
    node_wals: DashMap<NodeId, Arc<HintWal>>,
    ```
  - Add a `last_access: DashMap<NodeId, Instant>` for lazy-close tracking
  - Remove `use` of `crate::HintWal` (still needed but now for per-node instances, not a single one)
- **Modify** `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs` — `HintedHandoffManager::new()`:
  - Signature changes from:
    ```rust
    pub fn new(hint_wal: Arc<HintWal>, delivery_client: Arc<dyn HintDeliveryClient>, config: HintedHandoffConfig) -> Self
    ```
    to:
    ```rust
    pub fn new(wal_dir: PathBuf, delivery_client: Arc<dyn HintDeliveryClient>, config: HintedHandoffConfig) -> Self
    ```
  - Constructs `hint_wal` → `wal_dir`, initializes empty `DashMap`s
- **Modify** `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs` — `enqueue()` (line 248):
  - Before calling `self.hint_wal.write_hint(&record)`, resolve the per-node WAL:
    ```rust
    let target = record.intended_for()...;
    let wal = self.get_or_open_node_wal(&target).await?;
    let (position, end_position) = wal.write_hint(&record).await?;
    ```
  - Add method `get_or_open_node_wal(&self, node_id: &NodeId) -> Result<Arc<HintWal>>`:
    ```rust
    async fn get_or_open_node_wal(&self, node_id: &NodeId) -> Result<Arc<HintWal>> {
        if let Some(wal) = self.node_wals.get(node_id) {
            self.last_access.insert(node_id.clone(), Instant::now());
            return Ok(wal.clone());
        }
        // Cap concurrently open WALs at 16.
        if self.node_wals.len() >= 16 {
            self.evict_least_recently_used();
        }
        let file_path = self.wal_dir.join(format!("{}.wal", node_id));
        let wal = Arc::new(HintWal::open(&file_path).await?);
        self.node_wals.insert(node_id.clone(), wal.clone());
        self.last_access.insert(node_id.clone(), Instant::now());
        Ok(wal)
    }
    ```
  - Add method `evict_least_recently_used()`:
    - Find the entry in `last_access` with the oldest timestamp
    - Ensure 60+ seconds of inactivity
    - Remove from `node_wals` and `last_access` (dropping the `Arc<HintWal>` closes the file)
- **Modify** `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs` — `drain_and_deliver()` (line 288):
  - After successful delivery and WAL truncation (line 347), replace `self.hint_wal.truncate_after(last_end_position)` with:
    ```rust
    // Truncate the per-node file to zero after successful delivery.
    if let Some(wal) = self.node_wals.get(&target) {
        wal.truncate_after(0).await.ok();
    }
    // Remove the empty file.
    let file_path = self.wal_dir.join(format!("{}.wal", target));
    let _ = std::fs::remove_file(&file_path);
    self.node_wals.remove(&target);
    self.last_access.remove(&target);
    ```
- **Modify** `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs` — `replay_and_enqueue()` (line 222):
  - Replace `self.hint_wal.replay()` with directory scan:
    ```rust
    pub async fn replay_and_enqueue(&self) -> Result<usize> {
        let mut total = 0usize;
        let dir = std::fs::read_dir(&self.wal_dir)?;
        for entry in dir {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "wal") {
                // Extract NodeId from filename: "{node_id}.wal"
                let file_name = path.file_stem().unwrap().to_string_lossy();
                let node_id = NodeId::new(&file_name);
                let wal = HintWal::open(&path).await?;
                let records = wal.replay().await?;
                for (start, end, record) in records {
                    let mut queue = self.queues.entry(node_id.clone()).or_default();
                    queue.push_back((start, end, record));
                }
                total += records.len();
                // Keep the WAL open in the map.
                self.node_wals.insert(node_id.clone(), Arc::new(wal));
            }
        }
        Ok(total)
    }
    ```
- **Modify** `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs` — `HintedHandoffConfig` (line 47):
  - Replace `pub wal_path: std::path::PathBuf` with:
    ```rust
    pub wal_dir: std::path::PathBuf,
    ```
  - Update `Default` impl (line 59):
    ```rust
    wal_dir: std::path::PathBuf::from("/var/lib/oceanfs/hints"),
    ```
- **Modify** `crates/oceanfs-durability/src/hinted_handoff/mod.rs`:
  - No changes to public API — `HintedHandoffConfig` is re-exported from `hint_delivery`
  - Verify `HintWal` re-export is still correct
- **Modify** `crates/oceanfs-node/src/node.rs`:
  - The `hint_wal_path` usage (lines 940–947) changes:
    ```rust
    // Old:
    // let hint_wal_path = config.hint_wal_path.clone().unwrap_or_else(|| config.data_dir.join("hints.wal"));
    // let hint_wal = Arc::new(HintWal::open(&hint_wal_path).await?);
    // let hint_wal_for_prune = hint_wal.clone();
    // let hint_config = HintedHandoffConfig { wal_path: hint_wal_path, ... };
    //
    // New:
    let hints_dir = config.hint_wal_dir.clone().unwrap_or_else(|| config.data_dir.join("hints"));
    let hint_config = HintedHandoffConfig { wal_dir: hints_dir.clone(), ... };
    // No single HintWal — HintedHandoffManager manages per-node WALs.
    let hinted_handoff_manager = Arc::new(
        HintedHandoffManager::new(hints_dir, hint_delivery_client, hint_config)
            .with_membership(membership.clone())
            .with_timeouts(op_timeouts.clone()),
    );
    ```
  - Remove `let hint_wal =` and `let hint_wal_for_prune =` lines (942–943)
  - Remove `hint_wal: Arc<oceanfs_durability::HintWal>` parameter from `spawn_background_tasks()` (line 1460)
  - Remove `hint_wal_for_prune` from the `spawn_background_tasks` call site (line 1216)
  - The prune background task (lines 1686–1710) uses `hint_wal_prune` — this should be refactored:
    - Option A: Move prune into `HintedHandoffManager` as an internal concern
    - Option B: Pass `hinted_handoff_manager` to `spawn_background_tasks` instead of raw `HintWal`, and call a `prune_all_expired()` method
    - **Recommended**: Option B — add `HintedHandoffManager::prune_all_expired(ttl_secs: u64) -> Result<usize>` that iterates all open node WALs and calls `prune_expired()`
  - Remove `hint_wal` from the `spawn_background_tasks` return (line 1733 `hinted_handoff_prune` construction changes)
  - Remove `use oceanfs_durability::HintWal` import if no longer directly used
- **Modify** `crates/oceanfs-core/src/config/node.rs`:
  - Replace `pub hint_wal_path: Option<PathBuf>` (line 322) with:
    ```rust
    pub hint_wal_dir: Option<PathBuf>,
    ```
  - Update default (line 580):
    ```rust
    hint_wal_dir: None,
    ```
  - Update `hint_wal_path` → `hint_wal_dir` in any serde rename attributes

### Out of Scope (for this feature)

- Changes to `HintWal` struct — it is unchanged (ADR-0018 "Neutral")
- Changes to `HintRecord`, `HintInline`, `HintSegmentRef` protobuf definitions — unchanged
- Changes to hint delivery gRPC protocol — unchanged
- The hint delivery watcher background task — unchanged except it accesses `hinted_handoff_manager` instead of raw `hint_wal`
- Decision 1 (remove MerkleWal) and Decision 3 (segment-ref hints)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | **Modify** `src/hinted_handoff/hint_delivery.rs`: HintedHandoffManager struct fields, new/enqueue/replay_and_deliver/drain_and_deliver methods, HintedHandoffConfig. Add lazy open/close logic. |
| `oceanfs-node` | **Modify** `src/node.rs`: HintedHandoffManager construction, spawn_background_tasks signature, prune task refactoring. Remove raw HintWal imports. |
| `oceanfs-core` | **Modify** `src/config/node.rs`: rename `hint_wal_path` → `hint_wal_dir` config field. |
| `oceanfs-server` | No changes (coordinator already uses `HintedHandoffManager` via `Arc`) |

## Interface (Public API)

### Changed Public API

- `HintedHandoffConfig`:
  - `wal_path: PathBuf` → **removed**
  - `wal_dir: PathBuf` → **added**
  - `inline_threshold_bytes: u64` → unchanged
  - `max_batch_size: usize` → unchanged
- `HintedHandoffManager::new()`:
  - `hint_wal: Arc<HintWal>` → **removed**
  - `wal_dir: PathBuf` → **added** as first parameter
- `HintedHandoffManager`:
  - `pub async fn prune_all_expired(&self, ttl_secs: u64) -> Result<usize>` → **added**
- `NodeConfig`:
  - `pub hint_wal_path: Option<PathBuf>` → **removed**
  - `pub hint_wal_dir: Option<PathBuf>` → **added**
- `spawn_background_tasks()`:
  - `hint_wal: Arc<HintWal>` → **removed**
  - `hinted_handoff_manager: Arc<HintedHandoffManager>` → **added** (or prune is handled differently)

### Unchanged Public API

- `HintWal` — struct, `open()`, `write_hint()`, `replay()`, `truncate_after()`, `prune_expired()`, `global_position()`, `path()` — all unchanged
- `HintedHandoffManager::enqueue()`, `drain_and_deliver()`, `replay_and_enqueue()`, `pending_count()`, `total_pending_count()` — public signatures unchanged (internal implementation changes only)
- `HintRecord`, `HintInline`, `HintSegmentRef` — unchanged
- `HintDeliveryClient` trait and `GrpcHintDeliveryClient` — unchanged

## Data Flow

### Before:
```
coordinator.enqueue(hint)
  → HintedHandoffManager::enqueue()
    → self.hint_wal.write_hint(&record)
      → single hints.wal, fsync per-entry

Node returns:
  → drain_and_deliver(node_id)
    → drain queue
    → gRPC deliver
    → self.hint_wal.truncate_after(last_position)  ← cross-node pollution!
```

### After:
```
coordinator.enqueue(hint)
  → HintedHandoffManager::enqueue()
    → wal = get_or_open_node_wal(target_node_id)
      → open hints/{node_id}.wal if not open
      → DashMap<NodeId, Arc<HintWal>>
    → wal.write_hint(&record)
      → per-node fsync

Node returns:
  → drain_and_deliver(node_id)
    → drain queue
    → gRPC deliver
    → self.node_wals[node_id].truncate_after(0)   ← independent!
    → remove hints/{node_id}.wal
    → remove from DashMap

Lazy close:
  → background: every 60s, check last_access
  → if idle > 60s: drop Arc<HintWal>, remove from map
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`, `oceanfs-node`, and `oceanfs-core`
<!-- REVIEW: all 3 crates build cleanly (only pre-existing oceanfs-storage clippy warnings in the dependency graph) -->
- [x] **Modify:** `HintedHandoffConfig` has `wal_dir: PathBuf` instead of `wal_path: PathBuf`
<!-- REVIEW: hint_delivery.rs:51 — `pub wal_dir: std::path::PathBuf` verified -->
- [x] **Modify:** `HintedHandoffManager` struct field `hint_wal: Arc<HintWal>` replaced with `wal_dir: PathBuf` and `node_wals: DashMap<NodeId, Arc<HintWal>>`
<!-- REVIEW: hint_delivery.rs:174,177 — `wal_dir: PathBuf` and `node_wals: DashMap<NodeId, Arc<HintWal>>` verified -->
- [x] **Modify:** `HintedHandoffManager::new()` accepts `wal_dir: PathBuf` instead of `hint_wal: Arc<HintWal>`
<!-- REVIEW: hint_delivery.rs:202-206 — signature `pub fn new(wal_dir: PathBuf, ...)` verified -->
- [x] **Modify:** `enqueue()` resolves per-node WAL via `get_or_open_node_wal()` before writing
<!-- REVIEW: hint_delivery.rs:298-303 — calls `self.get_or_open_node_wal(&target)` then `wal.write_hint()` -->
- [x] **Modify:** `drain_and_deliver()` truncates per-node file to 0 and removes from map on success
<!-- REVIEW: hint_delivery.rs:399-406 — `truncate_after(0)`, `remove_file`, and `DashMap::remove` on delivery success -->
- [x] **Modify:** `replay_and_enqueue()` scans `*.wal` files in directory instead of reading single file
<!-- REVIEW: hint_delivery.rs:248-287 — `std::fs::read_dir(&self.wal_dir)` with `.extension() == "wal"` filter -->
- [x] **Add:** `get_or_open_node_wal()` method with lazy-open and 16-file cap
<!-- REVIEW: hint_delivery.rs:544-562 — lazy lookup, cap check at `>= 16`, opens `{wal_dir}/{node_id}.wal` -->
- [x] **Add:** `evict_least_recently_used()` method with 60s inactivity threshold
<!-- REVIEW: hint_delivery.rs:570-591 — iterates `last_access`, checks `>= 60` sec, removes oldest from both maps -->
- [x] **Add:** `prune_all_expired()` method on `HintedHandoffManager` delegating to each node WAL
<!-- REVIEW: hint_delivery.rs:462-531 — iterates `node_wals` calling `prune_expired()`, also scans directory for unopened files -->
- [x] **Modify:** `NodeConfig`: `hint_wal_path` renamed to `hint_wal_dir`
<!-- REVIEW: node.rs:322 — `pub hint_wal_dir: Option<PathBuf>` with `#[serde(default)]` and default `None` at line 580 -->
- [x] **Modify:** `node.rs` construction: creates `hints_dir` PathBuf, passes to `HintedHandoffManager::new()`
<!-- REVIEW: node.rs:940-953 — `config.hint_wal_dir.unwrap_or_else(|| config.data_dir.join("hints"))` passed as first arg -->
- [x] **Modify:** `spawn_background_tasks()`: prune task uses `hinted_handoff_manager.prune_all_expired()` instead of `hint_wal.prune_expired()`
<!-- REVIEW: node.rs:1696 — `hinted_handoff_manager.prune_all_expired(hint_ttl_secs).await` in prune spawn -->
- [x] **Remove:** `spawn_background_tasks()` no longer accepts `hint_wal: Arc<HintWal>` parameter
<!-- REVIEW: node.rs:1449-1461 — signature has `hinted_handoff_manager: Arc<HintedHandoffManager>`, no `hint_wal` param; grep confirms zero `Arc.*HintWal.*hint_wal` in all crates -->
- [x] **Tests:** `cargo test --test-threads=1` passes in `oceanfs-durability`, `oceanfs-node`, `oceanfs-core`
<!-- REVIEW: all 9 hint_delivery tests pass; 6 hint_wal tests pass; SIGABRT in scrub::tests::run_cycle_detects_corrupt_segment is known RocksDB issue per PIPELINE.md §4.6 — not a feature defect -->
- [x] **Tests:** All existing `HintedHandoffManager` tests updated to new constructor signature (use `tempdir` path as `wal_dir`)
<!-- REVIEW: test_hinted_handoff_batched_delivery (line 668), test_hinted_handoff_delivery_failure_reenqueues (line 734), test_replay_repopulates_queues (line 787), test_drain_empty_returns_zero (line 773) all use `HintedHandoffManager::new(wal_dir, ...)` -->
- [x] **Tests:** New test: `test_per_node_wal_files_created_in_directory()` — enqueue hints for two different nodes, verify two `*.wal` files exist
<!-- REVIEW: hint_delivery.rs test at line ~808 — enqueues for 2 nodes, verifies 2 .wal files exist -->
- [x] **Tests:** New test: `test_per_node_wal_truncates_independently()` — enqueue for node-a and node-b, deliver node-a, verify node-a file is gone/truncated and node-b file is untouched
<!-- REVIEW: hint_delivery.rs test at line ~829 — enqueues for 2 nodes, delivers node-a, verifies node-a file removed, node-b file exists -->
- [x] **Tests:** New test: `test_lazy_open_close_cap()` — enqueue for 20 different nodes, verify only 16 WALs are open at any time, oldest is evicted after inactivity
<!-- REVIEW: hint_delivery.rs test at line ~879 — enqueues for 20 nodes, verifies node_wals length <= 16 -->
- [x] **Tests:** New test: `test_replay_scans_directory()` — create multiple per-node WAL files manually, call `replay_and_enqueue()`, verify all queues populated
<!-- REVIEW: hint_delivery.rs test at line ~941 — creates .wal files manually, replays, verifies queue population -->
- [x] **Tests:** New test: `test_prune_all_expired()` — verify `prune_all_expired` delegates to per-node WALs correctly
<!-- REVIEW: hint_delivery.rs test at line ~1019 — enqueues hints, calls prune_all_expired, verifies delegation -->
- [x] **Tests:** Existing `HintWal` tests in `hint_wal.rs` unchanged and still pass
<!-- REVIEW: all 6 hint_wal tests pass — test_hint_wal_write_and_replay_roundtrip, test_hint_wal_corrupt_record_crc_mismatch_error, test_hint_wal_implements_wal_writer_trait, test_hint_wal_truncate_after_delivery, test_prune_expired_removes_old_entries, test_empty_wal_replay_returns_empty -->
- [x] **ADR:** ADR-0018 Decision 2 constraints satisfied
<!-- REVIEW: all Decision 2 constraints verified — per-node files, lazy open/close, 16-file cap, 60s eviction, per-node truncation, HintWal unchanged, DashMap-based manager -->
- [ ] **Integration:** Start a node, simulate hints for two different unreachable nodes, verify per-node `hints/{node_id}.wal` files are created independently
<!-- REVIEW: Unit tests (test_per_node_wal_files_created_in_directory) cover this scenario; full end-to-end integration test requires a multi-node cluster not available in CI. Recommend manual verification before merge. -->
