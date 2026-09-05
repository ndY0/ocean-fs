---
feature: "c4: Extract NetworkModule Builder"
epic: "refactoring/composition-root-decomposition"
status: done
priority: medium
owner: ""
dependencies:
  - feature: c3-split-server-builder
    epic: refactoring/composition-root-decomposition
    reason: DataPlaneModule::serve binds the router + DataPlaneServices built by c3; the membership-plane bind and join must follow that bind (bind-before-join)
adr:
  - 0028-membership-plane-full-swim-gossip
perf: []
created: 2026-09-04
updated: 2026-09-05
---

# c4: Split Network Wiring into Membership + Data-Plane Modules

## Summary

Extract the node-side wiring that a single `NetworkModule` was originally
planned to own into **two modules split along the ADR-0028 wire
architecture (planes)**: `modules/membership.rs` (`MembershipModule` — the
membership plane) and `modules/data_plane.rs` (`DataPlaneModule` — the
data plane). The `modules/network.rs` name is **retired**. This is still
**one c4 feature implemented in one pass** — there is no c4a/c4b split
(user-approved amendment, 2026-09-05; see Implementation Notes).

`MembershipModule` is the membership plane. Its early
`build(&NodeConfig, ring_cache) -> Result<Self, String>` owns (node.rs
§4/4a/4b/5, membership parts): strict address parsing — `grpc_listen_addr`
(already strict) and `membership_listen_addr`, whose silent
`unwrap_or_else(0.0.0.0:9002)` fallback becomes a hard parse error (review
#64 closure) — the membership announce-address derivation (ADR-0028 D1),
`Membership::new`, the rejoin state store (`MembershipStateStore` + load +
incarnation bump + write-through persist, ADR-0022), `ManifestCache`, and
the membership-plane connection pool
(`oceanfs_membership::plane::membership_pool`) + `membership.set_pool`.
Its late `async start_plane_and_join(&self, metrics: Arc<MetricsRegistry>,
registry: &PoolRegistry) -> Result<(), String>` owns (node.rs
§15b/15c/15d/15e): gossip/probe service construction (re-seated from
`modules/server.rs`), the membership-plane listener bind + tonic serve
spawn, `membership.start()`, membership metrics registration (must stay
after `start()`), manifest declaration + routing-cache self-seed
(ADR-0029 D2), the routing-cache event subscriber, and join + background
rejoin + post-join fallback-seed snapshot. `cluster_ready_gate_opens`
moves here as `pub(crate)` (shared with node.rs's §11 ready-gate task and
tests).

`DataPlaneModule` is the data plane. Its early `build(&NodeConfig) ->
Result<Self, String>` owns (node.rs §5): data-plane `ConnectionPool`
construction + `RpcConfig::default()` + quickack/busy_poll (the
`[review][config][high]` "no rpc config from config is operational" marker
travels here) + the strict `grpc_listen_addr` parse for the bind. Its late
`async serve(&self, router: axum::Router, grpc: DataPlaneServices) ->
Result<BoundDataPlane, String>` owns (node.rs §14/§15): HTTP listener bind
+ axum serve spawn (hard-fail on bind error as today), the data-plane
tonic router assembly (the 4-line `.add_service` chain over `server.grpc`
fields) + `grpc_shutdown` token + serve spawn (reuseport-listener
soft-failure inside the task, as today), returning
`BoundDataPlane { server_addr, grpc_addr, http_shutdown, grpc_shutdown,
grpc_server_handle }`.

The c3 `ServerModule` loses its `membership_pool` build param and its
`gossip_service`/`probe_service` fields/construction (recorded as a
deviation on the c3 doc). `Node` keeps its flat fields; the modules are
NOT stored on `Node`.

## Target structure

```rust
// modules/membership.rs — the membership plane (ADR-0028)
pub(crate) struct MembershipModule {
    pub membership: Arc<oceanfs_membership::Membership>,
    pub manifest_cache: Arc<crate::routing_cache::ManifestCache>,
    // node.rs §17 watcher clones it
    pub membership_state_store: MembershipStateStore,
    pub announce_incarnation: u64,
    pub is_cluster_node: bool,
    pub grpc_addr: SocketAddr, // §11 hinted-handoff self-address consumer
    // membership_pool is PRIVATE: wired via membership.set_pool in build()
}

impl MembershipModule {
    pub fn build(config: &NodeConfig, ring_cache: Arc<RingCache>)
        -> Result<Self, String>;

    pub async fn start_plane_and_join(
        &self,
        metrics: Arc<MetricsRegistry>,
        registry: &PoolRegistry,
    ) -> Result<(), String>;

    // relocated from node.rs; pub(crate) because node.rs's §11 ready-gate
    // task and the gate tests share it
    pub(crate) fn cluster_ready_gate_opens(
        ring_nodes: usize,
        min_quorum_nodes: u64,
        deadline_elapsed: bool,
    ) -> bool;
}

// modules/data_plane.rs — the data plane
pub(crate) struct DataPlaneModule {
    pub pool: Arc<oceanfs_network::ConnectionPool>,
    pub grpc_addr: SocketAddr, // strict parse; used for the bind
}

impl DataPlaneModule {
    pub fn build(config: &NodeConfig) -> Result<Self, String>;

    pub async fn serve(
        &self,
        router: axum::Router,
        grpc: crate::modules::server::DataPlaneServices,
    ) -> Result<BoundDataPlane, String>;
}

pub(crate) struct BoundDataPlane {
    pub server_addr: SocketAddr,          // bound HTTP address
    pub grpc_addr: SocketAddr,            // bound gRPC address
    pub http_shutdown: CancellationToken, // axum graceful shutdown
    pub grpc_shutdown: CancellationToken, // tonic graceful shutdown
    pub grpc_server_handle: JoinHandle<()>,
}
```

## Scope

### In Scope

- **`modules/membership.rs` — `MembershipModule::build(&NodeConfig,
  ring_cache)`** (node.rs §4/4a/4b/5 membership parts):
  - STRICT address parsing: `grpc_listen_addr` (already strict) and
    `membership_listen_addr` — its silent `unwrap_or_else(0.0.0.0:9002)`
    fallback becomes a hard parse error (review #64 closure: no silent
    default network addresses at startup).
  - Membership announce-address derivation (ADR-0028 D1: the membership
    plane's listen address with the data-plane's advertised IP substituted
    for `0.0.0.0`).
  - `Membership::new` and the rejoin state store (`MembershipStateStore`:
    load persisted state + incarnation bump + write-through persist,
    ADR-0022).
  - `ManifestCache` (peer-side routing cache, ADR-0029 §D5).
  - Membership-plane connection pool (`oceanfs_membership::plane::membership_pool`)
    + `membership.set_pool` (pool stays a private field).
- **`modules/membership.rs` — `MembershipModule::start_plane_and_join`**
  (node.rs §15b/15c/15d/15e + join/bootstrap):
  - gossip_service + probe_service **construction** — re-seated from
    `modules/server.rs`; they wrap only membership-plane inputs
    (membership, membership_pool, node_id, `gossip.failure_timeout_ms`).
  - Membership-plane listener bind + tonic serve spawn. Bind semantics
    identical to the former node.rs §15b: the LISTENER bind hard-fails
    startup (logged + returned from `start_plane_and_join`); serve errors
    are logged inside the spawned task.
  - `membership.start()` and membership metrics registration (registration
    must stay after `start()` — the gossip series is created inside it).
  - Manifest declaration + routing-cache self-seed (ADR-0029 D2).
  - Routing-cache event subscriber.
  - Join + background rejoin + post-join fallback-seed snapshot
    (persistence semantics identical to today).
  - `cluster_ready_gate_opens` as `pub(crate)` — shared with node.rs's §11
    ready-gate task and the gate tests.
- **`modules/data_plane.rs` — `DataPlaneModule::build(&NodeConfig)`**
  (node.rs §5):
  - Data-plane `ConnectionPool` construction + `RpcConfig::default()` +
    quickack/busy_poll. The `[review][config][high]` "no rpc config from
    config is operational" marker travels here.
  - Strict `grpc_listen_addr` parse for the bind.
- **`modules/data_plane.rs` — `DataPlaneModule::serve`** (node.rs
  §14/§15):
  - HTTP listener bind + axum serve spawn (hard-fail on bind error, as
    today).
  - Data-plane tonic router assembly — the 4-line `.add_service` chain
    over `server.grpc` fields (segment/healing/cache/scrub) — +
    `grpc_shutdown` token + serve spawn (reuseport-listener soft-failure
    inside the task, as today).
  - Returns `BoundDataPlane { server_addr, grpc_addr, http_shutdown,
    grpc_shutdown, grpc_server_handle }`; node.rs §16/§17 wiring consumes
    the returned tokens/handles.
- **c3 adjustment** (recorded as a deviation on the c3 doc; c3's status
  stays `done` — a deliberate follow-up correction made by c4 for
  ownership reasons): `ServerModule` loses its `membership_pool` build
  param and its `gossip_service`/`probe_service` fields/construction; it
  keeps `router`, `DataPlaneServices` (segment/healing/cache/scrub), and
  `prefetch_engine`.
- **node.rs** (this feature's node-side half): rewire `start()` to the
  two-call module sequence below; §11/§16/§17 consumers switch to the
  module fields and returned handles. `Node` keeps its flat fields
  (`membership` Arc, `http_shutdown`, `grpc_shutdown`, `background`, …).

### Out of Scope

- gRPC service **construction** for segment/healing/cache/scrub stays in
  `modules/server.rs` (c3) — c4 only binds. (gossip/probe are the
  exception: their construction re-seats INTO the membership module, per
  the approved design.)
- Background-task spawns (node.rs §16, §16b–e, §17, the process/metric
  poller) — c5.
- The hinted-handoff manager and the §11 ready-gate **task** stay in
  node.rs §11 (they consume module fields: `membership`, `pool`,
  `grpc_addr`, `is_cluster_node`); only the `cluster_ready_gate_opens`
  helper relocates.
- `Node` struct restructure — modules are not stored on `Node`.
- Any behavior change outside the pure move (same regression bar as c1–c3:
  "node boots, e2e write/read passes, existing node tests pass").

## Data Flow — node.rs after c4

```
infra (§0–3)
  → MembershipModule::build(&config, ring_cache)        // membership plane, early
  → DataPlaneModule::build(&config)                     // data plane, early
  → storage + durability builds + §7b recovery
  → §11 hinted-handoff manager + ready-gate task        // consumes module fields
  → ServerModule::build (c3 call — minus membership_pool;
                          gossip/probe no longer returned)
  → data_plane.serve(server.router, server.grpc)        // HTTP + gRPC data-plane binds
  → membership.start_plane_and_join(metrics, &storage.registry)
                                                        // membership-plane bind + join
  → §16 / §16b–e / §17 → Node
```

The **bind-before-join** ordering invariant (peers probe and push to the
data-plane listeners immediately after the join announcement — t5/t21) is
preserved as a documented two-call sequence: `data_plane.serve(...)` must
complete before `membership.start_plane_and_join(...)`. The module methods
are called in that order from `Node::start()`; each module performs its
own binds and spawns.

## Definition of Done

- [x] `modules/membership.rs` (MembershipModule) and `modules/data_plane.rs`
      (DataPlaneModule) exist; the §4/4a/4b/5/14/15/15b–15e wiring is
      extracted into them — one c4 pass, no c4a/c4b split.
<!-- REVIEW: verified — modules/membership.rs (474 ln) + modules/data_plane.rs (183 ln) exist; node.rs §4-5 and §14-15 replaced by the two module builds and the two-call sequence; node.rs 2937→2606; only the documented hunks changed (git diff), no renumbering of remaining §6/7/7b/11/16/16b-e/17/shutdown markers. -->
- [x] HTTP/gRPC data-plane binds + serves spawn from
      `DataPlaneModule::serve`; membership-plane bind + serve, gossip/probe
      construction, join/bootstrap, manifest declaration, and the
      routing-cache subscriber spawn from
      `MembershipModule::start_plane_and_join`.
<!-- REVIEW: verified — data_plane.rs:107-182 (HTTP bind hard-fail; 4-line .add_service over DataPlaneServices; grpc_shutdown token; reuseport soft-fail inside task; BoundDataPlane); membership.rs:246-454 (gossip/probe construction, plane bind hard-fail then serve spawn, membership.start() hard-fail, metrics AFTER start, manifest + self-seed, subscriber, join soft-fail, rejoin loop, no-wipe snapshot); node.rs:561-562 order = serve → start_plane_and_join. -->
- [x] Membership join/rejoin/fallback-seed persistence behavior identical
      (write-through incarnation bump before announce; post-join
      fallback-seed snapshot that never wipes a last-known list).
<!-- REVIEW: verified — code moved byte-identical modulo &self/field access (membership.rs:141-164 bump+persist before announce; 435-451 no-wipe snapshot); e2e crash_restart + wal_recovery + cluster_lifecycle re-run green. -->
- [x] Review #64 closed: no silent default network addresses —
      `membership_listen_addr` parse is now strict (no
      `unwrap_or_else(0.0.0.0:9002)` fallback).
<!-- REVIEW: verified — membership.rs:105-108 hard parse error "invalid membership_listen_addr"; grep confirms no unwrap_or_else fallback remains in the node crate; oceanfs-core default (0.0.0.0:9002) untouched per Implementation Notes. -->
- [x] gossip/probe services re-seated into `MembershipModule`
      (membership-plane inputs only); `ServerModule`'s `membership_pool`
      build param and `gossip_service`/`probe_service` fields removed.
<!-- REVIEW: code verified (server.rs fields/param/construction removed; membership.rs:254-262 sole construction site; node.rs server-build call lost only the membership_pool arg). Doc gap: the ServerModule struct-level doc comment at modules/server.rs:36-41 still lists "gossip_service/probe_service feed the membership-plane bind (§15b)" as if fields existed — contradicts the NOTE at server.rs:56-62 and c3 deviation #9. Fix: drop the gossip/probe clause from the struct doc. — RESOLVED before landing (implementer, 2026-09-05): the struct doc at modules/server.rs:41-43 now records the re-seat. -->
- [x] `cluster_ready_gate_opens` relocated to the membership module as
      `pub(crate)`; node.rs §11 ready-gate task and the gate tests compile
      against it.
<!-- REVIEW: verified — membership.rs:468-474 pub(crate) fn; node.rs:29 imports it; ready-gate task node.rs:429 and gate tests node.rs:2100-2115 compile+pass (66 lib tests). -->
- [x] `cargo build --all-targets` succeeds; rustdoc/clippy rules pass per
      the epic bar.
<!-- REVIEW: verified — clean rebuild of oceanfs-node from `cargo clean -p oceanfs-node`; workspace `cargo build --all-targets` Finished; `cargo clippy -p oceanfs-node --lib -- -D warnings` clean; RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p oceanfs-node clean; cargo fmt --check clean. (Pre-existing, non-blocking: unused-import ObjectMetadata warning in node.rs cfg(test) at HEAD too; one warning in oceanfs-durability lib test — both outside this diff.) -->
- [x] Node tests green; multi-node membership tests green; e2e write/read
      green on the sanctioned allowlist (no load suites locally —
      PIPELINE.md §6).
<!-- REVIEW: node lib 66 passed; all node integration suites green (30 test binaries, --test-threads=1); doc tests 38 passed. E2e allowlist re-verified by reviewer: crash_restart (4), cluster_lifecycle (1), cluster_write_path (5), cluster_read_path (6), wal_recovery (1), segment_lifecycle (1) — all green on a freshly rebuilt binary. garbage_collection + rewrite_leak_test green pre-review (implementer), not independently re-run; they exercise c1/c2 wiring untouched by this diff. NO load suites run (PIPELINE.md §6). -->
<!-- REVIEW: 2026-09-05 reviewer iteration 1 — verdict PASS. Single LOW gap: stale ServerModule struct doc (server.rs:36-41); optional 1-line doc fix before landing. — LOW fix applied by the implementer before landing; see Implementation Notes / Accepted Deviations (a). -->

## Implementation Notes / Accepted Deviations

- **Amendment record (user-approved 2026-09-05):** the original plan for
  this feature extracted a single `NetworkModule` owning membership +
  pools + HTTP/gRPC/membership-plane binds + bootstrap. The approved
  redesign splits the node-side wiring **along the ADR-0028 planes** into
  `MembershipModule` + `DataPlaneModule`, still implemented as **one c4
  feature — one pass, no c4a/c4b split**. `modules/network.rs` is retired.
  This document was rewritten to the approved shape; at amendment time
  `status` stayed `proposed` (c4 was not yet implemented) — it flipped to
  `done` at landing on 2026-09-05 (see the Landing record below). The
  companion c3 deviation entry and the epic README record the same
  amendment.
- **Review #64 (B2) phrasing.** Wave-0/1 f1 closed the B2 instance that
  silently fell back to `127.0.0.1:9001` for the hint-fetch self-address
  (2026-09-04). The `membership_listen_addr`
  `unwrap_or_else(0.0.0.0:9002)` fallback in node.rs §4 is the remaining
  silent-default instance; this feature's strict parse closes it. (The
  config default for `membership_listen_addr` is `"0.0.0.0:9002"` — a
  valid address — so the strict parse only hard-errors on hand-supplied
  garbage, which today silently falls back.)
- **Socket treatment on the membership-plane bind.** Today both binds
  apply quickack/busy-poll to accepted sockets (perf guideline 4.3). The
  approved seating puts `RpcConfig::default()` + quickack/busy_poll in
  `DataPlaneModule::build`; the membership-plane serve spawn inside
  `start_plane_and_join` must keep its current fd treatment. Confirm the
  value source in review (e.g. the membership module reading its own
  `RpcConfig::default()` — a `Default` struct — keeps behavior identical).
- Frontmatter note: the `feature:` identity fields (`feature`, `epic`,
  `adr`, `created`) are unchanged by the amendment; only the `updated`
  date and the dependency reason moved. Cross-references (DAG, c5) use the
  file slug `c4-split-network-builder` and are unaffected.

### Landing record (2026-09-05) — status flipped to `done`

c4 was implemented in **one pass** and the independent review returned
**PASS on iteration 1 — 0 blocking gaps, 1 LOW item** (a stale doc
comment, fixed by the implementer before landing). The Scope and DoD
above are unchanged by landing; this record documents the landed shape
and the reviewer-verified deltas against it, following the c2/c3
precedent. All DoD boxes carry `[x]` + REVIEW verification comments.

- **(a) Reviewer verdict.** Review iteration 1 returned PASS with a
  single LOW gap: the `ServerModule` struct-level doc comment in
  `modules/server.rs` still listed `gossip_service`/`probe_service` as
  if the c3 fields existed (contradicting the module's own NOTE and c3
  deviation #9). The implementer applied the 1-line fix before landing —
  the struct doc at `modules/server.rs:41-43` now records the re-seat —
  and re-verified the doc path. 0 blocking gaps.
- **(b) Approved deltas applied as scoped.** Review #64 is closed: the
  `membership_listen_addr` parse in `MembershipModule::build` is strict
  (`membership.rs:105-108` — hard error, no silent `0.0.0.0:9002`
  fallback; grep confirms no `unwrap_or_else` fallback remains in the
  node crate; the `oceanfs-core` config default `"0.0.0.0:9002"` is a
  valid address and is untouched). The gossip/probe re-seat out of
  `ServerModule` (c3 deviation #9) landed as approved:
  `membership.rs:254-262` is the sole construction site, wrapping only
  membership-plane inputs (membership, membership_pool, node_id,
  `gossip.failure_timeout_ms`); `ServerModule` lost the
  `membership_pool` build param and now carries only `router`,
  `DataPlaneServices` (`grpc`), and `prefetch_engine`.
- **(c) Bind failure semantics preserved — identical to the former
  node.rs §15b.** The membership-plane LISTENER bind hard-fails startup:
  `create_reuseport_listener` failure is logged AND returned as an error
  from `start_plane_and_join` (`membership.rs:275-288`), failing
  `Node::start()`. Serve errors are logged inside the spawned task
  (`membership.rs:308-310`). The pre-implementation In-Scope phrasing
  "(soft-failure on bind error inside the task, as today)" conflated this
  bind with the data-plane gRPC reuseport listener, whose creation
  failure is soft *inside* its own serve task (`data_plane.rs:154-160`,
  as in the inline §15); the In-Scope sentence above is corrected to the
  landed semantics. The HTTP bind in `DataPlaneModule::serve` also
  hard-fails startup (`data_plane.rs:113-115`), as documented.
- **(d) `is_cluster_node` derived in `build`, not at the §11 ready
  gate.** The flag is computed inside `MembershipModule::build` from the
  durable state already loaded for the rejoin logic
  (`membership.rs:188-193`: `!seed_nodes.is_empty() ||
  !fallback_seeds.is_empty()`) — equivalent to the former §11 second
  store load: nothing writes the membership state store between
  `build`'s load and the §11 ready-gate task that consumes the flag.
- **(e) Modules are not stored on `Node`.** Flat `Node` fields are
  preserved (`membership` `Arc`, `server_addr`/`grpc_addr` +
  `http_shutdown`/`grpc_shutdown` from `BoundDataPlane`, `background`,
  …); liveness past `start()` is via the serve tasks and `Arc` clones
  handed to the §11/§16/§17 consumers, not via module structs stored on
  `Node`. The landed module structs carry a few more private fields than
  the plan sketch (e.g. `membership_pool`, `node_id`, `membership_addr`,
  `quickack`/`busy_poll`, `probe_timeout_ms` on `MembershipModule`) so
  the gossip/probe construction and the accepted-socket fd treatment stay
  module-local — no behavior delta.
- **Landing evidence:** `node.rs` 2937 → 2606 lines; `Node::start()`
  body ~1179 → ~864; `modules/membership.rs` ~474 lines;
  `modules/data_plane.rs` ~183 lines; `start()` calls
  `data_plane.serve(...)` then `membership.start_plane_and_join(...)`
  (`node.rs:561-562` — bind-before-join preserved). Verification: node
  lib 66, doc 38, all 30 integration suites, clippy `-D warnings`,
  rustdoc, fmt clean; e2e allowlist green (crash_restart, wal_recovery,
  segment_lifecycle, cluster_lifecycle, cluster_write_path,
  cluster_read_path, garbage_collection, rewrite_leak_test — 6/8
  independently re-run by the reviewer); no load suites (PIPELINE.md
  §6). The §4/§5/§14/§15/§15b–e section markers were removed by the
  extraction without renumbering the surviving sections (done once in
  c5).
