# ROADMAP Evaluation

**Evaluator:** Brainstorm Agent (Architect)
**Date:** 2026-08-05
**Source:** `ROADMAP.md`
**Method:** Full spec + ADR + feature-doc + code-graph structural analysis

---

## Summary Judgment Table

| # | Roadmap Item | Soundness | Effort | Desirability | Realistic | Category |
|---|---|---|---|---|---|---|
| 3 | Code audit (concision, smells, coupling) | ⭐⭐⭐⭐⭐ | Low | High | Very | **Operational** |
| 4 | Hot-path performance audit | ⭐⭐⭐⭐⭐ | Low-Med | Very High | Very | **Operational** |
| 5 | Transaction mechanism | ⭐⭐ | Enormous | Low | Not yet | **Overreach** |
| 6 | Encryption | ⭐⭐⭐⭐ | Medium | Very High | Yes | **Spec Gap** |
| 7 | Production backend store | ⭐⭐⭐ | Medium | High | Yes, but... | **Infrastructure** |
| 8 | Pluggable GC strategy | ⭐⭐⭐ | Medium | Medium | Yes, deferred | **Enhancement** |
| 9 | Critical path perf improvement | ⭐⭐⭐⭐ | Medium | Very High | Yes | **Operational** |
| 10 | Network optimizations | ⭐⭐⭐⭐⭐ | High | Very High | Yes, phased | **Architecture** |
| 11 | Security audit | ⭐⭐⭐⭐⭐ | Medium | Critical | Yes | **Security** |
| 12 | Pluggable tracing | ⭐⭐⭐⭐⭐ | Medium | Critical | Yes | **Observability** |
| 13 | User authentication | ⭐⭐⭐⭐ | Large | Very High | Yes, but large | **Security** |
| 14 | Event hooks | ⭐⭐⭐ | Medium | Medium | Yes, low priority | **Enhancement** |
| 15 | Cloud/platform optimizations | ⭐⭐⭐ | Medium-Large | High | Yes, phased | **Deployment** |
| 16 | Platform supervision | ⭐⭐⭐⭐ | Large | High | Yes, post-mvp | **Operations** |
| 17 | Stress test suite | ⭐⭐⭐⭐⭐ | Medium | Critical | Yes | **QA** |
| 18 | Complex scenarios / degraded mode tests | ⭐⭐⭐⭐⭐ | Medium-Large | Critical | Yes | **QA** |

---

## Detailed Evaluation

---

### #3 — Code audit for concision, code smells, code reuse, class graph, complexity

**Soundness: ⭐⭐⭐⭐⭐** — Exactly the right thing to do before declaring features "done." The code-graph index has 26,947 symbols indexed. The coupling hotspots show heavy concentration in `oceanfs-core::types` (e.g., `id::new` with 789 in-degree) which is expected, but also `oceanfs-storage::gc::default` (274 in-degree) which is suspicious for a module that should be self-contained.

**Effort: Low** — A structural audit already has tooling via `code-graph_get_coupling_hotspots`, `get_cross_module_boundary`, and `get_module_tree`. The auditor subagent can produce a report in one session.

**Desirability: Very High** — The audit report already exists at `docs/audit-report.md` (425 lines, covers Phase 1 storage engine at ~85-90%). Extending this to cover all crates would surface problems before they ossify.

**Honest Judgment: Very realistic and useful.** Should be done iteratively — one crate at a time. Start with the tightly-coupled crates (`oceanfs-server`, `oceanfs-storage`).

---

### #4 / #9 — Hot-path and critical-path performance audit

**Soundness: ⭐⭐⭐⭐⭐** — The performance guidelines (`guidelines/performance.md`, 1,029 lines) are comprehensive and actionable. Every rule has a "verify" clause. The audit simply checks compliance.

**Note: Items #4 and #9 are duplicates.** Both say "audit hot path / critical path performance." They should be merged.

**Effort: Low-Medium** — Audit the hot paths against the 14-rule review checklist in §14 of performance.md. This is grep + code review, not implementation. The biggest gap known from the audit report is `io_uring` (Perf rule 3.5) being unimplemented.

**Key hot paths to audit:**
- Write path: segment append → WAL → ack → EC encode (in-flight, not yet fully integrated)
- Read path: cache check → metadata lookup → shard fetch → decode → verify
- EC encode/decode inner loop (already uses rayon per rule 2.1)
- WAL group commit (rule 3.4 — audit report confirms ✅)
- Segment buffer sharding (rule 2.5 — audit report confirms ✅ after fix)

**Desirability: Very High** — Performance is a design goal (spec §1.1: "Maximize throughput"). The read-optimized and write-optimized profiles are already spec'd; the audit verifies they're achievable.

**Honest Judgment: Very realistic.**

---

### #5 — Transaction mechanism

**Soundness: ⭐⭐** — This is a major architectural addition that doesn't appear in the spec. The spec's consistency model (§7) is based on quorum writes + hinted handoff + anti-entropy — an **AP** system (in CAP terms) with tunable consistency per bucket. Adding transactions would require a fundamental shift: cross-object atomicity, two-phase commit or Paxos-style consensus, conflict serialization, and likely a WAL-per-transaction model.

**Effort: Enormous** — This would touch every crate: `oceanfs-storage` (transactional WAL), `oceanfs-server` (coordinator becomes a transaction manager), `oceanfs-routing` (transaction-aware routing), `oceanfs-core` (new transaction types). It would also require a new ADR and likely a spec amendment.

**Desirability: Low (for now)** — S3 itself doesn't offer transactions. The spec's target use case is blob storage, not a transactional database. If multi-object atomicity is needed, it's a separate product (or a layer on top). The spec's §16 doesn't even mention transactions.

**Honest Judgment: Not realistic in this codebase scope.** This is a "different product" feature. If pursued, it needs its own spec section, ADR, and feasibility study before any implementation. **Recommendation: punt to "Future Work / v2" with a brief ADR explaining why it's out of scope for now.**

---

### #6 — Encryption

**Soundness: ⭐⭐⭐⭐** — The spec §9.6.3 already addresses AES-GCM encryption with AES-NI acceleration and explicitly defers GPU encryption as non-critical ("the throughput bottleneck for a blob store is EC, not encryption"). The architecture is sound; it just needs implementation.

**Effort: Medium** — The `aes-gcm` crate handles the hard parts. The work is:
1. Per-bucket encryption key management (derive from bucket policy or external KMS).
2. Encrypt blob data before segment append (or after, at rest — spec doesn't decide).
3. Key rotation story.
4. Secure key storage (never in config files — environment/secrets management).

**Current state:** The code-graph shows no encryption symbols in any crate. This is a genuine gap in the implementation.

**Desirability: Very High** — Encryption at rest is table-stakes for any storage system targeting production use. The roadmap's phrasing ("dont forget about encryption !") is correct — it's been deferred too long.

**Honest Judgment: Realistic and urgent.** Should be a Phase 6 or 7 deliverable. An ADR on encryption key management (KMS integration, per-bucket vs. per-node keys) is needed before implementation.

---

### #7 — Real production backend store for production and load tests

**Soundness: ⭐⭐⭐** — Currently the system uses RocksDB as the metadata store. The "production backend store" likely refers to replacing or supplementing this for segment data storage (currently file-based). The spec §16 mentions "Tiered storage: Cold segments to S3/NFS/tape via pluggable storage backends" as future work.

**Effort: Medium** — The trait-based architecture already supports this. `oceanfs-storage-api` exists as a separate crate. Adding a new backend (e.g., direct NVMe LBA access, SPDK, or cloud blob storage) means implementing the existing traits. The bigger question is whether the current `SegmentStore` trait is the right abstraction — it might need refinement before a production backend.

**Desirability: High** — For production load tests, testing against real storage hardware (not a tmpfs) is essential.

**Honest Judgment: Realistic but premature.** The system needs the read/write integration complete first (Phase 4 is in-flight). A production backend is a Phase 8+ concern. **Recommendation: for now, focus on making the file-based backend production-grade (O_DIRECT, io_uring, proper fsync semantics) rather than adding a new backend.**

---

### #8 — Pluggable GC strategy (compaction speed vs. throughput)

**Soundness: ⭐⭐⭐** — The spec §10 already defines a liveness-ratio-based GC with configurable `gc_compact_threshold` and `gc_interval_sec`. Offering a strategy choice (e.g., "aggressive compaction" vs "lazy cleanup") is a natural extension.

**Effort: Medium** — The GC module already exists at `oceanfs-storage/src/gc.rs`. Adding a strategy enum is straightforward. The hard part is implementing the alternative strategies (e.g., time-based compaction, size-tiered compaction, concurrent compaction that doesn't block writes).

**Desirability: Medium** — Most deployments will use the default. Power users benefit. Not a launch-critical feature.

**Honest Judgment: Realistic as a post-Phase-7 enhancement.** The basic GC already needs to work first. Strategy selection adds a configuration knob that few operators will touch.

---

### #10 — Network optimizations

**Soundness: ⭐⭐⭐⭐⭐** — This is the most architecturally interesting item. The spec already addresses:
- Persistent gRPC connection pooling (§11.1)
- Batching (group commit for WAL, §3.1 of performance guidelines)
- The performance guidelines cover streaming gRPC (4.4), TCP_NODELAY (4.3), adaptive timeouts (4.5), HTTP/2 multiplexing (4.2)

What's missing and worth brainstorming:
- **Load/datacenter-aware routing:** The current ring is purely hash-based. A weighted variant that considers node load, latency, or DC proximity is an enhancement to `oceanfs-routing`.
- **Compression on the wire:** gRPC already supports compression (gzip, snappy). Enabling it per-channel is configuration.
- **Weighted routing with cyclic health checks:** An intelligent router that probes peer latency and adjusts weights dynamically. This is a non-trivial feature requiring a new component in `oceanfs-network`.

**Effort: High (if all items)** — Wire compression is configuration (low effort). DC-aware routing is medium. Weighted routing with health probes is high effort — it's a new subsystem with its own control loop.

**Desirability: Very High** — Network is the bottleneck in a distributed storage system. Every optimization here directly improves throughput and latency.

**Honest Judgment: Realistic, but should be phased.**
- **Phase 1 (low effort):** Enable gRPC compression, verify TCP_NODELAY is set everywhere, tune timeouts.
- **Phase 2 (medium):** DC-aware routing hint in `RingCache`.
- **Phase 3 (high):** Weighted routing with health probes. Needs its own ADR and feature doc.

---

### #11 — Security audit

**Soundness: ⭐⭐⭐⭐⭐** — A security audit is mandatory for any system handling user data. This should cover:
- TLS configuration (mTLS for node-to-node, TLS for client API)
- Authentication bypass vectors
- Input validation (protobuf deserialization, HTTP request parsing)
- `unsafe` block audit (by spec: `oceanfs-accel`, `oceanfs-hash`, `oceanfs-ec` only — verify no others)
- Secret management (keys in memory, logging of sensitive data)
- DoS vectors (unbounded channels — perf rule 2.6 says bounded, verify)

**Effort: Medium** — Primarily review work. The architecture guidelines §7.3 already require `#![forbid(unsafe_code)]` in most crates. The audit checks compliance.

**Desirability: Critical** — No production deployment without a security audit.

**Honest Judgment: Realistic and essential.** Should be done before any "production-ready" claim. Engage someone external if possible — self-audits miss things.

---

### #12 — Pluggable tracing with agnostic backend

**Soundness: ⭐⭐⭐⭐⭐** — The spec §9.8.2 already describes `tracing` spans. The ecosystem has `opentelemetry-rust` for the pluggable backend. This is well-trodden ground.

**Effort: Medium** — The `tracing` crate is already in use (per spec). Adding `opentelemetry` integration means:
1. A tracing layer that exports spans to OTLP (OpenTelemetry Protocol).
2. Configuration for the exporter endpoint (`otlp_endpoint`).
3. Feature gates for different backends (Jaeger, Zipkin, Datadog, etc.).

**Current state:** The spec's config has `[logging]` but no `[tracing]` section. The spec §16 mentions "Distributed tracing (OpenTelemetry) for end-to-end request flows."

**Desirability: Critical** — Observability is non-negotiable for distributed systems. Tracing is the minimum viable observability (logs + traces, metrics are already spec'd at §9.8.1).

**Honest Judgment: Realistic and high-priority.** Should be in Phase 5 or 6 — before the system goes multi-node. Debugging distributed write/read paths without tracing is miserable.

---

### #13 — User authentication (scopes, per bucket, mechanisms)

**Soundness: ⭐⭐⭐⭐** — The spec §12.1 mentions "Standard S3 authentication (AWS Signature V4, configurable) plus optional mTLS." The spec §16 asks "Auth model: Multi-tenancy (IAM-style policies)?" This roadmap item fills that gap.

**Effort: Large** — S3 auth involves:
1. AWS Signature V4 implementation (or integration with an existing crate).
2. Per-bucket access policies (read, write, list, admin).
3. User/credential management (where are credentials stored? RocksDB? External IAM?).
4. Token/session management.
5. mTLS certificate validation.

**Current state:** No auth symbols in the code-graph. The `oceanfs-server` has `bucket_config` and `admin` modules, but no auth.

**Desirability: Very High** — Without auth, the system is only usable in trusted network environments. Even internal deployments need service-level auth.

**Honest Judgment: Realistic but large.** This needs a dedicated epic (Phase 9?). At minimum:
- **MVP:** AWS Signature V4, config-level credentials (file-based secrets), per-bucket read/write policy.
- **Full:** IAM integration, JWT/OIDC, mTLS, fine-grained scopes (prefix-level permissions).

An ADR is needed to decide: integrate with an existing S3 auth crate, or implement from scratch? (Recommendation: use an existing crate — `aws-sigv4` or similar.)

---

### #14 — Event hooks on bucket/prefix/blob/system events

**Soundness: ⭐⭐⭐** — This is an S3 feature (S3 Event Notifications). The spec doesn't mention it. It would require:
1. An event system (pub/sub or webhook delivery).
2. Event types: `s3:ObjectCreated:*`, `s3:ObjectRemoved:*`, etc.
3. Delivery targets: HTTP webhooks, SQS, SNS, Kafka.
4. Filtering by prefix/suffix.

**Effort: Medium** — Not architecturally complex, but a new subsystem. The event hook needs to fire at the right points in the write/delete path without adding latency (fire-and-forget with a bounded channel).

**Desirability: Medium** — Important for production integrations (e.g., trigger a Lambda on upload), but not a launch blocker. The roadmap note "wich ones tbd" (which ones to be determined) confirms it's still fuzzy.

**Honest Judgment: Realistic as a post-GA feature.** Build the basic write/read path first. Event hooks are a natural Phase 9 feature. The spec should be updated to include event types before implementation.

---

### #15 — Cloud/platform optimizations (bare metal, VM, containers)

**Soundness: ⭐⭐⭐** — This is deployment-engineering territory, not systems architecture. The items break down as:
- **Bare metal:** NUMA-aware allocation, CPU pinning, huge pages, DPDK for networking (extreme).
- **VM:** Virtio optimizations, balloon driver coordination, disk I/O tuning (O_DIRECT matters more in VMs).
- **Containers:** Multi-container orchestration, health checks, config via env vars, graceful shutdown signals.

**Effort: Medium-Large** — These are mostly configuration + documentation + CI testing on different platforms, not new code. Except NUMA awareness and DPDK, which are significant engineering efforts.

**Desirability: High** — The deployment surface determines adoption. If it only runs well on bare metal with specific kernel versions, nobody will use it.

**Honest Judgment: Realistic but phased.**
- **Immediately:** Containerization (Dockerfile), env-var-based config overrides, graceful shutdown on SIGTERM. Low effort.
- **Post-GA:** Platform-specific tuning guides (VM disk config, bare-metal sysctl tweaks).
- **V2:** NUMA-aware memory allocation, kernel-bypass networking (extreme, unlikely to be needed).

---

### #16 — Platform supervision

**Soundness: ⭐⭐⭐⭐** — The spec already has `/admin/metrics` (Prometheus), `/admin/cluster`, `/admin/segments`, `/admin/caches`, `/admin/scrub`. "Platform supervision" means:
1. Aggregated cluster health dashboard.
2. Alerting rules (node down, disk full, heal backlog, scrub errors).
3. Integration with existing supervision stacks (Prometheus + Grafana, Datadog, etc.).

**Effort: Large** — Not because the code is hard, but because supervision is an ongoing operational practice. The code work is:
1. Comprehensive Prometheus metrics (many are spec'd at §9.8.1).
2. Grafana dashboard JSON (or equivalent).
3. Alertmanager rules.
4. Health check endpoints for Kubernetes/load balancers.
5. Documentation for operators.

**Desirability: High** — Distributed storage without supervision is a black box. Operators need visibility.

**Honest Judgment: Realistic, essential for production, but mostly non-code.** The metrics are already spec'd. The supervision "platform" is Prometheus + Grafana + Alertmanager, which are external to OceanFS. The work is: (1) ensure metrics are correct and useful, (2) provide dashboards and alerting rules, (3) document operations. **Recommendation: add a `deploy/observability/` directory with dashboards and alert rules.**

---

### #17 — Stress test suite

**Soundness: ⭐⭐⭐⭐⭐** — Critical for a storage system. A stress test suite exercises the system under load to find:
- Throughput ceilings (what's the max PUT/s? At what segment size?).
- Latency degradation under concurrency.
- Memory leaks under sustained load.
- GC pauses under churn.
- Healing throughput under node failure.

**Effort: Medium** — The `benches/` directory already has `ec_benchmark.rs`, `hash_benchmark.rs`, `storage_benchmark.rs`. A stress suite is a different beast: multi-client, sustained, with chaos injection. The `e2e/` directory exists for end-to-end tests.

**Desirability: Critical** — Without stress testing, performance claims are speculation.

**Honest Judgment: Realistic and urgent.** Should be built alongside Phase 4 (distributed read/write). The stress suite doesn't need to pass — it needs to reveal where things break. **Recommendation: start with a single `scripts/stress.sh` that runs concurrent PUTs for 10 minutes and reports p50/p99/p999 latency.**

---

### #18 — Complex scenarios: degraded modes, extreme payloads, edge cases

**Soundness: ⭐⭐⭐⭐⭐** — Distributed storage has notoriously complex failure modes:
- Network partition (split-brain between replica sets).
- Partial writes (coordinator crashes mid-quorum).
- Cascading failures (heal storm from simultaneous node deaths).
- Disk full scenarios.
- Clock skew (HLC drift between nodes).
- Extreme payloads (0-byte objects, maximum-size objects, millions of small objects).

**Effort: Medium-Large** — Each scenario needs a test harness and a way to inject failures. The `e2e/` directory and the `cluster-bootstrap/` and `cluster-mode-e2e-tests/` features suggest this is already planned.

**Desirability: Critical** — These tests are what separate a prototype from a production system.

**Honest Judgment: Realistic and essential.** The existing `cluster-mode-e2e-tests/` feature doc likely covers many of these. The challenge is execution: chaos engineering takes time and infrastructure. **Recommendation: prioritize the most dangerous failures first — (1) coordinator crash during write, (2) node death requiring heal, (3) disk full.** Each is a self-contained test scenario.

---

## Cross-Cutting Observations

1. **The roadmap is honest but underspecified.** Most items are one-liners (e.g., "transaction mechanism ?"). The question marks indicate the author knows these need discussion. The spec is far more detailed and should be the source of truth for prioritization.

2. **Items #4 and #9 are duplicates.** Both say "audit hot path / critical path performance." Merge them.

3. **The spec's §15 (Implementation Phases) is a better roadmap.** Phases 0-8 are well-defined, and the current features directory tracks progress against them. The roadmap items are largely "post-Phase-8" concerns.

4. **No item is fundamentally unrealistic.** Every item has prior art in other distributed storage systems. The question is priority and phasing.

5. **The biggest gap between the roadmap and reality is the unfinished integration.** Phase 1 is at ~85-90%, and Phases 2-8 have features but the read/write end-to-end integration (`final-integration-read-write-end-to-end/`) is still a feature doc, not implemented. The roadmap's ambitions assume this integration is done. It's not.

---

## Recommended Priority Order

```
IMMEDIATE (Phase 5-7 window):
  #6  Encryption                    ← spec gap, table-stakes
  #12 Pluggable tracing             ← observability, essential for debugging
  #17 Stress test suite             ← reveals problems early
  #11 Security audit                ← before any "production" claim
  #4/#9 Performance audits          ← verifies design goals

POST-PHASE-8 (near-term):
  #3  Code audit                    ← keeps quality high
  #18 Complex scenario tests        ← production hardening
  #10 Network optimizations         ← Phase 1 (compression), Phase 2 (DC-aware)
  #7  Production backend store      ← when integration is stable

POST-GA (medium-term):
  #13 User authentication           ← large but necessary
  #16 Platform supervision          ← operations enablement
  #8  Pluggable GC strategy         ← power-user optimization
  #15 Cloud/platform optimizations  ← containerization first, then tuning

FUTURE WORK (long-term / v2):
  #14 Event hooks                   ← nice-to-have, S3 parity
  #5  Transaction mechanism         ← different product scope
```

---

## Items That Need ADRs Before Implementation

| Item | ADR Topic | Why |
|---|---|---|
| #5 | Transaction model scope decision | Decides whether this is in or out of scope |
| #6 | Encryption key management | Per-bucket vs. per-node keys, KMS integration |
| #10 | Weighted routing architecture | New subsystem, needs design before code |
| #13 | Authentication & authorization model | AWS SigV4 + IAM vs. custom auth |

---

## Overall Verdict

The roadmap is a reasonable wishlist from someone who understands distributed storage. Every item is either already in the spec or a natural extension of it. The items are real, useful, and collectively paint a picture of a production-grade system. The primary risk is not feasibility but **sequencing** — do the fundamentals (encryption, tracing, testing) before the optimizations (network tuning, platform tuning) and before the moonshots (transactions). The spec's Phase 1-8 structure is the right skeleton; the roadmap items fill in the gaps and post-GA ambitions.
