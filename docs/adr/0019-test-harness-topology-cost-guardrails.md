# ADR-0019: Test Harness Operational Topology and Cost Guardrails

**Status:** Accepted
**Date:** 2026-08-10
**Deciders:** Brainstorm Agent (Architect)

> **Corrigendum (2026-08-16):** Hetzner retired the cx22/cx32 VM line. The
> provisioning scripts (`scripts/vm-provision.sh`) use **cx23** (2 vCPU / 4 GB)
> and **cx33** (4 vCPU / 8 GB) instead — `MAX_AGENT_VM_TYPE="cx33"`. The VM
> sizes cited throughout this ADR keep the original cx22/cx32 names; treat the
> script as the authoritative mapping. The ADR's decisions (two-VM topology,
> four-layer cost guardrails, network analysis) are unchanged.

---

## Context

The test harness epic ([`docs/features/test-harness/README.md`](../features/test-harness/README.md))
defines a phased load test campaign (Phase 0–6) derived from three brainstorm
design documents: `load-test-campaign.md`, `load-test-framework.md`, and
`load-test-metrics.md`. The campaign progresses from in-CI concurrency tests
(Phase 1) through sustained single-node load (Phase 2), cluster churn (Phase 3),
degraded-mode failure injection (Phase 4), and eventually to medium-cluster
scale testing (Phase 5) on real cloud VMs.

**Epic 4 (agent-skills)** defines agent-invocable skills for VM lifecycle
management: `vm-up`, `vm-down`, `vm-deploy`, and `vm-test-phase`. These skills
wrap `scripts/vm-provision.sh` (Epic 3, Feature 3.3), which calls the Hetzner
Cloud CLI (`hcloud server create`) to provision real cloud VMs on demand.

**Three architectural concerns emerged during review:**

### Concern 1: Co-Location Contention

The current topology co-locates the e2e load harness with the OceanFS system
under test (SUT) on the same VM:

```
┌─ Cloud VM ────────────────────────────────────────┐
│  oceanfs server (tokio, RocksDB, gRPC, gossip)     │
│  e2e harness (16-32 concurrent Workers, BLAKE3)    │
│  prometheus                                        │
│  Shared CPU, RAM, disk I/O, FS cache               │
└────────────────────────────────────────────────────┘
```

The developer works from a laptop with limited resources. The harness
**cannot** run on the laptop — 16-32 concurrent tokio Workers generating
random blobs, computing BLAKE3 hashes, and writing JSON reports for 30+
minutes would overwhelm a typical development laptop. The harness **must**
run on cloud infrastructure.

Co-location creates phase-specific problems:

| Phase | Concern | Severity |
|---|---|---|
| **Phase 1** (concurrency + TSAN, 60s) | Minimal. Goal is correctness (data races), not performance. CPU contention does not invalidate TSAN findings. | Low |
| **Phase 2** (sustained single-node, 30-60 min) | The harness's own memory, FDs, and CPU are mixed into the VM's resource picture. However, OceanFS reports its own per-process RSS/FDs/RocksDB stats via `/admin/metrics` — the metrics are not system-wide. Harness overhead does not contaminate these measurements. Additionally, Phase 2 is about relative trends (RSS drift, FD growth, SST accumulation), not absolute values. | Low-Medium |
| **Phase 3** (cluster churn, 3-5 nodes, 2-5 min) | Running 3-5 OceanFS processes **plus** the harness on a 4-vCPU VM creates artificial CPU starvation. SWIM gossip, failure detection, and churn convergence are all sensitive to scheduling delays. The harness's 16 Workers can starve SWIM heartbeat tasks, causing false gossip timeouts. | High |
| **Phase 4** (degraded mode, failure injection, 5-15 min) | Failure injectors (`tc netem` for latency, `dd` for disk fill, `kill -9` for node crash) run on the same machine as the harness. Disk fill to 95% could prevent the harness from writing its JSON report. SIGKILL on an OceanFS process is safe (separate process), but the harness's own resource usage adds noise to degraded-mode observability. | High |

### Concern 2: Runaway Cost Risk

The `vm-up` skill allows any agent (Architect, Reviewer, Implementer) to
provision cloud VMs on demand by calling `scripts/vm-provision.sh --phase N`,
which invokes `hcloud server create`. The vm-provisioning feature explicitly
lists "Cost tracking or budget enforcement" as **out of scope**.

There are zero guardrails:
- No confirmation gate ("this will cost ~€X/hour, proceed?")
- No budget cap or maximum spend limit
- No auto-shutdown timer (VM stays up until explicitly `vm-down`'d)
- No audit log of who provisioned what and why
- No prevention against leaving a VM running for days or weeks

A forgotten VM at the Phase 3-4 size (CX32, 4 vCPU, 8 GB) on Hetzner costs
approximately €0.04/hour. Over a weekend (48h): ~€2. Over a forgotten week
(168h): ~€7. Modest per-incident, but the absence of any safety net is a
design gap.

### Concern 3: Network Bandwidth Limits

Hetzner Cloud VMs have bandwidth caps: CX22 at ~1 Gbps, CX32 at ~2 Gbps.
With the harness generating sustained PUT/GET traffic, there is a question
of whether the network cap becomes the bottleneck before the VM's CPU or
disk does, invalidating load test measurements.

---

## Decision

### Decision 1: Two-VM Topology for Cloud-Based Phases (Phase 2–4)

The harness runs on a dedicated **Harness VM** (CX22, 2 vCPU, 4 GB). The
OceanFS SUT runs on a separate **SUT VM** (CX22 for Phase 2, CX32 for Phase
3-4). The two VMs communicate over Hetzner's internal network, which is
free, uncapped, and sub-millisecond latency.

```
┌─ Developer Laptop ─────────────────────────────────────┐
│                                                         │
│  Grafana :3000  (datasource → laptop Prometheus :9091)  │
│  laptop Prometheus :9091  (federates tunneled :9090,    │
│                            365-day retention,           │
│                            mcps/docker-compose.yml)     │
│  ssh oceanfs-sut     (SUT VM)                           │
│  ssh oceanfs-harness (Harness VM, optionally)           │
│                                                         │
│  (Zero load generation. SSH + browser + 2 small         │
│   containers; see the Decision 5 corrigendum below.)    │
└──────────┬──────────────────────────┬───────────────────┘
           │ SSH                      │ SSH
           ▼                          ▼
┌─ SUT VM ──────────────────┐  ┌─ Harness VM (CX22) ─────┐
│                            │  │                          │
│  oceanfs (1-5 processes)   │  │  e2e harness             │
│    EC, RocksDB, gRPC       │  │    Worker tasks          │
│    gossip, SWIM            │  │    Manifest tracker      │
│    /admin/metrics :9000    │◄─│    Metrics scraper       │
│                            │  │    Report writer (/tmp)  │
│  prometheus :9090          │  │                          │
│    scrapes localhost:9000  │  │  Rust toolchain          │
│                            │  │  cargo build             │
│  No harness.               │  │                          │
│  No compile toolchain.     │  │  Targets SUT VM via      │
│                            │  │  internal 10.x.x.x:9000  │
│                            │  │                          │
│  Internal net: 10.0.0.x    │  │  Internal net: 10.0.0.x  │
└────────────────────────────┘  └──────────────────────────┘
         ▲                               │
         └─── Hetzner internal network ──┘
              (free, uncapped, <0.5ms RTT)
```

**Per-phase VM allocation:**

| Phase | SUT VM | Harness VM | Rationale |
|---|---|---|---|
| **Phase 1** | None (CI runner) | None (CI runner) | Phase 1 runs entirely in CI. TSAN + concurrency test fits in <2 minutes on a single CI runner. No cloud VMs needed. |
| **Phase 2** | CX22 (2 vCPU, 4 GB) | CX22 (2 vCPU, 4 GB) | Single OceanFS node + Prometheus. Sustained 30-60 min. Harness on separate VM for clean resource measurements. |
| **Phase 3** | CX32 (4 vCPU, 8 GB) | CX22 (2 vCPU, 4 GB) | 3-5 OceanFS processes + Prometheus need 4 vCPU. Harness stays on CX22 (load generation is not the bottleneck). |
| **Phase 4** | CX32 (4 vCPU, 8 GB) | CX22 (2 vCPU, 4 GB) | Same as Phase 3 plus failure injection overhead. SUT VM needs headroom for `tc netem`, disk fill, node kill/restart. |
| **Phase 5** | Fleet (TBD) | CX32+ (dedicated loadgen) | Separate operational model; not covered by this ADR. Phase 5 was already scoped as a remote-targeting loadgen binary in the brainstorm documents. |

**Harness requires a remote target mode.** Currently, the `e2e` harness spawns
OceanFS as child processes via `NodeProcess` and `Cluster` abstractions. For
the two-VM model, it must connect to already-running OceanFS instances at
`TARGET_HOST:9000`. This is the same architecture required for Phase 5
(remote cluster targeting) — the work is pulled forward, not duplicated.

```rust
// New env var for the harness:
TARGET_HOST=10.0.0.5:9000                          // single-node (Phase 2)
TARGET_HOSTS=10.0.0.5:9000,10.0.0.5:9001,...      // multi-node (Phase 3-4)
```

The `NodeProcess`/`Cluster` abstractions get a `Remote` variant that connects
to running processes instead of spawning them. The local spawning path is
preserved for Phase 1 in CI.

### Decision 2: Cost Guardrails (Four-Layer Defense)

All guardrails are enforced **server-side** in `scripts/vm-provision.sh`.
The skill files (`.opencode/skills/vm-*.md`) are thin wrappers that call the
script. Skills are documentation, not enforcement — a determined agent could
bypass them entirely by running `hcloud` CLI directly. The guardrails live
in the script, which is the single chokepoint for VM provisioning.

#### Layer 1: Hard VM Size Cap

```bash
# In vm-provision.sh — immutable ceiling
MAX_AGENT_VM_TYPE="cx32"  # 4 vCPU / 8 GB
```

Any `--phase` or explicit `--type` that maps to a VM larger than CX32 is
**rejected** with an error message directing the human to provision manually.
Currently all phases 1-4 max out at CX32, so this is a safety net for future
VM type additions. Phase 5 and beyond use a separate provisioning model.

#### Layer 2: Size-Based Confirmation Gate

| VM Type | Phase(s) | Agent Can Provision? | Rule |
|---|---|---|---|
| CX22 (2 vCPU, 4 GB, ~€0.015/hr) | 1-2 | Yes, auto-approved | Cheap; accidental provisioning cost is negligible |
| CX32 (4 vCPU, 8 GB, ~€0.03/hr) | 3-4 | Requires `--confirm yes` | Moderate cost; explicit confirmation signals intent |
| ≥ CX42 (8+ vCPU, ~€0.06+/hr) | 5+ | **Rejected** | Must be provisioned manually by a human |

The `--confirm` flag is a simple intent gate. The agent must explicitly pass
`--confirm yes` or the script exits with:
```
This will provision a CX32 VM (~€0.03/hour, ~€0.70/day).
Re-run with --confirm yes to proceed.
```

This is not cryptographic security — it is an "are you sure" gate that
prevents accidental provisioning.

#### Layer 3: Auto-Shutdown TTL Timer

Every provisioned VM gets a systemd timer that powers off the VM after a
configurable TTL:

```bash
# In vm-provision.sh, after VM is ready:
TTL_HOURS="${LOAD_TEST_TTL_HOURS:-4}"

ssh "root@${VM_IP}" <<EOF
cat > /etc/systemd/system/oceanfs-ttl.service <<'UNIT'
[Unit]
Description=Auto-shutdown OceanFS test VM after TTL expiry

[Service]
Type=oneshot
ExecStart=/usr/local/bin/hcloud server poweroff \$(hostname)
UNIT

cat > /etc/systemd/system/oceanfs-ttl.timer <<'TIMER'
[Unit]
Description=TTL timer for OceanFS test VM
After=network.target

[Timer]
OnBootSec=${TTL_HOURS}h
OnUnitActiveSec=${TTL_HOURS}h

[Install]
WantedBy=timers.target
TIMER

systemctl daemon-reload
systemctl enable --now oceanfs-ttl.timer
EOF
```

**Key properties:**
- Default TTL: **4 hours** (configurable via `LOAD_TEST_TTL_HOURS` env var)
- The timer uses `poweroff`, not `delete` — data is preserved and the VM can
  be restarted manually if needed. A powered-off VM costs only disk storage
  (~€0.50/month for 80 GB).
- The timer fires on boot and every `TTL_HOURS` thereafter. An agent can
  **extend** the TTL (by re-running vm-provision.sh with a longer TTL or by
  restarting the timer), but **cannot disable** it without root access to the
  VM — which agents do not have directly (they go through the skill, which
  does not expose `systemctl stop oceanfs-ttl.timer`).
- After poweroff, the VM remains in the Hetzner project. A human can power
  it back on, resize it, or delete it at their discretion.

#### Layer 4: Budget Gate (Deferrable)

```bash
# Optional, env-var gated:
MAX_MONTHLY_EUR="${LOAD_TEST_MAX_MONTHLY_EUR:-}"
if [ -n "$MAX_MONTHLY_EUR" ]; then
    CURRENT_SPEND=$(hcloud billing sum-current-month --output json | jq '.total')
    if (( $(echo "$CURRENT_SPEND + $ESTIMATED_COST > $MAX_MONTHLY_EUR" | bc -l) )); then
        echo "ERROR: Estimated cost €${ESTIMATED_COST} would exceed monthly budget €${MAX_MONTHLY_EUR}."
        echo "Current month spend: €${CURRENT_SPEND}. Manual provisioning required."
        exit 1
    fi
fi
```

This is marked as **v2 / deferrable**. The TTL and size cap carry 90% of the
safety value at 10% of the implementation effort. The budget gate requires
a Hetzner API token with billing read scope and adds ongoing maintenance
burden (API format changes). It is included in the ADR for completeness but
not required for MVP.

### Decision 3: Network Bandwidth — No Architectural Change Needed

**Hetzner bandwidth limits per VM type:**

| VM Type | Bandwidth Cap | Monthly Traffic Included |
|---|---|---|
| CX22 | ~1 Gbps (125 MB/s) | 20 TB |
| CX32 | ~2 Gbps (250 MB/s) | 20 TB |

**Analysis by phase:**

**Phase 2 (CX22 harvester → CX22 SUT):** Internal network, uncapped.

The internal network between VMs in the same Hetzner project is **free and
uncapped** — bandwidth limits apply only to internet-bound traffic. Since the
harness targets the SUT VM via internal IP (10.0.0.x), the 1 Gbps cap is
irrelevant. The actual bottleneck is the SUT VM's CPU and disk, not the
network.

Even if the traffic did cross the internet gateway, the worst-case bandwidth
(all 5 MB+ multi-segment blobs, 32 concurrent Workers) would be:

| Parameter | Value |
|---|---|
| Workers | 32 |
| Avg blob size | 5 MB (pathological: all multi-segment) |
| Ops/sec per worker | ~1 req/s (large blobs are bandwidth-limited) |
| Egress (PUT to VM) | 32 × 0.5 × 5 MB = **80 MB/s (640 Mbps)** |
| Ingress (GET from VM) | 32 × 0.4 × 5 MB = **64 MB/s (512 Mbps)** |
| Total | **~1.15 Gbps** |

This would exceed the CX22 cap by ~15%, but only in the pathological case of
all multi-segment blobs. The realistic mixed-tier workload (avg blob ~100 KB,
ops/sec per worker ~8) uses ~138 Mbps — 14% of the cap. Additionally, with
the two-VM topology, all test traffic uses the **internal network** (uncapped).

**Traffic volume:** At 138 Mbps sustained for 60 minutes = ~62 GB. Far below
the 20 TB monthly allowance. Even running Phase 2 daily for 8 hours at the
pathological 1.15 Gbps rate = ~4 TB/month. Still well within limits.

**Conclusion:** No architectural change needed. Add a documentation note
in `vm-provision.sh`: if Phase 2 with all-large-blob profile on a single-VM
fallback, warn that internet bandwidth may cap throughput. The internal
network path makes this largely moot.

### Decision 4: Phase-Specific Co-Location Mitigations (When Single-VM Is Used)

The two-VM topology eliminates co-location concerns for normal operation.
However, a `--single-vm` budget option is provided for Phase 2 where the
harness and SUT share a single CX22. The following mitigations apply:

**Phase 2 (single-VM fallback):**
- Harness writes reports to `/tmp` (tmpfs), not the OceanFS data directory
  on the SSD. This avoids disk I/O contention for report writes.
- The `/admin/metrics` endpoint reports **per-process** memory/FD/RocksDB
  stats — not system-wide. Harness overhead does not contaminate OceanFS
  metrics. The Phase 2 assertions (RSS drift, FD growth, SST accumulation)
  remain valid on a single VM.
- Monitor `process_open_fds` and `process_resident_memory_bytes` from the
  harness process separately (via `/proc` in the harness itself) and include
  them in the LoadReport as metadata — not assertions, but available for
  post-hoc analysis if results are borderline.

**Phase 3-4 (single-VM, NOT recommended):**
- If single-VM is forced for Phase 3-4 via `--single-vm`, the script prints
  a **WARNING** banner:
  ```
  WARNING: Phase 3-4 on a single VM will cause CPU contention between the
  harness and OceanFS processes. SWIM gossip timing may be affected.
  Gossip intervals will be relaxed (gossip_interval=3s, suspicion_timeout=10s)
  to compensate. Use the two-VM topology for reliable results.
  ```
- The harness configures OceanFS with relaxed gossip parameters:
  | Parameter | Two-VM (normal) | Single-VM (fallback) |
  |---|---|---|
  | `gossip_interval_ms` | 1000 | 3000 |
  | `suspicion_timeout_ms` | 3000 | 10000 |
  | `failure_timeout_ms` | 8000 | 30000 |

**Phase 4 disk-fill test:** Regardless of topology, the harness **always**
writes its JSON report to `/tmp` (tmpfs). The `dd if=/dev/zero of=...` that
fills the SSD to 95% will not prevent the harness from saving results.

### Decision 5: Developer Laptop — Zero Load Generation

> **Corrigendum (2026-08-16):** the laptop additionally runs a small
> **persistent Prometheus container** (mcps/docker-compose.yml, host
> port 9091, `network_mode: host`) that federates the SUT Prometheus
> through the observe.sh tunnel (365-day retention). This is the same
> class of local service as the already-permitted laptop Grafana
> (negligible CPU, no load generation — the ADR's actual concern), and it
> preserves run metrics across VM teardown. The sentence below ("The
> laptop never runs: … Prometheus") refers to a full Prometheus *server
> workload* for the SUT; the persistent container is a thin federation
> sink.

The developer laptop is responsible for:
1. **SSH to both VMs** — negligible CPU (<1%)
2. **Grafana** (browser, rendering dashboards from the persistent laptop
   Prometheus at :9091) — moderate
   RAM when dashboards are visible, near-zero CPU when idle
3. **SSH tunnel** (`ssh -L 9090:localhost:9090 -N oceanfs-sut`) — kernel-level
   port forwarding, negligible CPU

The laptop **never** runs:
- The e2e harness (no Worker tasks, no BLAKE3 hashing)
- The OceanFS server (no RocksDB, no EC)
- Prometheus
- Cargo builds (build happens on the Harness VM)

This is enforced architecturally: the harness binary does not exist on the
laptop. It is built and executed on the Harness VM via `vm-deploy` +
`vm-test-phase`. The laptop is purely an observation and command-and-control
terminal.

---

## Consequences

### Positive

1. **No resource contention.** The harness VM has dedicated CPU and memory.
   OceanFS metrics reflect the SUT alone. Phase 3 gossip timing is reliable.
   Phase 4 failure injectors cannot affect the harness.

2. **Realistic network path.** Harness → OceanFS traffic goes over real TCP
   through the real gRPC stack. No localhost shortcuts. Catches serialization
   bugs, connection pool edge cases, and protocol issues hidden by loopback.

3. **Safe failure injection.** `tc netem`, disk fill, and SIGKILL on the SUT
   VM cannot harm the harness VM. The harness continues scraping metrics and
   writing reports through any failure scenario.

4. **Cost safety.** The four-layer guardrail system (size cap, confirmation
   gate, auto-shutdown TTL, budget gate) means a forgotten VM costs at most
   4 hours of runtime (€0.06–0.12) before auto-poweroff.

5. **Laptop unaffected.** SSH + browser only. The developer can continue
   other work during a multi-hour Phase 2 run.

6. **Internal network is free and uncapped.** All test traffic between VMs
   uses Hetzner's internal network — no bandwidth charges, no caps, no
   internet latency.

7. **Forward-compatible with Phase 5.** The remote target mode required for
   the two-VM topology is the same architecture Phase 5 needs for targeting
   a 20-50 node cluster. Work is pulled forward, not duplicated.

### Negative

1. **Two VMs to manage.** `vm-provision.sh` must provision, configure, and
   tear down two VMs instead of one. The script grows from ~150 lines to
   ~300 lines. The `vm-up`/`vm-down` skills must handle the two-VM lifecycle
   (or provide separate `vm-up-sut`/`vm-up-harness` variants — TBD by
   implementer).

2. **Slightly higher cost.** Phase 2 costs ~€0.03/hr (two CX22) instead of
   ~€0.015/hr (one CX22). With the 4-hour TTL, a full dev day costs ~€0.12
   instead of ~€0.06. Phase 3-4 costs ~€0.05/hr (CX32 + CX22) instead of
   ~€0.03/hr (one CX32). This is well within reason for a development tool.

3. **Harness needs remote target mode.** The `e2e` crate's `NodeProcess` and
   `Cluster` abstractions must grow a `Remote` variant. This is moderate
   implementation work (~200-400 lines of Rust) but was already planned for
   Phase 5. The work is pulled forward to Phase 2.

4. **Deploy step now builds on Harness VM, deploys binary to SUT VM.**
   `vm-deploy` must `cargo build` on the Harness VM (which has Rust), then
   `scp` the oceanfs binary to the SUT VM. This adds a step: previously,
   build and run happened on the same VM. The overhead is one `scp` over
   the internal network (<1 second for a 50 MB binary).

### Neutral

1. **Increased operational surface.** Two VMs means two systemd units to
   configure, two SSH config entries, two `hcloud` resources to track. The
   additional complexity is bounded (script-driven, not manual) but real.

2. **TTL timer is best-effort, not air-tight.** A network partition between
   the VM and the Hetzner API could prevent the systemd timer from executing
   `hcloud server poweroff`. This is an accepted risk — the timer is a safety
   net, not a hard real-time guarantee. A human should still verify `vm-status`
   periodically.

3. **`--single-vm` flag adds a maintenance branch.** The single-VM fallback
   path (for budget-conscious Phase 2 runs) needs the relaxed gossip parameters
   and the `/tmp` report path. These are minor but must be tested.

---

## Considered Alternatives

### Alternative 1: Single-VM Co-Located (Current Design)

Keep the load generator on the same VM as OceanFS. No topology change.

| Pros | Cons | Why Rejected |
|---|---|---|
| Simplest operational model (one VM) | Phase 3-4 CPU contention causes false gossip timeouts | The primary purpose of Phases 3-4 is to find distributed protocol bugs. False positives from CPU starvation undermine that goal. |
| Cheapest per-hour (~€0.015-0.03/hr) | Phase 4 failure injectors risk harming the harness | Losing the test report to a disk-fill test defeats the purpose of running the test. |
| No remote-target refactoring needed | Developer laptop must run the harness (impractical) | A 32-worker concurrent load generator running for 60 minutes would saturate a typical development laptop. |
| Already fully specified in feature docs | Phase 2 RSS/FD measurements include harness overhead | Per-process metrics mitigate this, but the ambiguity remains for post-hoc analysis. |

### Alternative 2: Laptop as Load Generator

Run the e2e harness on the developer's laptop, targeting the cloud VM over SSH
tunnel or direct HTTP.

| Pros | Cons | Why Rejected |
|---|---|---|
| Only one cloud VM needed | Laptop CPU saturated by 16-32 concurrent Workers doing BLAKE3, HTTP, and JSON serialization | A typical development laptop (4-8 cores) cannot sustain this workload for 30-60 minutes without severe thermal throttling and system unresponsiveness. |
| Zero additional cloud cost | Laptop network may bottleneck — home internet upload is typically 10-50 Mbps, far below the 100+ Mbps the harness can generate | The test would measure the developer's home internet connection, not OceanFS. |
| Simplest harness refactoring (just change endpoint IP) | 30-60 minute sustained test means the laptop is unusable for other work | Unacceptable developer experience. |

### Alternative 3: Persistent Always-On VM

Pre-provision one or two VMs manually, keep them running 24/7, and agents only
deploy and run tests against them. No `vm-up`/`vm-down` in the agent skill set.

| Pros | Cons | Why Rejected |
|---|---|---|
| Zero provisioning risk — VMs are human-managed | Costs accumulate 24/7 (CX22+CX32 = ~€16/month idle) | The TTL auto-shutdown achieves the same safety with lower idle cost. |
| Simplest agent workflow (status, deploy, test, results only) | Less flexibility — VM size is fixed, resizing requires human intervention | Phases 2, 3, and 4 use different VM sizes. A persistent VM would need to be oversized (CX32) for all phases, wasting money on Phase 2. |
| No guardrails needed | Manual teardown/startup friction | The point of agent skills is to reduce friction. Requiring human intervention for every VM lifecycle event defeats that. |

### Alternative 4: Agent Provisioning with No Guardrails

Keep `vm-up`/`vm-down` but add no guards. Trust agents to be responsible.

| Pros | Cons | Why Rejected |
|---|---|---|
| Zero implementation effort (already spec'd) | Single forgotten VM over a long weekend = €2-7 waste | The TTL timer costs ~10 lines of bash and a systemd unit. The cost-to-safety ratio is overwhelmingly favorable. |
| Maximum flexibility for agents | No audit trail of who provisioned what | When the Hetzner bill arrives, there's no way to correlate charges to specific test runs. |
| | Encourages sloppy behavior (leave VM running "just in case I need it later") | The TTL creates a healthy default: VMs are ephemeral, reprovision when needed. |

---

## Impact on Existing Feature Documents

The following feature documents require updates to align with this ADR:

| Document | Changes Required |
|---|---|
| `docs/features/test-harness/operational-tooling/vm-provisioning/feature.md` | Add two-VM provisioning logic, `--single-vm` flag, `--confirm` gate, TTL timer setup, size cap enforcement, budget gate scaffolding. VM type mapping now outputs two VMs for phases 2-4. |
| `docs/features/test-harness/agent-skills/vm-skills/feature.md` | `vm-up` returns `{sut: {ip, name}, harness: {ip, name}}` for two-VM topology. `vm-down` accepts `--preserve-data` and tears down both VMs. `vm-deploy` builds on harness VM and `scp`s binary to SUT VM. |
| `docs/features/test-harness/agent-skills/test-execution-skills/feature.md` | `vm-test-phase` must pass `TARGET_HOST` env var to the harness on the Harness VM. Relaxed gossip params for single-VM `--single-vm` mode. |
| `docs/features/test-harness/README.md` | Update topology diagram from single-VM to two-VM. Update execution order (Epic 3 vm-provisioning now depends on this ADR's guardrail design). Update phase-to-epic mapping to note Phase 1 stays in CI. |
| `docs/brainstorm/load-test-framework.md` | Update §1.1 topology diagram and §1.2 "Why This Model" to reflect two-VM split. Update §2.1 crate layout to add `Remote` variants for `NodeProcess`/`Cluster`. |
| `docs/features/test-harness/test-harness-extensions/` (all sub-features) | No changes required. The harness types (`Manifest`, `LoadScenario`, `Worker`, etc.) are topology-agnostic. Only the harness entrypoint needs `TARGET_HOST` env var support. |

---

## References

- [`docs/features/test-harness/README.md`](../features/test-harness/README.md) — Test harness master index (Epics 1-4)
- [`docs/features/test-harness/operational-tooling/vm-provisioning/feature.md`](../features/test-harness/operational-tooling/vm-provisioning/feature.md) — VM provisioning feature
- [`docs/features/test-harness/agent-skills/vm-skills/feature.md`](../features/test-harness/agent-skills/vm-skills/feature.md) — Agent VM skills
- [`docs/brainstorm/load-test-framework.md`](../brainstorm/load-test-framework.md) — Load test framework operational design
- [`docs/brainstorm/load-test-campaign.md`](../brainstorm/load-test-campaign.md) — Phased load test roadmap
- [`guidelines/architecture.md`](../../guidelines/architecture.md) — Crate dependency rules, testing boundaries
- [`guidelines/performance.md`](../../guidelines/performance.md) — Performance guidelines, instrumentation rules
- Hetzner Cloud pricing and network limits — https://www.hetzner.com/cloud (CX22: ~€0.015/hr, CX32: ~€0.03/hr, internal network: free/uncapped)
