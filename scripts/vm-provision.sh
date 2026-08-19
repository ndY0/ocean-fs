#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# vm-provision.sh — Cloud Lifecycle with Cost Guardrails
#
# Provisions the load-test topology on Hetzner Cloud per ADR-0019 and
# ADR-0026. The SUT VMs run OceanFS (and Prometheus on node 0) only — no
# harness, no toolchain. The Harness VM runs the e2e harness and Rust
# toolchain, targeting the SUT over Hetzner's internal network.
#
# Usage:
#   ./scripts/vm-provision.sh --phase N [OPTIONS]
#
# VM Size Mapping (per phase):
#   Phase 1: N/A (runs in CI, no cloud VMs needed)
#   Phase 2: SUT=CX33 (4 vCPU, 8 GB, 80 GB), Harness=CX23 (2 vCPU, 4 GB, 40 GB)
#            (SUT is CX33, not CX23: the load-test deploy profile in
#            sut-deploy.sh targets the 8 GB CX33 — generous caches, 16 MiB
#            bodies — so CPU (hashing, EC encode) is the bottleneck; on a
#            4 GB CX23 that profile OOM-kills the SUT mid-run. See commits
#            7e4c3b4 and 5e7aa70.)
#   Phase 3-4: CLUSTER of SUT node VMs (default 3, --nodes N) each CX33,
#              + Harness=CX43 (8 vCPU, 16 GB, 80 GB) per ADR-0026. Nodes are
#              named ${prefix}-sut-0..N-1; node 0 is the bootstrap and hosts
#              Prometheus scraping every node. No port juggling: every node
#              listens on :9000/:9001; nodes differ by internal IP.
#   Phase 5+: N/A (separate provisioning model)
#
# Guardrails (four-layer defense per ADR-0019, retained by ADR-0026):
#   Layer 1 — Hard VM size cap: MAX_AGENT_VM_TYPE="cx43" (harness role;
#             SUT nodes stay capped at cx33 — raised 2026-08-19 per
#             ADR-0026 Decision 2)
#   Layer 2 — Confirmation gate: REMOVED (2026-08-17) — CX33 is the standard
#             sizing for phases 2-4, so no size-based confirmation is required.
#             The --confirm flag is still accepted for compatibility but is a
#             no-op (see check_confirmation_gate).
#   Layer 3 — Auto-shutdown TTL: systemd timer on each VM (default 4h)
#   Layer 4 — Budget gate: scaffolding (deferrable/v2)
# Security: Hetzner VMs ship with NO firewall — managed firewalls are
#   created and applied by default (SUT nodes: SSH + internal-net 9000/9001;
#   Harness: SSH only). Disable with --no-firewall (not recommended).
#
# Requirements:
#   - hcloud CLI installed and authenticated. HCLOUD_TOKEN is auto-loaded
#     from .hetzner/.env by lib/env-hetzner.sh (or set it in the environment)
#   - jq for JSON parsing
#   - SSH key at .hetzner/.ssh/hetzner-ssh.pub (loaded into ssh-agent by
#     lib/env-hetzner.sh), or --ssh-key PATH / ~/.ssh/id_rsa.pub
#
# Environment Variables:
#   HCLOUD_TOKEN              Hetzner Cloud API token (required)
#   LOAD_TEST_TTL_HOURS       Override default TTL (default: 4)
#   LOAD_TEST_CLUSTER_NODES   Override default cluster size for phase 3-4
#                             (default: 3)
#   LOAD_TEST_MAX_MONTHLY_EUR Optional monthly budget cap (deferrable, v2)
#
# Author: OceanFS
# Date: 2026-08-11 (phase 3+ fleet topology per ADR-0026: 2026-08-19)
# ---------------------------------------------------------------------------

set -euo pipefail

# Load .hetzner/.env (HCLOUD_TOKEN), ensure ssh-agent + the Hetzner key, and
# set the default provisioning key (HETZNER_SSH_PUBLIC_KEY). No-op without
# .hetzner/ (e.g. on the Harness VM).
# shellcheck source=lib/env-hetzner.sh
_ENV_HETZNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/env-hetzner.sh"
[ -f "$_ENV_HETZNER" ] && . "$_ENV_HETZNER"
unset _ENV_HETZNER

# ---------------------------------------------------------------------------
# Constants & defaults
# ---------------------------------------------------------------------------
# Layer 1 hard cap — raised to cx43 for the Harness role per ADR-0026
# Decision 2 (Phase 3+ drives ~3× the load from the harness). SUT node
# sizing remains capped at cx33 via resolve_phase.
readonly MAX_AGENT_VM_TYPE="cx43"
readonly DEFAULT_PROVIDER="hetzner"
readonly DEFAULT_BRANCH="main"
readonly DEFAULT_REPO="https://github.com/ndY0/ocean-fs.git"
readonly DEFAULT_IMAGE="ubuntu-24.04"
readonly DEFAULT_TTL_HOURS="${LOAD_TEST_TTL_HOURS:-4}"
readonly NETWORK_NAME="oceanfs-testnet"
readonly NETWORK_CIDR="10.0.0.0/24"

# VM type pricing (approximate, for cost estimates)
declare -A VM_HOURLY_COST
# Approximate hourly prices (2026-08 Hetzner table; verify with
# `hcloud server-type list --output json`). cx23/cx33 replaced the
# retired cx22/cx32 (now cx23/cx33); the cpx2x family is the same size class.
VM_HOURLY_COST[cx23]="0.015"
VM_HOURLY_COST[cpx22]="0.015"
VM_HOURLY_COST[cx33]="0.03"
VM_HOURLY_COST[cpx32]="0.03"
VM_HOURLY_COST[cx43]="0.06"
VM_HOURLY_COST[cx53]="0.12"
VM_HOURLY_COST[cpx62]="0.24"

# VM type ordering for comparison (lower index = smaller)
declare -A VM_TYPE_RANK
VM_TYPE_RANK[cx23]=1
VM_TYPE_RANK[cpx22]=1
VM_TYPE_RANK[cx33]=2
VM_TYPE_RANK[cpx32]=2
VM_TYPE_RANK[cx43]=3
VM_TYPE_RANK[cx53]=4
VM_TYPE_RANK[cpx62]=5

# ---------------------------------------------------------------------------
# Script state
# ---------------------------------------------------------------------------
PHASE=""
PROVIDER="${DEFAULT_PROVIDER}"
BRANCH="${DEFAULT_BRANCH}"
REPO_URL="${DEFAULT_REPO}"
COMMIT=""
SSH_KEY_PATH=""
NAME_PREFIX=""
IMAGE="${DEFAULT_IMAGE}"
DRY_RUN=false
DEBUG=false
KEEP_ON_FAILURE=false
FIREWALLS=true
OBSERVABILITY=true
SSH_SOURCE_IP="0.0.0.0/0"
SINGLE_VM=false
CONFIRM=""
TTL_HOURS="${DEFAULT_TTL_HOURS}"
DESTROY_NAME=""
STATUS_NAME=""

SUT_TYPE=""
SUT_IP=""
SUT_PUBLIC_IP=""
SUT_NAME=""
HARNESS_TYPE=""
HARNESS_IP=""
HARNESS_PUBLIC_IP=""
HARNESS_NAME=""
# Phase 3+ cluster fleet size (per ADR-0026): number of dedicated SUT node
# VMs. Node 0 is the bootstrap and hosts Prometheus. Override via
# LOAD_TEST_CLUSTER_NODES or --nodes N.
CLUSTER_NODES="${LOAD_TEST_CLUSTER_NODES:-3}"
# Phase 3+ node VM names/IPs, indexed 0..N-1.
SUT_NODE_NAMES=()
SUT_NODE_IPS=()
SUT_NODE_PUBLIC_IPS=()

# Track created VMs for cleanup on failure
CREATED_VM_NAMES=()

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

log_info() {
    echo "[INFO]  $(date '+%H:%M:%S') $*" >&2
}

log_warn() {
    echo "[WARN]  $(date '+%H:%M:%S') $*" >&2
}

log_error() {
    echo "[ERROR] $(date '+%H:%M:%S') $*" >&2
}

die() {
    log_error "$@"
    cleanup
    exit 1
}

# Compare two VM types: returns 0 if $1 <= $2, 1 otherwise
vm_type_lte() {
    local type_a="${1,,}"  # lowercase
    local type_b="${2,,}"
    local rank_a="${VM_TYPE_RANK[$type_a]:-99}"
    local rank_b="${VM_TYPE_RANK[$type_b]:-0}"
    [ "$rank_a" -le "$rank_b" ]
}

# Get VM type cost per hour
vm_hourly_cost() {
    local type="${1,,}"
    echo "${VM_HOURLY_COST[$type]:-0.0}"
}

# ---------------------------------------------------------------------------
# Phase validation & VM size mapping
# ---------------------------------------------------------------------------

resolve_phase() {
    local phase="$1"

    case "$phase" in
        1)
            SUT_TYPE=""
            HARNESS_TYPE=""
            ;;
        2)
            # SUT is CX33 (8 GB) — the deploy profile targets it; CX23 OOMs.
            SUT_TYPE="cx33"
            HARNESS_TYPE="cx23"
            ;;
        3|4)
            # Per ADR-0026: a fleet of dedicated node VMs (default 3) plus
            # an upgraded CX43 harness that must sustain ~3× the Phase 2
            # load (2-vCPU CX23 was maxed at ~150 ops/s in Phase 2 runs).
            SUT_TYPE="cx33"
            HARNESS_TYPE="cx43"
            ;;
        5|6)
            # Phase 5+ uses separate provisioning model
            cat <<'GUIDANCE'
Phase 5+ requires a separate provisioning model (fleet/custom cluster).
For medium-to-large cluster testing:
  - Use Terraform/OpenTofu for infrastructure-as-code
  - Provision VMs from the Hetzner Cloud console or API directly
  - See docs/brainstorm/load-test-campaign.md §6 for deployment guidelines
  - See docs/adr/0019-test-harness-topology-cost-guardrails.md for cost guardrails

This script provisions at most two VMs (SUT + Harness).
For Phase 5+, provision the cluster manually or use the dedicated cluster
tooling (not yet implemented).
GUIDANCE
            exit 0
            ;;
        *)
            die "Invalid phase: $phase. Valid phases are 1-6."
            ;;
    esac

    # Validate the fleet size for cluster phases.
    if [ "$phase" = "3" ] || [ "$phase" = "4" ]; then
        if ! [[ "$CLUSTER_NODES" =~ ^[0-9]+$ ]] || [ "$CLUSTER_NODES" -lt 3 ]; then
            die "Invalid cluster size: '$CLUSTER_NODES'. Phase 3-4 needs at least 3 nodes (quorum semantics). Use --nodes N or LOAD_TEST_CLUSTER_NODES."
        fi
        if [ "$CLUSTER_NODES" -gt 5 ]; then
            die "Invalid cluster size: '$CLUSTER_NODES'. Phase 3-4 supports at most 5 nodes per ADR-0026. Larger fleets need the Phase 5 provisioning model."
        fi
        log_info "Phase $phase: provisioning cluster of $CLUSTER_NODES SUT node VMs (cx33 each) + Harness (cx43)."
    fi
}

# ---------------------------------------------------------------------------
# Guardrail checks
# ---------------------------------------------------------------------------

# Layer 1: Hard VM size cap
check_hard_cap() {
    local type="${1,,}"

    if [ -z "$type" ]; then
        return 0
    fi

    # If the type is larger than MAX_AGENT_VM_TYPE, reject
    if ! vm_type_lte "$type" "$MAX_AGENT_VM_TYPE"; then
        die "VM type '${type}' exceeds the maximum allowed type '${MAX_AGENT_VM_TYPE}'." \
            "Larger VMs (≥ CX43) must be provisioned manually via the Hetzner Cloud console." \
            "See ADR-0019 for cost guardrail details."
    fi
}

# Layer 2: Size-based confirmation gate (REMOVED — CX33 is the standard
# sizing for phases 2-4, so no confirmation is required. Kept as a no-op
# so `--confirm yes` in existing invocations keeps working.)
check_confirmation_gate() {
    local type="${1,,}"
    local phase="${2:-}"

    if [ -z "$type" ]; then
        return 0
    fi

    if [ "$CONFIRM" == "yes" ]; then
        log_info "Note: --confirm yes is no longer required (confirmation gate removed — CX33 is the standard sizing)."
    fi
}

# Layer 4: Budget gate (scaffolding, deferrable/v2)
check_budget_gate() {
    local max_budget="${LOAD_TEST_MAX_MONTHLY_EUR:-}"

    if [ -z "$max_budget" ]; then
        # Silently skip — budget gate is optional scaffolding
        return 0
    fi

    log_info "Budget gate: LOAD_TEST_MAX_MONTHLY_EUR=${max_budget} — checking Hetzner billing..."

    local billing_output
    if ! billing_output=$(hcloud billing sum-current-month --output json 2>/dev/null); then
        log_warn "Budget gate: could not query Hetzner billing API. Skipping budget check."
        log_warn "Ensure HCLOUD_TOKEN has billing read scope."
        return 0
    fi

    local current_total
    current_total=$(echo "$billing_output" | jq -r '.total // "0"' 2>/dev/null || echo "0")

    # Estimate cost for this run
    local total_hourly
    if [ "$SINGLE_VM" = true ] && [ -n "$SUT_TYPE" ]; then
        # Single VM: only pay for one (SUT is the provisioned VM)
        total_hourly=$(vm_hourly_cost "$SUT_TYPE")
    else
        # SUT (+ fleet nodes for phase 3-4) + Harness hourly rates
        local s_cost h_cost
        s_cost="0"
        h_cost="0"
        if [ -n "$SUT_TYPE" ]; then
            s_cost=$(vm_hourly_cost "$SUT_TYPE")
            if [ "$PHASE" = "3" ] || [ "$PHASE" = "4" ]; then
                # One VM per cluster node (ADR-0026)
                s_cost=$(echo "$s_cost * $CLUSTER_NODES" | bc -l 2>/dev/null || echo "$s_cost")
            fi
        fi
        if [ -n "$HARNESS_TYPE" ]; then
            h_cost=$(vm_hourly_cost "$HARNESS_TYPE")
        fi
        total_hourly=$(echo "$s_cost + $h_cost" | bc -l 2>/dev/null || echo "0")
    fi

    local estimated_total
    estimated_total=$(echo "$TTL_HOURS * $total_hourly" | bc -l 2>/dev/null || echo "0")

    local would_exceed
    would_exceed=$(echo "$current_total + $estimated_total > $max_budget" | bc -l 2>/dev/null || echo "0")

    if [ "$would_exceed" = "1" ]; then
        die "Budget gate: Estimated cost €${estimated_total} would exceed monthly budget €${max_budget}." \
            "Current month spend: €${current_total}. Manual provisioning required."
    fi

    log_info "Budget gate: OK. Current spend: €${current_total}, estimated: €${estimated_total}, budget: €${max_budget}."
}

# Run all guardrails
run_guardrails() {
    check_hard_cap "$SUT_TYPE"
    check_hard_cap "$HARNESS_TYPE"

    # Check confirmation on the larger type (SUT is always >= Harness)
    local larger_type="$SUT_TYPE"
    check_confirmation_gate "$larger_type" "$PHASE"

    check_budget_gate
}

# ---------------------------------------------------------------------------
# VM provisioning steps
# ---------------------------------------------------------------------------

wait_for_vm() {
    local name="$1"
    local max_retries=60
    local retry=0

    log_info "Waiting for VM '${name}' to be ready..."

    while [ "$retry" -lt "$max_retries" ]; do
        local status
        status=$(hcloud server describe "$name" --output json 2>/dev/null | jq -r '.status // "unknown"')
        if [ "$status" = "running" ]; then
            log_info "VM '${name}' is running."
            return 0
        fi
        if [ "$status" = "error" ]; then
            # Surface the server's error details (rescue/info from hcloud).
            local details
            details=$(hcloud server describe "$name" --output json 2>/dev/null \
                | jq -r 'if .error then .error.message // .error.code else "status=error" end' 2>/dev/null \
                || true)
            die "VM '${name}' entered error state: ${details:-no details available}"
        fi
        retry=$((retry + 1))
        sleep 5
    done

    die "VM '${name}' did not become ready within ${max_retries} retries."
}

get_vm_ips() {
    local name="$1"
    local server_info

    server_info=$(hcloud server describe "$name" --output json 2>/dev/null)

    local private_ip public_ip
    private_ip=$(echo "$server_info" | jq -r '.private_net[0].ip // ""')
    public_ip=$(echo "$server_info" | jq -r '.public_net.ipv4.ip // ""')

    echo "${private_ip} ${public_ip}"
}

create_network() {
    # If the network exists, it must have at least one subnetwork —
    # Hetzner rejects server attachment to networks without one
    # ("networks must have at least one subnetwork"). Networks created
    # by this script always have one; manually created ones may not.
    if hcloud network describe "$NETWORK_NAME" --output json >/dev/null 2>&1; then
        local subnet_count
        subnet_count=$(hcloud network describe "$NETWORK_NAME" --output json 2>/dev/null \
            | jq '[.subnets[]?] | length' 2>/dev/null || echo 0)
        if [ "${subnet_count:-0}" -gt 0 ]; then
            log_info "Network '${NETWORK_NAME}' already exists with ${subnet_count} subnetwork(s)."
            return 0
        fi
        log_warn "Network '${NETWORK_NAME}' exists but has NO subnetwork — adding ${NETWORK_CIDR}..."
        # The hcloud CLI takes the network POSITIONALLY for add-subnet
        # (`hcloud network add-subnet <network> --ip-range …`); older
        # versions used a --network flag. Try the positional form first,
        # fall back to the flag form, and report BOTH errors if neither
        # works so CLI drift stays diagnosable.
        local out1 out2
        if out1=$(hcloud network add-subnet \
            "$NETWORK_NAME" \
            --type cloud \
            --network-zone eu-central \
            --ip-range "$NETWORK_CIDR" \
            2>&1); then
            log_info "Subnetwork ${NETWORK_CIDR} added to '${NETWORK_NAME}'."
            return 0
        fi
        if out2=$(hcloud network add-subnet \
            --network "$NETWORK_NAME" \
            --type cloud \
            --network-zone eu-central \
            --ip-range "$NETWORK_CIDR" \
            2>&1); then
            log_info "Subnetwork ${NETWORK_CIDR} added to '${NETWORK_NAME}' (flag form)."
            return 0
        fi
        die "Failed to add subnetwork to '${NETWORK_NAME}': positional: ${out1} | flag: ${out2}"
    fi

    log_info "Creating network '${NETWORK_NAME}' with CIDR ${NETWORK_CIDR}..."
    local net_output
    if ! net_output=$(hcloud network create \
        --name "$NETWORK_NAME" \
        --ip-range "$NETWORK_CIDR" \
        2>&1); then
        die "Failed to create network '${NETWORK_NAME}': ${net_output}"
    fi
    log_info "Network '${NETWORK_NAME}' created."
}

# ---------------------------------------------------------------------------
# Firewall helpers
#
# Hetzner VMs have NO firewall by default — every port is world-exposed.
# The SUT's OceanFS API (:9000) and gRPC (:9001) must only be reachable
# from the internal test network; Prometheus (:9090) is reached via an
# SSH tunnel only and stays closed. Managed firewalls are applied right
# after each VM is created, BEFORE the config steps (SSH must stay open).
# ---------------------------------------------------------------------------

# JSON rule file for the SUT firewall. The current hcloud CLI unmarshals
# the file directly into []schema.FirewallRule — a bare JSON array of
# rule objects (NOT a {"rules": [...]} wrapper; that fails with
# 'cannot unmarshal object into Go value of type []schema.FirewallRule').
sut_rules_json() {
    cat <<EOF
[
  {"direction": "in", "protocol": "tcp", "port": "22", "source_ips": ["${SSH_SOURCE_IP}", "${NETWORK_CIDR}", "::/0"]},
  {"direction": "in", "protocol": "tcp", "port": "9000", "source_ips": ["${NETWORK_CIDR}"]},
  {"direction": "in", "protocol": "tcp", "port": "9001", "source_ips": ["${NETWORK_CIDR}"]},
  {"direction": "in", "protocol": "icmp", "source_ips": ["0.0.0.0/0", "::/0"]}
]
EOF
}

# JSON rule file for the Harness firewall: SSH only (bare array, same
# contract as the SUT rules).
harness_rules_json() {
    cat <<EOF
[
  {"direction": "in", "protocol": "tcp", "port": "22", "source_ips": ["${SSH_SOURCE_IP}", "::/0"]},
  {"direction": "in", "protocol": "icmp", "source_ips": ["0.0.0.0/0", "::/0"]}
]
EOF
}

# Creates (or replaces the rules of) a managed firewall and applies it
# to the server. Idempotent: re-running with the same name prefix
# replaces the rules in place.
#
# The hcloud CLI was rewritten and changed three contracts: the rules
# file is JSON (not YAML), rule updates go through `replace-rules`
# (positional firewall), and applying uses `apply-to-resource`
# (singular, positional). Each step tries the current syntax first and
# falls back to the legacy one, reporting both errors on failure so CLI
# drift stays diagnosable.
ensure_firewall() {
    local fw_name="$1"
    local server_name="$2"
    local rules_json="$3"

    log_info "Ensuring firewall '${fw_name}' on server '${server_name}'..."

    local rules_file
    rules_file=$(mktemp)
    printf '%s' "$rules_json" > "$rules_file"

    local out1 out2
    if hcloud firewall describe "$fw_name" >/dev/null 2>&1; then
        if ! out1=$(hcloud firewall replace-rules --rules-file "$rules_file" "$fw_name" 2>&1); then
            if ! out2=$(hcloud firewall update "$fw_name" --rules-file "$rules_file" 2>&1); then
                rm -f "$rules_file"
                die "Failed to update firewall '${fw_name}': replace-rules: ${out1} | update: ${out2}"
            fi
        fi
        log_info "Firewall '${fw_name}' rules updated."
    else
        if ! out1=$(hcloud firewall create --name "$fw_name" --rules-file "$rules_file" 2>&1); then
            rm -f "$rules_file"
            die "Failed to create firewall '${fw_name}': ${out1}"
        fi
        log_info "Firewall '${fw_name}' created."
    fi
    rm -f "$rules_file"

    if ! out1=$(hcloud firewall apply-to-resource --type server --server "$server_name" "$fw_name" 2>&1); then
        if ! out2=$(hcloud firewall apply-to-resources --firewall "$fw_name" --server "$server_name" 2>&1); then
            die "Failed to apply firewall '${fw_name}' to '${server_name}': apply-to-resource: ${out1} | apply-to-resources: ${out2}"
        fi
    fi
    log_info "Firewall '${fw_name}' applied to '${server_name}'."
}

create_vm() {
    local name="$1"
    local type="$2"
    local image="$3"
    local ssh_key="$4"

    log_info "Creating VM '${name}' (type=${type}, image=${image})..."

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] hcloud server create --name '${name}' --type '${type}' --image '${image}' --network '${NETWORK_NAME}' --ssh-key '${ssh_key}'"
        DRY_RUN_VMS+=("${name} ${type} ${image}")
        return 0
    fi

    # Fail fast with a clear message when the name is already taken
    # (e.g. a leftover from a manual attempt) instead of an opaque
    # hcloud "name already exists" error mid-provisioning.
    if hcloud server describe "$name" >/dev/null 2>&1; then
        die "VM '${name}' already exists. Re-run with a different --name prefix, or remove it: hcloud server delete ${name}"
    fi

    # Capture hcloud's output so a failure reports the REAL reason
    # (quota, location, image, key, permissions) instead of silence.
    local create_output
    if ! create_output=$(hcloud server create \
        --name "$name" \
        --type "$type" \
        --image "$image" \
        --network "$NETWORK_NAME" \
        --ssh-key "$ssh_key" \
        2>&1); then
        die "Failed to create VM '${name}': ${create_output}"
    fi

    CREATED_VM_NAMES+=("$name")
    log_info "VM '${name}' creation initiated."

    wait_for_vm "$name"

    # Get IPs
    local ips
    ips=$(get_vm_ips "$name")
    local private_ip="${ips%% *}"
    local public_ip="${ips##* }"

    echo "${private_ip} ${public_ip}"
}

# Waits for SSH to become available on a freshly created VM.
#
# 90 retries x 5s = 7.5 min budget: first boot (cloud-init, sshd
# startup) is the slowest provisioning step and varies per VM/location
# (observed: the second VM can exceed the old 150s budget). BatchMode
# makes key problems fail fast instead of hanging on a password prompt.
wait_for_ssh() {
    local public_ip="$1"
    local ssh_retries=90
    local ssh_retry=0
    while [ "$ssh_retry" -lt "$ssh_retries" ]; do
        if ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=yes \
               "root@${public_ip}" "echo ok" >/dev/null 2>&1; then
            log_info "SSH to ${public_ip} is up (attempt $((ssh_retry + 1)))."
            return 0
        fi
        ssh_retry=$((ssh_retry + 1))
        if [ $((ssh_retry % 15)) -eq 0 ]; then
            log_info "Still waiting for SSH on ${public_ip} (${ssh_retry}/${ssh_retries})..."
        fi
        sleep 5
    done
    log_error "SSH connection to ${public_ip} timed out after ${ssh_retries} attempts."
    log_error "Check the VM state: hcloud server describe --output json <name> | jq '.status'"
    log_error "Check the firewall allows tcp/22 from your IP: hcloud firewall describe <name>-fw"
    return 1
}

# Directory this script lives in (for locating sibling scripts like
# setup-observability.sh regardless of the invocation cwd).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Installs the Prometheus observability stack on the SUT (scrape job for
# :9000/admin/metrics + node exporter + textfile collector). Idempotent;
# run by the provisioner by default and re-ensured by setup-harness.sh.
install_observability() {
    local public_ip="$1"
    # Optional second arg: comma-separated scrape targets for the OceanFS
    # job (ADR-0026 fleet mode — node 0 scrapes every node). Unset/empty
    # keeps the historical single-SUT localhost:9000 scrape.
    local scrape_targets="${2:-}"
    log_info "Installing observability stack on ${public_ip}..."
    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] scp ${SCRIPT_DIR}/setup-observability.sh -> root@${public_ip}:/root/ && run it"
        return 0
    fi
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 \
        "${SCRIPT_DIR}/setup-observability.sh" "root@${public_ip}:/root/setup-observability.sh" \
        || { log_error "Failed to copy setup-observability.sh to ${public_ip}"; return 1; }
    local obs_args=()
    if [ -n "$scrape_targets" ]; then
        obs_args=(--scrape-targets "$scrape_targets")
    fi
    if ! ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" \
        "bash /root/setup-observability.sh ${obs_args[*]+"${obs_args[*]}"}"; then
        log_error "Observability setup failed on ${public_ip} (Prometheus unavailable; the harness's own scrape still covers the run)."
        return 1
    fi
    log_info "Observability stack installed on ${public_ip} (Prometheus :9090 — tunnel-only)."
}

# Configure SUT VM (minimal — no Rust toolchain)
# Optional second arg: comma-separated OceanFS scrape targets for node 0's
# Prometheus (fleet mode, ADR-0026).
configure_sut_vm() {
    local name="$1"
    local public_ip="$2"
    local scrape_targets="${3:-}"

    log_info "Configuring SUT VM '${name}' (${public_ip})..."

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] ssh root@${public_ip} apt-get update && apt-get install -y curl"
        log_info "[DRY-RUN] (No Rust toolchain installed on SUT VM)"
        return 0
    fi

    wait_for_ssh "$public_ip" || return 1

    # Install minimal dependencies (curl for Prometheus setup script)
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" <<'SUT_SETUP'
set -euo pipefail
apt-get update -qq
apt-get install -y -qq curl
SUT_SETUP
    if [ "$OBSERVABILITY" = true ]; then
        install_observability "$public_ip" "$scrape_targets" || log_warn "Observability install failed (non-fatal)."
    else
        log_warn "Observability DISABLED (--no-observability) — Prometheus will not be installed."
    fi
    log_info "SUT VM '${name}' configured (curl + observability, no Rust toolchain)."
}

# Configure Harness VM (Rust toolchain + repo + build)
configure_harness_vm() {
    local name="$1"
    local public_ip="$2"

    log_info "Configuring Harness VM '${name}' (${public_ip})..."

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] ssh root@${public_ip} apt-get update && apt-get install -y build-essential pkg-config libssl-dev curl"
        log_info "[DRY-RUN] ssh root@${public_ip} curl ... rustup ..."
        log_info "[DRY-RUN] ssh root@${public_ip} git clone ... --branch ${BRANCH}"
        if [ -n "$COMMIT" ]; then
            log_info "[DRY-RUN] ssh root@${public_ip} git checkout ${COMMIT}"
        fi
        log_info "[DRY-RUN] ssh root@${public_ip} cargo build --release -p oceanfs -p e2e"
        return 0
    fi

    wait_for_ssh "$public_ip" || return 1

    # Install system dependencies
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" <<HARNESS_SETUP
set -euo pipefail
apt-get update -qq
# protobuf-compiler:   required by oceanfs-core's prost-build (protoc)
# libclang-dev:        required by bindgen (zstd-sys etc.)
# librocksdb-dev:      required by the workspace .cargo/config.toml — the
#                      crate links the SYSTEM rocksdb instead of compiling
#                      ~500 KLoC of C++ (PIPELINE.md §4.5); without it the
#                      build fails on a 2-vCPU/4GB CX23
apt-get install -y -qq build-essential pkg-config libssl-dev curl git protobuf-compiler libclang-dev librocksdb-dev
HARNESS_SETUP

    # Install Rust
    log_info "Installing Rust toolchain on Harness VM..."
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" \
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y" \
        || log_error "Rust installation failed on Harness VM."

    # Clone repo
    log_info "Cloning repository (branch=${BRANCH}) on Harness VM..."
    if [ -n "$COMMIT" ]; then
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" \
            "git clone ${REPO_URL} --branch ${BRANCH} /root/ocean-fs && cd /root/ocean-fs && git checkout ${COMMIT}" \
            || log_error "Git clone/checkout failed on Harness VM."
    else
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" \
            "git clone ${REPO_URL} --branch ${BRANCH} /root/ocean-fs" \
            || log_error "Git clone failed on Harness VM."
    fi

    # Build
    log_info "Building oceanfs + e2e on Harness VM... (this may take several minutes)"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" \
        "source /root/.cargo/env && cd /root/ocean-fs && cargo build --release -p oceanfs -p e2e" \
        || log_error "Build failed on Harness VM."

    log_info "Harness VM '${name}' configured."
}

setup_ttl_timer() {
    local name="$1"
    local public_ip="$2"

    log_info "Setting up TTL timer (${TTL_HOURS}h) on VM '${name}' (${public_ip})..."

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] Setup systemd oceanfs-ttl.timer on ${public_ip} (TTL=${TTL_HOURS}h)"
        return 0
    fi

    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" <<TTL_SETUP
set -euo pipefail

# Create the auto-shutdown service
cat > /etc/systemd/system/oceanfs-ttl.service <<'SERVICE_UNIT'
[Unit]
Description=Auto-shutdown OceanFS test VM after TTL expiry
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/hcloud server poweroff \$(hostname)
RemainAfterExit=no
SERVICE_UNIT

# Create the timer unit
cat > /etc/systemd/system/oceanfs-ttl.timer <<TIMER_UNIT
[Unit]
Description=TTL timer for OceanFS test VM (${TTL_HOURS}h)
After=network.target

[Timer]
OnBootSec=${TTL_HOURS}h
OnUnitActiveSec=${TTL_HOURS}h
Persistent=false

[Install]
WantedBy=timers.target
TIMER_UNIT

# Create a wrapper script for hcloud if needed
mkdir -p /usr/local/bin
cat > /usr/local/bin/hcloud <<'HCLOUD_WRAPPER'
#!/usr/bin/env bash
# Wrapper to run hcloud commands. The hcloud CLI should be installed on the VM
# or the API token should be made available.
# In practice, this relies on hcloud CLI being installed on the VM image,
# or the token being injected at provision time.
exec /usr/bin/hcloud "\$@"
HCLOUD_WRAPPER
chmod +x /usr/local/bin/hcloud 2>/dev/null || true

systemctl daemon-reload
systemctl enable --now oceanfs-ttl.timer

logger -t oceanfs-ttl "TTL timer enabled: ${TTL_HOURS}h auto-shutdown for VM $(hostname)"
TTL_SETUP

    # Verify timer is active
    local timer_status
    timer_status=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "root@${public_ip}" \
        "systemctl is-active oceanfs-ttl.timer" 2>/dev/null || echo "inactive")
    if [ "$timer_status" = "active" ]; then
        log_info "TTL timer active on '${name}'."
    else
        log_warn "TTL timer status on '${name}': ${timer_status}. Manual verification recommended."
    fi
}

# ---------------------------------------------------------------------------
# Cleanup on failure
# ---------------------------------------------------------------------------

cleanup() {
    if [ "$KEEP_ON_FAILURE" = true ]; then
        log_warn "KEEP_ON_FAILURE set — leaving created VMs in place for inspection:"
        for vm_name in "${CREATED_VM_NAMES[@]:-}"; do
            log_warn "  ${vm_name}"
        done
        return 0
    fi
    if [ ${#CREATED_VM_NAMES[@]} -gt 0 ]; then
        log_warn "Cleanup: attempting to delete VMs created during this run..."
        for vm_name in "${CREATED_VM_NAMES[@]}"; do
            log_info "Deleting VM '${vm_name}'..."
            hcloud server delete "$vm_name" >/dev/null 2>&1 || log_warn "Failed to delete VM '${vm_name}'. Manual cleanup may be needed."
        done
        CREATED_VM_NAMES=()
    fi
}

# ---------------------------------------------------------------------------
# Destroy VMs
# ---------------------------------------------------------------------------

destroy_vms() {
    local prefix="$1"

    log_info "Looking for VMs matching '${prefix}-sut*' and '${prefix}-harness'..."

    local sut_exists=false
    local harness_exists=false

    # Delete every SUT VM (single '${prefix}-sut' for phase 2, or the
    # '${prefix}-sut-0..N-1' fleet for phase 3+ per ADR-0026).
    local sut_vms
    sut_vms=$(hcloud server list --output json 2>/dev/null | jq -r \
        --arg p "${prefix}-sut" '.[].name | select(startswith($p))' || true)
    if [ -n "$sut_vms" ]; then
        sut_exists=true
        while IFS= read -r name; do
            [ -n "$name" ] || continue
            log_info "Deleting VM '${name}'..."
            hcloud server delete "$name" || log_error "Failed to delete VM '${name}'."
        done <<< "$sut_vms"
    else
        log_info "No SUT VM matching '${prefix}-sut*' found."
    fi

    # Check and delete Harness VM
    if hcloud server describe "${prefix}-harness" --output json >/dev/null 2>&1; then
        harness_exists=true
        log_info "Deleting VM '${prefix}-harness'..."
        hcloud server delete "${prefix}-harness" || log_error "Failed to delete VM '${prefix}-harness'."
    else
        log_info "VM '${prefix}-harness' not found."
    fi

    if [ "$sut_exists" = false ] && [ "$harness_exists" = false ]; then
        log_warn "No VMs matching '${prefix}' found."
    else
        log_info "Destroy complete for prefix '${prefix}'."
    fi
}

# ---------------------------------------------------------------------------
# Status check
# ---------------------------------------------------------------------------

check_status() {
    local prefix="$1"

    local sut_json sut_status sut_ip sut_public_ip sut_type
    local harness_json harness_status harness_ip harness_public_ip harness_type

    sut_json=$(hcloud server describe "${prefix}-sut" --output json 2>/dev/null || true)
    harness_json=$(hcloud server describe "${prefix}-harness" --output json 2>/dev/null || true)

    # SUT
    if [ -n "$sut_json" ]; then
        sut_status=$(echo "$sut_json" | jq -r '.status // "unknown"')
        sut_ip=$(echo "$sut_json" | jq -r '.private_net[0].ip // ""')
        sut_public_ip=$(echo "$sut_json" | jq -r '.public_net.ipv4.ip // ""')
        sut_type=$(echo "$sut_json" | jq -r '.server_type // ""')
    else
        sut_status="not_found"
        sut_ip=""
        sut_public_ip=""
        sut_type=""
    fi

    # Harness
    if [ -n "$harness_json" ]; then
        harness_status=$(echo "$harness_json" | jq -r '.status // "unknown"')
        harness_ip=$(echo "$harness_json" | jq -r '.private_net[0].ip // ""')
        harness_public_ip=$(echo "$harness_json" | jq -r '.public_net.ipv4.ip // ""')
        harness_type=$(echo "$harness_json" | jq -r '.server_type // ""')
    else
        harness_status="not_found"
        harness_ip=""
        harness_public_ip=""
        harness_type=""
    fi

    # Output JSON — sut_nodes[] (phase 3+ fleet) or sut (phase 2)
    local sut_json_part
    local sut_vms
    sut_vms=$(hcloud server list --output json 2>/dev/null | jq -r \
        --arg p "${prefix}-sut" '.[].name | select(startswith($p))' || true)

    if [ -n "$sut_vms" ]; then
        # Fleet (or legacy single SUT): one object per VM, sorted.
        sut_json_part=$(hcloud server list --output json 2>/dev/null | jq \
            --arg p "${prefix}-sut" \
            '[.[] | select(.name | startswith($p)) | sort_by(.name)[] | {
                name: .name,
                status: .status,
                internal_ip: (.private_net[0].ip // ""),
                public_ip: .public_net.ipv4.ip,
                type: .server_type
            }]' || echo "[]")
        if [ "$(echo "$sut_json_part" | jq 'length')" = "1" ]; then
            # Phase 2 shape: single `sut` object (backward compatible).
            sut_json_part=$(echo "$sut_json_part" | jq '.[0]')
            jq -n \
                --argjson sut "$sut_json_part" \
                --arg harness_status "$harness_status" \
                --arg harness_ip "$harness_ip" \
                --arg harness_public_ip "$harness_public_ip" \
                --arg harness_name "${prefix}-harness" \
                --arg harness_type "$harness_type" \
                --arg name "$prefix" \
                '{
                    name: $name,
                    sut: $sut,
                    harness: {
                        name: $harness_name,
                        status: $harness_status,
                        internal_ip: $harness_ip,
                        public_ip: $harness_public_ip,
                        type: $harness_type
                    }
                }'
        else
            jq -n \
                --argjson sut_nodes "$sut_json_part" \
                --arg harness_status "$harness_status" \
                --arg harness_ip "$harness_ip" \
                --arg harness_public_ip "$harness_public_ip" \
                --arg harness_name "${prefix}-harness" \
                --arg harness_type "$harness_type" \
                --arg name "$prefix" \
                '{
                    name: $name,
                    sut_nodes: $sut_nodes,
                    harness: {
                        name: $harness_name,
                        status: $harness_status,
                        internal_ip: $harness_ip,
                        public_ip: $harness_public_ip,
                        type: $harness_type
                    }
                }'
        fi
    else
        jq -n \
            --arg sut_status "$sut_status" \
            --arg sut_ip "$sut_ip" \
            --arg sut_public_ip "$sut_public_ip" \
            --arg sut_name "${prefix}-sut" \
            --arg sut_type "$sut_type" \
            --arg harness_status "$harness_status" \
            --arg harness_ip "$harness_ip" \
            --arg harness_public_ip "$harness_public_ip" \
            --arg harness_name "${prefix}-harness" \
            --arg harness_type "$harness_type" \
            --arg name "$prefix" \
            '{
                name: $name,
                sut: {
                    name: $sut_name,
                    status: $sut_status,
                    internal_ip: $sut_ip,
                    public_ip: $sut_public_ip,
                    type: $sut_type
                },
                harness: {
                    name: $harness_name,
                    status: $harness_status,
                    internal_ip: $harness_ip,
                    public_ip: $harness_public_ip,
                    type: $harness_type
                }
            }'
    fi
}

# ---------------------------------------------------------------------------
# Core provisioning orchestration
# ---------------------------------------------------------------------------

provision_vms() {
    local sut_name="${NAME_PREFIX}-sut"
    local harness_name="${NAME_PREFIX}-harness"

    # Single-VM mode: Phase 2 only (ADR-0026 removed the Phase 3-4
    # single-VM fallback — co-location corrupts gossip timing).
    if [ "$SINGLE_VM" = true ]; then
        if [ "$PHASE" = "2" ]; then
            log_info "Single-VM mode: co-locating SUT + Harness on one CX33."
            log_info "Reports will be written to /tmp (tmpfs) to avoid disk I/O contention."
        elif [ "$PHASE" = "3" ] || [ "$PHASE" = "4" ]; then
            die "--single-vm is not supported for phase 3-4 (superseded by ADR-0026). Use the dedicated node-VM fleet: --phase $PHASE --nodes $CLUSTER_NODES."
        fi
    fi

    # Create network (idempotent)
    if [ "$DRY_RUN" = false ]; then
        create_network
    else
        log_info "[DRY-RUN] Ensure network '${NETWORK_NAME}' exists."
    fi

    # ── Phase 3+ cluster fleet (ADR-0026) ────────────────────────────────
    # Create ALL node VMs first (so node 0's Prometheus can scrape the
    # full fleet), then configure each. Node 0 is the bootstrap; it hosts
    # Prometheus scraping every node endpoint.
    if [ "$PHASE" = "3" ] || [ "$PHASE" = "4" ]; then
        if [ -n "$SUT_TYPE" ]; then
            # 1. Create + firewall every node VM.
            for ((i = 0; i < CLUSTER_NODES; i++)); do
                local node_name="${NAME_PREFIX}-sut-${i}"
                log_info "Provisioning SUT node ${i}: name=${node_name}, type=${SUT_TYPE}..."
                local node_ips
                node_ips=$(create_vm "$node_name" "$SUT_TYPE" "$IMAGE" "$SSH_KEY_PATH")
                SUT_NODE_NAMES+=("$node_name")
                SUT_NODE_IPS+=("${node_ips%% *}")
                SUT_NODE_PUBLIC_IPS+=("${node_ips##* }")
                log_info "SUT node ${i}: internal_ip=${SUT_NODE_IPS[$i]}, public_ip=${SUT_NODE_PUBLIC_IPS[$i]}"

                if [ "$DRY_RUN" = false ]; then
                    if [ "$FIREWALLS" = true ]; then
                        ensure_firewall "${NAME_PREFIX}-sut-fw" "$node_name" "$(sut_rules_json)"
                    else
                        log_warn "Firewalls DISABLED (--no-firewall) — ${node_name} is exposed to the internet."
                    fi
                else
                    if [ "$FIREWALLS" = true ]; then
                        log_info "[DRY-RUN] Create/update firewall '${NAME_PREFIX}-sut-fw' and apply to '${node_name}'"
                    fi
                fi
            done

            # 2. Configure each node. Node 0's Prometheus scrapes the whole
            #    fleet (localhost + peers over the internal network).
            local scrape_targets=""
            for ((i = 0; i < CLUSTER_NODES; i++)); do
                scrape_targets=""
                if [ "$OBSERVABILITY" = true ] && [ "$i" -eq 0 ]; then
                    scrape_targets="localhost:9000"
                    for ((j = 1; j < CLUSTER_NODES; j++)); do
                        scrape_targets="${scrape_targets},${SUT_NODE_IPS[$j]}:9000"
                    done
                fi
                if [ "$DRY_RUN" = false ]; then
                    configure_sut_vm "${SUT_NODE_NAMES[$i]}" "${SUT_NODE_PUBLIC_IPS[$i]}" "$scrape_targets" \
                        || die "SUT node ${i} configuration failed."
                    setup_ttl_timer "${SUT_NODE_NAMES[$i]}" "${SUT_NODE_PUBLIC_IPS[$i]}" \
                        || log_warn "TTL timer setup failed on ${SUT_NODE_NAMES[$i]}. Manual TTL enforcement may be needed."
                else
                    log_info "[DRY-RUN] Configure ${SUT_NODE_NAMES[$i]}${scrape_targets:+ (scrape: $scrape_targets)} + TTL"
                fi
            done

            # Single-VM compatibility view: node 0 is "the SUT".
            SUT_NAME="${SUT_NODE_NAMES[0]}"
            SUT_IP="${SUT_NODE_IPS[0]}"
            SUT_PUBLIC_IP="${SUT_NODE_PUBLIC_IPS[0]}"
        fi
    # ── Phase 2 single-SUT path (unchanged) ──────────────────────────────
    elif [ -n "$SUT_TYPE" ]; then
        log_info "Provisioning SUT VM: type=${SUT_TYPE}..."
        local sut_ips
        sut_ips=$(create_vm "$sut_name" "$SUT_TYPE" "$IMAGE" "$SSH_KEY_PATH")
        SUT_IP="${sut_ips%% *}"
        SUT_PUBLIC_IP="${sut_ips##* }"
        SUT_NAME="$sut_name"

        log_info "SUT VM: name=${SUT_NAME}, internal_ip=${SUT_IP}, public_ip=${SUT_PUBLIC_IP}"

        if [ "$DRY_RUN" = false ]; then
            # Firewall BEFORE configuration: SSH stays open (rule above),
            # everything else is denied from the internet by default.
            if [ "$FIREWALLS" = true ]; then
                ensure_firewall "${NAME_PREFIX}-sut-fw" "$sut_name" "$(sut_rules_json)"
            else
                log_warn "Firewalls DISABLED (--no-firewall) — ${sut_name} is exposed to the internet."
            fi
            configure_sut_vm "$sut_name" "$SUT_PUBLIC_IP" || die "SUT VM configuration failed."
            setup_ttl_timer "$sut_name" "$SUT_PUBLIC_IP" || log_warn "TTL timer setup failed on SUT VM. Manual TTL enforcement may be needed."
        else
            if [ "$FIREWALLS" = true ]; then
                log_info "[DRY-RUN] Create/update firewall '${NAME_PREFIX}-sut-fw' and apply to '${sut_name}'"
            fi
        fi
    fi

    # Harness VM provisioning (skip if single-VM mode and SUT was provisioned)
    if [ "$SINGLE_VM" = true ]; then
        log_info "Single-VM mode: Harness co-located on SUT VM. Skipping separate Harness VM provisioning."
        HARNESS_TYPE=""
        HARNESS_NAME=""
        HARNESS_IP=""
        HARNESS_PUBLIC_IP=""
    elif [ -n "$HARNESS_TYPE" ]; then
        log_info "Provisioning Harness VM: type=${HARNESS_TYPE}..."
        local harness_ips
        harness_ips=$(create_vm "$harness_name" "$HARNESS_TYPE" "$IMAGE" "$SSH_KEY_PATH")
        HARNESS_IP="${harness_ips%% *}"
        HARNESS_PUBLIC_IP="${harness_ips##* }"
        HARNESS_NAME="$harness_name"

        log_info "Harness VM: name=${HARNESS_NAME}, internal_ip=${HARNESS_IP}, public_ip=${HARNESS_PUBLIC_IP}"

        if [ "$DRY_RUN" = false ]; then
            if [ "$FIREWALLS" = true ]; then
                ensure_firewall "${NAME_PREFIX}-harness-fw" "$harness_name" "$(harness_rules_json)"
            else
                log_warn "Firewalls DISABLED (--no-firewall) — ${harness_name} is exposed to the internet."
            fi
            configure_harness_vm "$harness_name" "$HARNESS_PUBLIC_IP" || die "Harness VM configuration failed."
            setup_ttl_timer "$harness_name" "$HARNESS_PUBLIC_IP" || log_warn "TTL timer setup failed on Harness VM. Manual TTL enforcement may be needed."
        else
            if [ "$FIREWALLS" = true ]; then
                log_info "[DRY-RUN] Create/update firewall '${NAME_PREFIX}-harness-fw' and apply to '${harness_name}'"
            fi
        fi
    fi
}

# ---------------------------------------------------------------------------
# Output JSON with VM details
# ---------------------------------------------------------------------------

output_json() {
    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] Would output JSON with VM details. Simulated VMs:"
        for vm_info in "${DRY_RUN_VMS[@]}"; do
            log_info "  ${vm_info}"
        done
        return 0
    fi

    local sut_json harness_json

    if [ "$SINGLE_VM" = true ]; then
        # In single-VM mode, both SUT and harness reference the same VM
        jq -n \
            --arg sut_ip "${SUT_IP:-}" \
            --arg sut_public_ip "${SUT_PUBLIC_IP:-}" \
            --arg sut_name "${SUT_NAME:-}" \
            --arg sut_type "${SUT_TYPE:-}" \
            --arg harness_ip "${SUT_IP:-}" \
            --arg harness_public_ip "${SUT_PUBLIC_IP:-}" \
            --arg harness_name "${SUT_NAME:-}-harness" \
            --arg harness_type "${SUT_TYPE:-}" \
            --argjson phase "$PHASE" \
            --arg provider "$PROVIDER" \
            --arg network "$NETWORK_CIDR" \
            --argjson ttl_hours "$TTL_HOURS" \
            --argjson single_vm true \
            '{
                sut: { ip: $sut_ip, public_ip: $sut_public_ip, name: $sut_name, type: $sut_type, internal_ip: $sut_ip },
                harness: { ip: $harness_ip, public_ip: $harness_public_ip, name: $harness_name, type: $harness_type, internal_ip: $harness_ip },
                phase: $phase,
                provider: $provider,
                network: $network,
                ttl_hours: $ttl_hours,
                single_vm: $single_vm
            }'
    else
        # Phase 3+ fleet (ADR-0026): sut_nodes[] array (one entry per node
        # VM). Phase 2 keeps the legacy single `sut` object.
        if [ "$PHASE" = "3" ] || [ "$PHASE" = "4" ]; then
            local nodes_json="[]"
            for ((i = 0; i < CLUSTER_NODES; i++)); do
                nodes_json=$(jq -n \
                    --argjson acc "$nodes_json" \
                    --arg name "${SUT_NODE_NAMES[$i]:-}" \
                    --arg ip "${SUT_NODE_IPS[$i]:-}" \
                    --arg public_ip "${SUT_NODE_PUBLIC_IPS[$i]:-}" \
                    --arg type "${SUT_TYPE:-}" \
                    '$acc + [{ name: $name, ip: $ip, public_ip: $public_ip, internal_ip: $ip, type: $type }]')
            done
            jq -n \
                --argjson sut_nodes "$nodes_json" \
                --arg harness_ip "${HARNESS_IP:-}" \
                --arg harness_public_ip "${HARNESS_PUBLIC_IP:-}" \
                --arg harness_name "${HARNESS_NAME:-}" \
                --arg harness_type "${HARNESS_TYPE:-}" \
                --arg harness_internal_ip "${HARNESS_IP:-}" \
                --argjson phase "$PHASE" \
                --arg provider "$PROVIDER" \
                --arg network "$NETWORK_CIDR" \
                --argjson ttl_hours "$TTL_HOURS" \
                --argjson cluster_nodes "$CLUSTER_NODES" \
                '{
                    sut_nodes: $sut_nodes,
                    harness: { ip: $harness_ip, public_ip: $harness_public_ip, name: $harness_name, type: $harness_type, internal_ip: $harness_internal_ip },
                    phase: $phase,
                    provider: $provider,
                    network: $network,
                    ttl_hours: $ttl_hours,
                    cluster_nodes: $cluster_nodes
                }'
        else
            jq -n \
                --arg sut_ip "${SUT_IP:-}" \
                --arg sut_public_ip "${SUT_PUBLIC_IP:-}" \
                --arg sut_name "${SUT_NAME:-}" \
                --arg sut_type "${SUT_TYPE:-}" \
                --arg sut_internal_ip "${SUT_IP:-}" \
                --arg harness_ip "${HARNESS_IP:-}" \
                --arg harness_public_ip "${HARNESS_PUBLIC_IP:-}" \
                --arg harness_name "${HARNESS_NAME:-}" \
                --arg harness_type "${HARNESS_TYPE:-}" \
                --arg harness_internal_ip "${HARNESS_IP:-}" \
                --argjson phase "$PHASE" \
                --arg provider "$PROVIDER" \
                --arg network "$NETWORK_CIDR" \
                --argjson ttl_hours "$TTL_HOURS" \
                '{
                    sut: { ip: $sut_ip, public_ip: $sut_public_ip, name: $sut_name, type: $sut_type, internal_ip: $sut_internal_ip },
                    harness: { ip: $harness_ip, public_ip: $harness_public_ip, name: $harness_name, type: $harness_type, internal_ip: $harness_internal_ip },
                    phase: $phase,
                    provider: $provider,
                    network: $network,
                    ttl_hours: $ttl_hours
                }'
        fi
    fi
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

usage() {
    cat <<'HELP'
Usage: ./scripts/vm-provision.sh [OPTIONS]

Provision the load-test topology (SUT + Harness) for OceanFS per ADR-0019
and ADR-0026.

OPTIONS:
  --phase N            Load test phase (1-6). Determines VM sizes. [required]
                       Phase 1: N/A (runs in CI, no cloud VMs)
                       Phase 2: SUT=CX33, Harness=CX23
                       Phase 3-4: N SUT node VMs (default 3, each CX33,
                       named {prefix}-sut-0..N-1) + Harness=CX43
                       Phase 5+: N/A (separate provisioning model)
  --nodes N            Cluster fleet size for phase 3-4 (default: 3, or
                       LOAD_TEST_CLUSTER_NODES; range 3-5)
  --provider NAME      Cloud provider: hetzner (default), gcp, aws
  --branch BRANCH      Git branch to clone on Harness VM (default: main)
  --repo URL           Git repo to clone on Harness VM (default: github.com/ndY0/ocean-fs)
  --commit SHA         Specific commit to check out on Harness VM
  --ssh-key PATH       SSH public key path (default: .hetzner/.ssh/
                       hetzner-ssh.pub when present, else ~/.ssh/id_rsa.pub)
  --name NAME          VM name prefix (default: oceanfs-loadtest-{phase})
  --image IMAGE        OS image (default: ubuntu-24.04)
  --single-vm          Co-locate SUT+Harness on single VM
                       (Phase 2 only; Phase 3-4 prints warning per ADR-0019)
  --confirm yes        Accepted for compatibility; no-op (confirmation gate removed)
  --ttl HOURS          Auto-shutdown TTL (default: 4, or LOAD_TEST_TTL_HOURS)
  --dry-run            Print actions without executing
  --debug              Enable shell tracing (set -x) for full visibility
  --keep-on-failure    Keep already-created VMs when a later step fails
                       (default: cleanup deletes them)
  --no-firewall        Do NOT create/apply Hetzner managed firewalls
  --no-observability   Do NOT install Prometheus on the SUT (default:
                       installed; scrape :9000 + node exporter, :9090
                       is tunnel-only)
                       (default: applied — SUT: SSH + internal-net
                       9000/9001 only; Harness: SSH only)
  --ssh-source-ip IP   CIDR allowed to SSH into the VMs (default:
                       0.0.0.0/0). The SUT additionally allows the
                       internal test network for harness crash control.
  --destroy NAME       Tear down all VMs with given name prefix
                       ({prefix}-sut, {prefix}-sut-0..N-1, {prefix}-harness)
  --status NAME        Check status of all VMs with given name prefix
  -h, --help           Show this help

Environment Variables:
  HCLOUD_TOKEN              Hetzner Cloud API token (required for hetzner;
                            auto-loaded from .hetzner/.env by
                            lib/env-hetzner.sh)
  LOAD_TEST_TTL_HOURS       Override default TTL (default: 4)
  LOAD_TEST_CLUSTER_NODES   Override cluster fleet size for phase 3-4
                            (default: 3)
  LOAD_TEST_MAX_MONTHLY_EUR Optional monthly budget cap (deferrable, v2)

Guardrails:
  - Hard VM size cap: VMs >= CX53 require manual provisioning (harness up
    to CX43 per ADR-0026; SUT nodes capped at cx33)
  - Confirmation gate: removed (CX33 is the standard sizing for phases 2-4)
  - Auto-shutdown TTL: systemd timer powers off VMs after TTL expires
  - Budget gate: optional LOAD_TEST_MAX_MONTHLY_EUR scaffolding

Output: JSON with sut (phase 2) or sut_nodes array (phase 3-4) + harness
VM objects on stdout.

Examples:
  vm-provision.sh --phase 2 --branch feature/test
  vm-provision.sh --phase 3
  vm-provision.sh --phase 3 --nodes 5
  vm-provision.sh --phase 1
  vm-provision.sh --destroy oceanfs-loadtest-2
  vm-provision.sh --status oceanfs-loadtest-2
  vm-provision.sh --phase 2 --dry-run
  vm-provision.sh --phase 2 --ttl 2 --single-vm
HELP
}

# ---------------------------------------------------------------------------
# Prerequisites check
# ---------------------------------------------------------------------------

check_prerequisites() {
    local missing=()

    if ! command -v hcloud &>/dev/null; then
        missing+=("hcloud CLI")
    fi

    if ! command -v jq &>/dev/null; then
        missing+=("jq")
    fi

    if ! command -v ssh &>/dev/null; then
        missing+=("ssh")
    fi

    if [ ${#missing[@]} -gt 0 ]; then
        die "Missing required tools: ${missing[*]}. Please install them before running this script."
    fi

    # Check HCLOUD_TOKEN
    if [ -z "${HCLOUD_TOKEN:-}" ]; then
        # hcloud may be configured via config file
        if ! hcloud server list --output json >/dev/null 2>&1; then
            die "HCLOUD_TOKEN environment variable is not set and hcloud is not configured." \
                "Set HCLOUD_TOKEN or run 'hcloud context create' to authenticate."
        fi
    fi

    # Check SSH key
    if [ -z "$SSH_KEY_PATH" ]; then
        die "SSH key path could not be resolved."
    fi
    if [ ! -f "$SSH_KEY_PATH" ]; then
        die "SSH public key not found at: ${SSH_KEY_PATH}"
    fi
}

# ---------------------------------------------------------------------------
# Main — argument parsing and dispatch
# ---------------------------------------------------------------------------

DRY_RUN_VMS=()

# Resolve SSH key path early for prerequisite check
SSH_KEY_PATH=""

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --phase)
                PHASE="${2:-}"
                shift 2
                ;;
            --nodes)
                CLUSTER_NODES="${2:-3}"
                shift 2
                ;;
            --provider)
                PROVIDER="${2:-hetzner}"
                shift 2
                ;;
            --branch)
                BRANCH="${2:-main}"
                shift 2
                ;;
            --repo)
                REPO_URL="${2:-}"
                shift 2
                ;;
            --commit)
                COMMIT="${2:-}"
                shift 2
                ;;
            --ssh-key)
                SSH_KEY_PATH="${2:-}"
                shift 2
                ;;
            --name)
                NAME_PREFIX="${2:-}"
                shift 2
                ;;
            --image)
                IMAGE="${2:-ubuntu-24.04}"
                shift 2
                ;;
            --single-vm)
                SINGLE_VM=true
                shift
                ;;
            --confirm)
                CONFIRM="${2:-}"
                shift 2
                ;;
            --ttl)
                TTL_HOURS="${2:-4}"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --debug)
                DEBUG=true
                shift
                ;;
            --keep-on-failure)
                KEEP_ON_FAILURE=true
                shift
                ;;
            --no-firewall)
                FIREWALLS=false
                shift
                ;;
            --no-observability)
                OBSERVABILITY=false
                shift
                ;;
            --ssh-source-ip)
                SSH_SOURCE_IP="${2:-}"
                shift 2
                ;;
            --destroy)
                DESTROY_NAME="${2:-}"
                shift 2
                ;;
            --status)
                STATUS_NAME="${2:-}"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                die "Unknown option: $1. Use --help for usage."
                ;;
        esac
    done
}

main() {
    parse_args "$@"

    # Full shell tracing for debugging: shows every command incl. hcloud
    # invocations (stderr is no longer suppressed under set -x since the
    # captured-output paths still report failures with details).
    if [ "$DEBUG" = true ]; then
        set -x
    fi

    # Resolve SSH key default: .hetzner/.ssh/<key>.pub (set by
    # lib/env-hetzner.sh), else the conventional ~/.ssh/id_rsa.pub.
    if [ -z "$SSH_KEY_PATH" ]; then
        SSH_KEY_PATH="${HETZNER_SSH_PUBLIC_KEY:-${HOME}/.ssh/id_rsa.pub}"
    fi

    # Handle --destroy
    if [ -n "$DESTROY_NAME" ]; then
        check_prerequisites
        destroy_vms "$DESTROY_NAME"
        exit 0
    fi

    # Handle --status
    if [ -n "$STATUS_NAME" ]; then
        check_prerequisites
        check_status "$STATUS_NAME"
        exit 0
    fi

    # --phase is required for provisioning
    if [ -z "$PHASE" ]; then
        die "--phase is required. Use --help for usage."
    fi

    # Resolve VM types
    resolve_phase "$PHASE"

    # Phase 1: CI only
    if [ "$PHASE" = "1" ]; then
        echo "Phase 1 runs in CI, no cloud VMs needed"
        exit 0
    fi

    # Set default name prefix if not provided
    if [ -z "$NAME_PREFIX" ]; then
        NAME_PREFIX="oceanfs-loadtest-${PHASE}"
    fi

    # Provider check (only hetzner supported initially)
    if [ "$PROVIDER" != "hetzner" ]; then
        die "Provider '${PROVIDER}' is not yet supported. Only 'hetzner' is currently available." \
            "Extensibility for gcp/aws is planned but not implemented."
    fi

    # Guardrails (must run before provisioning prerequisites; hard size cap
    # should fire even if SSH keys aren't set up yet)
    run_guardrails

    # Validate SSH key (needed for provisioning, not for dry-run)
    if [ "$DRY_RUN" = false ]; then
        if [ ! -f "$SSH_KEY_PATH" ]; then
            die "SSH public key not found at: ${SSH_KEY_PATH}"
        fi
    fi

    # Check provisioning prerequisites (hcloud CLI, jq, ssh) — needed for
    # actual provisioning, not for dry-run simulation
    if [ "$DRY_RUN" = false ]; then
        check_prerequisites
    else
        # For dry-run, just verify ssh is available (for printing commands)
        if ! command -v ssh &>/dev/null; then
            log_warn "ssh not found — dry-run output will still show commands but actual provisioning would need ssh."
        fi
    fi

    # Provision
    if [ "$PHASE" = "3" ] || [ "$PHASE" = "4" ]; then
        log_info "Starting cluster provisioning for Phase ${PHASE}: ${CLUSTER_NODES} SUT node VMs (${SUT_TYPE:-N/A} each) + Harness (${HARNESS_TYPE:-N/A}) per ADR-0026..."
    else
        log_info "Starting two-VM provisioning for Phase ${PHASE} (SUT=${SUT_TYPE:-N/A}, Harness=${HARNESS_TYPE:-N/A})..."
    fi
    log_info "TTL: ${TTL_HOURS}h, Provider: ${PROVIDER}, Branch: ${BRANCH}, Image: ${IMAGE}"
    if [ "$DRY_RUN" = true ]; then
        log_info "DRY-RUN mode: no actual VMs will be provisioned."
    fi

    provision_vms

    # Output
    if [ "$DRY_RUN" = false ]; then
        log_info "Provisioning complete. Outputting JSON..."
        # Persist the record so follow-up tooling (setup-harness.sh) can
        # derive IPs, repo/branch, and the SSH key without re-querying.
        local record
        record=$(output_json)
        printf '%s\n' "$record"
        mkdir -p .hetzner
        printf '%s\n' "$record" | jq --arg repo "$REPO_URL" --arg branch "$BRANCH" \
            --arg commit "$COMMIT" --arg ssh_key "$SSH_KEY_PATH" \
            '. + {repo: $repo, branch: $branch, commit: $commit, ssh_key: $ssh_key}' \
            > ".hetzner/provision-${NAME_PREFIX}.json"
        log_info "Provisioning record written to .hetzner/provision-${NAME_PREFIX}.json"
    else
        output_json
    fi
}

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

main "$@"
