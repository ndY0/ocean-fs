#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# vm-provision.sh — Two-VM Cloud Lifecycle with Cost Guardrails
#
# Provisions two VMs (SUT + Harness) on Hetzner Cloud for OceanFS load testing
# per ADR-0019. The SUT VM runs OceanFS and Prometheus only (no harness, no
# toolchain). The Harness VM (always CX22) runs the e2e harness and Rust
# toolchain, targeting the SUT VM over Hetzner's internal network.
#
# Usage:
#   ./scripts/vm-provision.sh --phase N [OPTIONS]
#
# VM Size Mapping (per phase):
#   Phase 1: N/A (runs in CI, no cloud VMs needed)
#   Phase 2: SUT=CX22 (2 vCPU, 4 GB, 40 GB), Harness=CX22 (2 vCPU, 4 GB, 40 GB)
#   Phase 3-4: SUT=CX32 (4 vCPU, 8 GB, 80 GB), Harness=CX22 (2 vCPU, 4 GB, 40 GB)
#   Phase 5+: N/A (separate provisioning model)
#
# Guardrails (four-layer defense per ADR-0019):
#   Layer 1 — Hard VM size cap: MAX_AGENT_VM_TYPE="cx32"
#   Layer 2 — Confirmation gate: CX22 auto-approved; CX32 requires --confirm yes
#   Layer 3 — Auto-shutdown TTL: systemd timer on each VM (default 4h)
#   Layer 4 — Budget gate: scaffolding (deferrable/v2)
#
# Requirements:
#   - hcloud CLI installed and authenticated (HCLOUD_TOKEN env var or config)
#   - jq for JSON parsing
#   - SSH key at ~/.ssh/id_rsa.pub (or --ssh-key PATH)
#
# Environment Variables:
#   HCLOUD_TOKEN              Hetzner Cloud API token (required)
#   LOAD_TEST_TTL_HOURS       Override default TTL (default: 4)
#   LOAD_TEST_MAX_MONTHLY_EUR Optional monthly budget cap (deferrable, v2)
#
# Author: OceanFS
# Date: 2026-08-11
# ---------------------------------------------------------------------------

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants & defaults
# ---------------------------------------------------------------------------
readonly MAX_AGENT_VM_TYPE="cx32"
readonly DEFAULT_PROVIDER="hetzner"
readonly DEFAULT_BRANCH="main"
readonly DEFAULT_IMAGE="ubuntu-24.04"
readonly DEFAULT_TTL_HOURS="${LOAD_TEST_TTL_HOURS:-4}"
readonly NETWORK_NAME="oceanfs-testnet"
readonly NETWORK_CIDR="10.0.0.0/24"

# VM type pricing (approximate, for cost estimates)
declare -A VM_HOURLY_COST
VM_HOURLY_COST[cx22]="0.015"
VM_HOURLY_COST[cx32]="0.03"
VM_HOURLY_COST[cx42]="0.06"
VM_HOURLY_COST[cx52]="0.12"
VM_HOURLY_COST[cx62]="0.24"

# VM type ordering for comparison (lower index = smaller)
declare -A VM_TYPE_RANK
VM_TYPE_RANK[cx22]=1
VM_TYPE_RANK[cx32]=2
VM_TYPE_RANK[cx42]=3
VM_TYPE_RANK[cx52]=4
VM_TYPE_RANK[cx62]=5

# ---------------------------------------------------------------------------
# Script state
# ---------------------------------------------------------------------------
PHASE=""
PROVIDER="${DEFAULT_PROVIDER}"
BRANCH="${DEFAULT_BRANCH}"
COMMIT=""
SSH_KEY_PATH=""
NAME_PREFIX=""
IMAGE="${DEFAULT_IMAGE}"
DRY_RUN=false
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

# Check if VM type requires confirmation (>= CX32)
vm_type_needs_confirm() {
    local type="${1,,}"
    if vm_type_lte "$type" "cx22"; then
        return 1  # CX22 or smaller: no confirm
    fi
    return 0  # CX32 or larger: needs confirm
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
            SUT_TYPE="cx22"
            HARNESS_TYPE="cx22"
            ;;
        3|4)
            SUT_TYPE="cx32"
            HARNESS_TYPE="cx22"
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
            "Larger VMs (≥ CX42) must be provisioned manually via the Hetzner Cloud console." \
            "See ADR-0019 for cost guardrail details."
    fi
}

# Layer 2: Size-based confirmation gate
check_confirmation_gate() {
    local type="${1,,}"
    local phase="${2:-}"

    if [ -z "$type" ]; then
        return 0
    fi

    if vm_type_needs_confirm "$type"; then
        local hourly
        hourly=$(vm_hourly_cost "$type")
        local daily
        daily=$(echo "scale=2; $hourly * 24" | bc 2>/dev/null || echo "unknown")
        # Ensure leading zero for values like ".72"
        if [[ "$daily" =~ ^\. ]]; then
            daily="0${daily}"
        fi

        if [ "$CONFIRM" != "yes" ]; then
            cat >&2 <<MSG

This phase (${phase}) provisions a ${type} VM (~€${hourly}/hour, ~€${daily}/day).
Re-run with --confirm yes to proceed.
MSG
            exit 1
        fi
        log_info "Confirmation gate: CX32 confirmed with --confirm yes."
    fi

    # CX22 with --confirm yes: accepted but note it wasn't required
    if [ "$CONFIRM" == "yes" ] && ! vm_type_needs_confirm "$type"; then
        log_info "Note: --confirm yes provided but not required for ${type} (CX22 is auto-approved)."
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
        # Two VMs: SUT + Harness hourly rates
        local s_cost h_cost
        s_cost="0"
        h_cost="0"
        if [ -n "$SUT_TYPE" ]; then
            s_cost=$(vm_hourly_cost "$SUT_TYPE")
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
            die "VM '${name}' entered error state."
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
    # Check if network exists; create if not
    if hcloud network describe "$NETWORK_NAME" --output json >/dev/null 2>&1; then
        log_info "Network '${NETWORK_NAME}' already exists."
        return 0
    fi

    log_info "Creating network '${NETWORK_NAME}' with CIDR ${NETWORK_CIDR}..."
    hcloud network create \
        --name "$NETWORK_NAME" \
        --ip-range "$NETWORK_CIDR" \
        >/dev/null 2>&1 || die "Failed to create network '${NETWORK_NAME}'."
    log_info "Network '${NETWORK_NAME}' created."
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

    hcloud server create \
        --name "$name" \
        --type "$type" \
        --image "$image" \
        --network "$NETWORK_NAME" \
        --ssh-key "$ssh_key" \
        >/dev/null 2>&1 || die "Failed to create VM '${name}'."

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

# Configure SUT VM (minimal — no Rust toolchain)
configure_sut_vm() {
    local name="$1"
    local public_ip="$2"

    log_info "Configuring SUT VM '${name}' (${public_ip})..."

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] ssh root@${public_ip} apt-get update && apt-get install -y curl"
        log_info "[DRY-RUN] (No Rust toolchain installed on SUT VM)"
        return 0
    fi

    # Wait for SSH to be available
    local ssh_retries=30
    local ssh_retry=0
    while [ "$ssh_retry" -lt "$ssh_retries" ]; do
        if ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 \
               "root@${public_ip}" "echo ok" >/dev/null 2>&1; then
            break
        fi
        ssh_retry=$((ssh_retry + 1))
        sleep 5
    done

    if [ "$ssh_retry" -ge "$ssh_retries" ]; then
        log_error "SSH connection to ${public_ip} timed out."
        return 1
    fi

    # Install minimal dependencies (curl for Prometheus setup script)
    ssh -o StrictHostKeyChecking=accept-new "root@${public_ip}" <<'SUT_SETUP'
set -euo pipefail
apt-get update -qq
apt-get install -y -qq curl
SUT_SETUP
    log_info "SUT VM '${name}' configured (curl only, no Rust toolchain)."
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

    # Wait for SSH to be available
    local ssh_retries=30
    local ssh_retry=0
    while [ "$ssh_retry" -lt "$ssh_retries" ]; do
        if ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 \
               "root@${public_ip}" "echo ok" >/dev/null 2>&1; then
            break
        fi
        ssh_retry=$((ssh_retry + 1))
        sleep 5
    done

    if [ "$ssh_retry" -ge "$ssh_retries" ]; then
        log_error "SSH connection to ${public_ip} timed out."
        return 1
    fi

    # Install system dependencies
    ssh -o StrictHostKeyChecking=accept-new "root@${public_ip}" <<HARNESS_SETUP
set -euo pipefail
apt-get update -qq
apt-get install -y -qq build-essential pkg-config libssl-dev curl git
HARNESS_SETUP

    # Install Rust
    log_info "Installing Rust toolchain on Harness VM..."
    ssh -o StrictHostKeyChecking=accept-new "root@${public_ip}" \
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y" \
        || log_error "Rust installation failed on Harness VM."

    # Clone repo
    log_info "Cloning repository (branch=${BRANCH}) on Harness VM..."
    if [ -n "$COMMIT" ]; then
        ssh -o StrictHostKeyChecking=accept-new "root@${public_ip}" \
            "git clone https://github.com/ocean-fs/ocean-fs.git --branch ${BRANCH} /root/ocean-fs && cd /root/ocean-fs && git checkout ${COMMIT}" \
            || log_error "Git clone/checkout failed on Harness VM."
    else
        ssh -o StrictHostKeyChecking=accept-new "root@${public_ip}" \
            "git clone https://github.com/ocean-fs/ocean-fs.git --branch ${BRANCH} /root/ocean-fs" \
            || log_error "Git clone failed on Harness VM."
    fi

    # Build
    log_info "Building oceanfs + e2e on Harness VM... (this may take several minutes)"
    ssh -o StrictHostKeyChecking=accept-new "root@${public_ip}" \
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

    ssh -o StrictHostKeyChecking=accept-new "root@${public_ip}" <<TTL_SETUP
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
    timer_status=$(ssh -o StrictHostKeyChecking=accept-new "root@${public_ip}" \
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

    log_info "Looking for VMs matching '${prefix}-sut' and '${prefix}-harness'..."

    local sut_exists=false
    local harness_exists=false

    # Check and delete SUT VM
    if hcloud server describe "${prefix}-sut" --output json >/dev/null 2>&1; then
        sut_exists=true
        log_info "Deleting VM '${prefix}-sut'..."
        hcloud server delete "${prefix}-sut" || log_error "Failed to delete VM '${prefix}-sut'."
    else
        log_info "VM '${prefix}-sut' not found."
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

    # Output JSON
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
}

# ---------------------------------------------------------------------------
# Core provisioning orchestration
# ---------------------------------------------------------------------------

provision_vms() {
    local sut_name="${NAME_PREFIX}-sut"
    local harness_name="${NAME_PREFIX}-harness"

    # Single-VM mode: warn for Phase 3-4
    if [ "$SINGLE_VM" = true ]; then
        if [ "$PHASE" = "2" ]; then
            log_info "Single-VM mode: co-locating SUT + Harness on one CX22."
            log_info "Reports will be written to /tmp (tmpfs) to avoid disk I/O contention."
        elif [ "$PHASE" = "3" ] || [ "$PHASE" = "4" ]; then
            cat >&2 <<'WARNING'

╔══════════════════════════════════════════════════════════════════════════╗
║                              WARNING                                     ║
║  Phase 3-4 on a single VM will cause CPU contention between the          ║
║  harness and OceanFS processes. SWIM gossip timing may be affected.      ║
║  Gossip intervals will be relaxed (gossip_interval=3s,                   ║
║  suspicion_timeout=10s) to compensate.                                   ║
║  Use the two-VM topology for reliable results.                           ║
║  Per ADR-0019 Decision 4.                                                ║
╚══════════════════════════════════════════════════════════════════════════╝

WARNING
        fi
    fi

    # Create network (idempotent)
    if [ "$DRY_RUN" = false ]; then
        create_network
    else
        log_info "[DRY-RUN] Ensure network '${NETWORK_NAME}' exists."
    fi

    # SUT VM provisioning
    if [ -n "$SUT_TYPE" ]; then
        log_info "Provisioning SUT VM: type=${SUT_TYPE}..."
        local sut_ips
        sut_ips=$(create_vm "$sut_name" "$SUT_TYPE" "$IMAGE" "$SSH_KEY_PATH")
        SUT_IP="${sut_ips%% *}"
        SUT_PUBLIC_IP="${sut_ips##* }"
        SUT_NAME="$sut_name"

        log_info "SUT VM: name=${SUT_NAME}, internal_ip=${SUT_IP}, public_ip=${SUT_PUBLIC_IP}"

        if [ "$DRY_RUN" = false ]; then
            configure_sut_vm "$sut_name" "$SUT_PUBLIC_IP" || die "SUT VM configuration failed."
            setup_ttl_timer "$sut_name" "$SUT_PUBLIC_IP" || log_warn "TTL timer setup failed on SUT VM. Manual TTL enforcement may be needed."
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
            configure_harness_vm "$harness_name" "$HARNESS_PUBLIC_IP" || die "Harness VM configuration failed."
            setup_ttl_timer "$harness_name" "$HARNESS_PUBLIC_IP" || log_warn "TTL timer setup failed on Harness VM. Manual TTL enforcement may be needed."
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
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

usage() {
    cat <<'HELP'
Usage: ./scripts/vm-provision.sh [OPTIONS]

Provision two VMs (SUT + Harness) for OceanFS load testing per ADR-0019.

OPTIONS:
  --phase N            Load test phase (1-6). Determines VM sizes. [required]
                       Phase 1: N/A (runs in CI, no cloud VMs)
                       Phase 2: SUT=CX22, Harness=CX22
                       Phase 3-4: SUT=CX32, Harness=CX22
                       Phase 5+: N/A (separate provisioning model)
  --provider NAME      Cloud provider: hetzner (default), gcp, aws
  --branch BRANCH      Git branch to clone on Harness VM (default: main)
  --commit SHA         Specific commit to check out on Harness VM
  --ssh-key PATH       SSH public key path (default: ~/.ssh/id_rsa.pub)
  --name NAME          VM name prefix (default: oceanfs-loadtest-{phase})
  --image IMAGE        OS image (default: ubuntu-24.04)
  --single-vm          Co-locate SUT+Harness on single VM
                       (Phase 2 only; Phase 3-4 prints warning per ADR-0019)
  --confirm yes        Required for VM types >= CX32 (confirmation gate)
  --ttl HOURS          Auto-shutdown TTL (default: 4, or LOAD_TEST_TTL_HOURS)
  --dry-run            Print actions without executing
  --destroy NAME       Tear down both VMs with given name prefix
  --status NAME        Check status of both VMs
  -h, --help           Show this help

Environment Variables:
  HCLOUD_TOKEN              Hetzner Cloud API token (required for hetzner)
  LOAD_TEST_TTL_HOURS       Override default TTL (default: 4)
  LOAD_TEST_MAX_MONTHLY_EUR Optional monthly budget cap (deferrable, v2)

Guardrails:
  - Hard VM size cap: VMs >= CX42 require manual provisioning
  - Confirmation gate: CX32 requires --confirm yes (CX22 auto-approved)
  - Auto-shutdown TTL: systemd timer powers off VMs after TTL expires
  - Budget gate: optional LOAD_TEST_MAX_MONTHLY_EUR scaffolding

Output: JSON with sut + harness VM objects on stdout.

Examples:
  vm-provision.sh --phase 2 --branch feature/test
  vm-provision.sh --phase 3 --confirm yes
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
            --provider)
                PROVIDER="${2:-hetzner}"
                shift 2
                ;;
            --branch)
                BRANCH="${2:-main}"
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

    # Resolve SSH key default
    if [ -z "$SSH_KEY_PATH" ]; then
        SSH_KEY_PATH="${HOME}/.ssh/id_rsa.pub"
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

    # Guardrails (must run before provisioning prerequisites — confirmation gate
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
    log_info "Starting two-VM provisioning for Phase ${PHASE} (SUT=${SUT_TYPE:-N/A}, Harness=${HARNESS_TYPE:-N/A})..."
    log_info "TTL: ${TTL_HOURS}h, Provider: ${PROVIDER}, Branch: ${BRANCH}, Image: ${IMAGE}"
    if [ "$DRY_RUN" = true ]; then
        log_info "DRY-RUN mode: no actual VMs will be provisioned."
    fi

    provision_vms

    # Output
    if [ "$DRY_RUN" = false ]; then
        log_info "Provisioning complete. Outputting JSON..."
    fi
    output_json
}

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

main "$@"
