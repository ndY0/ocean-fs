#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# setup-observability.sh — Idempotent Prometheus setup for OceanFS load test VM
#
# Installs and configures Prometheus as a systemd service scraping OceanFS
# nodes at :9000/admin/metrics every 15s, plus a textfile collector for
# harness events. Creates the directory and permissions for the textfile
# collector so the harness can atomically write load test phase markers and
# test metadata.
#
# Usage:
#   ./scripts/setup-observability.sh [OPTIONS]
#
# Options:
#   --textfile-dir DIR    Textfile collector directory (default: /var/lib/prometheus/textfile)
#   --retention-days N    TSDB retention in days (default: 7)
#   --scrape-interval S   Scrape interval in seconds (default: 15)
#   -h, --help            Show this help
#
# Components:
#   - Prometheus (via apt or official binary download)
#   - prometheus.yml scrape config:
#       job "oceanfs": localhost:9000/admin/metrics every 15s
#       job "load_test": textfile collector from /var/lib/prometheus/textfile/
#   - systemd unit prometheus.service (enabled, started)
#   - /var/lib/prometheus/textfile/ directory (writable by harness user)
#
# After setup, Prometheus is available at http://localhost:9090.
#
# SSH Tunnel (for laptop Grafana):
#   ssh -L 9090:localhost:9090 -N <vm-ip>
#
# Note: the recommended laptop flow is the persistent Prometheus + Grafana
# stack in mcps/docker-compose.yml (Solution B): run ./scripts/observe.sh,
# then `docker compose -f mcps/docker-compose.yml up -d prometheus grafana`.
# The laptop Prometheus federates THIS SUT Prometheus through the tunnel
# (365-day retention) and Grafana reads from it at http://127.0.0.1:9091.
# Importing the dashboard manually is only needed for a direct-tunnel setup.
#
# Author: OceanFS
# Date: 2026-08-11
# ---------------------------------------------------------------------------

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
TEXTFILE_DIR="/var/lib/prometheus/textfile"
RETENTION_DAYS=7
SCRAPE_INTERVAL=15
PROMETHEUS_VERSION="2.52.0"
PROMETHEUS_PORT=9090
NODE_EXPORTER_PORT=9100
# Set by create_prometheus_config when it (re)writes prometheus.yml; consumed
# by create_systemd_unit to decide whether a running service must restart.
CONFIG_WRITTEN=false

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log_info()  { echo "[INFO]  $(date '+%H:%M:%S') $*"; }
log_warn()  { echo "[WARN]  $(date '+%H:%M:%S') $*"; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }
log_ok()    { echo "[OK]    $(date '+%H:%M:%S') $*"; }

step() {
    echo ""
    echo "── ${1} ──"
}

# Detect architecture for binary download
detect_arch() {
    local arch
    arch=$(uname -m)
    case "$arch" in
        x86_64|amd64)  echo "amd64" ;;
        aarch64|arm64) echo "arm64"  ;;
        *)
            log_error "Unsupported architecture: $arch"
            exit 1
            ;;
    esac
}

# ---------------------------------------------------------------------------
# Install Prometheus (apt or binary download)
# ---------------------------------------------------------------------------

install_prometheus_apt() {
    log_info "Attempting Prometheus installation via apt..."

    # Update package lists if needed
    if ! apt-cache show prometheus &>/dev/null; then
        apt-get update -qq
    fi

    if apt-get install -y -qq prometheus 2>/dev/null; then
        log_ok "Prometheus installed via apt."
        return 0
    fi

    log_warn "apt install failed. Falling back to binary download."
    return 1
}

install_prometheus_binary() {
    local arch
    arch=$(detect_arch)

    log_info "Downloading Prometheus v${PROMETHEUS_VERSION} (${arch})..."

    local tarball="prometheus-${PROMETHEUS_VERSION}.linux-${arch}.tar.gz"
    local url="https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/${tarball}"

    mkdir -p /tmp/prometheus-install
    cd /tmp/prometheus-install

    if ! curl -sSLO "$url"; then
        log_error "Failed to download Prometheus from $url"
        exit 1
    fi

    tar xzf "$tarball"
    cd "prometheus-${PROMETHEUS_VERSION}.linux-${arch}"

    # Install binaries
    cp prometheus /usr/local/bin/
    cp promtool   /usr/local/bin/
    chmod +x /usr/local/bin/prometheus /usr/local/bin/promtool

    # Install consoles and console_libraries (optional but standard)
    mkdir -p /etc/prometheus/consoles /etc/prometheus/console_libraries
    cp -r consoles/*         /etc/prometheus/consoles/         2>/dev/null || true
    cp -r console_libraries/* /etc/prometheus/console_libraries/ 2>/dev/null || true

    cd /tmp
    rm -rf /tmp/prometheus-install

    log_ok "Prometheus v${PROMETHEUS_VERSION} installed via binary download."
}

install_prometheus() {
    step "Installing Prometheus"

    # Check if already installed
    if command -v prometheus &>/dev/null; then
        local installed_ver
        installed_ver=$(prometheus --version 2>&1 | head -1 || echo "unknown")
        log_info "Prometheus already installed: ${installed_ver}"
        return 0
    fi

    if ! install_prometheus_apt; then
        install_prometheus_binary
    fi
}

# ---------------------------------------------------------------------------
# Install Node Exporter (for textfile collector)
# ---------------------------------------------------------------------------

install_node_exporter() {
    step "Installing Node Exporter"

    # Check if already installed
    if command -v prometheus-node-exporter &>/dev/null; then
        log_info "Node Exporter already installed."
        return 0
    fi

    log_info "Installing prometheus-node-exporter via apt..."

    if apt-get install -y -qq prometheus-node-exporter 2>/dev/null; then
        log_ok "Node Exporter installed via apt."
        return 0
    fi

    log_warn "apt install of prometheus-node-exporter failed. Textfile collector metrics will not be available."
    log_warn "To manually install: apt-get install prometheus-node-exporter"
}

# ---------------------------------------------------------------------------
# Prometheus configuration
# ---------------------------------------------------------------------------

create_prometheus_config() {
    step "Configuring Prometheus"

    local config_dir="/etc/prometheus"
    mkdir -p "$config_dir"

    local config_file="${config_dir}/prometheus.yml"

    # If config exists and matches our expected content, skip
    if [ -f "$config_file" ]; then
        local scrape_oceanfs
        scrape_oceanfs=$(grep -c "job_name: 'oceanfs'" "$config_file" 2>/dev/null || echo "0")
        local scrape_loadtest
        scrape_loadtest=$(grep -c "job_name: 'load_test'" "$config_file" 2>/dev/null || echo "0")
        if [ "$scrape_oceanfs" -ge 1 ] && [ "$scrape_loadtest" -ge 1 ]; then
            log_info "Prometheus config already contains oceanfs + load_test scrape jobs. Keeping existing."
            return 0
        fi
        log_info "Updating existing Prometheus config..."
        # Back up the old config
        cp "$config_file" "${config_file}.bak.$(date +%s)"
        CONFIG_WRITTEN=true
    fi

    log_info "Writing Prometheus configuration to ${config_file}..."
    CONFIG_WRITTEN=true

    cat > "$config_file" <<PROMETHEUS_CONFIG
# ---------------------------------------------------------------------------
# OceanFS Load Test — Prometheus Configuration
# Generated by scripts/setup-observability.sh
# ---------------------------------------------------------------------------

global:
  scrape_interval: ${SCRAPE_INTERVAL}s
  evaluation_interval: ${SCRAPE_INTERVAL}s
  scrape_timeout: 10s

# Alertmanager (not configured — load test assertions are in-harness)
# alerting:
#   alertmanagers: []

# Rule files (not configured — see §Out of Scope in feature.md)
# rule_files: []

scrape_configs:
  # ── OceanFS SUT ─────────────────────────────────────────────────────────
  - job_name: 'oceanfs'
    metrics_path: '/admin/metrics'
    static_configs:
      - targets: ['localhost:9000']
        labels:
          instance: 'oceanfs-sut'
          role: 'sut'

  # ── Load Test Harness (textfile via Node Exporter) ─────────────────────
  - job_name: 'load_test'
    scrape_interval: 30s
    static_configs:
      - targets: ['localhost:9100']
        labels:
          instance: 'harness'
          role: 'harness'
    # Node Exporter serves textfile metrics from
    # /var/lib/prometheus/textfile/*.prom on :9100/metrics.
    # The harness writes atomic updates via write-rename pattern.
    # Metrics exposed:
    #   load_test_phase{test="..."} N
    #   process_open_fds_at_end N

# TSDB retention: ${RETENTION_DAYS} days
PROMETHEUS_CONFIG

    log_ok "Prometheus configuration written to ${config_file}."
}

# ---------------------------------------------------------------------------
# Textfile collector directory
# ---------------------------------------------------------------------------

setup_textfile_dir() {
    step "Setting up textfile collector directory"

    mkdir -p "$TEXTFILE_DIR"

    # Determine the prometheus user
    local prom_user="prometheus"
    if ! id "$prom_user" &>/dev/null; then
        prom_user="nobody"
        log_warn "prometheus user not found — using '${prom_user}' for ownership."
    fi

    chown "${prom_user}:${prom_user}" "$TEXTFILE_DIR" 2>/dev/null || {
        log_warn "Could not chown ${TEXTFILE_DIR} to ${prom_user}. Setting world-writable as fallback."
        chmod 1777 "$TEXTFILE_DIR"
        log_ok "Textfile directory ${TEXTFILE_DIR} (world-writable fallback)."
        return 0
    }

    chmod 0755 "$TEXTFILE_DIR"
    log_ok "Textfile directory ${TEXTFILE_DIR} (owner: ${prom_user})."

    # If the harness user is different, add ACL or make group-writable
    local harness_user="${SUDO_USER:-}"
    if [ -n "$harness_user" ] && [ "$harness_user" != "root" ] && [ "$harness_user" != "$prom_user" ]; then
        if command -v setfacl &>/dev/null; then
            setfacl -m "u:${harness_user}:rwx" "$TEXTFILE_DIR" 2>/dev/null && \
                log_info "ACL granted to harness user '${harness_user}' on ${TEXTFILE_DIR}." || \
                log_warn "Could not set ACL for harness user '${harness_user}'. Harness must write as ${prom_user}."
        else
            log_info "setfacl not available. Harness user '${harness_user}' should write files as ${prom_user} or via sudo."
        fi
    fi
}

# ---------------------------------------------------------------------------
# Systemd unit
# ---------------------------------------------------------------------------

create_systemd_unit() {
    step "Creating systemd unit"

    local unit_file="/etc/systemd/system/prometheus.service"
    local storage_dir="/var/lib/prometheus/data"

    mkdir -p "$storage_dir"

    # Only write if not already present or if forcing update
    if [ -f "$unit_file" ]; then
        log_info "systemd unit ${unit_file} already exists. Checking if active..."
        systemctl daemon-reload
        if systemctl is-active --quiet prometheus 2>/dev/null; then
            if [ "${CONFIG_WRITTEN:-false}" = "true" ]; then
                # The scrape config was (re)written during THIS run but the
                # running process still uses the pre-existing config loaded
                # at startup (apt installs start prometheus with the stock
                # config; our oceanfs/load_test jobs would never load).
                log_info "Config changed this run — restarting prometheus to load it."
                systemctl restart prometheus
            else
                log_ok "Prometheus service is already running."
            fi
            return 0
        fi
        log_info "Unit exists but service is not running. Restarting..."
        systemctl enable prometheus 2>/dev/null || true
        systemctl restart prometheus 2>/dev/null || true
        return 0
    fi

    # Determine Prometheus binary path
    local prom_bin
    if [ -x /usr/local/bin/prometheus ]; then
        prom_bin="/usr/local/bin/prometheus"
    elif [ -x /usr/bin/prometheus ]; then
        prom_bin="/usr/bin/prometheus"
    else
        log_error "Cannot find prometheus binary."
        exit 1
    fi

    local prom_user="prometheus"
    if ! id "$prom_user" &>/dev/null; then
        prom_user="nobody"
    fi

    log_info "Writing systemd unit: ${unit_file}..."

    cat > "$unit_file" <<SYSTEMD_UNIT
[Unit]
Description=Prometheus — OceanFS Load Test Observability
Documentation=https://prometheus.io/docs/
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${prom_user}
Group=${prom_user}
ExecStart=${prom_bin} \\
    --config.file=/etc/prometheus/prometheus.yml \\
    --storage.tsdb.path=${storage_dir} \\
    --storage.tsdb.retention.time=${RETENTION_DAYS}d \\
    --web.listen-address=0.0.0.0:${PROMETHEUS_PORT} \\
    --web.enable-lifecycle \\
    --web.console.templates=/etc/prometheus/consoles \\
    --web.console.libraries=/etc/prometheus/console_libraries

ExecReload=/bin/kill -HUP \$MAINPID
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

# Security hardening
NoNewPrivileges=yes
ProtectHome=yes
ProtectSystem=strict
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
ReadWritePaths=${storage_dir} /var/lib/prometheus

[Install]
WantedBy=multi-user.target
SYSTEMD_UNIT

    # Set correct ownership for storage
    chown -R "${prom_user}:${prom_user}" "$storage_dir" 2>/dev/null || \
        log_warn "Could not chown storage directory ${storage_dir}."

    systemctl daemon-reload
    systemctl enable prometheus
    systemctl start prometheus

    # Verify
    sleep 2
    if systemctl is-active --quiet prometheus; then
        log_ok "Prometheus service started successfully."
    else
        log_error "Prometheus service failed to start. Check with: systemctl status prometheus"
        journalctl -u prometheus --no-pager -n 20
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Node Exporter systemd unit (textfile collector)
# ---------------------------------------------------------------------------

create_node_exporter_unit() {
    step "Configuring Node Exporter textfile collector"

    # Node Exporter must be installed to configure
    if ! command -v prometheus-node-exporter &>/dev/null; then
        log_warn "Node Exporter not installed. Skipping textfile collector configuration."
        log_warn "Install with: apt-get install prometheus-node-exporter"
        return 0
    fi

    local override_dir="/etc/systemd/system/prometheus-node-exporter.service.d"
    mkdir -p "$override_dir"

    local override_file="${override_dir}/textfile-collector.conf"

    if [ -f "$override_file" ]; then
        # Check if textfile directory is already configured
        if grep -q "collector.textfile.directory" "$override_file" 2>/dev/null; then
            log_info "Node Exporter already configured with textfile directory. Keeping existing."
            systemctl daemon-reload
            systemctl restart prometheus-node-exporter 2>/dev/null || true
            return 0
        fi
        log_info "Updating Node Exporter override to add textfile collector..."
    fi

    log_info "Writing Node Exporter override: ${override_file}..."

    cat > "$override_file" <<NODE_EXPORTER_OVERRIDE
# ---------------------------------------------------------------------------
# OceanFS Node Exporter override — adds textfile collector
# Generated by scripts/setup-observability.sh
# ---------------------------------------------------------------------------
[Service]
ExecStart=
ExecStart=/usr/bin/prometheus-node-exporter \\
    --collector.textfile.directory=${TEXTFILE_DIR} \\
    --web.listen-address=0.0.0.0:${NODE_EXPORTER_PORT} \\
    --collector.textfile
NODE_EXPORTER_OVERRIDE

    systemctl daemon-reload
    systemctl enable prometheus-node-exporter 2>/dev/null || true
    systemctl restart prometheus-node-exporter

    sleep 2
    if systemctl is-active --quiet prometheus-node-exporter; then
        log_ok "Node Exporter started with textfile collector (directory: ${TEXTFILE_DIR})."
    else
        log_error "Node Exporter failed to start. Check: systemctl status prometheus-node-exporter"
        journalctl -u prometheus-node-exporter --no-pager -n 20
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

verify_prometheus() {
    step "Verifying Prometheus"

    log_info "Checking Prometheus HTTP endpoint..."

    # Wait up to 10 seconds for Prometheus to respond
    local attempt=0
    local max_attempts=10
    local up_url="http://localhost:${PROMETHEUS_PORT}/api/v1/query?query=up"

    while [ "$attempt" -lt "$max_attempts" ]; do
        if curl -sf "$up_url" >/dev/null 2>&1; then
            break
        fi
        attempt=$((attempt + 1))
        sleep 2
    done

    if [ "$attempt" -ge "$max_attempts" ]; then
        log_error "Prometheus did not become responsive within 20 seconds."
        log_error "Check logs: journalctl -u prometheus --no-pager -n 50"
        exit 1
    fi

    # Verify textfile directory is writable
    local test_file="${TEXTFILE_DIR}/.setup-test-$$"
    if touch "$test_file" 2>/dev/null; then
        rm -f "$test_file"
        log_ok "Textfile directory ${TEXTFILE_DIR} is writable."
    else
        log_warn "Textfile directory ${TEXTFILE_DIR} may not be writable by current user."
        log_warn "Harness must write textfile metrics as the prometheus user or with sudo."
    fi

    # Verify Node Exporter is serving textfile metrics
    if command -v prometheus-node-exporter &>/dev/null && \
       systemctl is-active --quiet prometheus-node-exporter 2>/dev/null; then
        if curl -sf "http://localhost:${NODE_EXPORTER_PORT}/metrics" >/dev/null 2>&1; then
            log_ok "Node Exporter is serving metrics at http://localhost:${NODE_EXPORTER_PORT}/metrics"
        else
            log_warn "Node Exporter is running but /metrics endpoint is not responding."
        fi
    fi

    log_ok "Verification complete. Prometheus is running at http://localhost:${PROMETHEUS_PORT}"
}

# ---------------------------------------------------------------------------
# SSH tunnel instructions
# ---------------------------------------------------------------------------

print_ssh_tunnel_instructions() {
    local hostname
    hostname=$(hostname 2>/dev/null || echo "<vm-ip>")

    cat <<TUNNEL

╔══════════════════════════════════════════════════════════════════════════╗
║                         SSH TUNNEL INSTRUCTIONS                          ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  From your laptop, open the tunnel (see scripts/observe.sh — idempotent, ║
║  reads the provisioning record):                                         ║
║                                                                          ║
║    ./scripts/observe.sh                                                  ║
║    # or manually: ssh -L 9090:localhost:9090 -N ${hostname}              ║
║                                                                          ║
║  Recommended (Solution B — persistent history): start the laptop stack:  ║
║                                                                          ║
║    docker compose -f mcps/docker-compose.yml up -d prometheus grafana    ║
║                                                                          ║
║  The laptop Prometheus federates this SUT (365-day retention) and        ║
║  Grafana reads from it at http://127.0.0.1:9091 — no manual datasource   ║
║  setup or dashboard import needed (auto-provisioned).                    ║
║                                                                          ║
║  Direct Prometheus queries on this SUT (no laptop stack):                ║
║    curl 'http://localhost:9090/api/v1/query?query=up'                    ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝

TUNNEL
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

usage() {
    cat <<HELP
Usage: ./scripts/setup-observability.sh [OPTIONS]

Idempotent setup of Prometheus for OceanFS load test VMs.

OPTIONS:
  --textfile-dir DIR    Textfile collector directory (default: /var/lib/prometheus/textfile)
  --retention-days N    TSDB retention in days (default: 7)
  --scrape-interval S   Scrape interval in seconds (default: 15)
  -h, --help            Show this help

After setup:
  - Prometheus is running at http://localhost:9090
  - Node Exporter is running at http://localhost:9100 (textfile collector)
  - OceanFS metrics at :9000/admin/metrics are scraped every ${SCRAPE_INTERVAL}s
  - Textfile collector reads *.prom from ${TEXTFILE_DIR} (via Node Exporter)
  - See SSH tunnel instructions printed at the end of setup
HELP
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --textfile-dir)
                TEXTFILE_DIR="${2:-$TEXTFILE_DIR}"
                shift 2
                ;;
            --retention-days)
                RETENTION_DAYS="${2:-7}"
                shift 2
                ;;
            --scrape-interval)
                SCRAPE_INTERVAL="${2:-15}"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1" >&2
                usage
                exit 1
                ;;
        esac
    done
}

main() {
    parse_args "$@"

    # Must run as root (or with sudo) to install packages, write systemd units, etc.
    if [ "$(id -u)" -ne 0 ]; then
        log_error "This script must be run as root (sudo)."
        exit 1
    fi

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  OceanFS — Observability Stack Setup"
    echo "  Prometheus: port ${PROMETHEUS_PORT}, scrape every ${SCRAPE_INTERVAL}s"
    echo "  Node Exporter: port ${NODE_EXPORTER_PORT}, textfile collector"
    echo "  Retention: ${RETENTION_DAYS} days"
    echo "  Textfile dir: ${TEXTFILE_DIR}"
    echo "═══════════════════════════════════════════════════════════════"

    install_prometheus
    install_node_exporter
    create_prometheus_config
    setup_textfile_dir
    create_systemd_unit
    create_node_exporter_unit
    verify_prometheus
    print_ssh_tunnel_instructions

    echo ""
    log_ok "Setup complete. Prometheus + Node Exporter are ready for OceanFS load testing."
    echo ""
}

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

main "$@"
