#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# env-hetzner.sh — shared environment bootstrap for OceanFS cloud scripts.
#
# SOURCED (not executed) by laptop-side scripts right after `set -euo pipefail`.
# Idempotent, and every step degrades gracefully — sourcing this NEVER
# fails the caller, even when .hetzner/ does not exist (e.g. when the repo
# is cloned on the Harness VM, where this file is a no-op).
#
# What it does (only when .hetzner/ is present, i.e. on the laptop):
#   1. Loads .hetzner/.env            -> HCLOUD_TOKEN (and any other vars)
#   2. Ensures an ssh-agent is running (reuses a live one, else starts one
#      and persists socket/pid in /tmp/oceanfs-ssh-agent.env for reuse)
#   3. Adds .hetzner/.ssh/<key> to the agent if not already loaded
#   4. Exports HETZNER_SSH_PUBLIC_KEY — the DEFAULT provisioning key for
#      vm-provision.sh (fallback: ~/.ssh/id_rsa.pub)
#
# Author: OceanFS
# Date: 2026-08-16
# ---------------------------------------------------------------------------

# Locate the repo root from this file's own path: scripts/lib/env-hetzner.sh
_ENV_HETZNER_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
_ENV_HETZNER_DIR="${_ENV_HETZNER_ROOT}/.hetzner"
unset _ENV_HETZNER_ROOT

# ── 1. Load .hetzner/.env (HCLOUD_TOKEN, ...) ──────────────────────────────
if [ -f "${_ENV_HETZNER_DIR}/.env" ]; then
    # set -a: variables defined in the file are exported to child processes
    # (hcloud CLI reads HCLOUD_TOKEN from the environment).
    set -a
    # shellcheck disable=SC1090
    . "${_ENV_HETZNER_DIR}/.env"
    set +a
fi

# ── 2+3. ssh-agent + Hetzner key ───────────────────────────────────────────
# The provisioning key lives in .hetzner/.ssh/. Only bootstrap the agent on
# the laptop (where that directory exists); on the Harness VM this is a
# no-op and ssh uses the seeded key file (~/.ssh/id_ed25519) directly.
_ENV_HETZNER_KEY=""
for _candidate in "${_ENV_HETZNER_DIR}/.ssh/hetzner-ssh" \
                  "${_ENV_HETZNER_DIR}/.ssh/id_ed25519" \
                  "${_ENV_HETZNER_DIR}/.ssh/id_rsa"; do
    if [ -f "$_candidate" ]; then
        _ENV_HETZNER_KEY="$_candidate"
        break
    fi
done

if [ -n "$_ENV_HETZNER_KEY" ]; then
    # 2a. Reuse a persisted agent from an earlier invocation, if it is alive.
    if [ -f /tmp/oceanfs-ssh-agent.env ]; then
        # shellcheck disable=SC1091
        . /tmp/oceanfs-ssh-agent.env
    fi

    # 2b. If no agent is reachable, start one and persist it.
    if [ -z "${SSH_AUTH_SOCK:-}" ] || [ ! -S "${SSH_AUTH_SOCK:-}" ] || \
       ! ssh-add -l >/dev/null 2>&1; then
        if [ -n "${SSH_AGENT_PID:-}" ] && kill -0 "$SSH_AGENT_PID" 2>/dev/null; then
            : # agent env file was stale but the pid is alive — trust it
        else
            # Quietly start a user agent (no passphrase prompt). The -s
            # output includes a trailing `echo Agent pid NNN;` line —
            # drop it so the caller's stdout stays clean (vm-provision.sh
            # prints JSON on stdout).
            eval "$(ssh-agent -s 2>/dev/null | sed '/^echo Agent/d')"
            # Restrict the persisted agent-info file to this user only,
            # then restore the caller's umask.
            _old_umask="$(umask)"
            umask 077
            printf 'SSH_AUTH_SOCK=%s\nexport SSH_AUTH_SOCK\nSSH_AGENT_PID=%s\nexport SSH_AGENT_PID\n' \
                "${SSH_AUTH_SOCK:-}" "${SSH_AGENT_PID:-}" \
                > /tmp/oceanfs-ssh-agent.env 2>/dev/null || true
            umask "$_old_umask"
            unset _old_umask
            # Note: the agent outlives this script by design (session
            # leader). If /tmp/oceanfs-ssh-agent.env is wiped while the
            # agent lives, the next invocation starts another (~3 MB,
            # dies on reboot) — acceptable for laptop tooling.
        fi
    fi

    # 3. Add the Hetzner key to the agent unless it is already loaded
    #    (compare fingerprints — works with keyring agents too).
    #    Guarded so a missing ssh-keygen can never exit the caller.
    _fp="$(ssh-keygen -lf "$_ENV_HETZNER_KEY" 2>/dev/null | awk '{print $2}')" || _fp=""
    if [ -n "$_fp" ] && ! ssh-add -l 2>/dev/null | grep -qF "$_fp"; then
        ssh-add -q "$_ENV_HETZNER_KEY" 2>/dev/null || true
    fi
    unset _fp
fi

# ── 4. Default provisioning key ────────────────────────────────────────────
if [ -n "$_ENV_HETZNER_KEY" ] && [ -f "${_ENV_HETZNER_KEY}.pub" ]; then
    export HETZNER_SSH_PUBLIC_KEY="${_ENV_HETZNER_KEY}.pub"
fi

unset _ENV_HETZNER_DIR _ENV_HETZNER_KEY _candidate
