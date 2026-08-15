#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# OceanFS — local CI checks.
#
# Run this before pushing to ensure all CI checks will pass.
# Equivalent to the GitHub Actions workflow in .github/workflows/ci.yml.
# ---------------------------------------------------------------------------
set -euo pipefail

echo "==> fmt"
cargo fmt --all -- --check

echo "==> clippy"
cargo clippy --all-targets --all-features -- -D warnings

echo "==> build"
cargo build --all-targets --all-features

echo "==> test"
# The Phase 2 sustained-load test (load_sustained) is deliberately excluded
# from the general pass: its quick mode runs 5 minutes plus a crash-recovery
# cycle, which is too heavy for every local check. Run it explicitly:
#   LOAD_TEST_DURATION_SECS=300 LOAD_TEST_SEED=42 cargo test -p e2e --test load_sustained
cargo test --all-targets --all-features -- --skip load_sustained

if [ "${RUN_PHASE2:-0}" = "1" ]; then
    echo "==> phase 2 sustained load (quick mode)"
    # Requires a freshly built binary (the harness refuses stale ones):
    cargo build --release -p oceanfs
    LOAD_TEST_DURATION_SECS="${LOAD_TEST_DURATION_SECS:-300}" \
        cargo test -p e2e --test load_sustained -- --test-threads=1
fi

echo "==> docs"
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

echo "==> core purity"
DEPS=$(cargo tree --edges normal -p oceanfs-core --depth 1 2>/dev/null | grep -E 'oceanfs-(?!core)' || true)
if [ -n "$DEPS" ]; then
    echo "ERROR: oceanfs-core depends on another oceanfs-* crate:"
    echo "$DEPS"
    exit 1
fi
echo "PASS: oceanfs-core has zero internal dependencies"

echo ""
echo "All checks passed."
