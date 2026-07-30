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
cargo test --all-targets --all-features

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
