#!/bin/bash
# Verify Cargo.toml workspace version, Chart.yaml appVersion, and CHANGELOG header all agree.
# Optionally accepts the expected version as $1 (e.g. called from CI with the git tag).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXPECTED="${1:-}"
EXPECTED="${EXPECTED#v}"  # strip leading 'v'

FAIL=0

# Read authoritative version from Cargo.toml [workspace.package]
CARGO_VERSION=$(sed -n '/^\[workspace\.package\]/,/^\[/p' "$REPO_ROOT/Cargo.toml" \
    | grep '^version = ' | head -1 | sed 's/version = "\(.*\)"/\1/')

if [[ -z "$CARGO_VERSION" ]]; then
    echo "FAIL: could not parse version from Cargo.toml" >&2
    exit 1
fi

if [[ -n "$EXPECTED" && "$CARGO_VERSION" != "$EXPECTED" ]]; then
    echo "FAIL: Cargo.toml version ($CARGO_VERSION) != expected ($EXPECTED)" >&2
    FAIL=1
fi

CHART_VERSION=$(grep '^version: ' "$REPO_ROOT/charts/miroir/Chart.yaml" | sed 's/version: //')
APP_VERSION=$(grep '^appVersion: '  "$REPO_ROOT/charts/miroir/Chart.yaml" | sed 's/appVersion: //' | tr -d '"')

if [[ "$CARGO_VERSION" != "$CHART_VERSION" ]]; then
    echo "FAIL: Cargo.toml version ($CARGO_VERSION) != Chart.yaml version ($CHART_VERSION)" >&2
    FAIL=1
fi

if [[ "$CARGO_VERSION" != "$APP_VERSION" ]]; then
    echo "FAIL: Cargo.toml version ($CARGO_VERSION) != Chart.yaml appVersion ($APP_VERSION)" >&2
    FAIL=1
fi

if ! grep -q "^## \[$CARGO_VERSION\]" "$REPO_ROOT/CHANGELOG.md"; then
    echo "FAIL: CHANGELOG.md missing section for version $CARGO_VERSION" >&2
    FAIL=1
fi

[[ $FAIL -eq 0 ]] && echo "OK: all versions consistent at $CARGO_VERSION"
exit $FAIL
