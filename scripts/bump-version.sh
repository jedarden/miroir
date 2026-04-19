#!/bin/bash
set -euo pipefail

NEW_VERSION="${1:?Usage: $0 <version>  (e.g. 0.2.0 or 0.2.0-rc.1)}"
NEW_VERSION="${NEW_VERSION#v}"  # strip leading 'v' if present

if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-].+)?$ ]]; then
    echo "error: version must be semver (e.g. 0.2.0 or 0.2.0-rc.1)" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Determine prerelease flag for ArtifactHub annotation
if [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+-.+ ]]; then
    PRERELEASE="true"
else
    PRERELEASE="false"
fi

sed -i "s/^version = .*/version = \"$NEW_VERSION\"/" "$REPO_ROOT/Cargo.toml"
sed -i "s/^version: .*/version: $NEW_VERSION/" "$REPO_ROOT/charts/miroir/Chart.yaml"
sed -i "s/^appVersion: .*/appVersion: $NEW_VERSION/" "$REPO_ROOT/charts/miroir/Chart.yaml"
sed -i "s/artifacthub\.io\/prerelease: .*/artifacthub.io\/prerelease: \"$PRERELEASE\"/" \
    "$REPO_ROOT/charts/miroir/Chart.yaml"

echo "Bumped to $NEW_VERSION:"
echo "  Cargo.toml"
echo "  charts/miroir/Chart.yaml (version, appVersion, prerelease=$PRERELEASE)"
