#!/bin/bash
# Check that the [Unreleased] section gained an entry under at least one subheading
# since the last release. Intended for CI validation on every PR.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHANGELOG="$REPO_ROOT/CHANGELOG.md"

# Find the last released version section (e.g., "## [0.1.0] - 2026-04-19")
# This marks the end of the [Unreleased] section.
LAST_VERSION_HEADER=$(grep -m1 '^## \[[0-9]' "$CHANGELOG" || echo "")

if [[ -z "$LAST_VERSION_HEADER" ]]; then
    echo "FAIL: No version section found in CHANGELOG.md (first release?)" >&2
    exit 1
fi

# Extract the [Unreleased] section (everything between "## [Unreleased]" and the first version header)
UNRELEASED_CONTENT=$(awk "/^## \[Unreleased\]/{found=1; next} /^## \[[0-9]/{exit} found{print}" "$CHANGELOG")

if [[ -z "$UNRELEASED_CONTENT" ]]; then
    echo "FAIL: [Unreleased] section is empty" >&2
    exit 1
fi

# Check for at least one entry under the standard subheadings
# (Added, Changed, Deprecated, Removed, Fixed, Security)
HAS_ENTRY=0

for category in Added Changed Deprecated Removed Fixed Security; do
    if echo "$UNRELEASED_CONTENT" | grep -q "^### $category"; then
        # Check that this category has at least one non-empty bullet
        CONTENT=$(echo "$UNRELEASED_CONTENT" | awk "/^### $category/{found=1; next} /^### /{exit} found{print}" | grep -v '^[[:space:]]*$' || true)
        if [[ -n "$CONTENT" ]]; then
            HAS_ENTRY=1
            break
        fi
    fi
done

if [[ $HAS_ENTRY -eq 0 ]]; then
    echo "FAIL: [Unreleased] section has no entries under any subheading (Added, Changed, etc.)" >&2
    exit 1
fi

echo "OK: [Unreleased] section has at least one entry"
exit 0
