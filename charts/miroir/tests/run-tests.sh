#!/usr/bin/env bash
# Run all values.schema.json and template validation tests for the miroir Helm chart.
# Exit non-zero if any test fails.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHART_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PASS=0 FAIL=0

red()  { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }

# expect_fail_lint VALUES_FILE DESCRIPTION
expect_fail_lint() {
  local vals="$1" desc="$2"
  if helm lint --strict "$CHART_DIR" -f "$vals" >/dev/null 2>&1; then
    red "FAIL: $desc (lint should have rejected $vals)"
    FAIL=$((FAIL+1))
  else
    green "PASS: $desc"
    PASS=$((PASS+1))
  fi
}

# expect_fail_template VALUES_FILE DESCRIPTION
expect_fail_template() {
  local vals="$1" desc="$2"
  if helm template test-release "$CHART_DIR" -f "$vals" >/dev/null 2>&1; then
    red "FAIL: $desc (template should have rejected $vals)"
    FAIL=$((FAIL+1))
  else
    green "PASS: $desc (template)"
    PASS=$((PASS+1))
  fi
}

# expect_pass_lint VALUES_FILE DESCRIPTION
expect_pass_lint() {
  local vals="$1" desc="$2"
  if helm lint --strict "$CHART_DIR" -f "$vals" >/dev/null 2>&1; then
    green "PASS: $desc"
    PASS=$((PASS+1))
  else
    red "FAIL: $desc (lint should have accepted $vals)"
    FAIL=$((FAIL+1))
  fi
}

# expect_pass_template VALUES_FILE DESCRIPTION
expect_pass_template() {
  local vals="$1" desc="$2"
  if helm template test-release "$CHART_DIR" -f "$vals" >/dev/null 2>&1; then
    green "PASS: $desc (template)"
    PASS=$((PASS+1))
  else
    red "FAIL: $desc (template should have accepted $vals)"
    FAIL=$((FAIL+1))
  fi
}

echo "=== Schema rejection tests (helm lint --strict) ==="
expect_fail_lint "$SCRIPT_DIR/invalid-multi-replica-sqlite.yaml" \
  "Rule 1: replicas>1 with sqlite backend"
expect_fail_lint "$SCRIPT_DIR/bad-hpa-no-redis.yaml" \
  "Rule 2a: hpa enabled with sqlite backend"
expect_fail_lint "$SCRIPT_DIR/bad-hpa-single-replica.yaml" \
  "Rule 2b: hpa enabled with replicas=1"
expect_fail_lint "$SCRIPT_DIR/bad-search-ui-rate-limit-local-multi.yaml" \
  "Rule 3: search_ui local rate limit with multi-replica"
expect_fail_lint "$SCRIPT_DIR/bad-admin-login-rate-limit-local-multi.yaml" \
  "Rule 4: admin_ui local rate limit with multi-replica"

echo ""
echo "=== Template rejection tests (helm template) ==="
expect_fail_template "$SCRIPT_DIR/bad-scoped-key-rotate-gte-max.yaml" \
  "Rule 5a: scoped_key_rotate >= scoped_key_max_age"
expect_fail_template "$SCRIPT_DIR/bad-scoped-key-rotate-gt-max.yaml" \
  "Rule 5b: scoped_key_rotate > scoped_key_max_age"

echo ""
echo "=== Positive tests (should all pass) ==="
expect_pass_lint "$SCRIPT_DIR/valid-single-replica-sqlite.yaml" \
  "valid: single replica, sqlite"
expect_pass_lint "$SCRIPT_DIR/valid-single-pod-oversized.yaml" \
  "valid: single-pod oversized (4 vCPU / 8 GB)"
expect_pass_lint "$SCRIPT_DIR/valid-multi-replica-redis.yaml" \
  "valid: multi replica, redis"
expect_pass_lint "$SCRIPT_DIR/good-production.yaml" \
  "valid: full production config"
expect_pass_lint "$SCRIPT_DIR/good-dev-no-ui.yaml" \
  "valid: dev defaults"
expect_pass_template "$SCRIPT_DIR/good-production.yaml" \
  "valid: full production config"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
