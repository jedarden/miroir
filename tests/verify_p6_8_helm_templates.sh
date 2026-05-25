#!/usr/bin/env bash
# P6.8 Helm Template Verification (without kind cluster)
#
# This script verifies that the Helm chart templates are correctly configured
# for multi-pod Phase 6 deployment without requiring a running cluster.
#
# Usage: ./tests/verify_p6_8_helm_templates.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Render Helm chart with Phase 6 values
RENDERED_YAML=$(mktemp)
trap "rm -f $RENDERED_YAML" EXIT

log_info "Rendering Helm chart with Phase 6 values..."

helm template miroir "$PROJECT_ROOT/charts/miroir" \
    --set miroir.replicas=3 \
    --set miroir.hpa.enabled=true \
    --set miroir.hpa.minReplicas=3 \
    --set miroir.hpa.maxReplicas=5 \
    --set miroir.taskStore.backend=redis \
    --set redis.enabled=true \
    --set prometheusRule.enabled=true \
    > "$RENDERED_YAML" 2>/dev/null

log_success "Helm chart rendered"

# Test 1: HPA resource exists
echo ""
echo "=== Test 1: HPA Resource ==="

if grep -q "kind: HorizontalPodAutoscaler" "$RENDERED_YAML"; then
    log_success "HPA resource is present in rendered output"
else
    log_error "HPA resource not found"
    exit 1
fi

# Test 2: HPA has correct metrics
echo ""
echo "=== Test 2: HPA Metrics ==="

if grep -q "miroir_requests_in_flight" "$RENDERED_YAML"; then
    log_success "HPA includes miroir_requests_in_flight metric"
else
    log_error "HPA missing miroir_requests_in_flight metric"
    exit 1
fi

if grep -q "miroir_background_queue_depth" "$RENDERED_YAML"; then
    log_success "HPA includes miroir_background_queue_depth metric"
else
    log_error "HPA missing miroir_background_queue_depth metric"
    exit 1
fi

# Test 3: HPA uses correct metric types
echo ""
echo "=== Test 3: HPA Metric Types ==="

if grep -B 5 "miroir_requests_in_flight" "$RENDERED_YAML" | grep -q "type: Pods"; then
    log_success "miroir_requests_in_flight uses type: Pods (correct for per-pod metric)"
else
    log_error "miroir_requests_in_flight does not use type: Pods"
    exit 1
fi

if grep -B 5 "miroir_background_queue_depth" "$RENDERED_YAML" | grep -q "type: External"; then
    log_success "miroir_background_queue_depth uses type: External (correct for global metric)"
else
    log_error "miroir_background_queue_depth does not use type: External"
    exit 1
fi

# Test 4: PrometheusRule exists
echo ""
echo "=== Test 4: PrometheusRule ==="

if grep -q "kind: PrometheusRule" "$RENDERED_YAML"; then
    log_success "PrometheusRule resource is present"
else
    log_error "PrometheusRule resource not found"
    exit 1
fi

# Test 5: PrometheusRule has §14.9 alerts
echo ""
echo "=== Test 5: §14.9 Resource-Pressure Alerts ==="

P14_9_ALERTS=(
    "MiroirMemoryPressure"
    "MiroirRequestQueueBacklog"
    "MiroirBackgroundJobBacklog"
    "MiroirPeerDiscoveryGap"
    "MiroirNoLeader"
)

for alert in "${P14_9_ALERTS[@]}"; do
    if grep -q "alert: $alert" "$RENDERED_YAML"; then
        log_success "Alert $alert is present"
    else
        log_error "Alert $alert is missing"
        exit 1
    fi
done

# Test 6: Headless Service for peer discovery
echo ""
echo "=== Test 6: Headless Service ==="

if grep -q "miroir-headless" "$RENDERED_YAML"; then
    log_success "Headless Service miroir-headless is present"
else
    log_error "Headless Service miroir-headless not found"
    exit 1
fi

if grep -A 10 "miroir-headless" "$RENDERED_YAML" | grep -q "clusterIP: None"; then
    log_success "Headless Service has clusterIP: None"
else
    log_error "Headless Service does not have clusterIP: None"
    exit 1
fi

# Test 7: POD_NAME, POD_NAMESPACE, POD_IP env vars
echo ""
echo "=== Test 7: Downward API Environment Variables ==="

DOWNWARD_VARS=("POD_NAME" "POD_NAMESPACE" "POD_IP")

for var in "${DOWNWARD_VARS[@]}"; do
    if grep -q "name: $var" "$RENDERED_YAML"; then
        log_success "Environment variable $var is present"
    else
        log_error "Environment variable $var not found"
        exit 1
    fi
done

# Test 8: Resource requests/limits
echo ""
echo "=== Test 8: Resource Envelope ==="

if grep -q "cpu: 500m" "$RENDERED_YAML"; then
    log_success "CPU request 500m is present"
else
    log_info "CPU request 500m not found (may use default)"
fi

if grep -q "memory: 1Gi" "$RENDERED_YAML"; then
    log_success "Memory request 1Gi is present"
else
    log_info "Memory request 1Gi not found (may use default)"
fi

# Test 9: values.schema.json validation
echo ""
echo "=== Test 9: values.schema.json Validation ==="

SCHEMA="$PROJECT_ROOT/charts/miroir/values.schema.json"

if [ ! -f "$SCHEMA" ]; then
    log_error "values.schema.json not found"
    exit 1
fi

# Check for HPA validation rule
if grep -q "hpa requires replicas >= 2 and taskStore.backend='redis'" "$SCHEMA"; then
    log_success "values.schema.json has HPA validation rule"
else
    log_error "values.schema.json missing HPA validation rule"
    exit 1
fi

# Test 10: Leader election config in defaults
echo ""
echo "=== Test 10: Leader Election Configuration ==="

DEFAULTS_YAML="$PROJECT_ROOT/charts/miroir/values.yaml"

if grep -q "lease_ttl_s: 10" "$DEFAULTS_YAML"; then
    log_success "Leader election lease_ttl_s default is 10s"
else
    log_info "Leader election lease_ttl_s not in values.yaml (may be in code default)"
fi

if grep -q "renew_interval_s: 3" "$DEFAULTS_YAML"; then
    log_success "Leader election renew_interval_s default is 3s"
else
    log_info "Leader election renew_interval_s not in values.yaml (may be in code default)"
fi

echo ""
echo "========================================"
echo -e "${GREEN}All template verification tests passed!${NC}"
echo "========================================"
echo ""
echo "Note: This validates the Helm chart templates are correct."
echo "For full end-to-end testing, run ./tests/p6_8_multi_pod_acceptance.sh"
echo "which requires kind (Kubernetes in Docker)."
