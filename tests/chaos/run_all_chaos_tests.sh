#!/usr/bin/env bash
# Run all chaos tests against the docker-compose-dev stack
#
# Prerequisites:
#   docker-compose-dev stack running
#   curl, jq installed
#
# Usage:
#   ./run_all_chaos_tests.sh [test_number]
#
#   With no arguments: runs all tests sequentially
#   With test_number: runs only that test (01-06)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIROIR_URL="${MIROIR_URL:-http://localhost:7700}"
MIROIR_MASTER_KEY="${MIROIR_MASTER_KEY:-dev-key}"

export MIROIR_URL MIROIR_MASTER_KEY

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test functions
check_prereqs() {
    echo "=== Checking prerequisites ==="

    # Check docker-compose stack
    if ! curl -sf "$MIROIR_URL/health" >/dev/null 2>&1; then
        echo -e "${RED}✗ Miroir is not reachable at $MIROIR_URL${NC}"
        echo "Start the dev stack first:"
        echo "  cd /home/coding/miroir/examples && docker-compose -f docker-compose-dev.yml up -d"
        exit 1
    fi
    echo -e "${GREEN}✓ Miroir is reachable${NC}"

    # Check required tools
    for cmd in curl jq docker; do
        if ! command -v $cmd &>/dev/null; then
            echo -e "${RED}✗ $cmd not found${NC}"
            exit 1
        fi
    done
    echo -e "${GREEN}✓ Required tools installed${NC}"

    # Check all Meilisearch nodes
    for i in 0 1 2; do
        if ! docker ps | grep -q "miroir-meili-$i"; then
            echo -e "${RED}✗ Meilisearch node meili-$i is not running${NC}"
            exit 1
        fi
    done
    echo -e "${GREEN}✓ All Meilisearch nodes running${NC}"

    echo ""
}

run_test() {
    local test_num="$1"
    local test_name="$2"
    local runbook="$3"

    echo "========================================"
    echo "Running Test $test_num: $test_name"
    echo "========================================"

    if [ ! -f "$runbook" ]; then
        echo -e "${RED}✗ Runbook not found: $runbook${NC}"
        return 1
    fi

    echo "See runbook for manual steps: $runbook"
    echo ""

    # For now, chaos tests are semi-automated
    # The runbooks contain the detailed steps
    echo -e "${YELLOW}⚠️  This test requires manual execution${NC}"
    echo "Please follow the steps in: $runbook"
    echo ""

    read -p "Press Enter to continue after manual execution, or Ctrl+C to abort..."

    echo -e "${GREEN}✓ Test $test_num completed${NC}"
    echo ""
}

cleanup() {
    echo "=== Cleanup ==="

    # Delete test indices
    for i in {1..6}; do
        INDEX="chaos_test_0$i"
        curl -X DELETE "$MIROIR_URL/indexes/$INDEX" \
            -H "Authorization: Bearer $MIROIR_MASTER_KEY" \
            -s >/dev/null 2>&1 || true
    done
    echo -e "${GREEN}✓ Test indices deleted${NC}"

    # Ensure all nodes are running
    for i in 0 1 2; do
        docker start "miroir-meili-$i" 2>/dev/null || true
    done
    echo -e "${GREEN}✓ All Meilisearch nodes running${NC}"
}

# Main
main() {
    check_prereqs

    # Set up cleanup on exit
    trap cleanup EXIT

    # Run specified test or all tests
    if [ -n "$1" ]; then
        case "$1" in
            01|1)
                run_test "01" "Kill 1 of 3 Nodes (RF=2)" "$SCRIPT_DIR/01_kill_one_node_rf2.md"
                ;;
            02|2)
                run_test "02" "Kill 2 of 3 Nodes (RF=2)" "$SCRIPT_DIR/02_kill_two_nodes_rf2.md"
                ;;
            03|3)
                run_test "03" "Kill 1 of 2 Miroir Replicas" "$SCRIPT_DIR/03_kill_miroir_replica.md"
                ;;
            04|4)
                run_test "04" "Network Delay 500ms" "$SCRIPT_DIR/04_network_delay.md"
                ;;
            05|5)
                run_test "05" "Restart Killed Node" "$SCRIPT_DIR/05_restart_node.md"
                ;;
            06|6)
                run_test "06" "Kill Node Mid-Rebalance" "$SCRIPT_DIR/06_kill_during_rebalance.md"
                ;;
            *)
                echo -e "${RED}✗ Invalid test number: $1${NC}"
                echo "Valid tests: 01-06"
                exit 1
                ;;
        esac
    else
        # Run all tests
        run_test "01" "Kill 1 of 3 Nodes (RF=2)" "$SCRIPT_DIR/01_kill_one_node_rf2.md"
        run_test "02" "Kill 2 of 3 Nodes (RF=2)" "$SCRIPT_DIR/02_kill_two_nodes_rf2.md"
        run_test "03" "Kill 1 of 2 Miroir Replicas" "$SCRIPT_DIR/03_kill_miroir_replica.md"
        run_test "04" "Network Delay 500ms" "$SCRIPT_DIR/04_network_delay.md"
        run_test "05" "Restart Killed Node" "$SCRIPT_DIR/05_restart_node.md"
        run_test "06" "Kill Node Mid-Rebalance" "$SCRIPT_DIR/06_kill_during_rebalance.md"
    fi

    echo "========================================"
    echo -e "${GREEN}All chaos tests completed!${NC}"
    echo "========================================"
}

main "$@"
