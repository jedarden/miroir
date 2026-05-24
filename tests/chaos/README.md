# Miroir Chaos Tests

This directory contains chaos engineering tests for Miroir. These tests verify system behavior under failure conditions and ensure the system meets its availability and consistency guarantees.

## Prerequisites

Start the test environment:

```bash
cd /home/coding/miroir/examples
docker-compose -f docker-compose-dev.yml up -d
```

Wait for all services to be healthy:
```bash
docker-compose ps
```

## Scenarios

Each scenario has its own runbook with detailed steps:

1. [Kill 1 of 3 nodes (RF=2)](./01_kill_one_node_rf2.md) — Continuous search; degraded writes warn via header
2. [Kill 2 of 3 nodes (RF=2)](./02_kill_two_nodes_rf2.md) — Shard loss; 503 or partial per policy
3. [Kill 1 of 2 Miroir replicas](./03_kill_miroir_replica.md) — Zero client-visible downtime
4. [Network delay 500ms]((./04_network_delay.md) — Search slows, no errors
5. [Restart killed node](./05_restart_node.md) — Miroir detects within health interval
6. [Kill node mid-rebalance](./06_kill_during_rebalance.md) — Pause + resume; no data loss

## Running Tests

### Automated
```bash
cd /home/coding/miroir/tests/chaos
./run_all_chaos_tests.sh
```

### Manual
Follow the steps in each scenario's runbook.

## Cleanup

Stop the test environment:
```bash
cd /home/coding/miroir/examples
docker-compose -f docker-compose-dev.yml down -v
```

## Expected Behaviors

### RF=2 Configuration
- **1 node down**: Continued reads, writes degrade with warning header
- **2 nodes down**: Shard unavailability, 503 errors or partial results

### Miroir Replica Resilience
- **1 replica down**: Zero client-visible downtime (load balancer fails over)

### Rebalance Safety
- **Node killed during rebalance**: Pauses, resumes on restart, no data loss

## Monitoring

During chaos tests, monitor:

- Miroir logs: `docker logs miroir-orchestrator -f`
- Meilisearch logs: `docker logs miroir-meili-0 -f`
- Health status: `curl http://localhost:7700/health`
