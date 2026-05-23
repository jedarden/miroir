# Integration Tests

This directory contains integration tests for Miroir that run against a real Docker Compose stack.

## Prerequisites

Start the Docker Compose development stack:

```bash
docker compose -f examples/docker-compose-dev.yml up -d
```

Wait for all containers to be healthy (may take 30-60 seconds):

```bash
docker compose -f examples/docker-compose-dev.yml ps
```

## Running Tests

Run all integration tests:

```bash
cargo test -p miroir-proxy --test docker_compose_integration -- --test-threads=1
```

Run a specific test:

```bash
cargo test -p miroir-proxy --test docker_compose_integration test_document_round_trip -- --exact
```

Run tests with output:

```bash
cargo test -p miroir-proxy --test docker_compose_integration -- --test-threads=1 --nocapture
```

## Test Coverage

| Test | Description | AC Reference |
|------|-------------|--------------|
| `test_document_round_trip` | Index and retrieve 1000 documents | §8.1 |
| `test_search_shard_coverage` | Verify search hits all 16 shards | §8.2 |
| `test_facet_aggregation` | Verify facet counts sum correctly | §8.3 |
| `test_offset_limit_paging` | Test pagination with offset/limit | §8.4 |
| `test_settings_broadcast` | Verify settings propagate to all nodes | §8.5 |
| `test_task_polling` | Test task status polling | §8.6 |
| `test_health_check` | Verify health endpoint responds | §8.7 |
| `test_direct_meilisearch_access` | Verify direct node access for debugging | §8.7 |
| `test_node_failure_rf2` | Test graceful degradation with node failure | §8.8 |

## RF=2 / High Availability Tests

The `test_node_failure_rf2` test requires the RF=2 docker-compose stack:

```bash
# Start the RF=2 stack (6 Meilisearch nodes, RF=2, RG=2)
docker compose -f examples/docker-compose-dev-rf2.yml up -d

# Run the node failure test
cargo test -p miroir-proxy --test docker_compose_integration test_node_failure_rf2 -- --ignored

# Clean up
docker compose -f examples/docker-compose-dev-rf2.yml down -v
```

The node failure test:
1. Indexes documents with RF=2 (each document replicated to both replica groups)
2. Stops a Meilisearch node mid-test (`docker stop miroir-meili-1`)
3. Verifies that searches still work using remaining replicas
4. Restarts the node and verifies recovery

## Cleanup

Stop and remove containers:

```bash
docker compose -f examples/docker-compose-dev.yml down -v
```

For RF=2 tests:

```bash
docker compose -f examples/docker-compose-dev-rf2.yml down -v
```

## Troubleshooting

### Containers not starting

Check container status:

```bash
docker compose -f examples/docker-compose-dev.yml ps
```

Check logs:

```bash
docker compose -f examples/docker-compose-dev.yml logs miroir
docker compose -f examples/docker-compose-dev.yml logs meili-0
```

### Port conflicts

If ports 7700-7703 are already in use, modify the port mappings in `examples/docker-compose-dev.yml`.

For RF=2 tests, ensure ports 7710-7716 and 6379 are available.

### Tests failing with connection refused

Ensure the Docker Compose stack is running:

```bash
docker compose -f examples/docker-compose-dev.yml ps
```

Wait for all containers to be healthy before running tests.

### Node failure test not working

The RF=2 test requires:
1. The RF=2 docker-compose stack to be running
2. Sufficient memory for 6 Meilisearch containers + Redis
3. The `docker` command to be accessible from the test environment

Run with `--ignored` flag:
```bash
cargo test --test integration test_node_failure_rf2 -- --ignored
```
