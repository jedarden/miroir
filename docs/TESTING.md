# Testing Guide

This document explains how to run tests for Miroir, including requirements for integration tests.

## Prerequisites

### Unit Tests
Unit tests run without external dependencies:
```bash
cargo nextest run
```

### Integration Tests

Integration tests require external services. Some tests can use either Docker containers or external services.

## Redis Integration Tests

The Redis integration tests (`task_store::redis::tests::integration`) test the Redis-backed task store.

### Option 1: Using Docker (testcontainers)

**Requirements:**
- Docker daemon running and accessible at `/var/run/docker.sock` or via `DOCKER_HOST`

**Run tests:**
```bash
cargo nextest run -E 'test(redis)'
```

The tests will automatically start a Redis container using testcontainers.

### Option 2: Using External Redis

**Requirements:**
- Redis server running and accessible

**Run tests:**
```bash
export MIROIR_TEST_REDIS_URL=redis://localhost:6379
cargo nextest run -E 'test(redis)'
```

### Option 3: Skip Docker Tests

If Docker is not available and you don't have an external Redis instance, you can skip these tests:

```bash
export MIROIR_TEST_SKIP_DOCKER=1
cargo nextest run -E 'test(redis)'
```

Tests will be skipped with a message indicating why.

## Docker Compose Integration Tests

The `docker_compose_integration` tests run against a full Miroir + Meilisearch stack.

### Option 1: Using Docker Compose

**Requirements:**
- Docker and docker-compose installed
- Start the stack: `docker compose -f examples/docker-compose-dev.yml up -d`

**Run tests:**
```bash
docker compose -f examples/docker-compose-dev.yml up -d
cargo nextest run -E 'test(docker_compose_integration)'
```

### Option 2: Using External Miroir

**Requirements:**
- A running Miroir instance accessible via HTTP

**Run tests:**
```bash
export MIROIR_TEST_MIROIR_URL=http://your-miroir-host:7700
cargo nextest run -E 'test(docker_compose_integration)'
```

### Option 3: Skip Docker Tests

If Docker is not available and you don't have an external Miroir instance, you can skip these tests:

```bash
export MIROIR_TEST_SKIP_DOCKER=1
cargo nextest run -E 'test(docker_compose_integration)'
```

Tests will be skipped with a message indicating why.

### Node Failure Test (RF=2)

The `test_node_failure_rf2` test requires the RF=2 docker-compose stack:

```bash
docker compose -f examples/docker-compose-dev-rf2.yml up -d
cargo nextest run -E 'test(node_failure_rf2)' --ignored
```

Or use an external RF=2 Miroir instance:

```bash
export MIROIR_TEST_MIROIR_URL=http://your-rf2-miroir:7710
cargo nextest run -E 'test(node_failure_rf2)' --ignored
```

## Phase Acceptance Tests

Phase acceptance tests (p10_*, p3_*) test specific feature integration.

**Requirements:**
- Docker for Redis (same as Redis integration tests)
- See individual test suites for specific requirements

## Running All Tests

To run all tests (unit + integration) without Docker-dependent tests:
```bash
MIROIR_TEST_SKIP_DOCKER=1 cargo nextest run
```

To run only unit tests (no external dependencies):
```bash
cargo nextest run --exclude 'integration|docker_compose|p10_|p3_'
```

## CI/CD

The Argo Workflows CI pipeline (see §7 CI/CD) runs all tests with:
- Docker daemon available for testcontainers
- Full docker-compose environment for integration tests

## Troubleshooting

### "SocketNotFoundError(/var/run/docker.sock)"
Docker is not running or not accessible. Options:
1. Start Docker: `sudo systemctl start docker` (Linux) or start Docker Desktop
2. Use external Redis: `export MIROIR_TEST_REDIS_URL=redis://...`
3. Skip Docker tests: `export MIROIR_TEST_SKIP_DOCKER=1`

### "Failed to start Redis: Connection refused"
External Redis is not running. Start Redis:
```bash
docker run -d -p 6379:6379 redis:alpine
# or
redis-server
```

### Tests timeout or hang
Some tests require proper cleanup. Use nextest's built-in timeout:
```bash
cargo nextest run --timeout 120
```
