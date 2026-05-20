# Miroir Docker Compose Examples

This directory contains example Docker Compose configurations for running Miroir locally. These are intended for development, testing, and onboarding — not for production deployments.

## Quick Start (5 minutes)

Start the development stack with 3 Meilisearch nodes and one Miroir orchestrator:

```bash
# From the repository root
docker compose -f examples/docker-compose-dev.yml up -d

# Verify health
curl http://localhost:7700/health
# Expected: {"status":"available"}

# Index documents (Meilisearch-compatible API)
curl -X POST http://localhost:7700/indexes/movies/documents \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '[{"id": 1, "title": "Inception"}, {"id": 2, "title": "Interstellar"}]'

# Search
curl -X POST http://localhost:7700/indexes/movies/search \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '{"q": "inception"}'

# Teardown (removes containers and volumes)
docker compose -f examples/docker-compose-dev.yml down -v
```

## Architecture

The development stack (`docker-compose-dev.yml`) consists of:

| Service | Container Name | Port | Purpose |
|---------|---------------|------|---------|
| miroir | miroir-orchestrator | 7700 | Miroir orchestrator (client-facing API) |
| meili-0 | miroir-meili-0 | 7701 | Meilisearch node 0 (shard replica group 0) |
| meili-1 | miroir-meili-1 | 7702 | Meilisearch node 1 (shard replica group 0) |
| meili-2 | miroir-meili-2 | 7703 | Meilisearch node 2 (shard replica group 0) |
| redis | miroir-redis | 6379 | Optional: Task store for multi-replica deployments |

### Sharding Configuration

The default `dev-config.yaml` configures:
- **16 logical shards** striped across 3 nodes
- **Replication factor: 1** (no redundancy; use RF≥2 for production)
- **1 replica group** (all nodes in the same failure domain)
- **Task store: SQLite** (use Redis for multi-replica deployments)

## Multi-Replica Setup with Redis

For testing multi-replica deployments (RF≥2), enable Redis:

1. Uncomment the `redis` service in `docker-compose-dev.yml`
2. Update `dev-config.yaml` to use Redis:

```yaml
task_store:
  backend: redis
  url: "redis://redis:6379"
```

3. Increase replication factor:

```yaml
replication_factor: 2
```

4. Restart the stack:

```bash
docker compose -f examples/docker-compose-dev.yml down -v
docker compose -f examples/docker-compose-dev.yml up -d
```

## Configuration

The Miroir orchestrator is configured via `dev-config.yaml`, which is mounted read-only into the container at `/etc/miroir/config.yaml`. Key settings:

| Setting | Value | Description |
|---------|-------|-------------|
| `master_key` | `dev-key` | Client API key (use for local testing) |
| `node_master_key` | `dev-node-key` | Key Miroir uses to authenticate to Meilisearch nodes |
| `shards` | `16` | Number of logical shards |
| `replication_factor` | `1` | Replication factor (increase for redundancy) |
| `task_store.backend` | `sqlite` | Task store backend (`sqlite` for dev, `redis` for multi-replica) |

## Direct Meilisearch Access

You can access individual Meilisearch nodes directly (useful for debugging):

```bash
# Node 0
curl http://localhost:7701/health

# Node 1
curl http://localhost:7702/health

# Node 2
curl http://localhost:7703/health
```

**Note:** Direct writes to Meilisearch nodes bypass Miroir's shard routing and are **not recommended**. Always write through the Miroir orchestrator.

## Logs

View logs for all services:

```bash
docker compose -f examples/docker-compose-dev.yml logs -f
```

View logs for a specific service:

```bash
docker compose -f examples/docker-compose-dev.yml logs -f miroir
docker compose -f examples/docker-compose-dev.yml logs -f meili-0
```

## Troubleshooting

### Containers not starting

```bash
# Check container status
docker compose -f examples/docker-compose-dev.yml ps

# Check logs for errors
docker compose -f examples/docker-compose-dev.yml logs
```

### Health check failing

```bash
# Wait for containers to become healthy (can take 30-60 seconds)
docker compose -f examples/docker-compose-dev.yml ps

# If health checks fail, check individual node health
curl http://localhost:7701/health
curl http://localhost:7702/health
curl http://localhost:7703/health
```

### Port conflicts

If ports 7700-7703 are already in use, modify the port mappings in `docker-compose-dev.yml`:

```yaml
ports:
  - "7700:7700"  # Change to "7710:7700" if 7700 is in use
```

### Reset everything

```bash
# Stop and remove all containers and volumes
docker compose -f examples/docker-compose-dev.yml down -v

# Restart from scratch
docker compose -f examples/docker-compose-dev.yml up -d
```

## Production Deployment

For production deployments on Kubernetes, use the [Miroir Helm chart](https://github.com/jedarden/miroir/tree/main/charts/miroir). See the main README.md for production deployment instructions.

## CI/CD

The Docker Compose stack is exercised by CI smoke tests on every PR. See [k8s/argo-workflows/miroir-ci-docker-compose-smoke.yaml](../k8s/argo-workflows/miroir-ci-docker-compose-smoke.yaml) for the test workflow.
