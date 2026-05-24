# Miroir Integration Tests

End-to-end integration tests for Miroir with 3 Meilisearch nodes via docker-compose.

Per plan §8: Integration tests validate end-to-end behavior including:
- Document round-trip with distribution verification
- Search covers all shards
- Facet aggregation across shards
- Offset/limit paging consistency
- Settings broadcast to all nodes
- Task polling for large batches
- Node failure with RF=2

## Prerequisites

Start the docker-compose dev stack:

```bash
cd /home/coding/miroir/examples
docker-compose -f docker-compose-dev.yml up -d
```

Wait for all services to be healthy (check with `docker-compose ps`).

## Running Tests

### All integration tests
```bash
cd /home/coding/miroir
cargo test --test integration -- --test-threads=1
```

**Important:** Use `--test-threads=1` to prevent concurrent tests from interfering with each other (they share indexes).

### Individual tests
```bash
cargo test --test integration document_round_trip -- --test-threads=1
cargo test --test integration search_covers_all_shards -- --test-threads=1
cargo test --test integration facet_aggregation -- --test-threads=1
cargo test --test integration offset_limit_paging -- --test-threads=1
cargo test --test integration settings_broadcast -- --test-threads=1
cargo test --test integration task_polling -- --test-threads=1
```

### Node failure test (requires RF=2 stack)

The `node_failure_rf2` test requires the RF=2 docker-compose stack with 6 nodes:

```bash
cd /home/coding/miroir/examples
docker-compose -f docker-compose-dev-rf2.yml up -d
```

Then run:
```bash
MIROIR_RF2_PORT=7700 cargo test --test integration node_failure_rf2 -- --test-threads=1 --ignored
```

## Test Descriptions

| Test | Description | Expected Behavior |
|------|-------------|-------------------|
| `document_round_trip` | Index 1000 documents, retrieve each by ID | All documents found; distributed across ≥2 nodes |
| `search_covers_all_shards` | Index 100 docs with unique keywords, search each | Every search returns exactly 1 hit |
| `facet_aggregation` | 100 docs across 3 colors, facet by color | Facet counts sum to 100 |
| `offset_limit_paging` | 50 docs, compare 5×paged vs single limit=50 | Same documents, same order, no duplicates |
| `settings_broadcast` | Add synonyms, verify on all 3 nodes | All nodes have synonyms; synonym search works |
| `task_polling` | Index 500 docs, poll until succeeded | Task succeeds; all 500 docs searchable |
| `node_failure_rf2` | With RF=2, verify search works with 1 node down | All results returned; no degraded header |

## Troubleshooting

**Connection refused:** Ensure docker-compose stack is running:
```bash
docker-compose -f ../docker-compose-dev.yml ps
```

**Tests timeout:** Increase health check timeout in `ensure_healthy()`.

**Index already exists:** Tests clean up existing indices automatically.

**Node failure test fails:** Ensure the RF=2 stack is running with 6 nodes + Redis.
