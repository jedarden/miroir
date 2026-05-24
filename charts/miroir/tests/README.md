# Miroir Helm Chart Tests

This directory contains test cases and validation scripts for the Miroir Helm chart.

## Schema Validation Tests

The `test_schema.py` script validates that the `values.schema.json` constraints are working correctly.

### Test Cases

| Test File | Description | Expected Result |
|-----------|-------------|-----------------|
| `replicas-1-sqlite.yaml` | Single replica with SQLite | PASS |
| `replicas-2-sqlite.yaml` | Multiple replicas with SQLite | FAIL (error: backend must be redis) |
| `replicas-2-redis.yaml` | Multiple replicas with Redis | PASS |

### Running Tests

```bash
# Using Python (works without helm installed)
python3 tests/test_schema.py

# Using helm lint (requires helm)
helm lint --strict -f tests/replicas-1-sqlite.yaml .
helm lint --strict -f tests/replicas-2-sqlite.yaml .  # Should fail
helm lint --strict -f tests/replicas-2-redis.yaml .
```

### Constraint Details

The values.schema.json enforces that when `miroir.replicas > 1`, the `taskStore.backend` must be `"redis"`. SQLite is a single-writer database and cannot be shared across multiple pods.

See values.schema.json lines 142-161 for the constraint implementation.
