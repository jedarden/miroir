# Helm Chart Tests

This directory contains test cases for validating the `values.schema.json` constraints.

## Running Tests

Use `helm lint --strict` with the test values files:

```bash
# Valid: single replica with SQLite (should pass)
helm lint --strict miroir -f miroir/tests/valid-single-replica-sqlite.yaml

# Invalid: multiple replicas with SQLite (should fail with constraint error)
helm lint --strict miroir -f miroir/tests/invalid-multi-replica-sqlite.yaml

# Valid: multiple replicas with Redis (should pass)
helm lint --strict miroir -f miroir/tests/valid-multi-replica-redis.yaml
```

## Test Cases

| Test Case | Description | Expected Result |
|-----------|-------------|-----------------|
| `valid-single-replica-sqlite.yaml` | `replicas: 1, backend: sqlite` | ✅ Pass |
| `invalid-multi-replica-sqlite.yaml` | `replicas: 2, backend: sqlite` | ❌ Fail with constraint error |
| `valid-multi-replica-redis.yaml` | `replicas: 2, backend: redis` | ✅ Pass |
