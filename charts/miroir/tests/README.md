# Helm Chart Validation Tests

## Running All Tests

```bash
./charts/miroir/tests/run-tests.sh
```

This runs both `helm lint --strict` (schema rules) and `helm template` (render-time rules) for all test cases.

## Test Cases

### Schema rejection tests (`helm lint --strict`)

| File | Rule | Description |
|------|------|-------------|
| `invalid-multi-replica-sqlite.yaml` | 1 | `replicas>1` with `taskStore.backend: sqlite` — SQLite cannot be shared across pods |
| `bad-hpa-no-redis.yaml` | 2a | `hpa.enabled: true` with `taskStore.backend: sqlite` — autoscaling requires Redis |
| `bad-hpa-single-replica.yaml` | 2b | `hpa.enabled: true` with `replicas: 1` — HPA requires `replicas >= 2` |
| `bad-search-ui-rate-limit-local-multi.yaml` | 3 | `search_ui.rate_limit.backend: local` with `replicas>1` — per-pod limits don't share state |
| `bad-admin-login-rate-limit-local-multi.yaml` | 4 | `admin_ui.login_rate_limit.backend: local` with `replicas>1` — per-pod limits don't share state |

### Template rejection tests (`helm template`)

| File | Rule | Description |
|------|------|-------------|
| `bad-scoped-key-rotate-gte-max.yaml` | 5a | `rotate_before_expiry >= max_age` — rotation fires at/before issuance |
| `bad-scoped-key-rotate-gt-max.yaml` | 5b | `rotate_before_expiry > max_age` — negative rotation window |

Rule 5 uses template-level `fail()` because JSON Schema draft-7 cannot compare sibling property values.

### Positive tests

| File | Description |
|------|-------------|
| `valid-single-replica-sqlite.yaml` | Single replica with SQLite (dev default) |
| `valid-single-pod-oversized.yaml` | Single-pod oversized mode (4 vCPU / 8 GB) for dev/small deployments |
| `valid-multi-replica-redis.yaml` | Multi-replica with Redis |
| `good-production.yaml` | Full production config with HPA, Redis rate limiting, and scoped keys |
| `good-dev-no-ui.yaml` | Minimal dev defaults |
