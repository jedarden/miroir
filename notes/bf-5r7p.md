# P11.8 Repo Structure Compliance Verification

## Decision: No migration needed — §12 already matches current layout

The plan §12 was already updated to reflect the idiomatic Rust workspace structure:

```
tests/
├── benches/             # Supplementary benchmarks (score-comparability)
└── fixtures/            # Test fixtures and reference configs
```

Integration tests correctly live in `crates/*/tests/` as specified:
- `crates/miroir-core/tests/`
- `crates/miroir-proxy/tests/`
- `crates/miroir-ctl/tests/`

## Verification results

| Directory | Status | Notes |
|-----------|--------|-------|
| `tests/benches/` | ✅ | Contains score-comparability benchmark suite |
| `tests/fixtures/` | ✅ | Contains YAML fixture files |
| `dashboards/` | ✅ | Contains miroir-overview.json (miroir-afh.3) |
| `examples/` | ✅ | Contains dev-config.yaml, docker-compose-dev.yml, README.md |
| `examples/sdk-tests/` | ⏳ | Covered by P11.7 |

## Chaos test material

Per plan §12: "Chaos test material is documented in `docs/chaos_testing_report.md`" — this is correct and complete.

## CI

The repo uses `cargo test --all --all-features` from root, which correctly runs all crate-level integration tests.
