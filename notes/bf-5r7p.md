# P11.8 Repo Structure Compliance Verification

## Task

Verify repo structure compliance with plan §12 "Repository structure" (lines 2161-2197).

## Finding

**The repository is ALREADY in full compliance with plan §12.**

The bead description contained an error: it claimed the plan specified `tests/integration/` at the top level. The actual plan §12 (lines 2173-2177, 2195) explicitly states:

> Integration tests live in `crates/*/tests/` following Rust workspace conventions.

This is the idiomatic Rust workspace layout, and that's exactly what exists.

## Verified Structure

| Plan §12 requirement | Current state | Status |
|---------------------|---------------|--------|
| `crates/miroir-core/tests/` | 11 test files | ✅ |
| `crates/miroir-proxy/tests/` | 7 test files | ✅ |
| `crates/miroir-ctl/tests/` | 1 test file | ✅ |
| `dashboards/miroir-overview.json` | Exists (from miroir-afh.3) | ✅ |
| `examples/` | docker-compose-dev.yml, dev-config.yaml, README.md | ✅ |
| `benches/` | Criterion benchmarks | ✅ |
| `Cargo.toml`, `Dockerfile`, `CHANGELOG.md`, `LICENSE` | All present | ✅ |

## Decision

**No migration required.** The plan §12 already correctly documents the current crate-level test layout. No amendments needed.

The only difference is `examples/sdk-tests/` subdirectories (python, javascript, go, rust) which don't exist yet — this is explicitly covered by P11.7.

## CI Verification

Tests run correctly from root:
```bash
cargo test --all --all-features
```

This matches plan §12 line 2195.
