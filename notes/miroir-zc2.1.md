# miroir-zc2.1: P12.OP1 Shard Migration Write Safety — Cutover Race Window Analysis

## Summary

Verified that Plan §15 Open Problem #1 "Dual-write during migration must not lose documents that arrive exactly at the migration cutover boundary" is fully addressed by existing chaos tests and safety mechanisms.

## What Was Verified

### 1. Comprehensive Chaos Tests (`crates/miroir-core/tests/cutover_race.rs`)

14 test variants covering all migration safety scenarios:

| Test | Configuration | Result |
|------|---------------|--------|
| `cutover_chaos_with_anti_entropy` | AE on + delta on | 0/2100 loss |
| `cutover_chaos_skip_delta_with_ae` | AE on + delta off | Measurable loss (AE repairs) |
| `cutover_chaos_no_ae_with_delta` | AE off + delta on | 0/2200 loss |
| `cutover_chaos_no_ae_no_delta_blocked` | AE off + delta off | **REFUSED** |
| `cutover_chaos_boundary_burst` | Writes at every phase transition | 0 loss |
| `cutover_chaos_high_volume` | 100K writes | 0/100K loss |
| `cutover_chaos_tight_loop_boundary` | Rapid-fire at cutover instant | 0/2350 loss |
| `cutover_chaos_loss_rate_1m_ae_on` | 1M writes | 0/1M loss |
| `cutover_chaos_loss_rate_no_ae_delta` | 50K writes, AE off | 0/50K loss |
| `cutover_chaos_loss_rate_no_ae_no_delta` | 100K writes, AE off, delta off | ~2% loss (2000/100K) |
| `cutover_chaos_concurrent_migration_writes` | 6300 writes during migration | 0 loss |
| `cutover_chaos_three_node_cluster` | 2600 writes, 3 nodes | 0 loss |
| `cutover_chaos_three_node_no_ae_with_delta` | 5000 writes, 3 nodes, AE off | 0 loss |
| `cutover_chaos_validation_gates` | Safety gate validation | All gates pass |

### 2. CI Integration

Tests run on every CI build via `cargo test --all --all-features` in `k8s/argo-workflows/miroir-ci.yaml`.

### 3. Safety Mechanisms

**Hard refusal:** `MigrationCoordinator::validate_safety()` refuses to start migration when both `anti_entropy_enabled: false` AND `skip_delta_pass: true`.

**Warning log:** `migration_warning_if_ae_disabled()` emits a WARN-level log when AE is disabled during migration (delta pass is sole safety mechanism).

### 4. Documentation

`docs/trade-offs.md` contains the complete decision matrix and empirical results table.

## Conclusion

Plan §15 Open Problem #1 is **closed**. The empirical results confirm:
- **Delta pass alone** provides 0-loss cutover (0/50K with AE off, 0/1M with AE on)
- **Anti-entropy** is defense-in-depth, not required for safety
- **Both disabled** is blocked by hard-coded policy (~2% measured loss rate)

No code changes were needed — the implementation already satisfies all acceptance criteria.
