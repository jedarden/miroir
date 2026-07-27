# `miroir-ctl doctor`

## Purpose

Diagnose deployment and runtime issues before they become outages. The `doctor` subcommands validate configuration and infrastructure dependencies.

## Preconditions

- `kubectl` configured with access to the target cluster (for secret checks)
- Network access to chart registries (for chart source checks)
- Network access to Redis instance (for task store checks)

## Examples

```bash
# Run pre-flight checks against a Helm values file
miroir-ctl doctor deploy-preflight --config values.yaml

# Run pre-flight checks against an ArgoCD Application spec
miroir-ctl doctor deploy-preflight --config k8s/argocd/miroir-application.yaml

# Specify namespace and context
miroir-ctl doctor deploy-preflight --config values.yaml --namespace miroir --context ardenone-cluster

# Exit with non-zero status on any check failure (for CI/CD pipelines)
miroir-ctl doctor deploy-preflight --config values.yaml --strict
```

## Gotchas

- **Chart source checks**: OCI chart registries require `helm` or `crane` CLI tools installed for full verification. The pre-flight check only verifies registry reachability, not chart existence.
- **Secret sync checks**: ExternalSecret sync status is read from Kubernetes — if the ExternalSecret controller is not running, sync status may appear healthy even if secrets are stale.
- **Task store checks**: Redis reachability checks only verify TCP connectivity, not authentication. The check cannot verify that the provided password is correct.
- **kubectl context**: The command uses the current kubectl context by default. Use `--context` to specify a different cluster context explicitly.

## Subcommands

### `deploy-preflight`

Run pre-flight checks before deploying Miroir. This command validates three critical dependencies:

1. **Chart source**: Verifies that the configured Helm chart source (OCI registry or Helm repo) is reachable and the pinned version resolves.
2. **Secret sync**: Verifies that the referenced Kubernetes Secret or ExternalSecret exists and is in Synced state.
3. **Task store**: Verifies that the configured task store backend (Redis or SQLite) is reachable.

**Why this matters**: ADR-1 (2026-07-20) documented a 3-month silent chart-pull failure when the OCI registry became unreachable. This command catches such infrastructure issues before deployment.

**Output format**:
```
=== Miroir Deploy Preflight Checks ===

Check 1: Chart Source
---------------------
  ✓ Helm repo reachable, chart miroir version 0.1.0 found

Check 2: Secret Sync Status
----------------------------
  ✓ ExternalSecret miroir-keys synced successfully in namespace miroir

Check 3: Task Store Reachability
---------------------------------
  ✓ Redis at redis://miroir-redis:6379 reachable (authentication not verified)

=== Summary ===
Total checks: 3
  Passed: 3
  Failed: 0
  Warnings: 0
```

**Exit codes**:
- `0`: All checks passed
- `1`: One or more checks failed (only when `--strict` flag is used)

## See also

- [Plan §6](../plan/plan.md#6-deployment) — Helm chart and GitOps configuration
- [Plan §9](../plan/plan.md#9-secrets-handling) — ExternalSecret and secret structure
- [ADR-1](../plan/plan.md#adr-1-2026-07-20) — Chart pull failure postmortem
- `miroir-ctl status` — Runtime cluster health checks
