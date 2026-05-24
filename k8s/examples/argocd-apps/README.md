# ArgoCD Application Templates for Miroir

This directory contains example ArgoCD Application manifests and supporting files for deploying Miroir via GitOps.

## Pattern

Each Miroir instance is deployed via ArgoCD using the following structure in `jedarden/declarative-config`:

```
k8s/
├── ardenone-cluster/
│   └── miroir/
│       ├── prod/                      # Production instance
│       │   ├── app.yaml               # ArgoCD Application
│       │   ├── Chart.yaml             # Shim referencing upstream OCI chart
│       │   ├── values.yaml            # Instance-specific values
│       │   └── external-secret.yaml   # ESO secret definitions (optional)
│       └── dev/                       # Development instance
│           ├── app.yaml
│           ├── Chart.yaml
│           └── values.yaml
├── ardenone-manager/
│   └── miroir/
│       └── staging/
│           ├── app.yaml
│           ├── Chart.yaml
│           └── values.yaml
└── rs-manager/
    └── miroir/
        └── prod/
            ├── app.yaml
            ├── Chart.yaml
            └── values.yaml
```

## Files

### app.yaml - ArgoCD Application

Defines the ArgoCD Application that syncs the instance. References the path in declarative-config where `Chart.yaml` and `values.yaml` live.

Key fields:
- `spec.source.path`: Points to `k8s/<cluster>/miroir/<instance>/`
- `spec.source.helm.valueFiles`: `["values.yaml"]`
- `spec.destination.namespace`: Target namespace
- `spec.syncPolicy.automated`: `prune: true, selfHeal: true`

### Chart.yaml - Shim Chart

References the upstream Miroir Helm chart from OCI registry. Allows declarative-config to pin to a specific chart version while keeping instance values separate.

```yaml
dependencies:
  - name: miroir
    repository: oci://ghcr.io/jedarden/charts
    version: ">=0.1.0"
```

### values.yaml - Instance Values

Instance-specific Helm values. See `values-prod.yaml` and `values-dev.yaml` for examples.

Key differences:
- **Production**: `replicas: 2`, `replicationFactor: 2`, `replicaGroups: 2`, Redis task store, HPA enabled
- **Development**: `replicas: 1`, `replicationFactor: 1`, `replicaGroups: 1`, SQLite task store, HPA disabled

### external-secret.yaml - ESO Secrets (Optional)

Defines ExternalSecret resources that pull secrets from OpenBao. Required when using ESO for secret management.

## Deployment Steps

1. **Create the directory structure in declarative-config:**
   ```bash
   cd jedarden/declarative-config
   mkdir -p k8s/ardenone-cluster/miroir/prod
   ```

2. **Copy and customize the templates:**
   ```bash
   cp /path/to/miroir/k8s/examples/argocd-apps/app.yaml k8s/ardenone-cluster/miroir/prod/app.yaml
   cp /path/to/miroir/k8s/examples/argocd-apps/Chart.yaml k8s/ardenone-cluster/miroir/prod/Chart.yaml
   cp /path/to/miroir/k8s/examples/argocd-apps/values-prod.yaml k8s/ardenone-cluster/miroir/prod/values.yaml
   cp /path/to/miroir/k8s/examples/argocd-apps/external-secret.yaml k8s/ardenone-cluster/miroir/prod/external-secret.yaml
   ```

3. **Update placeholders in values.yaml:**
   - `<secret-name>`: Name of K8s Secret or ExternalSecret
   - `<namespace>`: Target namespace
   - `<ingress-host>`: Ingress hostname (if enabled)
   - `<issuer-name>`: cert-manager cluster issuer (if using ingress)

4. **Create the ArgoCD Application:**
   ```bash
   kubectl --kubeconfig=$HOME/.kube/ardenone-manager.kubeconfig apply -f k8s/ardenone-cluster/miroir/prod/app.yaml
   ```

5. **Verify in ArgoCD UI:**
   - Navigate to `https://argocd-ardenone-manager-ts.ardenone.com:8444`
   - Check that the application syncs successfully

## Multi-Cluster

Each cluster gets its own directory under `k8s/`. The Application manifest's `spec.destination.server` always points to `https://kubernetes.default.svc` (in-cluster) since ArgoCD runs on the same cluster.

Supported clusters:
- `apexalgo-iad`
- `ardenone-cluster`
- `ardenone-manager`
- `rs-manager`
- `ord-devimprint`
- `iad-kalshi`
- `iad-options`

## Validation

Before pushing to declarative-config:

1. **Validate values.yaml against the chart schema:**
   ```bash
   helm lint k8s/ardenone-cluster/miroir/prod
   ```

2. **Dry-run the install:**
   ```bash
   helm install miroir-test ghcr.io/jedarden/charts/miroir --version 0.1.0 \
     --namespace miroir-prod --dry-run --debug \
     -f k8s/ardenone-cluster/miroir/prod/values.yaml
   ```

## Acceptance Criteria (P8.5)

- [x] ArgoCD Application manifest template follows plan §6 pattern
- [x] Chart.yaml shim references upstream OCI chart
- [x] Example values.yaml for production (RF=2, RG=2, Redis)
- [x] Example values.yaml for development (RF=1, RG=1, SQLite)
- [x] ExternalSecret template for ESO integration
- [x] Documentation for deployment steps

## References

- Plan §6: Deployment
- Plan §9: Secrets Handling (ESO integration)
- `/home/coding/miroir/charts/miroir/` - Upstream Helm chart
