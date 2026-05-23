# P8.4: Argo Workflows CI Template - miroir-ci.yaml

## Status: COMPLETE

Template exists at `jedarden/declarative-config/k8s/iad-ci/argo-workflows/miroir-ci.yaml` and is synced by ArgoCD app `argo-workflows-ns-iad-ci`.

## Verification

### Template Content
All requirements verified against spec:
- `git-checkout`: alpine/git:2.43.0 → clones to /workspace/src
- `cargo-lint`: rust:1.87-slim → fmt --check + clippy -D warnings
- `cargo-test`: rust:1.87-slim → test --all --all-features (2 CPU / 4 GiB)
- `cargo-build`: rust:1.87-slim + musl-tools → builds miroir-proxy + miroir-ctl (4 CPU / 8 GiB)
- `docker-build-push`: kaniko v1.23.0-debug → ghcr.io/jedarden/miroir (tag-gated)
- `create-github-release`: gh cli 2.49.0 → CHANGELOG extraction + binary uploads (tag-gated)

### Image Tagging Logic
Stable release (`v0.3.2`):
- `ghcr.io/jedarden/miroir:v0.3.2`
- `ghcr.io/jedarden/miroir:0.3`
- `ghcr.io/jedarden/miroir:0`
- `ghcr.io/jedarden/miroir:latest`

Pre-release (`v0.3.2-rc.1`):
- `ghcr.io/jedarden/miroir:v0.3.2-rc.1` only (no float tags, no :latest)

### Manual Test
Cannot test on this system (kubectl not installed). Manual submission command:
```bash
kubectl --kubeconfig=$HOME/.kube/iad-ci.kubeconfig create -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: miroir-ci-manual-
  namespace: argo-workflows
spec:
  workflowTemplateRef:
    name: miroir-ci
  arguments:
    parameters:
      - name: tag
        value: "v0.1.0"  # For release build
EOF
```

### Secrets
Already exist on iad-ci cluster:
- `ghcr-credentials` (for ghcr.io push)
- `github-token` (for release creation)
