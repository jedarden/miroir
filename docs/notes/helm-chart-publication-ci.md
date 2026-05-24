# Helm Chart Publication CI

This document describes how Helm chart publication works in the miroir CI/CD pipeline.

## Overview

The Helm chart is published automatically when a git tag is pushed. The chart is published to two locations:

1. **GitHub Pages** (`https://jedarden.github.io/miroir`) - Primary repository for `helm repo add`
2. **OCI Registry** (`oci://ghcr.io/jedarden/charts/miroir`) - For air-gapped environments

## Argo Workflow Tasks

The `miroir-ci` WorkflowTemplate in `declarative-config` includes three tasks for Helm chart publication:

### 1. helm-package

- Runs after checkout when a tag is provided
- Updates `Chart.yaml` version and appVersion to match the tag
- Packages the chart using `helm package charts/miroir -d dist/`

### 2. helm-publish-ghpages

- Publishes the packaged chart to the gh-pages branch
- Creates the gh-pages branch if it doesn't exist
- Updates `index.yaml` with the new chart version
- Commits and pushes to the gh-pages branch

### 3. helm-publish-oci

- Publishes the chart to GHCR OCI registry
- Uses the same `ghcr-credentials` secret as Kaniko
- Parses Docker config JSON for GHCR authentication

## Usage

### Adding the Helm repository

```bash
helm repo add miroir https://jedarden.github.io/miroir
helm repo update
```

### Installing from GitHub Pages

```bash
helm install my-miroir miroir/miroir --version 0.1.0
```

### Installing from OCI registry

```bash
helm install my-miroir oci://ghcr.io/jedarden/charts/miroir --version 0.1.0
```

## Chart Versioning

- Chart version tracks app version by default
- For chart-only fixes (e.g., template changes without code changes), the chart version should be bumped separately
- TODO: Implement chart-only detection to skip binary rebuild when only chart files change

## Secrets

The following secrets are required in the `argo-workflows` namespace:

- `ghcr-credentials`: Docker config JSON for GHCR push (used by both Kaniko and Helm OCI)
- `github-token`: GitHub token for repository operations (used by gh-pages push)

## References

- Plan §12: Delivered Artifacts
- Bead miroir-uyx.6: P11.6 Helm chart publication
