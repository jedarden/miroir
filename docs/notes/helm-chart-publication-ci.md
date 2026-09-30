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

## miroir-release: post-publish smoke-test gates

Chart publication now ships in the `miroir-release` WorkflowTemplate (`declarative-config` → `k8s/iad-ci/argo-workflows/miroir-release-workflowtemplate.yml`), not `miroir-ci`: the original `miroir-ci` template whose three tasks are broken down above is retired (`miroir-ci.yaml.disabled`), and its package/publish work is now the single `helm-publish` task in the `miroir-release` pipeline (`check-release-ready` → `build` → `publish-chart` → `github-release`).

`helm-publish` ends with two smoke-test gates that verify the chart actually landed before the GitHub release is cut. The task runs under `set -e`, so a non-zero exit from either gate fails the `helm-publish` task — and the pipeline's next step, `create-github-release`, never runs on a broken publish.

### 1. OCI re-pull gate — after `helm push`, before gh-pages publication

Re-pulls the just-pushed chart version back out of ghcr.io to prove the OCI artifact is readable:

```sh
mkdir -p /verify-oci
helm pull oci://ghcr.io/jedarden/charts/miroir --version "$VERSION" -d /verify-oci
```

If the push silently failed or ghcr rejected the version, `helm pull` exits non-zero and the task fails here — gh-pages is never updated with a version whose OCI artifact cannot be pulled.

### 2. gh-pages index gate — after the gh-pages push, before `create-github-release`

Fetches the live GitHub Pages `index.yaml` and requires the new version entry:

```sh
wget -qO /tmp/index.yaml https://jedarden.github.io/miroir/index.yaml
grep -q "version: ${VERSION}" /tmp/index.yaml
```

`wget` fails if Pages is not serving at all, and `grep -q` fails until the pushed `index.yaml` entry is live (including Pages' redeploy lag). Either way the task fails before `create-github-release` runs, so a release is never announced for a chart that is not installable from the documented channels.

### Cross-reference: ADR-1

These gates close the silent-publication-failure gap documented in `docs/plan/plan.md` **ADR-1 (2026-07-20)**: the audit there found both published channels dark for roughly three months (gh-pages `index.yaml` 404, GHCR 403) with nothing noticing, because nothing verified that a publish had actually landed. ADR-1's decision removes the fleet's own GitOps dependency on those channels; the `miroir-release` gates are the complementary publish-side guard for the external/air-gapped consumers that decision keeps on GHCR OCI and GitHub Pages.

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
- Plan ADR-1 (2026-07-20): silent publication-failure audit — the gap the `miroir-release` smoke-test gates close
- Bead miroir-uyx.6: P11.6 Helm chart publication
