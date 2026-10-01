# Miroir Secrets Setup Guide

This guide covers setting up Miroir secrets with OpenBao and External Secrets
Operator (ESO) on the **live ardenone-cluster OpenBao v2 cell**. Every step
below operates on the existing deployment — there is no separate fresh-install
path here. The plan §9 reference layout for a hypothetical standalone install
is retained in [Appendix A](#appendix-a-plan-9-reference-layout-non-live) as a
clearly-labeled non-live alternative; do not run it against ardenone-cluster.

## Live cluster layout (ardenone-cluster, verified 2026-09-29, re-confirmed 2026-09-30)

- **Server:** `https://openbao-v2-ardenone-cluster-active.openbao-v2.svc.cluster.local:8200`
  (TLS; CA from Secret `openbao-v2-internal-tls`, ns `openbao-v2`, key
  `ca.crt`). Statefulset pod `openbao-v2-ardenone-cluster-0` in ns `openbao-v2`.
- **KV v2 mount:** `secret` — not `kv`. API data paths are
  `secret/data/<cluster>/<namespace>/keys`; the cell has no `kv` mount
  (`kv/search/miroir` belonged to the decommissioned v1 cell and must never be
  addressed).
- **Auth:** Kubernetes mount `kubernetes`, role
  `external-secrets-ardenone-cluster` bound to ServiceAccount
  `external-secrets-ardenone-cluster` (ns `external-secrets`, audience
  `openbao-v2`). Workloads hold no OpenBao tokens; they read secrets through
  ESO only.
- **Store:** ClusterSecretStore `openbao-v2` (Valid, ReadWrite).
- **ExternalSecrets:** `miroir-keys` (ns `miroir`) and `miroir-dev-keys` (ns
  `miroir-dev`), each targeting a same-named Secret, with remote keys
  `ardenone-cluster/miroir/keys` and `ardenone-cluster/miroir-dev/keys`
  (properties: `masterKey`, `nodeMasterKey`, `adminApiKey`).
- **Status:** both ExternalSecrets report `SecretSyncedError` ("could not get
  secret data from provider") because those remote keys are not populated yet —
  a missing-secret condition, not a store or auth defect (the store is Ready).

The GitOps sources of record for everything above live in
`jedarden/declarative-config`:
`k8s/ardenone-cluster/external-secrets/openbao-v2-cluster-secret-store.yml`
(the store), `k8s/ardenone-cluster/miroir/miroir-externalsecret.yml` and
`k8s/ardenone-cluster/miroir-dev/miroir-dev-externalsecret.yml` (the syncs),
and `k8s/ardenone-cluster/openbao-v2/reconciler-config.yml` (policies and
auth roles). The repo file [`openbao-policy.hcl`](openbao-policy.hcl)
documents the ESO read policy's shape, narrowed to exactly the two miroir
keys docs, at the live path shape.

## Credential source

Where each credential involved in operating Miroir secrets comes from, and how
a team member obtains it. This section names sources and locations only — no
secret values or key material are committed to this repository.

### Operator cluster access (kubectl)

- **Read path:** the read-only kubectl proxy at
  `http://traefik-ardenone-cluster:8001` (Tailscale-only), backed by the
  `devpod-observer` ServiceAccount's read-only RBAC. Any team member on the
  tailnet can use it directly; no kubeconfig or token is handed out.
- **Write path:** GitOps only. Edit the manifest in
  `jedarden/declarative-config`, push, and let ArgoCD sync the change. The
  proxy cannot write, and direct mutations of managed resources are reverted
  by ArgoCD `selfHeal` anyway — there is no operator kubectl write credential
  to obtain.

### OpenBao administration (ardenone-cluster)

- **Day-to-day admin credential:** a scoped periodic OpenBao token (720 h)
  minted for the operator by an existing admin. Verify a fresh grant with
  `bao token lookup` (expected: your scoped policy only, periodic,
  renewable). Expired tokens are re-minted by an admin — they are never
  shared or stored.
- **Root tokens:** live in the team password manager. They are never written
  into the cluster, git, or dotfiles on shared hosts. A team member who needs
  one for a break-glass operation obtains it from an operator, uses it, and
  lets it stay escrowed.
- **Unseal / recovery key material:** the live v2 cell auto-unseals via cloud
  KMS, so routine operation never handles an unseal key. Its single recovery
  share exists only inside the PGP-encrypted initialization JSON held in the
  operator's holder bundle (see
  `k8s/ardenone-cluster/openbao-v2/recovery-share-secret.yml.template` in
  declarative-config); rekey ceremonies escrow a share transiently as a
  SealedSecret and delete it in the closing commit. Access goes through the
  operators — never ad-hoc reconstruction.
  **Name-collision warning:** the KV object
  `ardenone-cluster/openbao/openbao-unseal-key` in this cell is the
  decommissioned v1 instance's Shamir unseal key, preserved deliberately
  during the 2026-08-29 decommission. Despite the name it is not this cell's
  recovery share and opens nothing that exists.
- **Policy and role provisioning** (the `bao policy write` and
  `bao write auth/kubernetes/role/...` calls shown in the plan §9 reference
  steps in [Appendix A](#appendix-a-plan-9-reference-layout-non-live)):
  declarative. The OpenBao reconciler in declarative-config
  (`k8s/ardenone-cluster/openbao-v2/reconciler-config.yml`) converges
  policies, Kubernetes auth roles, and AppRoles from git on every cycle, and
  reverts hand-created API objects. To change provisioning, push to
  declarative-config. Interactive `bao` writes against the live cell (e.g.
  populating the keys doc) use the dedicated **miroir write token** — a
  periodic AppRole-issued client token bound to the `miroir-write` policy, not
  an admin-minted token. See
  [Runbook: obtaining the miroir write token](openbao-write-token.md) for the
  full obtain-and-verify procedure; it is the credential behind every `bao kv
  put` / `bao kv patch` the populate steps need on the live cell.

### Workload credentials (miroir pods)

Miroir pods hold **no** OpenBao tokens or provider credentials of their own.
All secret reads flow through External Secrets Operator, which authenticates
to OpenBao with Kubernetes auth:

- ServiceAccount `external-secrets-ardenone-cluster` (ns `external-secrets`)
- auth role `external-secrets-ardenone-cluster` (policy
  `eso-ardenone-cluster`), audience `openbao-v2`

This identity is created and owned by GitOps
(`k8s/ardenone-cluster/external-secrets/openbao-v2-cluster-secret-store.yml`
and the reconciler config in declarative-config). There is nothing for a team
member to obtain, copy, or rotate by hand.

### Runtime admin API key (`adminApiKey`)

The key checked by the proxy's admin auth headers and sent by `miroir-ctl`:

- **In-cluster:** not a manual credential — ESO syncs it from the OpenBao
  remote key `ardenone-cluster/miroir/keys` (property `adminApiKey`) into
  Secret `miroir-keys` (ns `miroir`) and `miroir-dev-keys` (ns `miroir-dev`).
  The value itself is generated once, by the operator populating the keys doc
  (the Step 3 pattern: `openssl rand -base64 32` or equivalent).
- **Operator workstation (`miroir-ctl`):** resolved in priority order
  (implementation:
  [`crates/miroir-ctl/src/credentials.rs`](../../crates/miroir-ctl/src/credentials.rs);
  usage notes: [`docs/ctl/README.md`](../ctl/README.md)):
  1. `MIROIR_ADMIN_API_KEY` environment variable
  2. `~/.config/miroir/credentials` (or `credentials.toml`), TOML section
     `[default]`, key `admin_api_key`
  3. `--admin-key` flag — last resort; visible in the process list

  To obtain it, read the `adminApiKey` property from the OpenBao remote key
  using a scoped operator token, or have an operator provision one. Never
  commit it to a repository or shell profile.

### Meilisearch node key (`MEILI_MASTER_KEY`)

Carried in the Meilisearch pod environment — not a separately distributed
credential. Its source of truth is the `nodeMasterKey` entry of the Secret
Inventory below (OpenBao → ESO → `miroir-keys`); rotate it with the
nodeMasterKey rotation procedure below, which moves the source of truth to a
new value in OpenBao and lets ESO re-sync.

## Prerequisites

- **Operator write credential:** the **miroir write token** — see
  [Credential source](#credential-source) and the runbook
  [`openbao-write-token.md`](openbao-write-token.md). Every `bao` write in
  Step 3 runs under it; nothing here uses an admin or root token, and no
  token is ever written into a workload or manifest.
- **`bao` CLI** (v2.6 family) with `BAO_ADDR` at the cell entrypoint
  (`https://openbao.ardenone.com:8444`) and `BAO_CACERT` from Secret
  `openbao-v2-internal-tls` (ns `openbao-v2`, key `ca.crt`) — the runbook's
  login step covers both.
- **Read-only kubectl** for verification via the proxy:
  `kubectl --server=http://traefik-ardenone-cluster:8001 ...`. Its RBAC
  cannot write; that is by design.
- **A declarative-config checkout with Forgejo push rights** — needed only
  when a step changes a GitOps artifact (policy, role, store, ExternalSecret,
  or deployment values). There is no direct-write path to this cluster, and
  ArgoCD `selfHeal` reverts out-of-band mutations.

## Secret Inventory

The live layout keeps **one KV v2 doc per environment** with camelCase
properties. ESO maps each property 1:1 onto a key of the target Secret:

| Property (remote keys doc) | Secret key after sync | Purpose | Rotation |
|----------------------------|-----------------------|---------|----------|
| masterKey | masterKey | Meilisearch master (admin) key | Manual |
| nodeMasterKey | nodeMasterKey | Meilisearch node key (cluster auth) | Zero-downtime (below) |
| adminApiKey | adminApiKey | Miroir admin API key (`MIROIR_ADMIN_API_KEY`) | Manual; leak procedure below |

Remote keys and targets:

- `ardenone-cluster/miroir/keys` → Secret `miroir-keys`, ns `miroir`
- `ardenone-cluster/miroir-dev/keys` → Secret `miroir-dev-keys`, ns `miroir-dev`

The plan §9 inventory carried further properties (`adminSessionSealKey`,
`searchUiJwtSecret`, `searchUiSharedKey`, `redis-password`, …); they are not
part of the live keys docs. Adding one is a two-sided GitOps change: write the
property into the keys doc **and** add a matching `data` entry to that
environment's ExternalSecret manifest in declarative-config — ESO prunes
target-Secret keys it does not manage on its next sync. The full plan §9
table is preserved in Appendix A.

## Step 1: Policies are declarative — no `bao policy write`

On the live cell, policies are converged from git by the OpenBao reconciler;
hand-created policy objects (CLI or UI) are reverted on the next cycle. The
two identities Miroir needs are therefore git artifacts, not commands:

- **ESO read identity:** policy `eso-ardenone-cluster`, attached by the
  Kubernetes auth role to ESO's logins. Declared in
  `k8s/ardenone-cluster/openbao-v2/reconciler-config.yml`;
  [`openbao-policy.hcl`](openbao-policy.hcl) documents its shape narrowed to
  exactly the two miroir keys docs (read-only, data + metadata paths).
- **Operator write identity:** policy `miroir-write` — `create`, `update`,
  `patch`, `read` on the two keys docs plus metadata `read`, nothing else —
  declared in the same reconciler config together with its AppRole.

To change either policy, edit `reconciler-config.yml` in declarative-config,
push, and let ArgoCD sync (force the Application sync if it lags). There is
no `miroir-policy` on the live cell — that name belongs to the non-live
Appendix A layout.

## Step 2: Kubernetes auth role — ESO only, no workload tokens

The Kubernetes auth role is declarative too and already exists. Verify it
with a read call (no values involved):

```bash
bao read auth/kubernetes/role/external-secrets-ardenone-cluster
```

Expected: bound service account `external-secrets-ardenone-cluster`, namespace
`external-secrets`, token policy `eso-ardenone-cluster`, audience
`openbao-v2`. The role is created by the reconciler from
`reconciler-config.yml`; changes go through declarative-config.

Nothing in this guide issues a token for a workload to hold: ESO logs in to
OpenBao with its own ServiceAccount when the ClusterSecretStore
authenticates, and Miroir pods consume only the resulting Kubernetes Secret.
If a step ever asks you to place an OpenBao token into a Secret, manifest, or
pod spec, that is the Appendix A shape — stop; it is not how the live
deployment works.

## Step 3: Populate the keys docs

Run under the miroir write token (see Prerequisites). The cell enforces CAS
on the `secret` mount, so pass `-cas` on every write — `-cas=0` creates
version 1 of a doc that does not exist yet:

```bash
bao kv put -cas=0 secret/ardenone-cluster/miroir/keys \
  masterKey="$(openssl rand -base64 32)" \
  nodeMasterKey="$(openssl rand -base64 32)" \
  adminApiKey="$(openssl rand -base64 32)"

bao kv put -cas=0 secret/ardenone-cluster/miroir-dev/keys \
  masterKey="$(openssl rand -base64 32)" \
  nodeMasterKey="$(openssl rand -base64 32)" \
  adminApiKey="$(openssl rand -base64 32)"
```

Later single-property changes patch the existing doc at its current version:

```bash
bao kv metadata get secret/ardenone-cluster/miroir/keys   # current version, no values
bao kv patch -cas=<version> secret/ardenone-cluster/miroir/keys \
  adminApiKey="$(openssl rand -base64 32)"
```

Do not enable any mount: `secret` already exists, and
`bao secrets enable -path=kv kv-v2` is Appendix A machinery — the live cell
has no `kv` mount.

> **Do not apply the old populate manifests.**
> [`../../scripts/populate-openbao-job.yaml`](../../scripts/populate-openbao-job.yaml)
> and
> [`../../scripts/populate-openbao-secrets.yaml`](../../scripts/populate-openbao-secrets.yaml)
> are older Job-manifest attempts at this step. They predate the current
> layout — wrong server, wrong auth role, no CAS, extra properties — and are
> flagged in [Artifacts and tools](#artifacts-and-tools) rather than updated.
> The commands above are the reconciled equivalent.

Populate is an operator action, run out-of-band on purpose; nothing in this
repository writes to the cluster. Once a keys doc exists, ESO syncs it into
the target Secret within `refreshInterval` (1h) and the ExternalSecret flips
from `SecretSyncedError` to Ready. Confirm with the read-only proxy:

```bash
kubectl --server=http://traefik-ardenone-cluster:8001 \
  get externalsecret -n miroir miroir-keys
kubectl --server=http://traefik-ardenone-cluster:8001 \
  get externalsecret -n miroir-dev miroir-dev-keys
```

## Step 4: ClusterSecretStore `openbao-v2` — verify, don't create

The store already exists and is GitOps-owned. Verify it read-only:

```bash
kubectl --server=http://traefik-ardenone-cluster:8001 \
  get clustersecretstore openbao-v2
```

Expected: `STATUS: Valid`, capabilities `ReadWrite`, `READY: True`. Its spec
(from `k8s/ardenone-cluster/external-secrets/openbao-v2-cluster-secret-store.yml`
in declarative-config) is the live reference shape:

```yaml
apiVersion: external-secrets.io/v1
kind: ClusterSecretStore
metadata:
  name: openbao-v2
spec:
  provider:
    vault:
      server: https://openbao-v2-ardenone-cluster-active.openbao-v2.svc.cluster.local:8200
      path: secret
      version: v2
      caProvider:
        type: Secret
        name: openbao-v2-internal-tls
        namespace: openbao-v2
        key: ca.crt
      auth:
        kubernetes:
          mountPath: kubernetes
          role: external-secrets-ardenone-cluster
          serviceAccountRef:
            name: external-secrets-ardenone-cluster
            namespace: external-secrets
            audiences:
              - openbao-v2
```

There is no `openbao-backend` store on this cluster — the
`http://openbao.openbao.svc` endpoint in Appendix A belongs to the non-live
layout. Corrections go through declarative-config (edit, push, let ArgoCD
sync), not `kubectl apply`.

## Step 5: Deploy Miroir against the ESO-managed Secret

Deployment is GitOps. ArgoCD Application `miroir-ardenone-cluster`
(`k8s/ardenone-cluster/miroir/miroir-application.yml` in declarative-config)
installs chart `miroir` 0.1.0 from `oci://ghcr.io/jedarden/charts` into ns
`miroir`, with the ESO-managed Secret wired to the pods:

```yaml
miroir:
  existingSecret: miroir-keys  # ESO-managed from remote key ardenone-cluster/miroir/keys
```

`miroir-dev-ardenone-cluster` is the dev twin (Secret `miroir-keys` in ns
`miroir-dev`). Deploying means editing the Application's Helm values in
declarative-config and letting ArgoCD sync — there is no operator
`helm install` step on this cluster. The chart injects the synced keys into
the pod environment, so after Step 3 pods come up with real values. Never
point a workload at a hand-made Secret: the only supported Secret is the one
the ExternalSecret owns.

Status snapshot (2026-09-30): the namespaces and ExternalSecrets exist; the
workloads do not — both Applications were showing a chart-pull
`ComparisonError` at the time of writing. Populate the keys docs first
(Step 3); the resulting sync flip to Ready is observable at any time with
read-only kubectl.

## Rotation Procedures

### nodeMasterKey Rotation (Zero-Downtime)

The same rotation exists as a runnable `miroir-ctl` flow — see
[`../runbooks/node-master-key-rotation.md`](../runbooks/node-master-key-rotation.md)
(`miroir-ctl key rotate-node-master`). On ardenone-cluster, follow the
procedure below as the source-of-truth update (the OpenBao patch); the CLI
flow's printed `kubectl patch secret` step is a hand edit of the ESO-owned
target and the next sync overwrites it. The runbook's `search`-namespace
examples are the plan §9 default — also the CLI's `--namespace` default — so
pass `--namespace miroir` (or `miroir-dev`) on this cluster.

1. Generate a new admin-scoped key on each running Meilisearch node:
   ```bash
   kubectl --server=http://traefik-ardenone-cluster:8001 exec -it \
     statefulset/meilisearch -n miroir -- bash -c '
     curl -X POST http://localhost:7700/keys \
       -H "Authorization: Bearer $MEILI_MASTER_KEY" \
       -H "Content-Type: application/json" \
       -d '"'"'{"actions": ["*"], "indexes": ["*"], "description": "miroir node key v2"}'"'"'
   '
   ```
2. Move the source of truth in OpenBao (miroir write token, at the doc's
   current version):
   ```bash
   bao kv patch -cas=<version> secret/ardenone-cluster/miroir/keys \
     nodeMasterKey="<new-key>"
   ```
3. ESO syncs the update within `refreshInterval` (1h on the live
   ExternalSecrets). To force it immediately, delete the ESO-owned target —
   ESO recreates it from OpenBao:
   ```bash
   kubectl --server=http://traefik-ardenone-cluster:8001 \
     delete secret miroir-keys -n miroir
   ```
4. Rolling-restart Miroir pods so they pick up the new key:
   ```bash
   kubectl --server=http://traefik-ardenone-cluster:8001 \
     rollout restart deployment/miroir -n miroir
   ```
5. Delete the old key from all nodes once the rollout is healthy:
   ```bash
   kubectl --server=http://traefik-ardenone-cluster:8001 exec -it \
     statefulset/meilisearch -n miroir -- bash -c '
     curl -X DELETE http://localhost:7700/keys/<old-uid> \
       -H "Authorization: Bearer $MEILI_MASTER_KEY"
   '
   ```

### masterKey Rotation (Startup `MEILI_MASTER_KEY`, Maintenance Window)

The inventory's `masterKey` property is Meilisearch's startup master key,
fixed for the life of the Meilisearch process, so rotating it is the
maintenance-window procedure in
[`../runbooks/startup-master-key-rotation.md`](../runbooks/startup-master-key-rotation.md).
Its Option B already targets the live source of truth — the
`secret/ardenone-cluster/miroir/keys` doc, property `masterKey`, written with
the miroir write token. The runbook's remaining `search`-namespace examples
are plan §9 defaults; on ardenone-cluster substitute ns `miroir` (or
`miroir-dev`) and the live resource names.

### JWT Secret Rotation (Zero-Downtime)

Automated via `miroir-ctl ui rotate-jwt-secret`:

```bash
miroir-ctl ui rotate-jwt-secret \
  --namespace=miroir \
  --secret-name=miroir-keys \
  --deployment-name=miroir
```

**Live-layout precondition:** the live keys docs carry only the three
inventory properties today. The rotation rewrites the target Secret in
place, and ESO prunes unmanaged keys on its next sync (1h) — so before
running it against the live cell, land the GitOps change from the Secret
Inventory section first (property `searchUiJwtSecret` /
`searchUiJwtSecretPrevious` in the keys doc plus matching ExternalSecret
`data` entries). Then rotation survives re-syncs, and the promotion of the
current value to the `…Previous` key still gives a zero-downtime overlap
window.

The plan §9 quarterly CronJob does not exist on this cluster and is listed
only in Appendix A.

## Leak Response

If a secret is leaked, immediate revocation — always at the OpenBao source of
truth, never by hand-editing the ESO-owned target Secret (the next sync
overwrites it):

1. **JWT secret leaked** — run the JWT rotation procedure above; it promotes
   the leaked value into `searchUiJwtSecretPrevious`, and the closing step of
   the rotation removes it once every pod holds the new secret.
2. **Admin API key leaked** — rotate at the source:
   ```bash
   bao kv patch -cas=<version> secret/ardenone-cluster/miroir/keys \
     adminApiKey="$(openssl rand -base64 32)"
   # Force the re-sync, then restart pods so they drop the leaked value
   kubectl --server=http://traefik-ardenone-cluster:8001 \
     delete secret miroir-keys -n miroir
   kubectl --server=http://traefik-ardenone-cluster:8001 \
     rollout restart deployment/miroir -n miroir
   ```
   Update operator workstations afterwards (`MIROIR_ADMIN_API_KEY`,
   `~/.config/miroir/credentials`, or the `--admin-key` flag — see
   [Credential source](#credential-source)).
3. **Node master key leaked** — follow the nodeMasterKey rotation procedure
   above.

## Validation

Verify secrets are loaded correctly — every command is read-only, via the
proxy:

```bash
# Check ESO sync status (Ready once the remote keys exist)
kubectl --server=http://traefik-ardenone-cluster:8001 \
  get externalsecret -n miroir

# Verify the target Secrets exist
kubectl --server=http://traefik-ardenone-cluster:8001 \
  get secret miroir-keys -n miroir

# Check Miroir pods came up with secrets
kubectl --server=http://traefik-ardenone-cluster:8001 \
  logs -l app.kubernetes.io/name=miroir -n miroir --tail=20
```

And verify the OpenBao side without reading values:

```bash
bao kv metadata get secret/ardenone-cluster/miroir/keys
bao kv metadata get secret/ardenone-cluster/miroir-dev/keys
```

## Artifacts and tools

Every procedural step above maps onto artifacts committed in this repository
or named by path in `jedarden/declarative-config` (the GitOps source of
record). State at HEAD:

| Procedure | Artifact | Status |
|-----------|----------|--------|
| Operator write credential (Prerequisites) | [`openbao-write-token.md`](openbao-write-token.md) | Current — the obtain-and-verify runbook for the `miroir-write` token |
| Step 1 — policies | [`openbao-policy.hcl`](openbao-policy.hcl); live objects in declarative-config `k8s/ardenone-cluster/openbao-v2/reconciler-config.yml` | Current — documents the live `eso-ardenone-cluster` read policy's shape; the policy objects themselves are declarative |
| Step 2 — auth role | none in this repo: a one-line `bao read` verification, and the role is declared in declarative-config's `reconciler-config.yml` | Absence is deliberate — the role is GitOps-owned; there is no manifest or script to run |
| Step 3 — populate | [`../../scripts/populate-openbao-job.yaml`](../../scripts/populate-openbao-job.yaml), [`../../scripts/populate-openbao-secrets.yaml`](../../scripts/populate-openbao-secrets.yaml) | **Predates the current layout — do not apply** (see the address reconciliation below); the reconciled equivalent is the Step 3 command pair under the `miroir-write` token |
| Step 4 — ClusterSecretStore | declarative-config `k8s/ardenone-cluster/external-secrets/openbao-v2-cluster-secret-store.yml` (spec quoted in Step 4) | Current |
| Step 5 — deploy / ExternalSecrets | declarative-config `k8s/ardenone-cluster/miroir/miroir-application.yml`, `miroir-externalsecret.yml`, `miroir-dev/miroir-dev-externalsecret.yml`; example [`../../k8s/examples/argocd-apps/external-secret.yaml`](../../k8s/examples/argocd-apps/external-secret.yaml) | Live objects current; the repo example predates the layout (Appendix A shape — see below) |
| nodeMasterKey rotation | [`../runbooks/node-master-key-rotation.md`](../runbooks/node-master-key-rotation.md) | Current flow; `search`-namespace examples are the plan §9 default (also the `miroir-ctl` `--namespace` default) — pass `--namespace miroir` on ardenone-cluster |
| masterKey (startup) rotation | [`../runbooks/startup-master-key-rotation.md`](../runbooks/startup-master-key-rotation.md) | Option B names the live keys doc; remaining examples carry the same §9 namespace defaults |
| JWT rotation | `miroir-ctl ui rotate-jwt-secret` — implementation [`../../crates/miroir-ctl`](../../crates/miroir-ctl), usage notes [`../ctl/README.md`](../ctl/README.md) | Current |
| Validation / leak response | read-only commands inline above; no artifact beyond the `miroir-ctl` binary and the read-only proxy | — |

### Removed artifact: the token-generation Job

`k8s/generate-miroir-openbao-token-job.yaml` **does not exist at HEAD**, and
this section records its absence rather than linking it. It was a
banned-shape Kubernetes Job manifest (this repository never creates new
`kind: Job` manifests) that minted an operator token against the
decommissioned v1 cell: plain-HTTP
`BAO_ADDR=http://openbao-ardenone-cluster.openbao.svc.cluster.local:8200`,
Kubernetes-auth role `openbao-ardenone-cluster`, and an inline
`miroir-policy-write` policy on the dead `kv/data/search/miroir` paths. It
was deleted on 2026-09-30 together with `scripts/generate-openbao-token.sh`
and the stale `k8s/*.hcl` policy drafts;
[`openbao-write-token.md`](openbao-write-token.md) records it among rejected
issuance mechanisms. The reconciled equivalent is that runbook: declarative
`miroir-write` policy plus AppRole in declarative-config, AppRole login, and
a scoped periodic client token.

### Address reconciliation: manifests vs live cell

The two populate manifests (and the removed token Job) export
`BAO_ADDR=http://openbao-ardenone-cluster.openbao.svc.cluster.local:8200` —
plain HTTP against the `openbao` service of the decommissioned v1 cell. The
live cell is
`https://openbao-v2-ardenone-cluster-active.openbao-v2.svc.cluster.local:8200`
with TLS (CA from Secret `openbao-v2-internal-tls`, ns `openbao-v2`). The
mismatch is flagged here rather than patched into the manifests: they are
banned `kind: Job` shapes, this repository never writes new ones, and nothing
at HEAD applies them. Beyond the address they also:

- log in with Kubernetes-auth role `eso` and a ServiceAccount token audience
  `openbao` — the live role is `external-secrets-ardenone-cluster` with
  audience `openbao-v2`, and neither is the credential a population run
  should use anyway (that is the operator's `miroir-write` token);
- write without the `-cas` version checks the live `secret` mount enforces;
- write eight properties on creation — the three live inventory properties plus five
  (`adminSessionSealKey`, `searchUiJwtSecret`, `searchUiJwtSecretPrevious`,
  `searchUiSharedKey`, `redis-password`) that are not in the live keys docs.

The reconciled equivalent of both manifests is the Step 3 procedure verbatim:
`bao kv put -cas=0` for first creation, `bao kv patch -cas=<version>` for
updates, run by an operator under the `miroir-write` token. The manifests are
retained only as historical reference.

The Step 5 example
[`../../k8s/examples/argocd-apps/external-secret.yaml`](../../k8s/examples/argocd-apps/external-secret.yaml)
carries the same flag for the same reason: it targets store `openbao-backend`
with `kv/search/miroir` snake_case remote keys under
`external-secrets.io/v1beta1` — the Appendix A shape for a sandbox install,
matching the chart's `charts/miroir/examples/eso-external-secret.yaml`. The
live ExternalSecrets are the declarative-config manifests named in Step 5,
and the live store is `openbao-v2`.

## Security Notes

- The ESO read policy is least-privilege: read-only access to
  `secret/data/ardenone-cluster/miroir/keys` and its `miroir-dev` twin
  ([`openbao-policy.hcl`](openbao-policy.hcl)); on the live cell the actual
  ESO read identity is the declarative `eso-ardenone-cluster` policy, and
  operator writes use the scoped `miroir-write` token
  ([`openbao-write-token.md`](openbao-write-token.md))
- Miroir never writes to OpenBao, and miroir pods hold no OpenBao tokens —
  they read via ESO only
- The live cell enforces CAS on the `secret` mount, so every write above is
  version-checked; concurrent operators cannot silently stomp a keys doc
- All secrets are base64-encoded in Kubernetes Secrets
- Use separate policies per environment (the `miroir-write` policy's exact
  two-path scope shows the pattern; split prod/dev further if desired)
- Rotate keys quarterly or immediately on leak
- Monitor ESO sync errors via Prometheus alerts

## Appendix A: plan §9 reference layout (non-live)

> **Warning — not the live deployment.** This appendix preserves the plan §9
> reference procedure for a *hypothetical fresh standalone deployment*:
> server `openbao.openbao.svc`, KV v2 mount `kv`, path `kv/search/miroir`,
> role `miroir`, policy `miroir-policy`, store `openbao-backend`, namespace
> `search`. **None of those objects exist on ardenone-cluster** — the live
> cell has no `kv` mount and no `search` namespace, and hand-created policies
> or roles on the live cell are reverted by the reconciler. Do not run these
> steps against ardenone-cluster. They are retained because the chart's
> optional `eso.*` plumbing (`charts/miroir/templates/miroir-externalsecret.yaml`,
> `charts/miroir/examples/eso-external-secret.yaml`) still implements this
> shape for an isolated from-scratch install; use it only in such a sandbox.

### Reference secret inventory (plan §9)

| Secret | OpenBao Key | ESO Target | Rotation |
|--------|-------------|------------|----------|
| masterKey | master_key | miroir-secret | Manual |
| nodeMasterKey | node_master_key | miroir-secret | Zero-downtime |
| adminApiKey | admin_api_key | miroir-secret | Manual |
| adminSessionSealKey | admin_session_seal_key | miroir-secret | Manual |
| searchUiJwtSecret | search_ui_jwt_secret | miroir-secret | Zero-downtime |
| searchUiJwtSecretPrevious | search_ui_jwt_secret_previous | miroir-secret | Overlap only |
| searchUiSharedKey | search_ui_shared_key | miroir-secret | Manual |
| redis-password | redis_password | miroir-secret | Manual |

### Reference Step 1: Create OpenBao Policy

Create the least-privilege policy for Miroir:

```bash
# Apply the policy to OpenBao
bao policy write miroir-policy docs/operations/openbao-policy.hcl
```

Or via the OpenBao UI:
1. Navigate to Policies
2. Create new policy named `miroir-policy`
3. Paste the contents of `docs/operations/openbao-policy.hcl`

### Reference Step 2: Create OpenBao Role and Token

```bash
# Create the role
bao write auth/kubernetes/role/miroir \
  bound_service_account_names=miroir \
  bound_service_account_namespaces=search \
  policies=miroir-policy \
  ttl=24h

# Verify the role
bao read auth/kubernetes/role/miroir
```

### Reference Step 3: Populate Secrets in OpenBao

Enable KV v2 secrets engine (if not already enabled):

```bash
bao secrets enable -path=kv kv-v2
```

Write the Miroir secrets:

```bash
# Generate secrets (use your preferred method)
bao kv put kv/search/miroir \
  master_key="$(openssl rand -base64 32)" \
  node_master_key="$(openssl rand -base64 32)" \
  admin_api_key="$(openssl rand -base64 32)" \
  admin_session_seal_key="$(openssl rand -base64 64)" \
  search_ui_jwt_secret="$(openssl rand -base64 64)"
```

For shared key mode (if using `search_ui.auth.mode: shared_key`):

```bash
bao kv patch kv/search/miroir \
  search_ui_shared_key="$(openssl rand -base64 32)"
```

For Redis password (if `redis.auth.enabled: true`):

```bash
bao kv patch kv/search/miroir \
  redis_password="$(openssl rand -base64 32)"
```

### Reference Step 4: Configure ESO ClusterSecretStore

Create the ClusterSecretStore for OpenBao:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: openbao-backend
spec:
  provider:
    vault:
      server: "http://openbao.openbao.svc:8200"
      path: "kv"
      version: "v2"
      auth:
        kubernetes:
          mountPath: "kubernetes"
          role: "miroir"
```

Apply:

```bash
kubectl apply -f - <<EOF
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: openbao-backend
spec:
  provider:
    vault:
      server: "http://openbao.openbao.svc:8200"
      path: "kv"
      version: "v2"
      auth:
        kubernetes:
          mountPath: "kubernetes"
          role: "miroir"
EOF
```

### Reference Step 5: Deploy Miroir with ESO Enabled

Update your Helm values:

```yaml
eso:
  enabled: true
  secretPath: "kv/search/miroir"
  includePreviousJwt: false  # Set true during JWT rotation
  includeSharedKey: false    # Set true when using shared_key mode
  includeRedisPassword: false # Set true when Redis auth is enabled

miroir:
  existingSecret: "miroir-secret"  # ESO-managed Secret
```

Deploy:

```bash
helm install miroir ./charts/miroir -f values.yaml
```

### Reference JWT rotation CronJob (plan §9)

The §9 layout optionally ships a suspended quarterly rotation CronJob
(`charts/miroir/templates/miroir-rotate-jwt-cronjob.yaml`):

```bash
# Enable the CronJob for automated rotation
kubectl patch cronjob miroir-rotate-jwt -n search -p '{"spec":{"suspend":false}}'

# Trigger manual rotation
kubectl create job --from=cronjob/miroir-rotate-jwt manual-rotation-$(date +%s) -n search
```

No such CronJob exists on ardenone-cluster; use the `miroir-ctl` procedure in
the main body instead.
