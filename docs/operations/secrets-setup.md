# Miroir Secrets Setup Guide (plan §9)

This guide covers setting up Miroir secrets with OpenBao and External Secrets Operator (ESO).

## Live cluster state (ardenone-cluster, verified 2026-09-29)

The steps below describe the plan §9 reference layout for a fresh deployment.
The live ardenone-cluster OpenBao differs structurally; integrate against the
existing deployment using these verified facts:

- **Server:** `https://openbao-v2-ardenone-cluster-active.openbao-v2.svc.cluster.local:8200`
  (TLS; CA from Secret `openbao-v2-internal-tls`, ns `openbao-v2`, key
  `ca.crt`). Statefulset pod `openbao-v2-ardenone-cluster-0` in ns `openbao-v2`.
- **KV v2 mount:** `secret` — not `kv`. API data paths are
  `secret/data/<cluster>/<namespace>/keys`.
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

The plan §9 artifacts below — server `openbao.openbao.svc`, mount `kv`, path
`kv/search/miroir`, role `miroir`, and namespace `search` — do not describe the
live deployment. Reconcile them with this section before populating secrets or
authoring policies for the live cluster. The companion
[`openbao-policy.hcl`](openbao-policy.hcl) is corrected to the live path shape
(read-only, ESO sync path). For the operator credential that writes the keys
docs on the live cell, follow
[Runbook: obtaining the miroir write token](openbao-write-token.md).

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
  `bao write auth/kubernetes/role/...` calls in the plan §9 steps below):
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
Inventory below (OpenBao → ESO → `miroir-secret`); rotate it with the
nodeMasterKey rotation procedure below, which moves the source of truth to a
new value in OpenBao and lets ESO re-sync.

## Prerequisites

- OpenBao deployed in the cluster with KV v2 secrets engine enabled
- External Secrets Operator installed
- Kubernetes cluster with miroir namespace

## Secret Inventory (plan §9)

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

## Step 1: Create OpenBao Policy

Create the least-privilege policy for Miroir:

```bash
# Apply the policy to OpenBao
bao policy write miroir-policy docs/operations/openbao-policy.hcl
```

Or via the OpenBao UI:
1. Navigate to Policies
2. Create new policy named `miroir-policy`
3. Paste the contents of `docs/operations/openbao-policy.hcl`

## Step 2: Create OpenBao Role and Token

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

## Step 3: Populate Secrets in OpenBao

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

## Step 4: Configure ESO ClusterSecretStore

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

## Step 5: Deploy Miroir with ESO Enabled

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

## Rotation Procedures

### nodeMasterKey Rotation (Zero-Downtime)

1. Generate new admin-scoped key on each Meilisearch node:
   ```bash
   kubectl exec -it statefulset/meilisearch -- bash -c '
     curl -X POST http://localhost:7700/keys \
       -H "Authorization: Bearer $MEILI_MASTER_KEY" \
       -H "Content-Type: application/json" \
       -d '"'"'{"actions": ["*"], "indexes": ["*"], "description": "miroir node key v2"}'"'"'
   '
   ```

2. Update OpenBao with the new key:
   ```bash
   bao kv patch kv/search/miroir node_master_key="<new-key>"
   ```

3. ESO will sync the update within `refreshInterval` (default 15m)

4. Rolling restart Miroir pods:
   ```bash
   kubectl rollout restart deployment/miroir -n search
   ```

5. Delete old keys from all nodes:
   ```bash
   kubectl exec -it statefulset/meilisearch -- bash -c '
     curl -X DELETE http://localhost:7700/keys/<old-uid> \
       -H "Authorization: Bearer $MEILI_MASTER_KEY"
   '
   ```

### JWT Secret Rotation (Zero-Downtime)

Automated via `miroir-ctl ui rotate-jwt-secret`:

```bash
miroir-ctl ui rotate-jwt-secret \
  --namespace=search \
  --secret-name=miroir-secret \
  --deployment-name=miroir
```

Or use the CronJob (quarterly, suspended by default):

```bash
# Enable the CronJob for automated rotation
kubectl patch cronjob miroir-rotate-jwt -n search -p '{"spec":{"suspend":false}}'

# Trigger manual rotation
kubectl create job --from=cronjob/miroir-rotate-jwt manual-rotation-$(date +%s) -n search
```

## Leak Response

If a secret is leaked, immediate revocation:

1. **JWT secret leaked** - Set `SEARCH_UI_JWT_SECRET_PREVIOUS` to empty string:
   ```bash
   kubectl patch secret miroir-secret -n search -p '{"stringData":{"searchUiJwtSecretPrevious":""}}'
   kubectl rollout restart deployment/miroir -n search
   ```

2. **Admin API key leaked** - Rotate immediately:
   ```bash
   # Generate new key
   bao kv patch kv/search/miroir admin_api_key="$(openssl rand -base64 32)"
   # Wait for ESO sync (or force delete Secret)
   kubectl delete secret miroir-secret -n search
   ```

3. **Node master key leaked** - Follow nodeMasterKey rotation procedure above

## Validation

Verify secrets are loaded correctly:

```bash
# Check ESO sync status
kubectl get externalsecret miroir-eso -n search -o yaml

# Verify Secret exists
kubectl get secret miroir-secret -n search

# Check Miroir pods have secrets
kubectl logs -l app.kubernetes.io/name=miroir -n search --tail=20 | grep -i jwt
```

## Security Notes

- The OpenBao policy is least-privilege: read-only access to
  `secret/data/ardenone-cluster/miroir/keys` and its `miroir-dev` twin
  ([`openbao-policy.hcl`](openbao-policy.hcl)); on the live cell the actual
  ESO read identity is the declarative `eso-ardenone-cluster` policy
- Miroir never writes to OpenBao; it only reads via ESO
- All secrets are base64-encoded in Kubernetes Secrets
- Use separate OpenBao policies per namespace/environment
- Rotate keys quarterly or immediately on leak
- Monitor ESO sync errors via Prometheus alerts
