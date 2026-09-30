# Runbook: obtaining the miroir write token (ardenone-cluster OpenBao)

How an operator obtains the credential that writes miroir's secrets on the
live ardenone-cluster OpenBao v2 cell — the credential behind every
`bao kv put` / `bao kv patch` operation in [`secrets-setup.md`](secrets-setup.md).
Decision of record (2026-09-30, split of workstream bf-3uvqo): **create-new** —
a dedicated ACL policy `miroir-write` plus a dedicated AppRole `miroir-write`,
authored declaratively. This runbook is self-contained: it names the source of
the credential and every command needed to obtain and verify it.

## Credential source

- **What:** an OpenBao **client token** (not a stored secret), issued by the
  cell's AppRole auth method, bound to exactly one app-scoped policy
  (`miroir-write`), periodic with `token_period=1h`.
- **Issued by:** the ardenone-cluster OpenBao v2 cell
  (`https://openbao.ardenone.com:8444`, v2.6.x, KV v2 mount `secret`), after a
  `bao login -method=approle` with the AppRole's identity material (Step 3).
- **Why AppRole and not the alternatives** (from the decision of record):
  - `bao token create` needs an already-privileged token — only the in-cluster
    reconciler holds the policy/`auth/token/create` rights — and API-only
    state is converged away by the reconciler.
  - A Kubernetes service-account token or an admin kubeconfig: ardenone-cluster
    has no admin kubeconfig off-cluster (read-only proxy only), and
    cluster-admin bypasses OpenBao policy enforcement entirely.
  - Existing write-capable identities are over-broad (`agent-superuser`,
    `coding-agent`, `secrets-sync-ardenone-cluster` span
    `secret/data/ardenone-cluster/*` wildcards) or capability-incomplete
    (no `patch`: `coding-agent-provision` and the `provision-$app` template).
- **Least privilege:** the token carries {`create`, `update`, `patch`, `read`}
  on the two exact keys paths below, plus metadata `read` parity (droppable).
  No `delete`, no `list`, no `sudo`, no other path. Periodic-1h means an
  abandoned token dies within an hour of its last renewal; the AppRole
  secret_id travels exactly once, response-wrapped.

## Live addressing

| What | Value |
|------|-------|
| KV v2 data paths | `secret/data/ardenone-cluster/miroir/keys` and `secret/data/ardenone-cluster/miroir-dev/keys` |
| Properties per doc | `masterKey`, `nodeMasterKey`, `adminApiKey` (one KV-v2 doc per environment) |
| Consumers | External Secrets Operator: ClusterSecretStore `openbao-v2` → ExternalSecrets `miroir-keys` (ns `miroir`) and `miroir-dev-keys` (ns `miroir-dev`) |

`kv/search/miroir` belongs to the decommissioned v1 cell and must never be
addressed; the live cell has no `kv` mount. `secrets-setup.md`'s plan-§9
reference section describes that old layout and is reconciled by its
live-layout section at the top of that file.

## Prerequisites

1. **declarative-config write access** — a checkout of
   `jedarden/declarative-config` with push rights to Forgejo origin. The
   policy and AppRole are GitOps artifacts; hand-creating them in the OpenBao
   API is reverted by the reconciler on its next cycle.
2. **An operator shell on ex44** with the existing `coding-agent` credential
   (the `bao-as openbao-v2` wrapper) — needed once, in Step 2, to unwrap the
   new AppRole's secret_id. No usable OpenBao identity exists on the lab box
   (its `~/.vault-token` is rejected by the v2 cell, and `bao` cannot perform
   the `curl --resolve` SNI override that unauthenticated health checks use).
3. **`bao` CLI** (v2.6 family) on ex44, plus `kubectl` against the read-only
   proxy `http://traefik-ardenone-cluster:8001` from any tailnet host.
4. **Cell health** (unauthenticated, works from any tailnet host):
   `curl --resolve openbao.ardenone.com:8444:100.71.31.73
   https://openbao.ardenone.com:8444/v1/sys/health` → HTTP 200, `"initialized":
   true, "sealed": false, "standby": false`. If the hostname does not resolve
   on the operator host, point it at the tailnet ingress in `/etc/hosts` (the
   SNI name must stay `openbao.ardenone.com`).
5. **Cell CA** — from Secret `openbao-v2-internal-tls` (ns `openbao-v2`, key
   `ca.crt`), for `BAO_CACERT`.
6. **Latency expectations** — pushing to declarative-config triggers ArgoCD
   (Application `ardenone-cluster`, auto-sync). The reconciler then converges
   the cell on its next cycle (every 15 min; 60 s backoff after a failure),
   and the publisher sidecar refreshes the role_id ConfigMap every 60 s.

## Step 1 — declare the policy and the AppRole (declarative-config)

Edit `k8s/ardenone-cluster/openbao-v2/reconciler-config.yml` in
declarative-config — a ConfigMap (`openbao-v2-reconciler`) whose `reconcile.sh`
key is the reconciler's actual script. Four touch points, following the
in-file precedents (app-scoped bespoke policy: `eso-ardenone-cluster`;
AppRole shape: `coding-agent`):

**(a) Policy block** — beside the other `bao policy write` heredocs:

```hcl
bao policy write miroir-write - <<'EOF'
path "secret/data/ardenone-cluster/miroir/keys" {
  capabilities = ["create", "update", "patch", "read"]
}
path "secret/data/ardenone-cluster/miroir-dev/keys" {
  capabilities = ["create", "update", "patch", "read"]
}
path "secret/metadata/ardenone-cluster/miroir/keys" {
  capabilities = ["read"]
}
path "secret/metadata/ardenone-cluster/miroir-dev/keys" {
  capabilities = ["read"]
}
EOF
```

Capability rationale (verified empirically against a v2.6 dev server and the
vendor docs during the decision work): `bao kv put` checks `create` when the
key is absent and `update` when present; native `bao kv patch` needs the
distinct `patch` capability — without it the CLI falls back to a
read-modify-write with a race window and prints a warning; `read` covers
`bao kv get` verification and that fallback. The metadata stanzas serve only
`bao kv metadata get`; keep them for parity with the ESO read policy or drop
them for a pure-write token. `patch` cannot create: the first version of each
keys doc must be a `put`. The paths are the exact keys docs, not a
`miroir/*` wildcard — one doc per environment holds all properties, so no
sibling-key creation rights are needed. (If tighter scoping is preferred, split
production and dev into two policies and two AppRoles; the decision sanctions
either shape.)

**(b) AppRole** — beside the `coding-agent` role writes:

```hcl
bao write auth/approle/role/miroir-write \
  token_policies=miroir-write token_period=1h \
  secret_id_num_uses=0 secret_id_ttl=0
```

`token_period=1h` makes logins renewable-and-periodic (an abandoned token dies
1 h after its last renewal); `secret_id_num_uses=0` / `secret_id_ttl=0` make
the secret_id itself unlimited and non-expiring — the single-use, 168 h
object is the *wrapping token* that delivers it (Step 2).

**(c) role_id publish loop** — add `miroir-write` to the list in the
`for r in coding-agent coding-agent-provision …` loop, so the publisher
sidecar exports its role_id:

```hcl
for r in coding-agent coding-agent-provision miroir-write $(for a in $PROVISION_APPS; do echo "provision-$a"; done); do
```

**(d) secret_id mint call** — beside the `coding-agent-provision` mint:

```hcl
mint_agent_secret_id miroir-write || echo "secret_id mint deferred to the next cycle" >&2
```

Commit and push to Forgejo origin (reconcile divergence with a merge commit;
never force-push). ArgoCD syncs Application `ardenone-cluster`; force a sync
from the ArgoCD Application if it lags. When the reconciler log prints
`minted wrapped secret_id for AppRole miroir-write`, the cell-side artifacts
exist. Watch with:

```bash
kubectl --server=http://traefik-ardenone-cluster:8001 logs -n openbao-v2 \
  openbao-v2-reconciler-0 -c reconciler | grep miroir-write
```

## Step 2 — collect the identity material (ex44)

**role_id** (the non-secret half; published by the sidecar into the
`openbao-agent-role-ids` ConfigMap, key `miroir-write`):

```bash
mkdir -p -m 0700 ~/.config/openbao/ardenone-cluster-miroir-write
kubectl --server=http://traefik-ardenone-cluster:8001 get configmap -n openbao-v2 \
  openbao-agent-role-ids \
  -o jsonpath='{.data.miroir-write}' \
  > ~/.config/openbao/ardenone-cluster-miroir-write/role_id
chmod 0600 ~/.config/openbao/ardenone-cluster-miroir-write/role_id
```

Empty output means the sidecar has not published yet — wait for the next
publisher cycle (60 s) and retry. Redirect into the file; do not print it.

**secret_id** (delivered response-wrapped; the reconciler stores only a
single-use 168 h wrapping token at
`secret/ardenone-cluster/openbao/agent-secret-ids/miroir-write` — that KV path
doubles as the mint idempotency marker). Unwrap it on ex44 through the
existing `coding-agent` credential, which may read the hand-off prefix
(its deny list covers only replicator-tokens, unseal-keys, and the recovery
bundle):

```bash
bao-as openbao-v2 bao kv get -field=wrapping_token \
    secret/ardenone-cluster/openbao/agent-secret-ids/miroir-write \
  | bao unwrap -field=secret_id - \
  > ~/.config/openbao/ardenone-cluster-miroir-write/secret_id
chmod 0600 ~/.config/openbao/ardenone-cluster-miroir-write/secret_id
```

(If this `bao` build's `unwrap` lacks `-field`, drop the flag and extract
`data.secret_id` from the JSON output instead.) Unwrapping consumes the
wrapping token. If it has expired (168 h) or was already unwrapped, have an
operator delete the KV marker path — deletions are an operator action through
an OIDC session, removed from agent policies since 2026-08-31 — and the next
reconciler cycle re-mints.

## Step 3 — log in and verify the token (no secret value touched)

On ex44, with `BAO_ADDR`/`BAO_CACERT` as used by the `bao-as openbao-v2`
wrapper (cell entrypoint `https://openbao.ardenone.com:8444`):

```bash
BAO_ROLE_ID="$(cat ~/.config/openbao/ardenone-cluster-miroir-write/role_id)" \
BAO_SECRET_ID="$(cat ~/.config/openbao/ardenone-cluster-miroir-write/secret_id)" \
  bao login -method=approle
```

(`bao write auth/approle/login role_id=… secret_id=…` is equivalent but puts
both halves in argv and shell history — prefer the env-var form.) The CLI
stores the resulting client token as the session token.

Verify, without reading or writing any secret value — `token lookup` and
`token capabilities` are ACL/token evaluations:

```bash
bao token lookup
# → policies: ["default", "miroir-write"]  (nothing else), period: 1h, renewable

bao token capabilities secret/data/ardenone-cluster/miroir/keys
# → create, update, patch, read

bao token capabilities secret/data/ardenone-cluster/miroir-dev/keys
# → create, update, patch, read

bao token capabilities secret/data/ardenone-cluster/other-app/keys
# → deny   (least-privilege negative check)
```

`default` is the standard empty policy attached to every token; the only
app-scoped policy is `miroir-write`.

## Step 4 — where the token goes afterward

- **Nothing consumes the token persistently.** It lives in the operator's
  shell for the run (`bao token renew` extends it; `bao token revoke` kills it
  immediately; otherwise it dies 1 h after its last renewal). The durable
  artifacts are the two declarative blocks from Step 1 plus the identity
  material under `~/.config/openbao/ardenone-cluster-miroir-write/` on ex44 —
  re-login any time with Step 3.
- **What the writes flow into:** secrets written with this token are picked up
  by the existing ESO chain — ClusterSecretStore `openbao-v2` (Valid,
  ReadWrite; authenticated by its own read-only kubernetes auth role) syncs
  the keys docs into Secret `miroir-keys` (ns `miroir`) and `miroir-dev-keys`
  (ns `miroir-dev`) within its refresh interval. Until first population those
  ExternalSecrets report `SecretSyncedError` ("could not get secret data from
  provider") purely because the remote keys are absent; no ESO-side change is
  needed. Confirm afterwards:

  ```bash
  kubectl --server=http://traefik-ardenone-cluster:8001 \
    get externalsecret -n miroir miroir-keys
  kubectl --server=http://traefik-ardenone-cluster:8001 \
    get externalsecret -n miroir-dev miroir-dev-keys
  ```

- **Miroir workloads never hold an OpenBao token** (by design — they read
  through ESO only). This credential is for operators only. Use it for the
  populate/patch operations in [`secrets-setup.md`](secrets-setup.md); on the
  live cell the mount is `secret` and `cas_required=true`, so always pass
  `-cas` (`-cas=0` for the first version):

  ```bash
  bao kv put -cas=0 secret/ardenone-cluster/miroir/keys \
    masterKey="$(openssl rand -base64 32)" \
    nodeMasterKey="$(openssl rand -base64 32)" \
    adminApiKey="$(openssl rand -base64 32)"
  ```

## Material this runbook supersedes

The v1-cell artifacts this procedure replaces (removed 2026-09-30) addressed
the decommissioned `kv/search/miroir` mount, omitted `patch`, or used the
rejected issuance mechanisms: `k8s/openbao-policy.hcl`,
`k8s/miroir-write-policy.hcl` (bf-wlbi7), `k8s/openbao-policy-write.hcl` and
`scripts/generate-openbao-token.sh` (bf-4h67m), and the token-minting manifest
`k8s/generate-miroir-openbao-token-job.yaml` (a Kubernetes Job against a
nonexistent service account — also a shape ArgoCD cannot manage idempotently).
The read-only miroir policy documentation
([`openbao-policy.hcl`](openbao-policy.hcl)) remains, corrected to the live
path shape, for the ESO sync path; on the live cell the actual ESO read
identity is the declarative `eso-ardenone-cluster` policy.
