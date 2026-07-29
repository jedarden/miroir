# OpenBao Setup at ardenone-cluster

## Overview

OpenBao is deployed on ardenone-cluster as a single-instance StatefulSet using the official OpenBao Helm chart (version 0.26.1, OpenBao 2.5.1).

### Deployment Details

- **Namespace**: `openbao`
- **StatefulSet**: `openbao-ardenone-cluster` (1 replica)
- **Storage**: 2Gi PersistentVolumeClaim (Longhorn storage class)
- **Auto-unseal**: Sidecar container using `openbao-unseal-key` secret

### Access Endpoints

| Endpoint | URL | Purpose |
|----------|-----|---------|
| **Cluster Internal** | `http://openbao-ardenone-cluster.openbao.svc.cluster.local:8200` | Internal cluster access |
| **Tailscale VPN** | `http://openbao-ardenone.tail1b1987.ts.net:8200` | External access via Tailscale |
| **UI** | Available at Tailscale endpoint on port 8200 | Web UI |

## Authentication Methods

ardenone-cluster OpenBao supports **two** authentication methods:

### 1. Kubernetes Authentication

Kubernetes authentication is the **primary method** for workloads running inside the cluster.

**How it works:**
- Workloads use their Kubernetes ServiceAccount JWT token
- OpenBao validates the token against the Kubernetes API server via TokenReview API
- No static tokens to manage or expire

**Required RBAC:**
```yaml
# OpenBao needs token review permissions
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: openbao-token-reviewer
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:auth-delegator
subjects:
  - kind: ServiceAccount
    name: openbao-ardenone-cluster
    namespace: openbao
```

**Configured Roles:**
- `openbao-replicator` - For cross-cluster secret replication
- `secrets-sync` - For syncing Kubernetes secrets to OpenBao KV

**Authentication example (from within cluster):**
```bash
# Get ServiceAccount JWT
SA_TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)

# Login to OpenBao
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"jwt\":\"$SA_TOKEN\",\"role\":\"openbao-replicator\"}" \
  http://openbao-ardenone-cluster.openbao.svc.cluster.local:8200/v1/auth/kubernetes/login

# Response contains client_token
```

### 2. Token Authentication

Token authentication is used for:
- Cross-cluster replication (periodic tokens)
- External access
- Backup/restoration operations
- Manual administrative access

**Token types:**
- **Periodic tokens** (preferred): Renewable with 720h (30-day) period
- **TTL tokens**: One-time tokens that expire after TTL

**Example token creation:**
```bash
# Set environment variables
export BAO_ADDR=http://openbao-ardenone.tail1b1987.ts.net:8200
export BAO_TOKEN=<root-token-or-admin-token>

# Create a periodic token
bao token create \
  -policy=<policy-name> \
  -period=720h \
  -display-name="descriptive-name" \
  -orphan
```

## Policy Structure

OpenBao policies are written in HCL (HashiCorp Configuration Language) and define access permissions to secrets and system paths.

### Example Policies

**1. Read-only policy (for replication):**
```hcl
path "secret/data/ardenone-cluster/*" {
  capabilities = ["read"]
}
path "secret/metadata/ardenone-cluster/*" {
  capabilities = ["read", "list"]
}
```

**2. Write policy (for replication to this cluster):**
```hcl
path "secret/data/ardenone-manager/*" {
  capabilities = ["create", "update", "read"]
}
path "secret/metadata/ardenone-manager/*" {
  capabilities = ["read", "list"]
}
```

**3. Backup policy (read all secrets and config):**
```hcl
# Read all secrets for backup
path "secret/data/*" {
  capabilities = ["read"]
}
path "secret/metadata/*" {
  capabilities = ["read", "list"]
}
# Read system config (policies, auth, mounts)
path "sys/policies/acl" {
  capabilities = ["read", "list"]
}
path "sys/policies/acl/*" {
  capabilities = ["read"]
}
path "sys/auth" {
  capabilities = ["read"]
}
path "sys/mounts" {
  capabilities = ["read"]
}
```

### Creating a New Policy

**Using the CLI:**
```bash
# Via stdin
bao policy write <policy-name> - <<'EOF'
path "secret/data/my-app/*" {
  capabilities = ["create", "read", "update", "delete"]
}
EOF

# Or from a file
bao policy write <policy-name> policy.hcl
```

**Using the API:**
```bash
curl -X PUT \
  -H "X-Vault-Token: $BAO_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"policy":"<hcl-policy-string>"}' \
  $BAO_ADDR/v1/sys/policies/acl/<policy-name>
```

## Secret Engine

ardenone-cluster uses **KV v2** secrets engine:
- **Mount path**: `secret/`
- **Cluster prefix**: `ardenone-cluster/`
- **API paths**:
  - Read: `v1/secret/data/<path>`
  - List: `v1/secret/metadata/<path>?list=true`
  - Write: `v1/secret/data/<path>`

## Generating a Token

### Step 1: Define a Policy

First, create a policy that defines what the token can access:

```bash
# Example: Policy for a CI/CD pipeline
bao policy write ci-cd-pipeline - <<'EOF'
path "secret/data/ardenone-cluster/ci-cd/*" {
  capabilities = ["read", "list"]
}
EOF
```

### Step 2: Choose Authentication Method

**Option A: Kubernetes Authentication (for workloads in cluster)**

1. Create a Kubernetes role mapping:
   ```bash
   # This must be done via OpenBao API or UI
   # Configure role: ci-cd-role
   # Bound ServiceAccount: ci-cd-sa
   # Namespace: your-namespace
   ```

2. Use in your workload:
   ```bash
   # Your Kubernetes pod automatically has SA token at:
   # /var/run/secrets/kubernetes.io/serviceaccount/token
   
   # Login
   SA_TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
   curl -X POST \
     -H "Content-Type: application/json" \
     -d "{\"jwt\":\"$SA_TOKEN\",\"role\":\"ci-cd-role\"}" \
     $BAO_ADDR/v1/auth/kubernetes/login
   ```

**Option B: Token Authentication (for external or manual access)**

```bash
# Create a periodic token (auto-renews every 30 days)
bao token create \
  -policy=ci-cd-pipeline \
  -period=720h \
  -display-name="ci-cd-token" \
  -orphan

# Output: Token and accessor
# Save the token securely!
```

### Step 3: Use the Token

```bash
# Set environment
export BAO_ADDR=http://openbao-ardenone.tail1b1987.ts.net:8200
export BAO_TOKEN=<your-token>

# Read a secret
bao kv get -field=example ardenone-cluster/ci-cd/my-secret

# Or via API
curl -H "X-Vault-Token: $BAO_TOKEN" \
  $BAO_ADDR/v1/secret/data/ardenone-cluster/ci-cd/my-secret
```

## Cross-Cluster Replication

ardenone-cluster participates in a **cross-cluster replication mesh** with:
- **ardenone-manager**: Primary management cluster
- **ardenone-hub**: Hub cluster

**Replication details:**
- **Direction**: All clusters replicate to all clusters (mesh topology)
- **Interval**: Every 30 minutes (1800 seconds)
- **Paths**: Each cluster replicates its own prefix (e.g., `ardenone-cluster/*`)
- **Transport**: Over Tailscale VPN (headscale routing)

**Replicator deployment:**
- Uses token authentication for remote clusters
- Uses Kubernetes authentication for local cluster
- Tokens are stored in `openbao-replicator-tokens-v2` secret (as SealedSecrets)

## Backup Strategy

**Local backup via Restic:**
- **Source**: ardenone-cluster OpenBao
- **Target**: Garage S3 (backed by Synology NAS)
- **Schedule**: Every 6 hours
- **What's backed up**: All secrets, policies, auth methods, mounts

**Cross-cluster replication:**
- All `ardenone-cluster/*` secrets are replicated to ardenone-manager and ardenone-hub
- Provides redundancy and disaster recovery capability

## Tools

### OpenBao CLI (bao)
- **Location**: `/home/coding/.nix-profile/bin/bao`
- **Version**: Matches server (2.5.1)

### Common bao Commands

```bash
# Check status
bao status

# Login
bao login <method> [options]

# Read secret (KV v2)
bao kv get -field=<key> <path>

# Write secret
bao kv put <path> <key>=<value>

# List secrets
bao kv list <path>

# Create policy
bao policy write <name> <file>

# Create token
bao token create -policy=<policy> -period=720h -display-name="name"

# Renew token
bao token renew
```

## Related Documentation

- **DR Runbook**: `~/declarative-config/k8s/openbao-dr-runbook.md`
- **Config templates**: `~/declarative-config/k8s/ardenone-manager/openbao/*.yml`
- **ardenone-cluster configs**: `~/declarative-config/k8s/ardenone-cluster/openbao/`

## Security Considerations

1. **Unseal key**: Stored in `openbao-unseal-key` secret (SealedSecret)
2. **Root tokens**: Should be stored in password manager, not in OpenBao
3. **Token renewal**: Periodic tokens must be actively renewed (720h period)
4. **Transport**: TLS disabled internally (`tls_disable = 1` in config)
5. **Access control**: Use scoped policies, never root tokens for workloads

## Troubleshooting

**Can't authenticate:**
- Check ServiceAccount has proper RBAC
- Verify OpenBao can reach Kubernetes API
- Ensure role is configured in OpenBao

**Token expired:**
- Periodic tokens: Check renewal is working
- TTL tokens: Create new token
- Use `bao token lookup <token>` to check status

**Permission denied on secret:**
- Verify policy includes the secret path
- Check if path uses correct prefix (e.g., `ardenone-cluster/`)
- Ensure capabilities are correct (read, list, create, update, delete)
