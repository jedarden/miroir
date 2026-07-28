# Bead bf-4h67m: OpenBao Token Generation for Miroir

## Status: PREPARED - Requires Cluster-Admin Access

**Completed:**
- ✅ Created OpenBao write policy (`k8s/openbao-policy-write.hcl`)
- ✅ Fixed YAML syntax in token generation job (`k8s/generate-miroir-openbao-token-job.yaml`)
- ✅ Created standalone generation script (`scripts/generate-openbao-token.sh`)
- ✅ Verified OpenBao is accessible and unsealed at `http://100.126.102.108:8200`

**Blocking Issue:**
- ❌ No cluster-admin access to ardenone-cluster (read-only proxy only)
- ❌ Cannot create Jobs or Secrets in openbao namespace

## What Was Prepared

### 1. OpenBao Write Policy
**File:** `k8s/openbao-policy-write.hcl`

Grants write access to `kv/search/miroir` path:
```hcl
path "kv/data/search/miroir" {
  capabilities = ["create", "update", "delete", "read"]
}
path "kv/metadata/search/miroir" {
  capabilities = ["create", "update", "delete", "read"]
}
path "kv/metadata/search" {
  capabilities = ["list"]
}
```

### 2. Token Generation Job (FIXED)
**File:** `k8s/generate-miroir-openbao-token-job.yaml`

Fixed duplicate `volumeMounts:` syntax error. Job will:
- Authenticate via Kubernetes service account
- Create `miroir-policy-write` policy
- Generate token with 24-hour TTL
- Store token in Kubernetes secret `miroir-openbao-token`

### 3. Standalone Generation Script
**File:** `scripts/generate-openbao-token.sh`

Manual token generation script requiring:
- `bao` CLI installed
- Admin token set as `BAO_TOKEN` environment variable
- OpenBao connectivity

## Completion Options

### Option A: Run Job via ArgoCD (GitOps)
Push the fixed job to declarative-config:
```bash
cd ~/declarative-config
cp /home/coding/miroir/k8s/generate-miroir-openbao-token-job.yaml k8s/ardenone-cluster/openbao/
git add k8s/ardenone-cluster/openbao/generate-miroir-openbao-token-job.yaml
git commit -m "feat(bf-4h67m): add OpenBao token generation job for miroir"
git push
```

Then apply manually:
```bash
kubectl --kubeconfig=<ardenone-cluster-admin-kubeconfig> apply -f k8s/ardenone-cluster/openbao/generate-miroir-openbao-token-job.yaml
```

### Option B: Manual Execution
1. Get an admin token for OpenBao at ardenone-cluster
2. Run the standalone script:
```bash
export BAO_ADDR="http://100.126.102.108:8200"
export BAO_TOKEN="<admin-token>"
./scripts/generate-openbao-token.sh
```

### Option C: Direct API Call
With admin token:
```bash
# 1. Create policy
curl -X POST http://100.126.102.108:8200/v1/sys/policies/miroir-policy-write \
  -H "X-Vault-Token: $BAO_TOKEN" \
  -H "Content-Type: application/json" \
  -d @- <<'EOF'
{
  "policy": "path \"kv/data/search/miroir\" {\n  capabilities = [\"create\", \"update\", \"delete\", \"read\"]\n}\npath \"kv/metadata/search/miroir\" {\n  capabilities = [\"create\", \"update\", \"delete\", \"read\"]\n}\npath \"kv/metadata/search\" {\n  capabilities = [\"list\"]\n}"
}
EOF

# 2. Generate token
curl -X POST http://100.126.102.108:8200/v1/auth/token/create \
  -H "X-Vault-Token: $BAO_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "policies": ["miroir-policy-write"],
    "ttl": "24h"
  }' | jq '.auth.client_token'
```

## OpenBao Details

**Cluster:** ardenone-cluster
**Namespace:** openbao
**Service:** openbao-ardenone-cluster (ClusterIP)
**Tailscale:** openbao-ardenone at `100.126.102.108:8200`
**Status:** Unsealed, Healthy
**Version:** 2.5.1

## Verification Steps

After token generation, verify:
1. Token has `miroir-policy-write` policy attached
2. Policy grants `create, update, delete, read` on `kv/data/search/miroir`
3. Token TTL is 24 hours
4. Secret `miroir-openbao-token` exists in openbao namespace

## Notes

- ardenone-cluster uses read-only proxy access (no direct kubeconfig available)
- Other clusters with admin access: ardenone-manager, rs-manager, iad-ci, iad-options
- This token is for SETUP PHASE ONLY and should be revoked after initial population
