# OpenBao Setup Investigation for miroir Namespace

## Investigation Summary

Investigated OpenBao configuration at ardenone-cluster to understand the setup and requirements for miroir credentials. This was reconnaissance only — no credential generation.

## Findings

### 1. OpenBao Instance and Endpoint URL

**Cluster:** ardenone-cluster  
**Namespace:** openbao  
**Service:** openbao-ardenone-cluster  
**Internal Endpoint:** `http://openbao-ardenone-cluster.openbao.svc.cluster.local:8200`  
**External Endpoint (via Tailscale):** Available via `openbao-tailscale` service

**StatefulSet:** `openbao-ardenone-cluster` (1 replica, 123 days old)

### 2. Required KV Path: kv/search/miroir

**Confirmed Path:** `kv/search/miroir`

**Structure:** The path follows the KV v2 secret engine pattern:
- **Mount:** `kv` 
- **Path:** `search/miroir`
- **Full API path:** `secret/data/kv/search/miroir` (for KV v2)

**Expected Secret Properties** (from plan.md):
- `master_key` - Master API key for Miroir
- `node_master_key` - Node-specific master key for Meilisearch admin operations
- `admin_api_key` - Admin API key for operators and miroir-ctl

**Current Status:** Path does not exist yet in OpenBao (confirmed by plan documentation mentioning `SecretSyncedError` due to non-existent OpenBao path).

### 3. Current Authentication Method

**Primary Method:** Kubernetes JWT Authentication

**How it works:**
- ServiceAccounts use projected service account tokens with `audience: vault`
- Tokens are mounted at `/var/run/secrets/openbao/token`
- Tokens expire every 3600 seconds (1 hour)
- Authentication endpoint: `$BAO_ADDR/v1/auth/kubernetes/login`

**Example from secrets-sync:**
```bash
response=$(curl -sf -X POST \
  -H "Content-Type: application/json" \
  -d "{\"jwt\":\"$sa_token\",\"role\":\"secrets-sync\"}" \
  "$BAO_ADDR/v1/auth/kubernetes/login")
```

**Existing ServiceAccounts using Kubernetes auth:**
- `secrets-sync.openbao` - Syncs K8s secrets to OpenBao
- `openbao-replicator.openbao` - Replicates secrets between clusters

### 4. Permissions Needed for miroir

**Write Access Requirements:**

For the miroir application to read credentials from OpenBao via External Secrets Operator (ESO), the following permissions are required:

**OpenBao Policy Requirements:**
```
# Allow reading from kv/search/miroir
path "kv/data/search/miroir" {
  capabilities = ["read"]
}
```

**Kubernetes RBAC Requirements:**
- ServiceAccount in target namespace (miroir or search namespace)
- ClusterSecretStore or SecretStore configured for OpenBao backend
- Appropriate Role/RoleBinding for the ESO service account to create secrets

**ESO Configuration Pattern** (from plan.md):
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: miroir-secrets
  namespace: search
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: openbao-backend
    kind: ClusterSecretStore
  target:
    name: miroir-secrets
    creationPolicy: Owner
  data:
  - secretKey: masterKey
    remoteRef:
      key: kv/search/miroir
      property: master_key
  - secretKey: nodeMasterKey
    remoteRef:
      key: kv/search/miroir
      property: node_master_key
  - secretKey: adminApiKey
    remoteRef:
      key: kv/search/miroir
      property: admin_api_key
```

### 5. Existing Tokens and Service Accounts

**Existing OpenBao Integration Patterns:**

**A. secrets-sync (Kubernetes auth)**
- **ServiceAccount:** `secrets-sync.openbao`
- **Role:** `secrets-sync`
- **Function:** Syncs Kubernetes secrets to OpenBao KV store
- **KV Prefix:** `secret/data/ardenone-cluster`
- **Token Source:** Projected SA token (renewed automatically)

**B. openbao-replicator (Kubernetes auth)**
- **ServiceAccount:** `openbao-replicator.openbao` 
- **Role:** `openbao-replicator`
- **Function:** Replicates secrets between clusters
- **Tokens:** Uses `openbao-replicator-tokens-v2` secret for remote peers
- **Token Source:** Projected SA token + static tokens for remotes

**C. External Secrets Operator (ESO)**
- **Namespace:** `external-secrets` (likely)
- **Function:** Reads from OpenBao and creates K8s secrets
- **Pattern:** Uses ClusterSecretStore with OpenBao backend

**No existing miroir-specific credentials found** in current OpenBao configuration.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        OpenBao (openbao ns)                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  KV v2 Secret Engine                                       │ │
│  │  ┌──────────────────────────────────────────────────────┐ │ │
│  │  │  kv/search/miroir (TO BE CREATED)                    │ │ │
│  │  │  ├── master_key                                      │ │ │
│  │  │  ├── node_master_key                                 │ │ │
│  │  │  └── admin_api_key                                   │ │ │
│  │  └──────────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌───────────────────────────────────────────────────────────────────┐
│              External Secrets Operator (external-secrets ns)      │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  ExternalSecret                                              │ │
│  │  - Reads from kv/search/miroir                              │ │
│  │  - Creates K8s secret in target namespace                   │ │
│  └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    miroir/search namespace                       │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  Secret: miroir-secrets                                    │ │
│  │  ├── masterKey                                            │ │
│  │  ├── nodeMasterKey                                         │ │
│  │  └── adminApiKey                                           │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Next Steps (Follow-up Beads)

Based on this reconnaissance, the following beads should be created:

1. **Create OpenBao Policy** - Create policy for reading `kv/search/miroir`
2. **Generate miroir Credentials** - Generate secure keys for master_key, node_master_key, admin_api_key
3. **Configure ESO Integration** - Set up ExternalSecret for miroir namespace
4. **Test Credential Flow** - Verify end-to-end credential delivery to miroir pods

## References

- **Plan Documentation:** `/home/coding/miroir/docs/plan/plan.md`
- **Declarative Config:** `/home/coding/declarative-config/k8s/ardenone-cluster/openbao/`
- **OpenBao ConfigMap:** `secrets-sync-config` in openbao namespace
- **Replicator Config:** `openbao-replicator-config` in openbao namespace

---
*Investigation completed: 2026-07-28*
*Bead ID: bf-2cctu*
