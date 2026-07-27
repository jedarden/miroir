# Miroir Secret Requirements Validation (bf-5clck)

## Executive Summary
Validation complete. All 8 required secrets have been identified from the documentation. The OpenBao policy and role structure is documented, but there are path inconsistencies between documentation and actual implementation.

## 8 Required Secrets (from docs/operations/secrets-setup.md)

| # | Secret Name | OpenBao Property | ESO Target Secret Key | Rotation Type | Purpose |
|---|-------------|------------------|----------------------|---------------|---------|
| 1 | masterKey | master_key | masterKey | Manual | Meilisearch master key |
| 2 | nodeMasterKey | node_master_key | nodeMasterKey | Zero-downtime | Miroir → Meilisearch auth |
| 3 | adminApiKey | admin_api_key | adminApiKey | Manual | Miroir admin API authentication |
| 4 | adminSessionSealKey | admin_session_seal_key | adminSessionSealKey | Manual | Admin session CSRF protection |
| 5 | searchUiJwtSecret | search_ui_jwt_secret | searchUiJwtSecret | Zero-downtime | Search UI JWT token signing |
| 6 | searchUiJwtSecretPrevious | search_ui_jwt_secret_previous | searchUiJwtSecretPrevious | Overlap only | JWT rotation overlap period |
| 7 | searchUiSharedKey | search_ui_shared_key | searchUiSharedKey | Manual | Optional shared key auth mode |
| 8 | redis-password | redis_password | redis-password | Manual | Redis authentication (optional) |

## OpenBao Policy Verification

### Policy Location: `/home/coding/miroir/k8s/openbao-policy.hcl`

**Policy Definition:**
```hcl
# Read secret values (KV v2 data path)
path "kv/data/search/miroir" {
  capabilities = ["read"]
}

# Read secret metadata (version info for change detection)
path "kv/metadata/search/miroir" {
  capabilities = ["read"]
}
```

**Policy Characteristics:**
- **Name**: miroir-policy
- **Type**: Least-privilege (read-only)
- **Scope**: Limited to `kv/data/search/miroir` and `kv/metadata/search/miroir`
- **Capabilities**: Read-only (no write, delete, or list permissions)
- **Purpose**: External Secrets Operator (ESO) authentication

### Documentation Reference (docs/operations/secrets-setup.md)

**Role Creation Commands:**
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

**Role Configuration:**
- **Name**: miroir
- **Auth Method**: Kubernetes
- **Bound Service Account**: miroir
- **Bound Namespace**: search
- **Policies**: miroir-policy
- **TTL**: 24 hours

## Path Inconsistency Discovery

**Critical Finding**: There are **two different secret paths** referenced in the codebase:

1. **Documentation Path**: `kv/search/miroir`
   - Used in: docs/operations/secrets-setup.md, k8s/openbao-policy.hcl
   - Policy grants access to: `kv/data/search/miroir` and `kv/metadata/search/miroir`

2. **Implementation Path**: `secret/ardenone-cluster/miroir/keys`
   - Used in: scripts/populate-openbao-job.yaml, scripts/populate-openbao-secrets.yaml
   - Confirmed by: bf-13sq3 bead mentioning SecretSyncedError at this path

**Evidence from Bead bf-13sq3:**
> "ExternalSecret miroir-keys (ns miroir) and miroir-dev-keys (ns miroir-dev) on ardenone-cluster both in SecretSyncedError: Secret does not exist, at OpenBao path ardenone-cluster/miroir/keys"

**Resolution Required**: The OpenBao policy needs to be updated to grant access to the correct path (`secret/ardenone-cluster/miroir/keys`) or the populate scripts need to be updated to use the documented path (`kv/search/miroir`).

## KV v2 Secrets Engine Status

**Required Command:**
```bash
bao secrets enable -path=kv kv-v2
```

**Status**: Cannot be verified without OpenBao access. Documentation assumes KV v2 is enabled at `kv` path, but populate scripts use `secret` path, suggesting KV v2 may be mounted at `secret` instead.

## ESO ClusterSecretStore Status

**Required Configuration** (from docs/operations/secrets-setup.md):
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

**Status**: ClusterSecretStore resource definition exists in documentation but not in the codebase. This needs to be deployed to the cluster (likely in declarative-config repo).

## Verification Commands

To verify the setup is complete, run these commands:

```bash
# 1. Verify OpenBao policy exists
bao policy read miroir-policy

# 2. Verify OpenBao role exists
bao read auth/kubernetes/role/miroir

# 3. Verify KV v2 secrets engine is enabled
bao secrets list

# 4. Verify secrets are populated (update path based on actual implementation)
bao kv get kv/search/miroir  # or: bao kv get secret/ardenone-cluster/miroir/keys

# 5. Verify ClusterSecretStore exists
kubectl get clustersecretstore openbao-backend

# 6. Verify ExternalSecret can sync
kubectl get externalsecret miroir-eso -n search -o yaml
```

## Next Steps

1. **Resolve path inconsistency**: Update either documentation or scripts to use consistent secret path
2. **Deploy ClusterSecretStore**: Apply the openbao-backend ClusterSecretStore to the cluster
3. **Verify policy permissions**: Ensure miroir-policy grants access to the actual secret path used
4. **Populate secrets**: Run the populate script to create all 8 secrets in OpenBao
5. **Test ESO sync**: Verify ExternalSecret operator can read and sync the secrets

## Related Beads

- **bf-13sq3**: Populate OpenBao secrets for miroir/miroir-dev ExternalSecrets
- **bf-1v1nu**: Obtain OpenBao write credentials for miroir path  
- **bf-28tbp**: Verify ExternalSecret operator can read all Miroir secrets
- **bf-2cqpo**: Populate miroir/keys path in OpenBao

## Completion Status

✅ **All 8 secrets identified and documented**
✅ **OpenBao policy structure verified**  
✅ **OpenBao role configuration documented**
⚠️ **Path inconsistency discovered** (documentation vs implementation)
❓ **KV v2 enablement status unknown** (requires OpenBao access)
❓ **ClusterSecretStore deployment status unknown** (likely in declarative-config)

**Acceptance Criteria Status:**
- [x] Identified all 8 required secrets
- [x] OpenBao policy documented (miroir-policy exists in codebase)
- [x] OpenBao role documented (miroir role configuration specified)
- [ ] Confirmed OpenBao KV v2 secrets engine is enabled (requires OpenBao access)
- [ ] Verified miroir-policy exists in OpenBao (requires OpenBao access)
- [ ] Verified miroir role exists in OpenBao (requires OpenBao access)

**Note**: This was a read-only validation step as specified. No secrets were written or OpenBao configurations modified.
