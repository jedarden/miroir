# OpenBao Policy Verification for bf-3ll0r

## Summary

**CRITICAL FINDING**: Path mismatch between populate scripts and ExternalSecret configuration.

## Documented Policy Structure

According to `docs/operations/openbao-policy.hcl` and `k8s/openbao-policy.hcl`:

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

**Expected path**: `kv/search/miroir` (ExternalSecret syntax)
**Mapped paths**: `kv/data/search/miroir` and `kv/metadata/search/miroir`

## Actual Populate Script Behavior

From `scripts/populate-openbao-job.yaml` and `scripts/populate-miroir-secrets.sh`:

```bash
bao kv get secret/ardenone-cluster/${NAMESPACE}/keys
bao kv put secret/ardenone-cluster/${NAMESPACE}/keys \
  masterKey="..." \
  nodeMasterKey="..." \
  adminApiKey="..." \
  ...
```

**Actual paths being written to**:
- `secret/ardenone-cluster/miroir/keys`
- `secret/ardenone-cluster/miroir-dev/keys`

## ExternalSecret Configuration

From `k8s/examples/argocd-apps/external-secret.yaml`:

```yaml
data:
  - secretKey: masterKey
    remoteRef:
      key: kv/search/miroir
      property: master_key
```

**Expected path**: `kv/search/miroir`

## Path Mismatch Analysis

In OpenBao KV v2:
- `secret/` prefix → **KV v1 legacy** (deprecated, but still functional)
- `kv/` prefix → **KV v2 current**
- Actual paths accessed:
  - `secret/ardenone-cluster/miroir/keys` → maps to `kv/data/ardenone-cluster/miroir/keys`
  - `kv/search/miroir` → maps to `kv/data/search/miroir`

**These are DIFFERENT paths!**

## Current Policy Coverage

The current `miroir` policy covers:
- ✅ `kv/data/search/miroir` (read)
- ✅ `kv/metadata/search/miroir` (read)

The policy does **NOT** cover:
- ❌ `kv/data/ardenone-cluster/miroir/keys`
- ❌ `kv/metadata/ardenone-cluster/miroir/keys`
- ❌ `kv/data/ardenone-cluster/miroir-dev/keys`
- ❌ `kv/metadata/ardenone-cluster/miroir-dev/keys`

## Required Policy Fix

To support the actual populate script paths, the policy should be:

```hcl
# Read secret values for both namespaces
path "kv/data/ardenone-cluster/miroir/keys" {
  capabilities = ["read"]
}

path "kv/data/ardenone-cluster/miroir-dev/keys" {
  capabilities = ["read"]
}

# Read secret metadata for both namespaces
path "kv/metadata/ardenone-cluster/miroir/keys" {
  capabilities = ["read"]
}

path "kv/metadata/ardenone-cluster/miroir-dev/keys" {
  capabilities = ["read"]
}
```

## Verification Status

**Policy existence**: Cannot verify directly due to read-only kubectl access
**Policy correctness**: ❌ **INCORRECT** - covers wrong paths
**ExternalSecret compatibility**: ❌ **BROKEN** - paths don't match

## Recommendations

1. **Immediate**: Update OpenBao policy to cover actual secret paths
2. **Fix populate scripts** OR **fix ExternalSecret examples** to use consistent paths
3. **Verify**: Test policy with actual OpenBao instance

## Next Steps

The bead acceptance criteria cannot be met until:
- [ ] Policy is verified to exist in OpenBao
- [ ] Policy covers the correct paths for both miroir and miroir-dev
- [ ] Path consistency is achieved between populate scripts and ExternalSecret configs
- [ ] Policy is attached to appropriate auth method/token for ESO

**STATUS**: ❌ **ACCEPTANCE CRITERIA NOT MET**
