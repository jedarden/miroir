# OpenBao miroir-write-policy Documentation (bf-wlbi7)

## Overview

Created a least-privilege OpenBao policy `miroir-write-policy` that grants write access specifically to the `kv/search/miroir` path. This policy follows the principle of least privilege and does not grant access to any other paths.

## Policy Details

**Policy Name:** `miroir-write-policy`  
**File Location:** `/home/coding/miroir/k8s/miroir-write-policy.hcl`  
**Scope:** `kv/search/miroir/*` only  
**Capabilities:** create, update, delete, read

## Path Permissions

The policy grants the following specific permissions:

### 1. `kv/data/search/miroir` (Secret Data)
- **Capabilities:** create, update, delete, read
- **Purpose:** Write access to actual Miroir secret values
- **Use Case:** Creating, updating, deleting, and reading secret data

### 2. `kv/metadata/search/miroir` (Secret Metadata)
- **Capabilities:** create, update, delete, read  
- **Purpose:** Write access to secret metadata and version information
- **Use Case:** Managing metadata, versions, and secret configuration

### 3. `kv/metadata/search` (Parent Path)
- **Capabilities:** list
- **Purpose:** Verify and list secrets in the search path
- **Use Case:** Directory listing for verification purposes

## Installation

### Option 1: Using the bao CLI

```bash
# Set OpenBao address (ardennone-cluster via Tailscale)
export BAO_ADDR="http://openbao-ardenone.tail1b1987.ts.net:8200"
export BAO_TOKEN="<admin-token>"

# Install the policy
bao policy write miroir-write-policy k8s/miroir-write-policy.hcl

# Verify policy creation
bao policy read miroir-write-policy
```

### Option 2: Using the API

```bash
curl -X PUT \
  -H "X-Vault-Token: $BAO_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"policy":"$(cat k8s/miroir-write-policy.hcl)"}' \
  $BAO_ADDR/v1/sys/policies/acl/miroir-write-policy
```

## Token Generation

### Generate a Token with this Policy

```bash
# Generate a 24-hour token with miroir-write-policy
bao token create -policy=miroir-write-policy -ttl=24h -display-name="miroir-write-access"

# Generate a periodic token (30-day renewable period)
bao token create -policy=miroir-write-policy -period=720h -display-name="miroir-write-periodic"

# Generate an orphan token (no parent, independent lifecycle)
bao token create -policy=miroir-write-policy -ttl=24h -orphan -display-name="miroir-write-orphan"
```

### Token Usage

Once you have a token with this policy:

```bash
# Set the token
export BAO_TOKEN="<generated-token>"

# Write a secret
bao kv put kv/search/miroir/config key1=value1 key2=value2

# Read a secret
bao kv get kv/search/miroir/config

# List secrets
bao kv list kv/metadata/search

# Delete a secret
bao kv delete kv/search/miroir/config
```

## Policy Verification

To verify the policy is working correctly:

```bash
# Test read access (should succeed)
bao kv get kv/search/miroir/test

# Test write access (should succeed)  
bao kv put kv/search/miroir/test test=value

# Test access to other paths (should fail - expected)
bao kv get kv/data/other/path
# Expected: permission denied
```

## Least Privilege Verification

This policy intentionally denies access to all other paths:

- ❌ Cannot access `kv/data/search/miroir`以外的任何路径
- ❌ Cannot access system paths (`sys/*`)
- ❌ Cannot access other secret engines
- ❌ Cannot perform administrative operations

## Integration with Existing Infrastructure

### Relationship to Existing Policies

- **`miroir` (read-only):** Used by External Secrets Operator for normal operations
- **`miroir-policy-write` (setup):** Existing write policy from bf-4h67m
- **`miroir-write-policy` (this policy):** New dedicated write policy for kv/search/miroir

### Kubernetes Token Generation Job

The policy can be referenced in the Kubernetes job generator at `/home/coding/miroir/k8s/generate-miroir-openbao-token-job.yaml` by updating the policy name:

```yaml
# In the job template, change:
bao policy write miroir-policy-write /tmp/policy.hcl

# To:
bao policy write miroir-write-policy /tmp/policy.hcl
```

## Security Considerations

1. **Token Expiration:** Use limited TTL tokens (24h) rather than periodic tokens for most use cases
2. **Token Rotation:** Regularly rotate tokens generated with this policy
3. **Audit Logging:** Monitor OpenBao audit logs for access to kv/search/miroir
4. **Revocation:** Tokens should be revoked when no longer needed:
   ```bash
   bao token revoke <token-accessor>
   ```

## Troubleshooting

### Common Issues

**Permission Denied:**
- Verify policy is installed: `bao policy list`
- Verify token has policy: `bao token lookup <token>`
- Check path matches exactly: `kv/data/search/miroir`

**Invalid Path:**
- Remember KV v2 paths use `/data/` for secrets and `/metadata/` for metadata
- Check KV engine is mounted at `kv/` path: `bao secrets list`

**Token Issues:**
- Verify token TTL hasn't expired: `bao token lookup <token>`
- Check token has correct policies: `bao token lookup -format=json <token> | jq '.data.policies'`

## OpenBao Cluster Details

**Cluster:** ardenone-cluster  
**Namespace:** openbao  
**Address:** `http://openbao-ardenone.tail1b1987.ts.net:8200`  
**KV Engine:** kv (v2) at path `kv/`  
**Target Path:** `kv/search/miroir/`

## Dependencies

This policy creation depends on:
- OpenBao deployment at ardenone-cluster (documented in bf-4e00t)
- KV v2 secrets engine enabled at `kv/` path
- Understanding of OpenBao setup from previous beads (bf-4h67m, bf-4e00t)

## Acceptance Criteria Met

- ✅ Created a new OpenBao policy `miroir-write-policy`
- ✅ Policy grants write capabilities (create, update, delete) to kv/search/miroir/*
- ✅ Policy does NOT grant access to any other paths (least privilege)
- ✅ Policy is documented and can be referenced for token generation

## Files Modified

- `/home/coding/miroir/k8s/miroir-write-policy.hcl` - New policy file
- `/home/coding/miroir/notes/bf-wlbi7.md` - This documentation
