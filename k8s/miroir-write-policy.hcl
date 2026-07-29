# OpenBao Write Policy for kv/search/miroir (bf-wlbi7)
#
# Least-privilege policy granting write access specifically to the
# kv/search/miroir path for Miroir secret management.
#
# Scope: ONLY kv/search/miroir/* - no other paths are accessible
# Capabilities: create, update, delete, read
#
# Install:
#   bao policy write miroir-write-policy k8s/miroir-write-policy.hcl
#
# Generate token with this policy:
#   bao token create -policy=miroir-write-policy -ttl=24h
#
# This policy is intentionally minimal and scoped to only the Miroir path.

# Write access to kv/data/search/miroir (actual secret data)
# Required capabilities: create, update, delete, read
path "kv/data/search/miroir" {
  capabilities = ["create", "update", "delete", "read"]
}

# Write access to kv/metadata/search/miroir (metadata and versions)
# Required capabilities: create, update, delete, read
path "kv/metadata/search/miroir" {
  capabilities = ["create", "update", "delete", "read"]
}

# List access to parent kv/search path for verification
path "kv/metadata/search" {
  capabilities = ["list"]
}

# Implicit default-deny: all other paths are denied
# This policy follows the principle of least privilege
