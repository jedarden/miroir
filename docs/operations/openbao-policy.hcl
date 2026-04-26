# OpenBao Policy for Miroir (plan §9)
#
# Least-privilege policy for the miroir Kubernetes service account.
# This policy grants read-only access to the Miroir secret path in OpenBao.
# Apply this policy to the miroir role in OpenBao.

# Path: kv/data/search/miroir
# Required capabilities: read (for ESO ExternalSecret sync)
path "kv/data/search/miroir" {
  capabilities = ["read"]
}

# Path: kv/metadata/search/miroir
# Required capabilities: read (for ESO to check secret metadata)
path "kv/metadata/search/miroir" {
  capabilities = ["read"]
}

# Deny all other paths (default-deny)
# The policy is least-privilege: only the two paths above are accessible.
