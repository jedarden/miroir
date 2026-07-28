# OpenBao Write Policy for Miroir Setup (bf-4h67m)
#
# Write-enabled policy for the miroir setup phase.
# This policy grants write access to the Miroir secret path in OpenBao.
# This policy should only be used during setup and should be revoked afterwards.

# Path: kv/data/search/miroir
# Required capabilities: create, update, delete, read (for setup and management)
path "kv/data/search/miroir" {
  capabilities = ["create", "update", "delete", "read"]
}

# Path: kv/metadata/search/miroir
# Required capabilities: create, update, delete, read (for setup and management)
path "kv/metadata/search/miroir" {
  capabilities = ["create", "update", "delete", "read"]
}

# List access to kv/search (for verification)
path "kv/metadata/search" {
  capabilities = ["list"]
}

# Deny all other paths (default-deny)
# The policy is least-privilege: only the paths above are accessible.
