# OpenBao least-privilege policy for Miroir (plan §9)
#
# Grants read-only access to the Miroir secret path in KV v2.
# Used by the External Secrets Operator ClusterSecretStore via
# the AppRole or Kubernetes auth method configured for the miroir role.
#
# Install:
#   bao policy write miroir k8s/openbao-policy.hcl
#
# Then assign to the auth role:
#   bao write auth/approle/role/miroir policies=miroir
#   # or for Kubernetes auth:
#   bao write auth/kubernetes/role/miroir policies=miroir
#
# The ESO ClusterSecretStore references this role via its auth configuration.

# Read secret values (KV v2 data path)
path "kv/data/search/miroir" {
  capabilities = ["read"]
}

# Read secret metadata (version info for change detection)
path "kv/metadata/search/miroir" {
  capabilities = ["read"]
}
