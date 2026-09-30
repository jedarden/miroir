# OpenBao Policy for Miroir (read-only, ESO sync path)
#
# Least-privilege READ-ONLY policy documenting the shape of miroir's secret
# access for External Secrets Operator (ESO). This is the live-path version of
# the plan §9 policy: the live ardenone-cluster cell mounts KV v2 at `secret`
# (it has no `kv` mount — `kv/search/miroir` belonged to the decommissioned
# v1 cell and must never be addressed).
#
# The live cell does not consume this file directly. The actual ESO read
# identity is the declarative `eso-ardenone-cluster` policy (reconciler config
# `k8s/ardenone-cluster/openbao-v2/reconciler-config.yml` in
# jedarden/declarative-config), whose `secret/data/ardenone-cluster/*` read
# stanza this file narrows to exactly the two miroir keys docs.
#
# The matching WRITE-capable policy (`miroir-write`) is declared in the same
# reconciler config; see [`openbao-write-token.md`](openbao-write-token.md)
# for how an operator obtains and verifies a token bound to it.

# Remote keys synced by ExternalSecrets `miroir-keys` (ns `miroir`) and
# `miroir-dev-keys` (ns `miroir-dev`) via ClusterSecretStore `openbao-v2`.
# Properties per doc: masterKey, nodeMasterKey, adminApiKey.
path "secret/data/ardenone-cluster/miroir/keys" {
  capabilities = ["read"]
}

path "secret/data/ardenone-cluster/miroir-dev/keys" {
  capabilities = ["read"]
}

# KV v2 metadata reads return version numbers and timestamps, never values —
# enough for `bao kv metadata get` and CAS-aware sync checks.
path "secret/metadata/ardenone-cluster/miroir/keys" {
  capabilities = ["read"]
}

path "secret/metadata/ardenone-cluster/miroir-dev/keys" {
  capabilities = ["read"]
}

# Deny all other paths (default-deny; stated for clarity). No write, delete,
# list, or sudo capability: miroir workloads never write to OpenBao — they
# read through ESO only, and writes belong to the operator's `miroir-write`
# token (see openbao-write-token.md).
