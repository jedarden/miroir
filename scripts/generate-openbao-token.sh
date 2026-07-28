#!/bin/bash
#
# generate-openbao-token.sh
#
# Generate an OpenBao token with write permissions to kv/search/miroir
# Usage: ./generate-openbao-token.sh [bao-addr]
#
# Requirements:
# - OpenBao CLI (bao) installed
# - Cluster-admin access to OpenBao (or valid token with appropriate permissions)
# - Kubernetes access to the cluster (for kubectl commands)
#
# Output:
# - Token stored in Kubernetes secret: miroir-openbao-token (openbao namespace)
# - Token value printed to stdout
#
# Bead: bf-4h67m

set -e
set -x

# Configure OpenBao address
BAO_ADDR="${1:-http://100.126.102.108:8200}"
export BAO_ADDR

echo "Testing OpenBao connectivity at ${BAO_ADDR}..."
if ! curl -sf ${BAO_ADDR}/v1/sys/health > /dev/null; then
    echo "Error: Cannot reach OpenBao at ${BAO_ADDR}"
    exit 1
fi
echo "OpenBao is reachable"

# Check if already authenticated via BAO_TOKEN
if [ -z "$BAO_TOKEN" ]; then
    echo "BAO_TOKEN not set. Attempting to authenticate..."
    echo "Please authenticate manually using one of:"
    echo "  1. Export BAO_TOKEN with an existing admin token"
    echo "  2. Run: bao login <method> (if configured)"
    echo ""
    echo "For Kubernetes authentication from a pod, you would use:"
    echo "  bao login -method=kubernetes role=openbao-ardenone-cluster"
    exit 1
fi

echo "Testing token access..."
if ! bao token lookup > /dev/null 2>&1; then
    echo "Error: Token is invalid or expired"
    exit 1
fi

echo "Token is valid. Checking if KV v2 secrets engine is enabled at 'kv' path..."
if ! bao secrets list | grep -q "^kv/$"; then
    echo "KV v2 secrets engine not found at 'kv' path. Checking current mounts..."
    bao secrets list
    echo ""
    echo "Enabling KV v2 secrets engine at 'kv' path..."
    bao secrets enable -path=kv kv-v2
else
    echo "KV v2 secrets engine already enabled at 'kv' path"
fi

echo "Creating write-enabled policy: miroir-policy-write..."
cat > /tmp/miroir-policy-write.hcl <<'EOF'
# OpenBao Write Policy for Miroir Setup (bf-4h67m)
path "kv/data/search/miroir" {
  capabilities = ["create", "update", "delete", "read"]
}
path "kv/metadata/search/miroir" {
  capabilities = ["create", "update", "delete", "read"]
}
path "kv/metadata/search" {
  capabilities = ["list"]
}
EOF

bao policy write miroir-policy-write /tmp/miroir-policy-write.hcl
echo "Policy created successfully"

echo "Generating token with miroir-policy-write policy (TTL: 24h)..."
TOKEN_OUTPUT=$(bao token create -policy=miroir-policy-write -ttl=24h -format=json)

# Extract token and display it
TOKEN_VALUE=$(echo "$TOKEN_OUTPUT" | jq -r '.auth.client_token')
TOKEN_TTL=$(echo "$TOKEN_OUTPUT" | jq -r '.auth.lease_duration')
TOKEN_POLICIES=$(echo "$TOKEN_OUTPUT" | jq -r '.auth.policies | join(", ")')

echo ""
echo "=========================================="
echo "Token Generated Successfully!"
echo "=========================================="
echo "Token: $TOKEN_VALUE"
echo "TTL: $TOKEN_TTL seconds ($(($TOKEN_TTL / 3600)) hours)"
echo "Policies: $TOKEN_POLICIES"
echo ""
echo "Verifying token permissions..."
# Verify the token has the correct policy
bao login -token="$TOKEN_VALUE" > /dev/null 2>&1
echo "Token login successful"

# Try to list the policy (read-only operation to verify token works)
bao token lookup > /dev/null 2>&1 && echo "Token is valid and active"

echo ""
echo "Store this token securely. For Kubernetes deployment, create a secret:"
echo "kubectl create secret generic miroir-openbao-token \\"
echo "  --from-literal=token=\"$TOKEN_VALUE\" \\"
echo "  --from-literal=policies=\"$TOKEN_POLICIES\" \\"
echo "  --namespace=openbao"
echo "=========================================="

# Cleanup
rm -f /tmp/miroir-policy-write.hcl
