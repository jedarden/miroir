#!/bin/bash
set -e

# This script requires OpenBao admin access to populate secrets
# It should be run from a location that can access OpenBao with admin credentials

BAO_ADDR="${BAO_ADDR:-http://openbao-ardenone-cluster.openbao.svc.cluster.local:8200}"

echo "Using OpenBao address: $BAO_ADDR"

# Function to generate random secret
generate_secret() {
  openssl rand -base64 32
}

# Function to populate secrets for a namespace
populate_secrets() {
  NAMESPACE=$1
  echo "Populating secrets for ${NAMESPACE}..."

  # Check if secret already exists
  if bao kv get secret/ardenone-cluster/${NAMESPACE}/keys 2>/dev/null; then
    echo "Secret secret/ardenone-cluster/${NAMESPACE}/keys already exists, updating..."
    bao kv patch secret/ardenone-cluster/${NAMESPACE}/keys \
      masterKey="$(generate_secret)" \
      nodeMasterKey="$(generate_secret)" \
      adminApiKey="$(generate_secret)" \
      adminSessionSealKey="$(openssl rand -base64 64)" \
      searchUiJwtSecret="$(openssl rand -base64 64)" \
      searchUiSharedKey="$(generate_secret)" \
      redis-password="$(generate_secret)"
  else
    echo "Creating new secret secret/ardenone-cluster/${NAMESPACE}/keys..."
    bao kv put secret/ardenone-cluster/${NAMESPACE}/keys \
      masterKey="$(generate_secret)" \
      nodeMasterKey="$(generate_secret)" \
      adminApiKey="$(generate_secret)" \
      adminSessionSealKey="$(openssl rand -base64 64)" \
      searchUiJwtSecret="$(openssl rand -base64 64)" \
      searchUiJwtSecretPrevious="" \
      searchUiSharedKey="$(generate_secret)" \
      redis-password="$(generate_secret)"
  fi

  echo "Successfully populated secrets for ${NAMESPACE}"

  # Verify the secret
  echo "Verifying secret secret/ardenone-cluster/${NAMESPACE}/keys..."
  bao kv get secret/ardenone-cluster/${NAMESPACE}/keys
}

# Check authentication
echo "Checking OpenBao authentication..."
if ! bao status &>/dev/null; then
  echo "Error: Cannot authenticate to OpenBao at $BAO_ADDR"
  echo "Please ensure:"
  echo "  1. OpenBao is accessible"
  echo "  2. BAO_ADDR is set correctly"
  echo "  3. BAO_TOKEN is set with admin privileges"
  exit 1
fi

# Populate secrets for both namespaces
populate_secrets "miroir"
populate_secrets "miroir-dev"

echo ""
echo "=========================================="
echo "All secrets populated successfully!"
echo "=========================================="
echo ""
echo "The ExternalSecrets should sync automatically within their refreshInterval (1h)."
echo "To force immediate sync, you can delete the ExternalSecrets and let ESO recreate them."
