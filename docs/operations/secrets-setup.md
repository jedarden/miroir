# Miroir Secrets Setup Guide (plan §9)

This guide covers setting up Miroir secrets with OpenBao and External Secrets Operator (ESO).

## Prerequisites

- OpenBao deployed in the cluster with KV v2 secrets engine enabled
- External Secrets Operator installed
- Kubernetes cluster with miroir namespace

## Secret Inventory (plan §9)

| Secret | OpenBao Key | ESO Target | Rotation |
|--------|-------------|------------|----------|
| masterKey | master_key | miroir-secret | Manual |
| nodeMasterKey | node_master_key | miroir-secret | Zero-downtime |
| adminApiKey | admin_api_key | miroir-secret | Manual |
| adminSessionSealKey | admin_session_seal_key | miroir-secret | Manual |
| searchUiJwtSecret | search_ui_jwt_secret | miroir-secret | Zero-downtime |
| searchUiJwtSecretPrevious | search_ui_jwt_secret_previous | miroir-secret | Overlap only |
| searchUiSharedKey | search_ui_shared_key | miroir-secret | Manual |
| redis-password | redis_password | miroir-secret | Manual |

## Step 1: Create OpenBao Policy

Create the least-privilege policy for Miroir:

```bash
# Apply the policy to OpenBao
bao policy write miroir-policy docs/operations/openbao-policy.hcl
```

Or via the OpenBao UI:
1. Navigate to Policies
2. Create new policy named `miroir-policy`
3. Paste the contents of `docs/operations/openbao-policy.hcl`

## Step 2: Create OpenBao Role and Token

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

## Step 3: Populate Secrets in OpenBao

Enable KV v2 secrets engine (if not already enabled):

```bash
bao secrets enable -path=kv kv-v2
```

Write the Miroir secrets:

```bash
# Generate secrets (use your preferred method)
bao kv put kv/search/miroir \
  master_key="$(openssl rand -base64 32)" \
  node_master_key="$(openssl rand -base64 32)" \
  admin_api_key="$(openssl rand -base64 32)" \
  admin_session_seal_key="$(openssl rand -base64 64)" \
  search_ui_jwt_secret="$(openssl rand -base64 64)"
```

For shared key mode (if using `search_ui.auth.mode: shared_key`):

```bash
bao kv patch kv/search/miroir \
  search_ui_shared_key="$(openssl rand -base64 32)"
```

For Redis password (if `redis.auth.enabled: true`):

```bash
bao kv patch kv/search/miroir \
  redis_password="$(openssl rand -base64 32)"
```

## Step 4: Configure ESO ClusterSecretStore

Create the ClusterSecretStore for OpenBao:

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

Apply:

```bash
kubectl apply -f - <<EOF
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
EOF
```

## Step 5: Deploy Miroir with ESO Enabled

Update your Helm values:

```yaml
eso:
  enabled: true
  secretPath: "kv/search/miroir"
  includePreviousJwt: false  # Set true during JWT rotation
  includeSharedKey: false    # Set true when using shared_key mode
  includeRedisPassword: false # Set true when Redis auth is enabled

miroir:
  existingSecret: "miroir-secret"  # ESO-managed Secret
```

Deploy:

```bash
helm install miroir ./charts/miroir -f values.yaml
```

## Rotation Procedures

### nodeMasterKey Rotation (Zero-Downtime)

1. Generate new admin-scoped key on each Meilisearch node:
   ```bash
   kubectl exec -it statefulset/meilisearch -- bash -c '
     curl -X POST http://localhost:7700/keys \
       -H "Authorization: Bearer $MEILI_MASTER_KEY" \
       -H "Content-Type: application/json" \
       -d '"'"'{"actions": ["*"], "indexes": ["*"], "description": "miroir node key v2"}'"'"'
   '
   ```

2. Update OpenBao with the new key:
   ```bash
   bao kv patch kv/search/miroir node_master_key="<new-key>"
   ```

3. ESO will sync the update within `refreshInterval` (default 15m)

4. Rolling restart Miroir pods:
   ```bash
   kubectl rollout restart deployment/miroir -n search
   ```

5. Delete old keys from all nodes:
   ```bash
   kubectl exec -it statefulset/meilisearch -- bash -c '
     curl -X DELETE http://localhost:7700/keys/<old-uid> \
       -H "Authorization: Bearer $MEILI_MASTER_KEY"
   '
   ```

### JWT Secret Rotation (Zero-Downtime)

Automated via `miroir-ctl ui rotate-jwt-secret`:

```bash
miroir-ctl ui rotate-jwt-secret \
  --namespace=search \
  --secret-name=miroir-secret \
  --deployment-name=miroir
```

Or use the CronJob (quarterly, suspended by default):

```bash
# Enable the CronJob for automated rotation
kubectl patch cronjob miroir-rotate-jwt -n search -p '{"spec":{"suspend":false}}'

# Trigger manual rotation
kubectl create job --from=cronjob/miroir-rotate-jwt manual-rotation-$(date +%s) -n search
```

## Leak Response

If a secret is leaked, immediate revocation:

1. **JWT secret leaked** - Set `SEARCH_UI_JWT_SECRET_PREVIOUS` to empty string:
   ```bash
   kubectl patch secret miroir-secret -n search -p '{"stringData":{"searchUiJwtSecretPrevious":""}}'
   kubectl rollout restart deployment/miroir -n search
   ```

2. **Admin API key leaked** - Rotate immediately:
   ```bash
   # Generate new key
   bao kv patch kv/search/miroir admin_api_key="$(openssl rand -base64 32)"
   # Wait for ESO sync (or force delete Secret)
   kubectl delete secret miroir-secret -n search
   ```

3. **Node master key leaked** - Follow nodeMasterKey rotation procedure above

## Validation

Verify secrets are loaded correctly:

```bash
# Check ESO sync status
kubectl get externalsecret miroir-eso -n search -o yaml

# Verify Secret exists
kubectl get secret miroir-secret -n search

# Check Miroir pods have secrets
kubectl logs -l app.kubernetes.io/name=miroir -n search --tail=20 | grep -i jwt
```

## Security Notes

- The OpenBao policy is least-privilege: read-only access to `kv/search/miroir`
- Miroir never writes to OpenBao; it only reads via ESO
- All secrets are base64-encoded in Kubernetes Secrets
- Use separate OpenBao policies per namespace/environment
- Rotate keys quarterly or immediately on leak
- Monitor ESO sync errors via Prometheus alerts
