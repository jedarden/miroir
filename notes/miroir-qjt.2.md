# P8.2: Helm Chart Structure Completion

## Task
Scaffold `charts/miroir/` per plan §6 with dev defaults and production override guidance.

## Status: Already Complete

The Helm chart structure was already in place with all required files:

### Chart Structure
- `Chart.yaml` - API v2, description, keywords, sources
- `values.yaml` - Dev defaults (replicas=1, RF=1, RG=1, sqlite, redis disabled)
- `values.schema.json` - JSON schema validation
- `templates/_helpers.tpl` - Node list DNS generation helpers
- `templates/miroir-deployment.yaml` - Miroir proxy deployment
- `templates/miroir-service.yaml` - ClusterIP service
- `templates/miroir-headless.yaml` - Headless service for peer discovery
- `templates/miroir-configmap.yaml` - Miroir YAML config
- `templates/miroir-secret.yaml` - Master keys and API keys
- `templates/miroir-hpa.yaml` - Horizontal Pod Autoscaler
- `templates/miroir-pvc.yaml` - CDC buffer PVC (conditional)
- `templates/meilisearch-statefulset.yaml` - Meilisearch StatefulSet
- `templates/meilisearch-service.yaml` - Meilisearch service
- `templates/redis-deployment.yaml` - Redis deployment (optional)
- `templates/serviceaccount.yaml` - ServiceAccount
- `templates/NOTES.txt` - Post-install instructions with prod override guidance
- `tests/connection-test.yaml` - Helm test pod

### Dev Defaults (values.yaml)
- `miroir.replicas: 1`
- `miroir.shards: 64`
- `miroir.replicationFactor: 1`
- `miroir.replicaGroups: 1`
- `miroir.hpa.enabled: false`
- `meilisearch.replicas: 2` (1 group × 2 nodes)
- `meilisearch.nodesPerGroup: 2`
- `redis.enabled: false`
- `taskStore.backend: sqlite`

### Production Override Guidance (NOTES.txt)
```
!!! PRODUCTION UPGRADE PATH !!!
These defaults are for dev/CI (single-pod evaluation). For production, override:
  miroir.replicas=2+
  miroir.replicationFactor=2
  miroir.replicaGroups=2
  taskStore.backend=redis
  redis.enabled=true
  hpa.enabled=true
```

### Node List DNS Generation (_helpers.tpl)
- `miroir.meilisearchNodeAddress` - Generates DNS for a single node
- `miroir.meilisearchNodeList` - Generates full node list for ConfigMap
- Format: `http://<release>-meili-<n>.<release>-meili-headless.<namespace>.svc.cluster.local:7700`

### Validation
Chart is packaged and validated in the `miroir-release` workflow (k8s/argo-workflows/miroir-release.yaml) using `helm package`.
