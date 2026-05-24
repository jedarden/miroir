# Runbook: Kill 1 of 2 Miroir replicas

**Scenario ID:** chaos_scenario_3_kill_miroir_replica

## Expected Result

Zero client-visible downtime (if running multiple Miroir replicas behind a load balancer).

## Precondition Check

- Multiple Miroir replicas running (e.g., 2 replicas)
- Load balancer (Kubernetes Service, Traefik, nginx) configured
- Health check endpoint responding on all replicas
- Test index with documents indexed

## Manual Reproduction Steps

```bash
# In a real deployment, Miroir runs as a Kubernetes Deployment/StatefulSet
# This simulates killing a pod

# Check current replicas
kubectl get pods -n miroir

# Kill one Miroir pod (simulate crash)
kubectl delete pod miroir-0 -n miroir

# Immediately run searches - should succeed
# Kubernetes Service automatically routes to surviving replica
curl -X POST 'http://miroir-service.miroir.svc.cluster.local:7700/indexes/test/search' \
  -H 'Authorization: Bearer $MASTER_KEY' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "test"}'

# Watch pod replacement
kubectl get pods -n miroir -w

# After new pod is ready, verify it's receiving traffic
kubectl port-forward miroir-1 -n miroir 7700:7700
curl http://localhost:7700/health
```

## Expected Observables

### Metrics

- `miroir_up` - Drops to 0 for killed replica, recovers when pod replaced
- `kiowala_request_duration_seconds` - May spike briefly during failover
- `kiowala_requests_total{pod="miroir-0"}` - Drops to zero
- `kiowala_requests_total{pod="miroir-1"}` - Increases (takes 100% of load)

### Kubernetes

- Pod `miroir-0` shows `Terminating` then disappears
- New pod `miroir-2` (or reusing `miroir-0`) appears and goes `Pending` → `Running` → `Ready`
- Service endpoints list updates automatically

### Client Errors

- Zero search failures (if health check is properly configured)
- Brief latency spike during failover (< 1s typically)
- No `X-Miroir-Degraded` header (backend nodes are healthy)

## Recovery Procedure

```bash
# Kubernetes automatically restarts the pod
# Verify the new pod is healthy
kubectl get pods -n miroir
kubectl describe pod miroir-2 -n miroir

# Check logs if pod fails to start
kubectl logs miroir-2 -n miroir

# Verify service endpoints
kubectl get endpoints miroir-service -n miroir
```

## How This Differs on Single Miroir Instance

With only one Miroir instance:

- Killing the instance causes total outage
- No failover until instance restarts
- Clients see connection errors
- This is why HA mode (2+ replicas) is recommended for production

## Notes

- Miroir itself is stateless in the request path
- All state is in the backend Meilisearch nodes
- Multiple Miroir replicas share the same backend cluster
- Health check at `/health` is critical for load balancer failover
- Consider using readiness/liveness probes in Kubernetes:
  ```yaml
  livenessProbe:
    httpGet:
      path: /health
      port: 7700
    initialDelaySeconds: 10
    periodSeconds: 5
  readinessProbe:
    httpGet:
      path: /health
      port: 7700
    initialDelaySeconds: 5
    periodSeconds: 2
  ```
