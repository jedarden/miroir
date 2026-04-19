# Home-Grown HA for Meilisearch (Community License)

**Research date:** April 2026  
**Meilisearch version context:** v1.37.x (latest stable at time of writing)  
**License context:** Community Edition (MIT) — Enterprise Edition (BUSL 1.1) locked behind commercial agreement

---

## 1. Community License Constraints

### What Changed and When

Meilisearch introduced a dual-license model in **v1.19.0** (August 2025). The core engine remains MIT-licensed; new enterprise features ship under the **Business Source License 1.1 (BUSL)**.

BUSL allows:
- Free use in non-production environments (dev, staging, testing)
- Open inspection of the source code
- Community contributions

BUSL prohibits (without a commercial license agreement):
- Production use of gated features

### What Is Gated

As of v1.37.0, only **one feature class** is formally behind the enterprise gate:

| Feature | CE (MIT) | EE (BUSL) |
|---------|----------|-----------|
| Full-text search, filters, facets, ranking | Yes | Yes |
| Snapshots (`.ms.snapshot`) | Yes | Yes |
| Dumps (`.dump`) | Yes | Yes |
| `--experimental-replication-parameters` (v1.7+) | Yes | Yes |
| Horizontal **sharding** (v1.19+) | No | **EE only** |
| **Replication** (v1.37+) | No | **EE only** |
| Fine-grained access controls (roadmap) | No | **EE only** |
| Analytics (roadmap) | No | **EE only** |

The practical bottom line: **you cannot run Meilisearch's native replication or sharding in production without an Enterprise license.** The network API that underpins both features — where instances coordinate as a topology — is gated.

### What `--experimental-replication-parameters` Actually Is

This flag (introduced in **v1.7.0**, March 2024) is **not** replication. It is a low-level primitive that modifies task-queue behavior to facilitate building external replication systems:

1. **Disables auto-purge** of completed tasks (you manage deletion; hard cap at 10 GiB before writes halt)
2. **Custom task IDs** via `TaskId:` header (must be monotonically greater than max seen)
3. **Dry-run** via `DryRun: true` header (validates a task without executing it)

This flag is MIT-licensed and available in the community edition. It is the building block that makes several home-grown approaches possible.

### What the Official Helm Chart Does

The official `meilisearch-kubernetes` Helm chart deploys a single **StatefulSet** with `replicas: 1`, `accessMode: ReadWriteOnce`, and no built-in replication logic. Running it with `replicas: 2` produces two independent instances with separate storage — they do not synchronize. The chart offers no HA primitives.

---

## 2. The HA Problem

Without native clustering, a single Meilisearch pod has these failure characteristics:

### Single Point of Failure (SPOF)
One pod. One PVC. Pod crash or node eviction → search unavailable. K8s will reschedule (typically 30–60 s), but during that window all search requests fail. With a `ReadWriteOnce` PVC, the rescheduled pod may also fail to start if the old PVC is still releasing from the previous node.

### No Automatic Failover
Meilisearch has no built-in leader election or automatic promotion. If the node dies, nothing promotes a standby. The team explicitly stated in Discussion #617 (2023): _"the current design lacks automatic leader re-election if the leader fails."_ This remains true as of the community edition.

### Single Writer, No Read Scaling
All reads and writes go to the same instance. Under high read load, you cannot scale horizontally — adding a second pod creates an independent, unsynchronized index, not a replica.

### Data Consistency Risks
LMDB (the storage engine) is ACID-transactional, but consistency guarantees apply within a single process. If you naively mount the same directory from two running processes, LMDB will corrupt. ReadWriteMany shared storage is therefore only safe with strict mutual exclusion enforced externally.

### LMDB Is Not Portable Hot-Copy Safe (Without Meilisearch's Own Snapshot API)
The raw `data.ms/` directory contains memory-mapped LMDB files. Copying them with `rsync` while Meilisearch is writing is not safe — you can catch files mid-transaction. LMDB does expose `mdb_env_copy2()` (its internal consistent-copy function), and Meilisearch's snapshot feature uses exactly this. **Snapshots are the only supported hot-copy mechanism.**

### RPO and RTO Baseline (No HA)
- **RPO (data loss on failure):** Up to the interval since the last snapshot, or unbounded if snapshots aren't scheduled
- **RTO (downtime):** 30–120 s for K8s pod reschedule + PVC reattach + Meilisearch startup (faster from snapshot than cold start, still not zero)

---

## 3. Home-Grown Approaches

### 3.1 Leader/Standby with Snapshotting

**How it works:**
- Primary runs with `--schedule-snapshot=300` (every 5 minutes, or tunable).
- Snapshots land in a shared volume or are uploaded to object storage (S3/MinIO/B2) by a sidecar.
- Standby pod runs in "hot standby" mode: it periodically downloads the latest snapshot and imports it. On startup it uses `--import-snapshot` to bootstrap. While in standby, it serves read traffic from its last-imported state.
- A liveness/readiness probe difference marks the standby as not-ready for writes.
- On primary failure: a controller (or manual op) deletes the primary PVC, patches the standby's labels, and the standby transitions to primary. If the standby's data directory is already populated from the last snapshot import, it starts in seconds; otherwise it imports the snapshot from object storage.

**Trade-offs:**

| Dimension | Detail |
|-----------|--------|
| RPO | Equal to snapshot interval (minimum ~60 s practical; default is 24 h if not configured) |
| RTO | 30–120 s (pod promotion + snapshot import if needed) |
| Read scaling | Standby can serve slightly stale reads (bounded by snapshot interval) |
| Complexity | Medium — sidecar upload logic, snapshot import init container, promotion controller or manual runbook |
| Data consistency | Standby is snapshot-consistent; writes to primary after last snapshot are lost on failover |
| K8s fit | Good — StatefulSet + sidecar + init container pattern is idiomatic |

**Limitations:**
- Snapshots overwrite themselves by default (`--schedule-snapshot` keeps only the latest); you must copy them out before they rotate.
- Import blocks all HTTP traffic during restore — standby is briefly unavailable.
- Requires careful pod lifecycle management to prevent split-brain on promotion (old primary must be completely stopped before standby is promoted).

**Snapshot upload sidecar pattern:**
```
meilisearch container: --schedule-snapshot=300 --snapshot-dir=/snapshots
sidecar container: watches /snapshots, uploads to S3 on mtime change
init container (standby): downloads latest snapshot from S3, places at /meili_data/snapshots/data.ms.snapshot
```

---

### 3.2 Shared Storage HA (ReadWriteMany + Lease)

**How it works:**
- Both pods mount the **same PVC** via `ReadWriteMany` (NFS, Longhorn RWX, Garage NFS gateway, or OpenEBS NFS provisioner).
- Only one pod is active at a time, enforced by a **Kubernetes Lease** object (`coordination.k8s.io/v1`). A lease sidecar renews the lease every N seconds; if it fails to renew, it sends `SIGTERM` to Meilisearch and releases the lease.
- The standby pod polls the Lease object. When the lease goes unclaimed, it acquires it and starts Meilisearch.
- Because the data is on shared storage, the promoted pod sees the exact state of the primary up to its last fsync.

**Trade-offs:**

| Dimension | Detail |
|-----------|--------|
| RPO | Near-zero (shared storage, no replication lag) |
| RTO | Lease TTL + Meilisearch startup (typically 15–30 s for small indexes, longer for large ones due to LMDB memory mapping) |
| Read scaling | None — still single active writer |
| Complexity | Medium-high — RWX storage provisioning is the hard part; lease sidecar is straightforward |
| Data consistency | Excellent within a single node transition; risk on split-brain if network partitions prevent lease release |
| K8s fit | Reasonable, but RWX storage has real operational overhead (Longhorn RWX uses an NFS server pod per PVC) |

**Limitations:**
- Longhorn RWX has known performance overhead (~30–40% vs RWO) and places an NFS server pod per PVC.
- Garage's S3/NFS gateway adds latency; not designed for LMDB's random-write pattern.
- Network partition can cause both pods to believe they hold the lease (split-brain) — requires careful lease TTL and fencing.
- On Azure/GCP/DO, default storage classes don't support RWX; you must bring your own provisioner.
- LMDB on NFS has historically had locking issues; need to ensure `MDB_NOLOCK` is not set and NFS lock daemon is healthy.

**Gotcha:** Running two Meilisearch processes concurrently against the same LMDB directory will corrupt it. The lease must have a hard guarantee that only one is running before the second starts. A pre-`exec` check against the Lease API from the init container helps.

---

### 3.3 Read Replica Fan-Out

**How it works:**
- One write pod handles all indexing.
- One or more read-replica pods run from copies of the index that are periodically refreshed.
- Replication mechanism: scheduled `rsync` of the `data.ms/` directory from primary to replicas, **triggered only when Meilisearch is idle** (no active tasks in the task queue — poll `GET /tasks?statuses=enqueued,processing` and wait for zero). This is the only safe window to copy LMDB files via rsync.
- Alternatively: primary uses `--schedule-snapshot`, replicas have an init container that downloads the snapshot and starts fresh.
- A reverse proxy (Nginx, Envoy, Traefik) or a K8s Service routing rule splits traffic: `POST /indexes/*/documents` and other write routes → write pod; `POST /indexes/*/search` and `GET` → replica pool.

**Trade-offs:**

| Dimension | Detail |
|-----------|--------|
| RPO | Equal to replication interval (snapshot-based: 5–30 min typical) |
| RTO | On primary failure: promote a replica (loses writes since last sync). On replica failure: K8s reschedules, replica re-downloads snapshot |
| Read scaling | Good — add replicas freely; each is an independent read pod |
| Complexity | High — traffic split at proxy, rsync timing coordination, replica lifecycle, primary failover runbook |
| Data consistency | Replicas serve stale data (bounded by sync interval) |
| K8s fit | Moderate — Service + proxy works natively; rsync coordination is custom |

**Replica sync approach options:**

1. **Snapshot-based (recommended):** Primary writes snapshots every N minutes to a shared volume or object store. Replicas run a sidecar that watches for new snapshots and does a rolling restart to import: stop Meilisearch → clear `/meili_data` → restart with `--import-snapshot`.

2. **Rsync-based (risky):** Only safe when primary task queue is empty and Meilisearch has flushed to disk. LMDB's MVCC means readers never block writers, but the files still need to be in a consistent MVCC snapshot state. Using Meilisearch's built-in snapshot mechanism is strongly preferred over raw rsync.

**Proxy config sketch (Traefik):**
```yaml
# Route by method + path: writes to primary, reads to replica pool
# IngressRoute with two services: meili-primary and meili-replicas
# Use middleware to match write HTTP methods (POST/PUT/PATCH/DELETE) on index mutation paths
```

**Limitations:**
- Primary remains a SPOF for writes. Failing over writes to a replica requires promoting it, which means replicas must be able to accept writes (they can — they're full Meilisearch instances, just not receiving them). The promotion runbook: update the proxy config, let the replica catch up, accept write divergence or re-index.
- Cannot do zero-downtime promotion without write loss or dual-write complexity.

---

### 3.4 Event-Sourced Re-Indexing

**How it works:**
- Meilisearch is treated as a **derived, rebuildable view** — not the source of truth. The source of truth is a message queue (NATS JetStream, Kafka/Redpanda, Redis Streams).
- Every document write your application makes is published to the queue first, then consumed by a single Meilisearch indexing worker.
- On failover: spin up a fresh Meilisearch instance, replay events from the queue (or from a snapshot offset), and it catches up.
- For faster recovery, combine with periodic snapshots: store the snapshot alongside the queue offset at snapshot time. On failover, import the snapshot and replay only events since the snapshot offset.

**Trade-offs:**

| Dimension | Detail |
|-----------|--------|
| RPO | Zero (if queue is durable) — no data is ever lost because the queue is authoritative |
| RTO | Minutes to hours, depending on corpus size and indexing speed. With snapshot+offset: minutes |
| Read scaling | Trivially spin up read replicas from snapshots; all catch up from same queue |
| Complexity | High — requires queue infrastructure, queue-aware indexing worker, offset tracking, snapshot-with-offset discipline |
| Data consistency | Eventual — replicas may lag while catching up from queue |
| K8s fit | Good if you already have NATS/Kafka; poor if you don't (adds substantial infra) |

**Practical queue sizing:**
- NATS JetStream: simplest to operate in K8s, built-in persistence, subject retention by time or bytes
- Redis Streams: already present in many stacks; limited retention without careful trimming
- Kafka/Redpanda: most operationally complex; best for high-volume; overkill for small search corpora

**When this is the right answer:** If your application already has an event bus, or if your data volume makes snapshot-based recovery too slow (>30 min to re-index from scratch), this is the most principled approach. It turns the HA problem into a rebuild problem, which is tractable.

**When to avoid:** If Meilisearch receives documents that don't exist elsewhere (i.e., it IS the source of truth), this approach doesn't apply without building a separate event log.

---

### 3.5 Dual-Write with Leader Election

**How it works:**
- Two independent Meilisearch pods, each with its own PVC.
- The **application** (or a proxy sidecar) writes every document mutation to **both** instances simultaneously (or fan-out via a small write-proxy service).
- Leader election (K8s Lease, or Redis `SET NX PX`, or etcd lock) determines which instance is the "primary" for reads.
- On pod failure: the surviving instance already has all the data (assuming both writes succeeded); it wins the election and serves all traffic.
- Uses `--experimental-replication-parameters` to ensure task IDs are sequenced consistently, preventing ID collisions across instances.

**Trade-offs:**

| Dimension | Detail |
|-----------|--------|
| RPO | Near-zero if both writes succeed before ack; writes that only reached one instance before failure are lost |
| RTO | Lease TTL (typically 10–30 s) + routing failover |
| Read scaling | Both instances can serve reads simultaneously (two separate read pools) |
| Complexity | Very high — write proxy or application-level fan-out, task ID sequencing, split-brain on partial write failure |
| Data consistency | Divergence risk if one write fails mid-batch and is not retried correctly |
| K8s fit | Moderate — K8s Lease is native; write proxy is a small custom service |

**The partial-write problem:** If your write proxy sends a batch to both instances and one times out, the instances are now diverged. Reconciliation requires either a re-index trigger or keeping a write log. Without `--experimental-replication-parameters` and careful task ID management, the instances will assign different task IDs to the same logical operation, making reconciliation harder.

**Sequence with `--experimental-replication-parameters`:**
1. Write proxy receives indexing request
2. Issues `DryRun: true` to instance A to get the next valid task ID
3. Issues the same request with explicit `TaskId:` to both A and B
4. Waits for both to ACK task enqueued (not completed)
5. Returns success; async processing on both instances

This is the most faithful community approximation of what the enterprise replication layer does internally, but the failure modes are subtle.

**Limitations:**
- Write proxy is a SPOF unless it itself is HA.
- Two PVCs means 2x storage cost and double indexing CPU.
- No guarantee of byte-level identical indexes (settings changes, index drops/creates need special handling).
- Task ID sequencing is manual and fragile under concurrent writers.

---

### 3.6 Velero/Volume Snapshot-Based Recovery

**How it works:**
- Not true HA — this is a fast-RTO disaster recovery approach.
- Velero takes scheduled volume snapshots of the Meilisearch PVC via CSI VolumeSnapshot (works with Longhorn, AWS EBS, GCE PD, etc.).
- On failure: restore the PVC from snapshot, restart the pod. Meilisearch starts from the snapshot state immediately (no re-indexing required, unlike dump restore).
- Combine with scheduled Meilisearch snapshots for belt-and-suspenders: Velero captures the full PVC; Meilisearch snapshot is a portable `.ms.snapshot` file.

**Trade-offs:**

| Dimension | Detail |
|-----------|--------|
| RPO | Equal to snapshot schedule (hourly is practical with CSI thin-clone snapshots) |
| RTO | 2–10 minutes (PVC restore + pod reschedule). CSI thin-clone snapshots restore nearly instantly; full-copy restores take longer |
| Read scaling | None |
| Complexity | Low — Velero is well-understood; no custom code |
| Data consistency | PVC snapshot captures a crash-consistent state; Meilisearch's LMDB will replay its WAL on next start |
| K8s fit | Excellent — fully GitOps-compatible via Schedule CRD |

**Velero Schedule example:**
```yaml
apiVersion: velero.io/v1
kind: Schedule
metadata:
  name: meilisearch-hourly
  namespace: velero
spec:
  schedule: "0 * * * *"
  template:
    includedNamespaces: [search]
    labelSelector:
      matchLabels:
        app: meilisearch
    snapshotVolumes: true
    ttl: 72h0m0s
```

**Limitations:**
- Restoring from Velero PVC snapshot requires the pod to be down — this is a recovery operation, not a failover.
- RTO is measured in minutes, not seconds.
- If you need sub-minute failover, this approach alone is insufficient.
- Longhorn's CSI VolumeSnapshot creates an internal thin clone; restore time is fast but dependent on Longhorn's snapshot reconciliation loop.

---

## 4. Recommended Approach

### Recommendation: Leader/Standby with Snapshotting + Object Storage

For a K8s homelab environment running GitOps/ArgoCD, the **leader/standby with snapshot upload to object storage** approach offers the best balance of:

- **Operational simplicity** — uses only Meilisearch's native snapshot mechanism (no external queue, no custom replication proxy)
- **Correctness** — snapshots are the only officially supported consistent-copy mechanism in CE
- **K8s native fit** — init containers, sidecar containers, and StatefulSets are standard patterns; object storage (S3/MinIO/B2) is already present in most homelabs
- **Acceptable RTO/RPO** for most search use cases — 5 min RPO, ~60–90 s RTO
- **GitOps-compatible** — all components declaratively defined; no stateful external coordinators

**Why not the others:**

| Approach | Rejection reason |
|----------|-----------------|
| Shared storage (RWX) | RWX on Longhorn/NFS adds operational overhead and latency; LMDB-on-NFS has historical reliability issues; split-brain fencing is hard |
| Read replica fan-out | Primary still a write SPOF; snapshot-import replica restart is disruptive; adds proxy complexity for marginal read scaling gain |
| Event sourcing | Excellent approach but requires queue infrastructure; significant complexity if queue doesn't already exist |
| Dual-write + leader election | Highest complexity, most failure modes; write proxy becomes a SPOF; task ID sequencing is fragile |
| Velero-only | RTO in minutes is acceptable for DR but not for an HA story |

**When to reconsider:** If your application already runs NATS JetStream or Kafka, the event-sourced approach becomes significantly more attractive — the infra cost is already paid. If your corpus is small (<1 GB) and re-indexing takes <5 minutes, event sourcing with full replay (no snapshot offset needed) is the simplest correct solution.

---

## 5. Implementation Sketch

### Architecture Overview

```
                        ┌─────────────────────────────────────┐
                        │         Kubernetes Namespace         │
                        │                                      │
  Clients ──────────────►  Service: meili (ClusterIP/Ingress)  │
                        │          │                           │
                        │    ┌─────▼──────┐                   │
                        │    │  Primary   │ StatefulSet        │
                        │    │ Pod (0)    │ meili-0            │
                        │    │            │                    │
                        │    │ meilisearch│──── PVC: meili-0   │
                        │    │ :7700      │     (RWO, 10Gi)    │
                        │    │            │                    │
                        │    │ sidecar:   │                    │
                        │    │ snap-upload│──── S3/MinIO       │
                        │    └────────────┘         │         │
                        │                           │         │
                        │    ┌────────────┐         │         │
                        │    │  Standby   │ Deployment        │
                        │    │ Pod        │ (or StatefulSet-1) │
                        │    │            │                    │
                        │    │ init:      │◄─── S3/MinIO       │
                        │    │ snap-fetch │                    │
                        │    │            │                    │
                        │    │ meilisearch│──── PVC: meili-1   │
                        │    │ :7700      │     (RWO, 10Gi)    │
                        │    │            │                    │
                        │    │ (ready for │                    │
                        │    │  reads,    │                    │
                        │    │  not write │                    │
                        │    │  endpoint) │                    │
                        │    └────────────┘                   │
                        └─────────────────────────────────────┘
```

### K8s Resources

#### 1. Primary StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: meilisearch-primary
  namespace: search
spec:
  serviceName: meilisearch-primary
  replicas: 1
  selector:
    matchLabels:
      app: meilisearch
      role: primary
  template:
    metadata:
      labels:
        app: meilisearch
        role: primary
    spec:
      containers:
      - name: meilisearch
        image: getmeili/meilisearch:v1.37.0
        args:
        - "--db-path=/meili_data/data.ms"
        - "--snapshot-dir=/meili_data/snapshots"
        - "--schedule-snapshot=300"        # snapshot every 5 minutes
        - "--master-key=$(MEILI_MASTER_KEY)"
        env:
        - name: MEILI_MASTER_KEY
          valueFrom:
            secretKeyRef:
              name: meilisearch-secrets
              key: master-key
        ports:
        - containerPort: 7700
        volumeMounts:
        - name: data
          mountPath: /meili_data
        livenessProbe:
          httpGet:
            path: /health
            port: 7700
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 7700

      - name: snapshot-uploader
        image: amazon/aws-cli:2.15.0   # or rclone/rclone for S3-compatible
        command: ["/bin/sh", "-c"]
        args:
        - |
          LAST=""
          while true; do
            SNAP=$(ls -t /meili_data/snapshots/*.ms.snapshot 2>/dev/null | head -1)
            if [ -n "$SNAP" ] && [ "$SNAP" != "$LAST" ]; then
              echo "Uploading $SNAP..."
              aws s3 cp "$SNAP" "s3://${S3_BUCKET}/meilisearch/latest.ms.snapshot" \
                --endpoint-url="${S3_ENDPOINT}"
              LAST="$SNAP"
            fi
            sleep 30
          done
        env:
        - name: S3_BUCKET
          value: "your-bucket"
        - name: S3_ENDPOINT
          value: "https://s3.example.com"
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: meilisearch-secrets
              key: s3-access-key
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: meilisearch-secrets
              key: s3-secret-key
        volumeMounts:
        - name: data
          mountPath: /meili_data

  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ReadWriteOnce]
      resources:
        requests:
          storage: 10Gi
```

#### 2. Standby Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: meilisearch-standby
  namespace: search
spec:
  replicas: 1
  selector:
    matchLabels:
      app: meilisearch
      role: standby
  template:
    metadata:
      labels:
        app: meilisearch
        role: standby
    spec:
      initContainers:
      - name: snapshot-fetch
        image: amazon/aws-cli:2.15.0
        command: ["/bin/sh", "-c"]
        args:
        - |
          mkdir -p /meili_data/snapshots
          # Only fetch if no existing database (avoid clobbering valid state)
          if [ ! -f /meili_data/data.ms/VERSION ]; then
            echo "Fetching snapshot from S3..."
            aws s3 cp "s3://${S3_BUCKET}/meilisearch/latest.ms.snapshot" \
              /meili_data/snapshots/data.ms.snapshot \
              --endpoint-url="${S3_ENDPOINT}" || echo "No snapshot found, starting fresh"
          fi
        env:
        - name: S3_BUCKET
          value: "your-bucket"
        - name: S3_ENDPOINT
          value: "https://s3.example.com"
        # ... credentials ...
        volumeMounts:
        - name: data
          mountPath: /meili_data

      containers:
      - name: meilisearch
        image: getmeili/meilisearch:v1.37.0
        args:
        - "--db-path=/meili_data/data.ms"
        - "--import-snapshot=/meili_data/snapshots/data.ms.snapshot"
        - "--ignore-snapshot-if-db-exists=true"  # skip re-import if DB exists
        - "--master-key=$(MEILI_MASTER_KEY)"
        env:
        - name: MEILI_MASTER_KEY
          valueFrom:
            secretKeyRef:
              name: meilisearch-secrets
              key: master-key
        ports:
        - containerPort: 7700
        volumeMounts:
        - name: data
          mountPath: /meili_data

      - name: snapshot-refresher
        # Watches S3 for a newer snapshot; when detected, rotates standby:
        # 1. Checks etag on S3 object vs last seen
        # 2. Downloads new snapshot to /meili_data/snapshots/
        # 3. Signals main container to restart (or uses a shared flag + liveness probe trick)
        # Simplest approach: set a TTL and let the pod restart on schedule
        image: amazon/aws-cli:2.15.0
        command: ["/bin/sh", "-c"]
        args:
        - |
          LAST_ETAG=""
          while true; do
            sleep 120
            ETAG=$(aws s3api head-object --bucket "${S3_BUCKET}" \
              --key meilisearch/latest.ms.snapshot \
              --endpoint-url="${S3_ENDPOINT}" \
              --query ETag --output text 2>/dev/null)
            if [ "$ETAG" != "$LAST_ETAG" ] && [ -n "$ETAG" ]; then
              echo "New snapshot available ($ETAG), downloading..."
              aws s3 cp "s3://${S3_BUCKET}/meilisearch/latest.ms.snapshot" \
                /meili_data/snapshots/new.ms.snapshot \
                --endpoint-url="${S3_ENDPOINT}"
              mv /meili_data/snapshots/new.ms.snapshot \
                 /meili_data/snapshots/data.ms.snapshot
              LAST_ETAG="$ETAG"
              # Signal Meilisearch to reload (requires restart of the main container)
              # Simplest: touch a flag file that a liveness probe checks, causing restart
            fi
          done
        volumeMounts:
        - name: data
          mountPath: /meili_data

      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: meilisearch-standby-data
```

#### 3. Services

```yaml
# Write endpoint — points only to primary
apiVersion: v1
kind: Service
metadata:
  name: meilisearch-write
  namespace: search
spec:
  selector:
    app: meilisearch
    role: primary
  ports:
  - port: 7700
    targetPort: 7700
---
# Read endpoint — can include both primary and standby
apiVersion: v1
kind: Service
metadata:
  name: meilisearch-read
  namespace: search
spec:
  selector:
    app: meilisearch        # matches both roles
  ports:
  - port: 7700
    targetPort: 7700
```

#### 4. Promotion Runbook (Manual, or via Controller)

When primary fails:
```bash
# 1. Verify primary is down
kubectl -n search get pods -l role=primary

# 2. Scale down the failed primary StatefulSet (if pod is in CrashLoop)
kubectl -n search scale statefulset meilisearch-primary --replicas=0

# 3. Patch standby labels to primary (swaps it into the write service)
kubectl -n search patch deployment meilisearch-standby \
  -p '{"spec":{"template":{"metadata":{"labels":{"role":"primary"}}}}}'

# 4. Point write service selector to the promoted pod
kubectl -n search patch service meilisearch-write \
  -p '{"spec":{"selector":{"app":"meilisearch","role":"primary"}}}'

# 5. Bring up a new standby (old primary's PVC may need to be cleared or
#    the new standby will re-bootstrap from object storage)
```

For automated failover, a minimal controller can watch the primary's endpoint health and execute the label swap. Projects like `kube-vip` or a simple custom controller using `controller-runtime` can implement this. Alternatively, a bash loop in a dedicated pod with RBAC to patch Services is a 50-line implementation.

#### 5. Standby Reload Strategy

The cleanest way to get the standby to pick up a new snapshot without a custom controller:

```yaml
# Add to standby Deployment — pod restarts every 6 hours automatically
spec:
  template:
    spec:
      containers:
      - name: meilisearch
        # Meilisearch re-runs --import-snapshot on startup if DB is absent
        # Combined with snapshot-refresher deleting the old DB dir before triggering:
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "rm -rf /meili_data/data.ms"]
```

Or use a CronJob that triggers a rolling restart of the standby deployment every snapshot window:
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: meilisearch-standby-refresh
  namespace: search
spec:
  schedule: "*/10 * * * *"   # every 10 minutes
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: meilisearch-roller
          containers:
          - name: roller
            image: bitnami/kubectl:latest
            command:
            - kubectl
            - rollout
            - restart
            - deployment/meilisearch-standby
            - -n
            - search
          restartPolicy: OnFailure
```

Note: Per the instructions in `CLAUDE.md`, K8s CronJobs should be avoided; instead, use a long-running Deployment with an internal scheduling loop. Replace the above CronJob with a dedicated `standby-refresher` Deployment that runs the rolling-restart logic on an internal ticker (e.g., a shell loop or a small Go/Rust binary).

---

## 6. Known Gaps and Open Questions

1. **Automatic leader election on primary failure** — no community-edition solution provides this without custom code. The K8s Lease API is the most practical tool.

2. **Write loss on failover** — any snapshot-based approach has a window of write loss equal to the snapshot interval. If your writes are coming from a queue or a database (i.e., replayable), configure re-indexing from source on failover instead of promoting the standby.

3. **Standby serves stale reads** — this is often acceptable for search but must be documented to application teams. The staleness bound equals the snapshot interval.

4. **Meilisearch startup time scales with index size** — LMDB memory-maps the entire data directory. On pod start, Meilisearch must warm up its in-memory structures. For very large indexes (>50 GB), RTO can be minutes even without snapshot import. Benchmark this for your corpus before committing to this design.

5. **`--experimental-replication-parameters` is not a replacement for replication** — it enables external coordination of task IDs but does not synchronize indexes. Do not confuse this flag with actual replication capability.

6. **The enterprise replication story (v1.37+)** — if budget allows, the EE license for a homelab is likely achievable via Meilisearch's stated free-license program for indie projects. Worth a request to their sales/support before investing engineering time in home-grown approaches.

---

## References

- [Meilisearch Enterprise Edition announcement](https://www.meilisearch.com/blog/enterprise-license) — BUSL licensing, August 2025
- [Enterprise Edition vs Community Edition docs](https://www.meilisearch.com/docs/learn/self_hosted/enterprise_edition)
- [Replication and sharding overview](https://www.meilisearch.com/docs/resources/self_hosting/sharding/overview) — EE v1.37+ only
- [Horizontal scaling with sharding](https://www.meilisearch.com/blog/horizontal-scaling-with-sharding) — Rendezvous Hashing, EE-only production use
- [Distributed Meilisearch — Discussion #617](https://github.com/orgs/meilisearch/discussions/617) — Community HA discussion, Feb 2025 update
- [About replicating Meilisearch — Issue #3494](https://github.com/meilisearch/meilisearch/issues/3494) — Three replication architectures discussed by the team
- [Experimental replication parameters — Discussion #725](https://github.com/orgs/meilisearch/discussions/725) — Task ID externalization, v1.7.0+
- [Snapshots vs Dumps](https://meilisearch.com/docs/learn/data_backup/snapshots_vs_dumps) — backup mechanism comparison
- [Snapshots documentation](https://meilisearch.com/docs/learn/data_backup/snapshots) — `--schedule-snapshot`, `.ms.snapshot` format
- [meilisearch-backup by akmalovaa](https://github.com/akmalovaa/meilisearch-backup) — community dump-to-S3 sidecar
- [meilisearch-kubernetes Helm chart](https://github.com/meilisearch/meilisearch-kubernetes) — official K8s deployment (single replica only)
- [Support multi replicas — Issue #111](https://github.com/meilisearch/meilisearch-kubernetes/issues/111) — unresolved K8s multi-replica question
- [High Availability roadmap item](https://roadmap.meilisearch.com/c/24-high-availibility) — status: in progress as of 2025
- [Storage engine (LMDB) documentation](https://www.meilisearch.com/docs/learn/engine/storage)
- [v1.37.0 release notes](https://github.com/meilisearch/meilisearch/releases/tag/v1.37.0) — replicated sharding, EE only
- [v1.19.0 release notes](https://github.com/meilisearch/meilisearch/releases/tag/v1.19.0) — initial sharding (EE), license change
