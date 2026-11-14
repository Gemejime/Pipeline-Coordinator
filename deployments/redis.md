# Dedicated Bitnami Redis 7.2 Cluster Guide

This document describes the topology, deployment operations, network routing, and Day-2 maintenance runbooks for the dedicated **Bitnami Redis 7.2 Cluster** backing the **`rag-ingestion-coordinator`** microservice.

---

## 1. Architectural Topology & Low-Latency Rationale

The `rag-ingestion-coordinator` utilizes Redis Pub/Sub as its high-throughput, decoupled telemetry event bus (ADR-004), broadcasting document chunking and partition milestone frames to `ingestion:events:<tenant_id>`.

```
               [ rag-ingestion-coordinator Pods ]
                               |
              (UniversalClient Cluster Connection)
                               |
                               v
+-----------------------------------------------------------------------------------------+
|                                Redis 7.2 Clustered Topology                             |
|                                                                                         |
|       Shard 1                        Shard 2                        Shard 3             |
|   +---------------+              +---------------+              +---------------+       |
|   | Primary Node  |              | Primary Node  |              | Primary Node  |       |
|   | Slots: 0-5460 |              | Slots:5461-10922             | Slots:10923-16383     |
|   +-------+-------+              +-------+-------+              +-------+-------+       |
|           | (Async Rep)                  | (Async Rep)                  | (Async Rep)   |
|           v                              v                              v               |
|   +---------------+              +---------------+              +---------------+       |
|   | Secondary Rep |              | Secondary Rep |              | Secondary Rep |       |
|   | (Replica 1)   |              | (Replica 2)   |              | (Replica 3)   |       |
|   +---------------+              +---------------+              +---------------+       |
|                                                                                         |
|   Persistent Volumes: 6 x 5Gi AOF/RDB PVs                                               |
|   Prometheus Exporters: Port 9121 on all 6 StatefulSet Pods                             |
+-----------------------------------------------------------------------------------------+
```

### Why a Dedicated In-Cluster Clustered Redis?
1. **Sub-Millisecond Tail Latency ($P99 < 1\text{ms}$)**: By collocating the 6-node Redis cluster within the same Kubernetes cluster and namespace (`rag-system`), network round-trips avoid VPC NAT gateways, cross-cloud hops, and external cloud ingress firewalls.
2. **Horizontal Pub/Sub Fanout**: Redis Cluster partitions key ranges across 16,384 hash slots. For publish operations, cluster nodes broadcast messages across the cluster bus to connected subscribers without central broker bottlenecks.
3. **High Availability with Automatic Failover**: If any Primary shard fails, its paired Secondary replica is promoted within 5 seconds (`cluster-node-timeout: 5000`), with zero data loss on AOF-synchronized events.
4. **Hardened Resource Isolation**: Dedicated container limits (`cpu: 500m`, `memory: 1Gi`) prevent noisy neighbors from impacting ingestion telemetry.

---

## 2. In-Cluster Headless DNS Routing Format

Pods discover the cluster nodes via the Kubernetes headless service:

```
<RELEASE_NAME>-<ORDINAL>.<RELEASE_NAME>-headless.<NAMESPACE>.svc.cluster.local:6379
```

For release `rag-ingestion-coordinator-redis` in namespace `rag-system`:

```env
REDIS_CLUSTER_ADDRS="rag-ingestion-coordinator-redis-0.rag-ingestion-coordinator-redis-headless.rag-system.svc.cluster.local:6379,rag-ingestion-coordinator-redis-1.rag-ingestion-coordinator-redis-headless.rag-system.svc.cluster.local:6379,rag-ingestion-coordinator-redis-2.rag-ingestion-coordinator-redis-headless.rag-system.svc.cluster.local:6379"
```

The Go `UniversalClient` automatically queries the cluster topology via `CLUSTER SLOTS` / `CLUSTER SHARDS` upon connection, discovers all 6 nodes (3 primaries + 3 secondaries), and routes commands directly to the authoritative shard.

---

## 3. Deployment Methods

### Method A: Local Workstation Development (Docker Compose)
Run a standalone Redis 7.2 container with AOF persistence enabled:
```bash
# Start local Redis container
docker compose -f deployments/docker/docker-compose.redis.yml up -d

# Verify local container health
docker compose -f deployments/docker/docker-compose.redis.yml ps

# Connect via CLI
docker exec -it rag-ingestion-coordinator-redis-local redis-cli -a redispassword123 ping
# Output: PONG
```

### Method B: Automated Kubernetes Script Deployment
Deploy the full 6-node cluster using the one-click deployment scripts:

**Linux / macOS (Bash):**
```bash
chmod +x scripts/deploy_redis_cluster.sh
./scripts/deploy_redis_cluster.sh
```

**Windows (PowerShell):**
```powershell
.\scripts\deploy_redis_cluster.ps1 -Namespace "rag-system"
```

### Method C: Declarative GitOps Deployment (ArgoCD)
Deploy via ArgoCD Application CRD:
```bash
kubectl apply -f deployments/argocd/redis-cluster-application.yaml -n argocd
```

ArgoCD will automatically track `https://charts.bitnami.com/bitnami` for chart `redis-cluster`, reconcile values, and enforce self-healing sync.

---

## 4. Day-2 Operational Runbooks

### Runbook 1: Inspecting Cluster Health & Slot Distribution
To verify that all 16,384 slots are assigned and all 6 nodes participate in quorum:
```bash
# Obtain password from Kubernetes Secret
REDIS_PASS=$(kubectl get secret rag-ingestion-coordinator-secrets -n rag-system -o jsonpath='{.data.REDIS_PASSWORD}' | base64 -d)

# Check cluster state
kubectl exec -it rag-ingestion-coordinator-redis-0 -n rag-system -c redis -- \
  redis-cli -a "$REDIS_PASS" cluster info

# Check node roles and slot ranges
kubectl exec -it rag-ingestion-coordinator-redis-0 -n rag-system -c redis -- \
  redis-cli -a "$REDIS_PASS" cluster nodes
```

Expected output:
```text
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:6
cluster_size:3
```

### Runbook 2: Real-Time Latency Verification
To measure network and execution latency directly against the cluster:
```bash
kubectl exec -it rag-ingestion-coordinator-redis-0 -n rag-system -c redis -- \
  redis-cli -a "$REDIS_PASS" --latency -h 127.0.0.1 -p 6379
```
*Acceptable SLA*: Average latency should remain $\le 0.5\text{ms}$. If latency exceeds $2\text{ms}$, inspect host disk I/O wait times and network throughput.

### Runbook 3: Prometheus Metrics Scraping
Each Redis cluster pod runs a Prometheus metrics sidecar on port `9121`.

Verify metrics emission:
```bash
kubectl exec -it rag-ingestion-coordinator-redis-0 -n rag-system -c metrics -- \
  wget -qO- http://localhost:9121/metrics | grep -E '^redis_(connected_clients|commands_total|uptime_in_seconds)'
```

Standard Prometheus scraping annotations configured:
```yaml
prometheus.io/scrape: "true"
prometheus.io/port: "9121"
prometheus.io/path: "/metrics"
```

### Runbook 4: Manual Failover Testing
To verify failover resilience without downtime:
```bash
# Connect to a secondary replica and issue a takeover
kubectl exec -it rag-ingestion-coordinator-redis-3 -n rag-system -c redis -- \
  redis-cli -a "$REDIS_PASS" cluster failover
```
Monitor the coordinator logs to verify seamless failover with zero dropped telemetry events.

