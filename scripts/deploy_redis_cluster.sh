#!/usr/bin/env bash
# ==============================================================================
# deploy_redis_cluster.sh - Deploy Bitnami Redis 7.2 Cluster to Kubernetes
# ==============================================================================
# Deploys a production-grade 6-node Redis Cluster in namespace 'rag-system'
# using official Bitnami Helm charts, verifies pod readiness, and confirms
# cluster slot allocation (16,384 slots assigned).
# ==============================================================================

set -euo pipefail

NAMESPACE="${NAMESPACE:-rag-system}"
RELEASE_NAME="${RELEASE_NAME:-rag-ingestion-coordinator-redis}"
VALUES_FILE="${VALUES_FILE:-deployments/helm/redis-cluster/values.yaml}"
TIMEOUT="${TIMEOUT:-300s}"

echo "=== [1/5] Verifying CLI Prerequisites ==="
if ! command -v helm &>/dev/null; then
  echo "[ERROR] 'helm' CLI is required but not installed or not in PATH." >&2
  exit 1
fi

if ! command -v kubectl &>/dev/null; then
  echo "[ERROR] 'kubectl' CLI is required but not installed or not in PATH." >&2
  exit 1
fi
echo "[OK] 'helm' and 'kubectl' found."

echo "=== [2/5] Ensuring Namespace '${NAMESPACE}' Exists ==="
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
echo "[OK] Namespace '${NAMESPACE}' ready."

echo "=== [3/5] Adding & Updating Bitnami Helm Repository ==="
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update >/dev/null 2>&1 || true
helm repo update bitnami
echo "[OK] Bitnami repo updated."

echo "=== [4/5] Deploying Redis Cluster via Helm (${RELEASE_NAME}) ==="
if [ ! -f "${VALUES_FILE}" ]; then
  echo "[ERROR] Values file not found at ${VALUES_FILE}" >&2
  exit 1
fi

helm upgrade --install "${RELEASE_NAME}" bitnami/redis-cluster \
  --namespace "${NAMESPACE}" \
  -f "${VALUES_FILE}" \
  --timeout "${TIMEOUT}"

echo "[INFO] Awaiting StatefulSet pod readiness in namespace '${NAMESPACE}'..."
kubectl rollout status statefulset/"${RELEASE_NAME}" \
  --namespace "${NAMESPACE}" \
  --timeout="${TIMEOUT}"

echo "=== [5/5] Verifying 16,384 Cluster Slots & Cluster State ==="
POD_NAME="${RELEASE_NAME}-0"
SECRET_NAME="rag-ingestion-coordinator-secrets"

# Retrieve password from shared secret if available
REDIS_PASSWORD=$(kubectl get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.REDIS_PASSWORD}' 2>/dev/null | base64 -d || true)

AUTH_ARGS=()
if [ -n "${REDIS_PASSWORD}" ]; then
  AUTH_ARGS=(-a "${REDIS_PASSWORD}")
fi

echo "[INFO] Querying cluster info from ${POD_NAME}..."
CLUSTER_INFO=$(kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -c redis -- redis-cli "${AUTH_ARGS[@]}" cluster info 2>/dev/null || true)

if echo "${CLUSTER_INFO}" | grep -q "cluster_state:ok"; then
  echo "[SUCCESS] Redis Cluster state is HEALTHY (cluster_state:ok)."
else
  echo "[WARN] Could not confirm cluster_state:ok immediately. Raw output:"
  echo "${CLUSTER_INFO}"
fi

SLOTS_ASSIGNED=$(echo "${CLUSTER_INFO}" | grep "cluster_slots_assigned" | tr -d '\r' || true)
echo "[INFO] Slot allocation status: ${SLOTS_ASSIGNED}"

if echo "${CLUSTER_INFO}" | grep -q "cluster_slots_assigned:16384"; then
  echo "[SUCCESS] All 16,384 Redis cluster slots successfully assigned."
else
  echo "[WARN] Cluster initialization may still be fanning out slots across nodes."
fi

echo ""
echo "=============================================================================="
echo " Redis Cluster Deployment Complete!"
echo " In-Cluster Headless Discovery Addresses:"
echo "   ${RELEASE_NAME}-0.${RELEASE_NAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
echo "   ${RELEASE_NAME}-1.${RELEASE_NAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
echo "   ${RELEASE_NAME}-2.${RELEASE_NAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
echo "=============================================================================="

