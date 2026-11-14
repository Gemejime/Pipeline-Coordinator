#!/usr/bin/env bash
# ==============================================================================
# inject_k8s_secrets.sh - CI/CD Kubernetes Secret Injection Helper
# ==============================================================================
# Injects required secrets (POSTGRES_DSN, AUTH_HMAC_SECRET, REDIS_PASSWORD, etc.)
# into target Kubernetes Secret safely without printing credentials to stdout.
# ==============================================================================

set -euo pipefail

# Ensure debugging traces do not print secret values
set +x

K8S_NAMESPACE="${K8S_NAMESPACE:-default}"
K8S_SECRET_NAME="${K8S_SECRET_NAME:-rag-ingestion-coordinator-secrets}"
DRY_RUN="${DRY_RUN:-false}"

echo "[INFO] Validating required secrets for ${K8S_SECRET_NAME}..."

if [ -z "${POSTGRES_DSN:-}" ]; then
  echo "[ERROR] POSTGRES_DSN environment variable is required but missing or empty." >&2
  exit 1
fi

if [ -z "${AUTH_HMAC_SECRET:-}" ]; then
  echo "[ERROR] AUTH_HMAC_SECRET environment variable is required but missing or empty." >&2
  exit 1
fi

if [ "${#AUTH_HMAC_SECRET}" -lt 32 ]; then
  echo "[ERROR] AUTH_HMAC_SECRET length (${#AUTH_HMAC_SECRET}) is less than required minimum of 32 characters." >&2
  exit 1
fi

echo "[INFO] Preparing secret injection (namespace: ${K8S_NAMESPACE}, secret: ${K8S_SECRET_NAME})..."

CMD=(kubectl create secret generic "${K8S_SECRET_NAME}" \
  --namespace="${K8S_NAMESPACE}" \
  --from-literal=POSTGRES_DSN="${POSTGRES_DSN}" \
  --from-literal=AUTH_HMAC_SECRET="${AUTH_HMAC_SECRET}" \
  --dry-run=client -o yaml)

if [ -n "${REDIS_PASSWORD:-}" ]; then
  CMD+=(--from-literal=REDIS_PASSWORD="${REDIS_PASSWORD}")
fi

if [ -n "${AUTH_STATIC_TOKENS_JSON:-}" ]; then
  CMD+=(--from-literal=AUTH_STATIC_TOKENS_JSON="${AUTH_STATIC_TOKENS_JSON}")
fi

if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  CMD+=(--from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}")
fi

if [ -n "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  CMD+=(--from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}")
fi

if [ "${DRY_RUN}" = "true" ]; then
  "${CMD[@]}" >/dev/null
  echo "[DRY-RUN] Secret manifest successfully generated and validated for ${K8S_SECRET_NAME}."
else
  "${CMD[@]}" | kubectl apply -f -
  echo "[SUCCESS] Successfully applied Secret ${K8S_SECRET_NAME} in namespace ${K8S_NAMESPACE}."
fi

