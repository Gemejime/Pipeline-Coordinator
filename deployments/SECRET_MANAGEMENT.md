# Secret Management & CI/CD Injection Guide

This document defines the cryptographic secret management architecture, CI/CD injection workflows, rotation runbooks, and audit protocols for the **`rag-ingestion-coordinator`** microservice.

---

## 1. Architectural Principles & Trust Boundaries

The `rag-ingestion-coordinator` strictly adheres to the **12-Factor App methodology** and **zero-trust Kubernetes security standards**:

```
+-----------------------------------------------------------------------------------------+
|                                GitHub Actions CI/CD Pipeline                            |
|                                                                                         |
|  GitHub Encrypted Secrets                      .env.prod (Single Source of Truth)       |
|  - POSTGRES_DSN                                - Non-sensitive runtime variables        |
|  - AUTH_HMAC_SECRET                            - PORT, GRPC_PORT, LOG_LEVEL, etc.       |
|  - REDIS_PASSWORD                                                                       |
+--------------------------+---------------------------------+----------------------------+
                           |                                 |
           (Masked & Injected via Script)         (Auto-synced via Python Tool)
                           |                                 |
                           v                                 v
+--------------------------+---------------------------------+----------------------------+
| Kubernetes Cluster       |                                 |                            |
|                          v                                 v                            |
|                +--------------------+            +--------------------+                 |
|                | Kubernetes Secret  |            |     ConfigMap      |                 |
|                | (Opaque / KMS etcd)|            | (Runtime Settings) |                 |
|                +---------+----------+            +---------+----------+                 |
|                          |                                 |                            |
|                          +----------------+----------------+                            |
|                                           |                                             |
|                                           v                                             |
|                     +---------------------------------------------+                     |
|                     |     Pod (Non-Root UID 65532 Distroless)     |                     |
|                     |     rag-ingestion-coordinator               |                     |
|                     +---------------------------------------------+                     |
+-----------------------------------------------------------------------------------------+
```

### Core Invariants:
1. **ConfigMap vs. Secret Boundary**: Non-sensitive variables are managed in `deployments/helm/values.yaml` and loaded via `ConfigMap`. Sensitive credentials and cryptographic keys **must never exist in Git or values.yaml**; they are strictly loaded from the `rag-ingestion-coordinator-secrets` Kubernetes Secret.
2. **Pre-Commit Enforcement**: The pre-commit hook runs `scripts/sync_env_to_deployments.py --check-only` and immediately aborts commits if any sensitive key (`PASSWORD`, `SECRET`, `TOKEN`, `KEY`, `DSN`) is placed in `values.yaml`.
3. **Secured Logging Invariant**: Under no circumstances may raw JWT tokens, database passwords, Redis credentials, or presigned S3 URLs (`X-Amz-Signature`, `X-Amz-Credential`) be written to stdout, stderr, or log collectors.

---

## 2. Microservice Secrets Catalog

The microservice requires the following Kubernetes Secret keys:

| Environment Variable | Required | Min Length / Format | Target Secret Key | Description & Impact |
| :--- | :---: | :--- | :--- | :--- |
| `POSTGRES_DSN` | **Yes** | `postgres://user:pass@host:5432/db?sslmode=require` | `POSTGRES_DSN` | Database connection string with embedded credentials. Used for pipeline configuration, metadata, and idempotent terminal state recording. |
| `AUTH_HMAC_SECRET` | **Yes** | $\ge 32\text{ characters}$ (High Entropy) | `AUTH_HMAC_SECRET` | Cryptographic symmetric secret used to sign and verify caller JWT tokens on HTTP REST and gRPC endpoints. Compromise permits tenant impersonation. |
| `REDIS_PASSWORD` | Optional | $\ge 16\text{ characters}$ | `REDIS_PASSWORD` | Password for Redis Pub/Sub cluster connections. Required in environments with password-protected Redis. |
| `AUTH_STATIC_TOKENS_JSON` | Optional | Valid JSON Object | `AUTH_STATIC_TOKENS_JSON` | JSON map of static bearer tokens to tenant scopes for legacy or intra-cluster service authentication. |
| `AWS_ACCESS_KEY_ID` | Optional | 20 alphanumeric chars | `AWS_ACCESS_KEY_ID` | Cloud storage credentials used by STS presigner (only required if not using AWS IRSA). |
| `AWS_SECRET_ACCESS_KEY` | Optional | 40 characters | `AWS_SECRET_ACCESS_KEY` | Cloud storage secret key used by STS presigner (only required if not using AWS IRSA). |

---

## 3. CI/CD Secret Injection (GitHub Actions)

Secret injection is fully automated through the `.github/workflows/cd-deploy.yml` workflow and the `scripts/inject_k8s_secrets.py` CLI utility.

### Step 1: Configure GitHub Repository / Environment Secrets
In your GitHub Repository, navigate to **Settings** $\rightarrow$ **Secrets and variables** $\rightarrow$ **Actions** (or create dedicated `staging` and `production` environments):

- `POSTGRES_DSN`: `postgres://rag_user:<secure_password>@pg-cluster.internal:5432/rag_coordinator?sslmode=require`
- `AUTH_HMAC_SECRET`: A secure 32+ character key, e.g. `openssl rand -hex 32`
- `REDIS_PASSWORD`: Redis cluster access password
- `KUBECONFIG`: Base64-encoded or raw Kubeconfig granting access to the target Kubernetes cluster (or configure AWS IAM OIDC via `AWS_ROLE_TO_ASSUME` and `AWS_EKS_CLUSTER_NAME`).

### Step 2: Automated Pipeline Execution
On push to `main` (or via manual trigger on `workflow_dispatch`), GitHub Actions executes `.github/workflows/cd-deploy.yml`:

1. **Secret Masking**: Explicitly invokes `::add-mask::` on all injected secrets to guarantee zero cleartext disclosure in GitHub runner output.
2. **Cluster Authentication**: Configures `~/.kube/config` using repository credentials.
3. **Idempotent Kubernetes Secret Injection**: Runs `scripts/inject_k8s_secrets.py`:
   ```bash
   python scripts/inject_k8s_secrets.py \
     --namespace "default" \
     --secret-name "rag-ingestion-coordinator-secrets"
   ```
   The script validates presence of required keys, enforces the $\ge 32$-character HMAC rule, and performs an atomic `kubectl apply -f -` without echoing credentials.
4. **Helm Deployment**: Deploys the Helm chart, binding the container environment to the newly injected secret:
   ```bash
   helm upgrade --install rag-ingestion-coordinator deployments/helm \
     --namespace default \
     --values deployments/helm/values.yaml \
     --set secrets.existingSecret="rag-ingestion-coordinator-secrets"
   ```
5. **Rollout Verification**: Waits for the deployment or Argo Rollout to complete successfully.

### Manual Secret-Only Injection Trigger
To update secrets without deploying new code:
1. Navigate to GitHub Actions $\rightarrow$ **CD - Secret Injection & Deployment**.
2. Click **Run workflow**.
3. Set **Execution Action** to `inject-secrets-only`.
4. Choose target environment (`staging` or `production`) and execute.

---

## 4. Alternative GitOps & Cloud Integration Patterns

For organizations utilizing pull-based GitOps (ArgoCD / Flux) or cloud-native identity:

### Pattern A: External Secrets Operator (ESO) + AWS Secrets Manager / Vault (Recommended)
Instead of injecting secrets directly via CI/CD, deploy an `ExternalSecret` resource in the cluster:
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: rag-ingestion-coordinator-secrets
  namespace: default
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  target:
    name: rag-ingestion-coordinator-secrets
    creationPolicy: Owner
  data:
    - secretKey: POSTGRES_DSN
      remoteRef:
        key: production/rag-ingestion-coordinator
        property: POSTGRES_DSN
    - secretKey: AUTH_HMAC_SECRET
      remoteRef:
        key: production/rag-ingestion-coordinator
        property: AUTH_HMAC_SECRET
    - secretKey: REDIS_PASSWORD
      remoteRef:
        key: production/rag-ingestion-coordinator
        property: REDIS_PASSWORD
```

### Pattern B: AWS IAM Roles for Service Accounts (IRSA)
For S3 presigning and object storage access, **never use static AWS keys**. Instead:
1. Annotate the service account in `deployments/helm/values.yaml`:
   ```yaml
   serviceAccount:
     create: true
     annotations:
       eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/rag-ingestion-coordinator
   ```
2. The AWS SDK in `internal/store/s3/presigner.go` will automatically assume the IAM role using the projected OIDC web identity token, eliminating the need for `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY`.

---

## 5. Secret Rotation Runbooks

### 5.1 Zero-Downtime PostgreSQL Password Rotation
1. In PostgreSQL, create a secondary database user with identical permissions, or update the existing user's password using standard dual-credential rotation:
   ```sql
   ALTER USER rag_coordinator WITH PASSWORD 'new_secure_password';
   ```
2. Update `POSTGRES_DSN` in GitHub Actions Secrets or Secrets Manager.
3. Trigger `.github/workflows/cd-deploy.yml` with `inject-secrets-only`.
4. Perform a rolling restart of the pods:
   ```bash
   kubectl rollout restart deployment/rag-ingestion-coordinator -n default
   ```
5. Confirm zero database errors in telemetry logs during rollout.

### 5.2 Zero-Downtime HMAC Secret Rotation
`AUTH_HMAC_SECRET` verifies caller authentication tokens:
1. Update `AUTH_HMAC_SECRET` in GitHub Secrets.
2. Run the CD Secret Injection workflow.
3. Perform a rolling restart (`kubectl rollout restart`).
4. Update client API gateway / caller token signing service to use the new secret key.

### 5.3 Zero-Downtime Redis Password Rotation
1. In Redis configuration, set both `requirepass` and `masterauth` to accept the new password.
2. Update `REDIS_PASSWORD` in GitHub Secrets.
3. Re-inject secret and trigger a rolling restart of coordinator pods.

---

## 6. Verification & Auditing Commands

### Safe Inspection of Cluster Secret (Values Redacted)
```bash
# Verify the secret exists and confirm expected keys are present (values remain base64/redacted):
kubectl get secret rag-ingestion-coordinator-secrets -n default -o jsonpath='{.data}' | jq 'keys'
# Output: ["AUTH_HMAC_SECRET", "POSTGRES_DSN", "REDIS_PASSWORD"]
```

### Inspect Pod Environment Binding Safely
```bash
# Verify that the pod has correctly received the environment variable bindings:
kubectl exec -it deployment/rag-ingestion-coordinator -n default -- env | grep -E '^(POSTGRES|AUTH|REDIS)' | sed 's/=.*/=[REDACTED]/'
```

### Local Dry-Run Injection Test
```bash
# Test the secret injection script locally in dry-run mode without modifying any cluster:
export POSTGRES_DSN="postgres://user:pass@localhost:5432/db"
export AUTH_HMAC_SECRET="$(openssl rand -hex 32)"
python scripts/inject_k8s_secrets.py --dry-run
```

