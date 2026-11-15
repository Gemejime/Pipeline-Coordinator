<#
.SYNOPSIS
    Deploy Bitnami Redis 7.2 Cluster to Kubernetes using Helm and PowerShell.
.DESCRIPTION
    Verifies helm and kubectl prerequisites, ensures the target namespace exists,
    deploys or upgrades the Bitnami Redis Cluster release, waits for statefulset
    readiness, and queries cluster slots via redis-cli.
#>

[CmdletBinding()]
param(
    [string]$Namespace = "rag-system",
    [string]$ReleaseName = "rag-ingestion-coordinator-redis",
    [string]$ValuesFile = "deployments/helm/redis-cluster/values.yaml",
    [string]$Timeout = "300s"
)

$ErrorActionPreference = "Stop"

Write-Host "=== [1/5] Verifying CLI Prerequisites ===" -ForegroundColor Cyan
if (-not (Get-Command "helm" -ErrorAction SilentlyContinue)) {
    Write-Error "'helm' CLI is required but not installed or not in PATH."
    exit 1
}
if (-not (Get-Command "kubectl" -ErrorAction SilentlyContinue)) {
    Write-Error "'kubectl' CLI is required but not installed or not in PATH."
    exit 1
}
Write-Host "[OK] 'helm' and 'kubectl' available." -ForegroundColor Green

Write-Host "=== [2/5] Ensuring Namespace '$Namespace' Exists ===" -ForegroundColor Cyan
kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f - | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to ensure namespace '$Namespace'"
    exit $LASTEXITCODE
}
Write-Host "[OK] Namespace '$Namespace' ready." -ForegroundColor Green

Write-Host "=== [3/5] Adding & Updating Bitnami Helm Repository ===" -ForegroundColor Cyan
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update *>$null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to add Bitnami Helm repository"
    exit $LASTEXITCODE
}
helm repo update bitnami | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to update Bitnami Helm repository"
    exit $LASTEXITCODE
}
Write-Host "[OK] Bitnami repo updated." -ForegroundColor Green

Write-Host "=== [4/5] Deploying Redis Cluster via Helm ($ReleaseName) ===" -ForegroundColor Cyan
if (-not (Test-Path $ValuesFile)) {
    Write-Error "Values file not found at $ValuesFile"
    exit 1
}

helm upgrade --install $ReleaseName bitnami/redis-cluster `
    --namespace $Namespace `
    -f $ValuesFile `
    --timeout $Timeout
if ($LASTEXITCODE -ne 0) {
    Write-Error "Helm upgrade/install failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "[INFO] Awaiting StatefulSet pod readiness in namespace '$Namespace'..." -ForegroundColor Yellow
kubectl rollout status "statefulset/$ReleaseName" `
    --namespace $Namespace `
    --timeout=$Timeout
if ($LASTEXITCODE -ne 0) {
    Write-Error "StatefulSet rollout failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "=== [5/5] Verifying 16,384 Cluster Slots & Cluster State ===" -ForegroundColor Cyan
$PodName = "$ReleaseName-0"
$SecretName = "rag-ingestion-coordinator-secrets"

$RedisPassword = ""
try {
    $b64 = kubectl get secret $SecretName -n $Namespace -o "jsonpath={.data.REDIS_PASSWORD}" 2>$null
    if ($b64) {
        $RedisPassword = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($b64))
    }
} catch {
    # Password retrieval optional for verification step
}

$AuthArgs = @()
if ($RedisPassword) {
    $AuthArgs += @("-a", $RedisPassword)
}

Write-Host "[INFO] Querying cluster info from $PodName (with retry)..." -ForegroundColor Yellow
$MaxAttempts = 12
$Attempt = 0
$ClusterHealthy = $false
$SlotsAssigned = $false
$ClusterInfo = ""

while ($Attempt -lt $MaxAttempts) {
    $Attempt++
    $ClusterInfo = kubectl exec -n $Namespace $PodName -c redis -- redis-cli @AuthArgs cluster info 2>$null
    $ExecExit = $LASTEXITCODE

    if ($ExecExit -eq 0 -and $ClusterInfo) {
        if ($ClusterInfo -match "cluster_state:ok") {
            $ClusterHealthy = $true
        }
        if ($ClusterInfo -match "cluster_slots_assigned:16384") {
            $SlotsAssigned = $true
        }
        if ($ClusterHealthy -and $SlotsAssigned) {
            break
        }
    }
    Write-Host "[WAIT] Cluster initializing (attempt $Attempt/$MaxAttempts)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
}

if (-not $ClusterHealthy) {
    Write-Error "Redis Cluster state unhealthy after $MaxAttempts attempts. Output: $ClusterInfo"
    exit 1
}
Write-Host "[SUCCESS] Redis Cluster state is HEALTHY (cluster_state:ok)." -ForegroundColor Green

if (-not $SlotsAssigned) {
    Write-Error "Redis Cluster slots incomplete after $MaxAttempts attempts. Output: $ClusterInfo"
    exit 1
}
Write-Host "[SUCCESS] All 16,384 Redis cluster slots successfully assigned." -ForegroundColor Green

Write-Host ""
Write-Host "==============================================================================" -ForegroundColor Cyan
Write-Host " Redis Cluster Deployment Complete!" -ForegroundColor Green
Write-Host " In-Cluster Headless Discovery Addresses:" -ForegroundColor Cyan
Write-Host "   $ReleaseName-0.$ReleaseName-headless.$Namespace.svc.cluster.local:6379"
Write-Host "   $ReleaseName-1.$ReleaseName-headless.$Namespace.svc.cluster.local:6379"
Write-Host "   $ReleaseName-2.$ReleaseName-headless.$Namespace.svc.cluster.local:6379"
Write-Host "==============================================================================" -ForegroundColor Cyan

