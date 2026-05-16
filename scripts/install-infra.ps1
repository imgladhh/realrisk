param(
    [ValidateSet("local", "prod")]
    [string]$Overlay = "local",
    [string]$Namespace = "realrisk",
    [string]$StrimziNamespace = "strimzi-system",
    [string]$StrimziChartVersion = "0.45.2"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$overlayRoot = Join-Path $repoRoot "k8s\overlays\$Overlay"
$redisValues = Join-Path $overlayRoot "helm-values\redis-values.yaml"
$postgresValues = Join-Path $overlayRoot "helm-values\postgres-values.yaml"

if (-not (Test-Path $redisValues)) {
    throw "Missing Redis Helm values file: $redisValues"
}

if (-not (Test-Path $postgresValues)) {
    throw "Missing PostgreSQL Helm values file: $postgresValues"
}

kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -

helm repo add strimzi https://strimzi.io/charts/ | Out-Null
helm repo add bitnami https://charts.bitnami.com/bitnami | Out-Null
helm repo update | Out-Null

helm upgrade --install strimzi-kafka-operator `
    strimzi/strimzi-kafka-operator `
    --version $StrimziChartVersion `
    --namespace $StrimziNamespace `
    --create-namespace `
    --set watchAnyNamespace=true

$serverVersion = kubectl version -o json | ConvertFrom-Json
$kubeMajor = $serverVersion.serverVersion.major
$kubeMinor = ($serverVersion.serverVersion.minor -replace '[^0-9].*$', '')

if (-not $kubeMajor -or -not $kubeMinor) {
    throw "Unable to determine Kubernetes server version for Strimzi compatibility override."
}

# Work around Fabric8/VersionInfo parsing regressions on newer Kubernetes releases
# by telling Strimzi exactly which server version to assume.
kubectl set env deployment/strimzi-cluster-operator `
    -n $StrimziNamespace `
    STRIMZI_KUBERNETES_VERSION="major=$kubeMajor,minor=$kubeMinor"

kubectl rollout status deployment/strimzi-cluster-operator `
    -n $StrimziNamespace `
    --timeout=300s

helm upgrade --install realrisk-redis `
    bitnami/redis `
    --namespace $Namespace `
    --create-namespace `
    -f $redisValues

helm upgrade --install realrisk-postgresql `
    bitnami/postgresql `
    --namespace $Namespace `
    --create-namespace `
    -f $postgresValues

kubectl rollout status statefulset/realrisk-redis-master `
    -n $Namespace `
    --timeout=300s

kubectl rollout status statefulset/realrisk-postgresql `
    -n $Namespace `
    --timeout=300s

Write-Host ""
Write-Host "Phase 6 infra installed for overlay '$Overlay'." -ForegroundColor Green
Write-Host "Next step: kubectl apply -k .\k8s\overlays\$Overlay" -ForegroundColor Green
Write-Host "Then wait for Kafka readiness: kubectl wait kafka/realrisk-kafka --for=condition=Ready -n $Namespace --timeout=300s" -ForegroundColor Yellow
