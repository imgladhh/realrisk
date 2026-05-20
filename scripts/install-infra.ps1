param(
    [ValidateSet("local", "prod")]
    [string]$Overlay = "local",
    [string]$Namespace = "realrisk",
    [string]$StrimziNamespace = "strimzi-system",
    [string]$StrimziChartVersion = "0.45.2",
    [string]$CnpgNamespace = "cnpg-system",
    [string]$CnpgChartVersion = "0.23.2"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$overlayRoot = Join-Path $repoRoot "k8s\overlays\$Overlay"
$redisValues = Join-Path $overlayRoot "helm-values\redis-values.yaml"

if (-not (Test-Path $redisValues)) {
    throw "Missing Redis Helm values file: $redisValues"
}

kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -

helm repo add strimzi https://strimzi.io/charts/ | Out-Null
helm repo add bitnami https://charts.bitnami.com/bitnami | Out-Null
helm repo update | Out-Null

& (Join-Path $PSScriptRoot "install-cnpg.ps1") `
    -Namespace $CnpgNamespace `
    -ChartVersion $CnpgChartVersion

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

kubectl wait --for=condition=Ready pod `
    -l app.kubernetes.io/name=redis `
    -n $Namespace `
    --timeout=300s

kubectl create secret generic realrisk-notification-secrets `
    -n $Namespace `
    --from-literal=slack-webhook-url=https://hooks.slack.invalid/services/placeholder `
    --from-literal=smtp-password=placeholder `
    --from-literal=smtp-from=realrisk@example.com `
    --from-literal=email-api-key=placeholder `
    --dry-run=client -o yaml | kubectl apply -f -

Write-Host ""
Write-Host "Phase 9 infra installed for overlay '$Overlay'." -ForegroundColor Green
Write-Host "Next step: kubectl apply -k .\k8s\overlays\$Overlay" -ForegroundColor Green
Write-Host "Then wait for Kafka readiness: kubectl wait kafka/realrisk-kafka --for=condition=Ready -n $Namespace --timeout=300s" -ForegroundColor Yellow
