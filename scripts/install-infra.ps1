param(
    [ValidateSet("local", "prod")]
    [string]$Overlay = "local",
    [string]$Namespace = "realrisk",
    [string]$StrimziNamespace = "strimzi-system",
    [string]$StrimziChartVersion = "0.45.2",
    [string]$CnpgNamespace = "cnpg-system",
    [string]$CnpgChartVersion = "0.23.2",
    [string]$SlackWebhookUrl = "https://hooks.slack.invalid/services/placeholder",
    [string]$SmtpPassword = "placeholder",
    [string]$SmtpFrom = "realrisk@example.com",
    [string]$EmailApiKey = "placeholder"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$overlayRoot = Join-Path $repoRoot "k8s\overlays\$Overlay"
$redisValues = Join-Path $overlayRoot "helm-values\redis-values.yaml"
$localHelm = Join-Path $repoRoot ".tools\helm\windows-amd64\helm.exe"
$helm = Get-Command helm -ErrorAction SilentlyContinue
if ($helm) {
    $helmCmd = $helm.Source
}
elseif (Test-Path $localHelm) {
    $helmCmd = $localHelm
}
else {
    throw "Helm not found in PATH and local Helm binary missing at $localHelm"
}

if (-not (Test-Path $redisValues)) {
    throw "Missing Redis Helm values file: $redisValues"
}

kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -

& $helmCmd repo add strimzi https://strimzi.io/charts/ | Out-Null
& $helmCmd repo add bitnami https://charts.bitnami.com/bitnami | Out-Null
& $helmCmd repo update | Out-Null

& (Join-Path $PSScriptRoot "install-cnpg.ps1") `
    -Namespace $CnpgNamespace `
    -ChartVersion $CnpgChartVersion

& $helmCmd upgrade --install strimzi-kafka-operator `
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

& $helmCmd upgrade --install realrisk-redis `
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
    --from-literal=slack-webhook-url=$SlackWebhookUrl `
    --from-literal=smtp-password=$SmtpPassword `
    --from-literal=smtp-from=$SmtpFrom `
    --from-literal=email-api-key=$EmailApiKey `
    --dry-run=client -o yaml | kubectl apply -f -

Write-Host ""
Write-Host "Phase 9 infra installed for overlay '$Overlay'." -ForegroundColor Green
Write-Host "Next step: kubectl apply -k .\k8s\overlays\$Overlay" -ForegroundColor Green
Write-Host "Then wait for Kafka readiness: kubectl wait kafka/realrisk-kafka --for=condition=Ready -n $Namespace --timeout=300s" -ForegroundColor Yellow
