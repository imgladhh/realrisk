param(
    [ValidateSet("local")]
    [string]$Overlay = "local",
    [string]$Namespace = "monitoring"
)

$ErrorActionPreference = "Stop"

$chartVersion = "65.1.0"
$repoRoot = Split-Path -Parent $PSScriptRoot
$valuesFile = Join-Path $repoRoot "k8s\overlays\$Overlay\helm-values\kube-prometheus-stack-values.yaml"
$monitoringBase = Join-Path $repoRoot "k8s\base\monitoring"

if (-not (Test-Path $valuesFile)) {
    throw "Missing kube-prometheus-stack values file: $valuesFile"
}

kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts/ | Out-Null
helm repo update | Out-Null

helm upgrade --install kube-prometheus-stack `
    prometheus-community/kube-prometheus-stack `
    --namespace $Namespace `
    --create-namespace `
    --version $chartVersion `
    -f $valuesFile

kubectl rollout status deployment/kube-prometheus-stack-operator `
    -n $Namespace `
    --timeout=300s

kubectl rollout status deployment/kube-prometheus-stack-grafana `
    -n $Namespace `
    --timeout=300s

kubectl apply -k $monitoringBase

Write-Host ""
Write-Host "Monitoring stack installed in namespace '$Namespace'." -ForegroundColor Green
Write-Host "Run this before applying the overlay so the ServiceMonitor/PodMonitor CRDs exist." -ForegroundColor Yellow
Write-Host "Next step: kubectl apply -k .\k8s\overlays\$Overlay" -ForegroundColor Green
Write-Host "Grafana port-forward: kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80 -n $Namespace" -ForegroundColor Yellow
