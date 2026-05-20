param(
    [string]$Namespace = "cnpg-system",
    [string]$ChartVersion = "0.23.2"
)

$ErrorActionPreference = "Stop"

kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -

helm repo add cloudnative-pg https://cloudnative-pg.github.io/charts/ | Out-Null
helm repo update | Out-Null

helm upgrade --install cnpg `
    cloudnative-pg/cloudnative-pg `
    --version $ChartVersion `
    --namespace $Namespace `
    --create-namespace

kubectl rollout status deployment/cnpg-cloudnative-pg `
    -n $Namespace `
    --timeout=300s

kubectl wait --for=condition=Established crd/clusters.postgresql.cnpg.io --timeout=300s

Write-Host ""
Write-Host "CloudNativePG operator installed in namespace '$Namespace'." -ForegroundColor Green
