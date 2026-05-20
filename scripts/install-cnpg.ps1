param(
    [string]$Namespace = "cnpg-system",
    [string]$ChartVersion = "0.23.2"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
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

kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -

& $helmCmd repo add cloudnative-pg https://cloudnative-pg.github.io/charts/ | Out-Null
& $helmCmd repo update | Out-Null

& $helmCmd upgrade --install cnpg `
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
