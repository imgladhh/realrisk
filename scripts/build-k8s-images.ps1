param(
    [string]$ApiImage = "realrisk-api:phase5",
    [string]$AlertImage = "realrisk-alert-service:phase5",
    [string]$FlinkImage = "realrisk-flink:phase5"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$mvn = Join-Path $repoRoot ".tools\maven\bin\mvn.cmd"

Push-Location $repoRoot
try {
    & $mvn -DskipTests package
    & $mvn -f .\alert-service\pom.xml -DskipTests package
    & $mvn -f .\flink-job\pom.xml -DskipTests package

    docker build -f .\Dockerfile.api-gateway -t $ApiImage .
    docker build -f .\alert-service\Dockerfile -t $AlertImage .\alert-service
    docker build -f .\flink-job\Dockerfile -t $FlinkImage .\flink-job
}
finally {
    Pop-Location
}
