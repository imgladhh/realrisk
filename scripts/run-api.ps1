param(
    [switch]$DisableRiskWorker,
    [string]$JavaHome = "C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot",
    [string]$DbUrl = "jdbc:postgresql://localhost:55432/realrisk",
    [string]$DbUser = "realrisk",
    [string]$DbPassword = "realrisk",
    [string]$RedisHost = "localhost",
    [int]$RedisPort = 6379,
    [string]$KafkaBootstrapServers = "localhost:9092",
    [string]$SchemaRegistryUrl = "http://localhost:8081",
    [int]$Port = 8080
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$repoMaven = Join-Path $repoRoot ".tools\maven\bin\mvn.cmd"

if (-not (Test-Path $repoMaven)) {
    throw "Repo Maven not found at $repoMaven"
}

if (-not (Test-Path (Join-Path $JavaHome "bin\java.exe"))) {
    throw "JAVA_HOME does not look valid: $JavaHome"
}

$env:JAVA_HOME = $JavaHome
$env:PATH = "$JavaHome\bin;$([System.IO.Path]::GetDirectoryName($repoMaven));$env:PATH"
$env:DB_URL = $DbUrl
$env:DB_USER = $DbUser
$env:DB_PASSWORD = $DbPassword
$env:REDIS_HOST = $RedisHost
$env:REDIS_PORT = "$RedisPort"
$env:KAFKA_BOOTSTRAP_SERVERS = $KafkaBootstrapServers
$env:SCHEMA_REGISTRY_URL = $SchemaRegistryUrl
$env:SERVER_PORT = "$Port"
$env:RISK_WORKER_ENABLED = if ($DisableRiskWorker) { "false" } else { "true" }

Write-Host "Starting RealRisk API with:" -ForegroundColor Cyan
Write-Host "  JAVA_HOME=$env:JAVA_HOME"
Write-Host "  DB_URL=$env:DB_URL"
Write-Host "  REDIS=$env:REDIS_HOST:$env:REDIS_PORT"
Write-Host "  KAFKA_BOOTSTRAP_SERVERS=$env:KAFKA_BOOTSTRAP_SERVERS"
Write-Host "  SCHEMA_REGISTRY_URL=$env:SCHEMA_REGISTRY_URL"
Write-Host "  SERVER_PORT=$env:SERVER_PORT"
Write-Host "  RISK_WORKER_ENABLED=$env:RISK_WORKER_ENABLED"

Push-Location $repoRoot
try {
    & $repoMaven spring-boot:run
} finally {
    Pop-Location
}
