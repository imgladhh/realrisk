<#
.SYNOPSIS
    Register all Avro schemas with the Schema Registry.

.DESCRIPTION
    Reads every .avsc file under src/main/avro/ and POSTs each schema to the
    Confluent Schema Registry REST API under the appropriate topic subject.

    Subject naming follows the TopicNameStrategy convention used by all
    producers and consumers in this project: <topic>-value

    Schema-to-subject mapping
    -------------------------
    RiskEventAvro     -> raw-events-value, raw-audit-value
    RiskDecisionAvro  -> decision-audit-value
    HighRiskEventAvro -> high-risk-events-value
    AlertEventAvro    -> alert-events-value
    RuleUpdateAvro    -> rule-updates-value

.PARAMETER SchemaRegistryUrl
    Base URL of the Schema Registry. Defaults to http://localhost:8081 for
    local Docker Compose. Pass http://<cluster-ip>:8081 or a port-forwarded
    URL for Kubernetes.

.EXAMPLE
    # Docker Compose (default)
    .\scripts\register-schemas.ps1

.EXAMPLE
    # Kubernetes - port-forward the in-cluster registry first:
    #   kubectl port-forward svc/realrisk-schema-registry 8081:8081 -n realrisk
    .\scripts\register-schemas.ps1 -SchemaRegistryUrl http://localhost:8081

.EXAMPLE
    # Kubernetes - direct cluster URL (from within the cluster or with host routing)
    .\scripts\register-schemas.ps1 `
        -SchemaRegistryUrl http://realrisk-schema-registry.realrisk.svc.cluster.local:8081
#>
param(
    [string]$SchemaRegistryUrl = "http://localhost:8081"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$avroDir = Join-Path $repoRoot "src\main\avro"

# Schema-to-subjects mapping
# Key   = .avsc filename (without extension, case-sensitive)
# Value = ordered list of SR subjects that use this schema
$schemaSubjects = [ordered]@{
    "RiskEventAvro"     = @("raw-events-value", "raw-audit-value")
    "RiskDecisionAvro"  = @("decision-audit-value")
    "HighRiskEventAvro" = @("high-risk-events-value")
    "AlertEventAvro"    = @("alert-events-value")
    "RuleUpdateAvro"    = @("rule-updates-value")
}

# Helpers
function Wait-SchemaRegistry {
    param([string]$Url, [int]$MaxAttempts = 20, [int]$DelaySeconds = 3)

    Write-Host "Waiting for Schema Registry at $Url ..." -ForegroundColor Cyan
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        try {
            $null = Invoke-RestMethod -Uri "$Url/subjects" -Method Get -TimeoutSec 5
            Write-Host "  Schema Registry is up." -ForegroundColor Green
            return
        } catch {
            Write-Host "  Attempt $i/$MaxAttempts - not ready yet, retrying in ${DelaySeconds}s ..."
            Start-Sleep -Seconds $DelaySeconds
        }
    }

    throw "Schema Registry did not become ready after $MaxAttempts attempts."
}

function Register-Schema {
    param(
        [string]$Url,
        [string]$Subject,
        [string]$SchemaJson
    )

    # Confluent SR expects the schema wrapped in {"schema": "<escaped-json>"}
    $payload = @{ schema = $SchemaJson } | ConvertTo-Json -Compress
    $endpoint = "$Url/subjects/$Subject/versions"

    try {
        $result = Invoke-RestMethod `
            -Uri $endpoint `
            -Method Post `
            -ContentType "application/vnd.schemaregistry.v1+json" `
            -Body $payload
        return $result.id
    } catch {
        $statusCode = $_.Exception.Response.StatusCode.value__
        $body = $_.ErrorDetails.Message
        throw "POST $endpoint failed ($statusCode): $body"
    }
}

# Main
Write-Host ""
Write-Host "Schema Registry : $SchemaRegistryUrl"
Write-Host "Avro sources    : $avroDir"
Write-Host ""

Wait-SchemaRegistry -Url $SchemaRegistryUrl

$registered = 0
$skipped = 0
$errors = 0

foreach ($schemaName in $schemaSubjects.Keys) {
    $avscPath = Join-Path $avroDir "$schemaName.avsc"

    if (-not (Test-Path $avscPath)) {
        Write-Warning "  [SKIP] $schemaName.avsc not found at $avscPath"
        $skipped++
        continue
    }

    # Read the raw JSON and collapse it to a single line. SR accepts multiline
    # schema JSON too, but a compact payload keeps the wrapper body predictable.
    $rawSchema = Get-Content -Raw $avscPath
    $schemaJson = ($rawSchema | ConvertFrom-Json | ConvertTo-Json -Compress -Depth 20)
    $subjects = $schemaSubjects[$schemaName]

    foreach ($subject in $subjects) {
        try {
            $id = Register-Schema -Url $SchemaRegistryUrl -Subject $subject -SchemaJson $schemaJson
            Write-Host ("  [OK]   {0,-30} -> subject '{1}'  (id={2})" -f "$schemaName.avsc", $subject, $id) -ForegroundColor Green
            $registered++
        } catch {
            Write-Host ("  [ERR]  {0,-30} -> subject '{1}'" -f "$schemaName.avsc", $subject) -ForegroundColor Red
            Write-Host "         $_" -ForegroundColor Red
            $errors++
        }
    }
}

Write-Host ""
Write-Host ("Finished: {0} registered, {1} skipped, {2} errors." -f $registered, $skipped, $errors)

if ($errors -gt 0) {
    Write-Host ""
    Write-Host "One or more registrations failed. Fix the errors above and re-run." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "All schemas registered successfully." -ForegroundColor Green
Write-Host "Tip: re-running this script is safe - Schema Registry returns the existing id for identical schemas."
