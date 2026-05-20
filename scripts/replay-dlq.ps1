param(
    [string]$SourceTopic = "alert-events-dlq",
    [string]$TargetTopic = "alert-events",
    [string]$OriginalTopic,
    [string]$FailedAfter,
    [string]$FailedBefore,
    [string]$Severity,
    [int]$MaxMessages = 100,
    [int]$MaxIdlePolls = 8,
    [int]$PollTimeoutMs = 500,
    [string]$Operator,
    [switch]$Execute,
    [string]$Namespace = "realrisk",
    [string]$Image = "realrisk-api:phase5"
)

$ErrorActionPreference = "Stop"

$bootstrapServers = "realrisk-kafka-kafka-bootstrap.$Namespace.svc.cluster.local:9092"

$toolArgs = New-Object System.Collections.Generic.List[string]
$toolArgs.Add("--bootstrap-servers")
$toolArgs.Add($bootstrapServers)
$toolArgs.Add("--source-topic")
$toolArgs.Add($SourceTopic)
$toolArgs.Add("--target-topic")
$toolArgs.Add($TargetTopic)
$toolArgs.Add("--max-messages")
$toolArgs.Add($MaxMessages.ToString())
$toolArgs.Add("--max-idle-polls")
$toolArgs.Add($MaxIdlePolls.ToString())
$toolArgs.Add("--poll-timeout-ms")
$toolArgs.Add($PollTimeoutMs.ToString())

if ($OriginalTopic) {
    $toolArgs.Add("--original-topic")
    $toolArgs.Add($OriginalTopic)
}
if ($FailedAfter) {
    $toolArgs.Add("--failed-after")
    $toolArgs.Add($FailedAfter)
}
if ($FailedBefore) {
    $toolArgs.Add("--failed-before")
    $toolArgs.Add($FailedBefore)
}
if ($Severity) {
    $toolArgs.Add("--severity")
    $toolArgs.Add($Severity)
}
if ($Execute) {
    if (-not $Operator) {
        throw "Parameter -Operator is required when -Execute is set."
    }
    $toolArgs.Add("--execute")
    $toolArgs.Add("--operator")
    $toolArgs.Add($Operator)
}

$podName = "dlq-replay-$(Get-Date -Format 'yyyyMMddHHmmss')"

Write-Host "Launching replay pod $podName in namespace $Namespace ..."

& kubectl run $podName `
    --rm --attach --restart=Never `
    --namespace $Namespace `
    --image $Image `
    --image-pull-policy IfNotPresent `
    --env LOADER_MAIN=com.realrisk.tools.DlqReplayTool `
    --command -- java `
        -jar /app/app.jar `
        @toolArgs

if ($LASTEXITCODE -ne 0) {
    throw "DLQ replay pod exited with code $LASTEXITCODE."
}
