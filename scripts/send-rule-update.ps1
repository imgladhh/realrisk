<#
.SYNOPSIS
    Send a rule update to the rule-updates Kafka topic.

.DESCRIPTION
    Encodes the rule update as Avro via the Confluent schema-registry container
    and publishes it to the compacted rule-updates topic.
    The Flink job picks it up via broadcast state within one processing cycle;
    decisions reflect the new rules after the next checkpoint (~30 s).

.PARAMETER RuleId
    Stable rule identifier used as the Kafka message key.
    Example: rule-large-amount-v1

.PARAMETER RuleType
    Rule category recognised by RuleSet.from():
      large_amount              - amountCents threshold + score
      withdrawal_without_device - score for deviceFp-less withdrawals
      merchant_multi_user_burst - distinct-user burst threshold + score
      global                    - reviewThreshold / blockThreshold overrides

.PARAMETER Enabled
    $true  - activate / update the rule (default).
    $false - deactivate; engine falls back to FlinkRiskJobConfig defaults.

.PARAMETER Parameters
    Hashtable of string key-value parameters for this rule type.

    large_amount:               amount_cents, score_delta
    withdrawal_without_device:  score_delta
    merchant_multi_user_burst:  burst_threshold, score_delta
    global:                     review_threshold, block_threshold

.EXAMPLE
    # Raise large-amount threshold to USD 30 000 (3 000 000 cents)
    .\scripts\send-rule-update.ps1 `
        -RuleId rule-large-amount-v1 `
        -RuleType large_amount `
        -Parameters @{ amount_cents = "3000000" }

.EXAMPLE
    # Disable the rule - engine falls back to config default (1 000 000 cents)
    .\scripts\send-rule-update.ps1 `
        -RuleId rule-large-amount-v1 `
        -RuleType large_amount `
        -Enabled $false

.EXAMPLE
    # Tighten global decision thresholds
    .\scripts\send-rule-update.ps1 `
        -RuleId rule-global-v1 `
        -RuleType global `
        -Parameters @{ review_threshold = "40"; block_threshold = "70" }
#>
param(
    [Parameter(Mandatory)][string]$RuleId,
    [Parameter(Mandatory)][string]$RuleType,
    [bool]$Enabled = $true,
    [hashtable]$Parameters = @{}
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Container         = "realrisk-schema-registry"
$Topic             = "rule-updates"
$BootstrapServers  = "kafka:29092"
$SchemaRegistryUrl = "http://schema-registry:8081"

# Build Avro-compatible JSON value
$paramsEntries = $Parameters.GetEnumerator() |
    ForEach-Object { '"' + $_.Key + '":"' + $_.Value + '"' }
$paramsJson = '{' + ($paramsEntries -join ',') + '}'
$enabledStr = if ($Enabled) { 'true' } else { 'false' }
$updatedAt  = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

$ruleValue = ('{"ruleId":"' + $RuleId +
              '","ruleType":"' + $RuleType +
              '","enabled":'  + $enabledStr +
              ',"parameters":' + $paramsJson +
              ',"updatedAt":' + $updatedAt + '}')
$message = $RuleId + '|' + $ruleValue

# Inline schema - single line, matches src/main/avro/RuleUpdateAvro.avsc
$schema = ('{"type":"record","name":"RuleUpdateAvro","namespace":"com.realrisk.avro",' +
           '"fields":[{"name":"ruleId","type":"string"},' +
                     '{"name":"ruleType","type":"string"},' +
                     '{"name":"enabled","type":"boolean"},' +
                     '{"name":"parameters","type":{"type":"map","values":"string"},"default":{}},' +
                     '{"name":"updatedAt","type":{"type":"long","logicalType":"timestamp-millis"}}]}')

# Bash script piped into container via stdin
# PowerShell interpolates $BootstrapServers / $Topic / etc. (desired).
# \$(...) escapes the dollar sign so bash evaluates $(cat ...) itself.
$bash = @"
set -e
printf '%s' '$schema' > /tmp/rule-schema.json
printf '%s\n' '$message' | kafka-avro-console-producer \
  --bootstrap-server $BootstrapServers \
  --topic $Topic \
  --property schema.registry.url=$SchemaRegistryUrl \
  --property parse.key=true \
  --property 'key.separator=|' \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property "value.schema=\$(cat /tmp/rule-schema.json)"
"@

Write-Host ""
Write-Host "Sending rule update to topic '$Topic':"
Write-Host "  key  : $RuleId"
Write-Host "  value: $ruleValue"
Write-Host ""

$bash | docker exec -i $Container bash

Write-Host ""
Write-Host "Done. Flink applies the change on the next broadcast cycle."
Write-Host "Decisions will reflect the new rule after the next checkpoint (~30 s)."
