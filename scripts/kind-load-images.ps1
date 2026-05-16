param(
    [string]$ClusterName = "realrisk",
    [string[]]$Images = @(
        "realrisk-api:phase5",
        "realrisk-alert-service:phase5",
        "realrisk-flink:phase5"
    )
)

$ErrorActionPreference = "Stop"

foreach ($image in $Images) {
    kind load docker-image $image --name $ClusterName
}
