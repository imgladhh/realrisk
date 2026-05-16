param(
    [string]$Version = "1.14.0",
    [string]$Namespace = "flink-operator"
)

$ErrorActionPreference = "Stop"

helm repo add flink-operator-repo "https://downloads.apache.org/flink/flink-kubernetes-operator-$Version/"
helm repo update
helm upgrade --install flink-kubernetes-operator `
    flink-operator-repo/flink-kubernetes-operator `
    --namespace $Namespace `
    --create-namespace `
    --set webhook.create=false
