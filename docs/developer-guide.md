# Developer Guide

This guide holds the operational detail that was previously in the top-level README.

## Local Run

Start the local Docker Compose dependencies:

```powershell
docker compose up -d
.\scripts\run-api.ps1
```

If the Flink job owns async decisions, disable the old Spring stub worker:

```powershell
.\scripts\run-api.ps1 -DisableRiskWorker
```

Check local Schema Registry connectivity:

```powershell
Invoke-RestMethod http://localhost:8081/subjects
```

## Flink Job

Build and test:

```powershell
mvn -f .\flink-job\pom.xml test
mvn -f .\flink-job\pom.xml -DskipTests package
```

Run locally:

```powershell
$env:RISK_WORKER_ENABLED="false"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:SCHEMA_REGISTRY_URL="http://localhost:8081"
$env:RULE_UPDATES_TOPIC="rule-updates"
java -jar .\flink-job\target\realrisk-flink-job-0.1.0-SNAPSHOT.jar
```

If Maven is not installed locally, use the repo-local binary:

```powershell
.\.tools\maven\bin\mvn.cmd -f .\flink-job\pom.xml -DskipTests package
```

## Dynamic Rules

Use the helper script for emergency direct Kafka rule updates:

```powershell
.\scripts\send-rule-update.ps1 `
    -RuleId rule-large-amount-v1 `
    -RuleType large_amount `
    -Parameters @{ amount_cents = "3000000" }
```

Disable a rule:

```powershell
.\scripts\send-rule-update.ps1 `
    -RuleId rule-large-amount-v1 `
    -RuleType large_amount `
    -Enabled $false
```

For the persisted path, use the admin API:

- `POST /admin/rules`
- `GET /admin/rules`
- `DELETE /admin/rules/{ruleId}`

## Redis Profile Enrichment

Flink reads:

- `blacklist:<userId>`
- `velocity:count:7d:<userId>`

Useful local Redis commands:

```powershell
docker exec realrisk-redis redis-cli SET blacklist:user-phase3 1
docker exec realrisk-redis redis-cli SET velocity:count:7d:user-phase3 125
docker exec realrisk-redis redis-cli DEL blacklist:user-phase3
docker exec realrisk-redis redis-cli DEL velocity:count:7d:user-phase3
```

## Alert Validation

Preferred forcing pattern for API Gateway ingress:

1. set `velocity:count:7d:<userId> = 200`
2. send a transaction with `amountCents > 1_000_000`
3. expect:
   - `decision = BLOCK`
   - `riskScore >= 90`
   - reasons include `high_velocity_7d` and `large_amount`

Important:

- Do not use Redis blacklist when validating through API Gateway; the gateway may return `403` before Kafka/Flink see the event.
- If `/events` returns `429`, vary the test user or clear rate-limit state.

## Kubernetes Workflow

Install core infra:

```powershell
.\scripts\install-infra.ps1 -Overlay local
.\scripts\install-flink-operator.ps1
.\scripts\install-monitoring.ps1
kubectl apply -k .\k8s\overlays\local
```

Build and load images:

```powershell
.\scripts\build-k8s-images.ps1
.\scripts\kind-load-images.ps1
```

When validating a new app build in kind, prefer a **unique image tag** plus:

```powershell
docker build -f .\Dockerfile.api-gateway -t realrisk-api:<unique-tag> .
kind load docker-image realrisk-api:<unique-tag> --name realrisk
kubectl set image deployment/realrisk-api-gateway api-gateway=realrisk-api:<unique-tag> -n realrisk
```

After any `kubectl apply -k .\k8s\overlays\local`, re-check the deployment image because the overlay can reset it.

## Notification Validation

Validated paths:

- Slack webhook delivery
- email delivery through Mailtrap

Note:

- Gmail SMTP auth failed during validation, but Mailtrap proved the email code path works.
- `alert_log.channels_notified` currently reflects attempted/configured channels, not confirmed successful delivery only.

## CI/CD Notes

- GitHub Actions uses `GITHUB_TOKEN` with `packages: write` for GHCR
- deploy is manual (`workflow_dispatch`) because local kubeconfig targets are not appropriate for every push
- `mvn -DskipTests package` still compiles test sources, so stale tests can still break image packaging

## Where to Look Next

- [memory.md](../memory.md) for validated phase history
- [ARCHITECTURE.md](../ARCHITECTURE.md) for the main system design
- [docs/architecture-diagram.md](architecture-diagram.md) for the diagram source
