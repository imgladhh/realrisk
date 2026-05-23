# RealRisk

RealRisk is a payment risk pipeline with a fast synchronous gateway and a slower streaming risk engine.

- **API Gateway**: validates requests, checks Redis blacklist and rate limits, and publishes Avro events
- **Flink job**: evaluates risk rules, enriches user profile data, and emits decisions and alerts
- **Alert Service**: consumes `alert-events`, deduplicates alerts, and notifies configured channels
- **PostgreSQL + Redis + Kafka**: persistence, state, and messaging backbone

The project now includes:

- dynamic rule persistence and DB outbox publishing
- in-cluster Kafka / Schema Registry / CloudNativePG / Redis Sentinel
- DLQ replay tooling
- admin API key protection for `/admin/**`
- API Gateway HPA manifests
- Slack and email notification paths validated end to end

## Architecture

- Main diagram: [ARCHITECTURE.md](ARCHITECTURE.md)
- Topology notes: [docs/architecture-diagram.md](docs/architecture-diagram.md)

## Repository Layout

- [src](src) - API Gateway
- [flink-job](flink-job) - streaming risk engine
- [alert-service](alert-service) - alert consumer and notifier
- [k8s](k8s) - base manifests and overlays
- [scripts](scripts) - local build, install, and validation helpers
- [memory.md](memory.md) - validated phase history and operational notes

## Quick Start

### Local Docker Compose path

```powershell
docker compose up -d
.\scripts\run-api.ps1
```

If the Flink job owns async decisions, disable the old Spring stub worker:

```powershell
.\scripts\run-api.ps1 -DisableRiskWorker
```

### Kubernetes path

Use the helper scripts and manifests under `k8s/`:

```powershell
.\scripts\install-infra.ps1 -Overlay local
.\scripts\install-flink-operator.ps1
.\scripts\install-monitoring.ps1
kubectl apply -k .\k8s\overlays\local
```

## Core Flows

### Event ingest

`POST /events` enters the fast path:

1. Redis blacklist and per-user rate limit
2. Avro publish to `raw-events`
3. Flink consumes and emits:
   - `decision-audit`
   - `high-risk-events`
   - `alert-events`

### Dynamic rules

Rules live in PostgreSQL and are published through the transactional outbox to the compacted `rule-updates` topic. Flink replays `rule-updates` to rebuild broadcast state and apply live rule changes.

### Alerts

`alert-service` consumes `alert-events`, stores deduplicated alert rows in `alert_log`, and sends notifications to:

- Slack / webhook
- email
- SMS stub

## Useful Scripts

- [scripts/run-api.ps1](scripts/run-api.ps1) - run the API Gateway locally
- [scripts/send-rule-update.ps1](scripts/send-rule-update.ps1) - emergency direct Kafka rule update path
- [scripts/replay-dlq.ps1](scripts/replay-dlq.ps1) - replay alert DLQ messages in-cluster
- [scripts/install-infra.ps1](scripts/install-infra.ps1) - install Strimzi, Redis, CNPG, and notification/admin secrets
- [scripts/install-monitoring.ps1](scripts/install-monitoring.ps1) - install kube-prometheus-stack
- [scripts/build-k8s-images.ps1](scripts/build-k8s-images.ps1) - build local images
- [scripts/kind-load-images.ps1](scripts/kind-load-images.ps1) - load images into kind
- [scripts/register-schemas.ps1](scripts/register-schemas.ps1) - register Avro schemas

## Status

The system has been validated through:

- dynamic rules and outbox publishing
- CNPG failover
- Redis Sentinel failover
- broker-side Kafka lag alerting
- DLQ replay
- admin API key auth
- API Gateway HPA wiring
- Slack delivery
- email delivery via Mailtrap

See [memory.md](memory.md) for the full validated history and known operational gotchas.

## Detailed Docs

The README keeps the main path only. Detailed runbooks and examples live here:

- [docs/developer-guide.md](docs/developer-guide.md) - local run, Flink run, rules, alerts, and K8s workflow details
- [memory.md](memory.md) - phase-by-phase validation evidence

