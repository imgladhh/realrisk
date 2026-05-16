# RealRisk Session Memory

> Living context for AI-assisted development sessions.
> Update `Done`, `In Progress`, `Next`, and `Known Issues` at the end of each session.
> Last updated: 2026-05-16

---

## Project Summary

RealRisk is a two-tier payment risk engine:

- Fast path: Spring Boot API Gateway -> Redis blacklist check + Lua per-user rate limit -> publish `RiskEventAvro` to `raw-events`
- Slow path: Flink job consumes `raw-events`, evaluates rules, emits `RiskDecisionAvro` to `decision-audit`, and emits `HighRiskEventAvro` / `AlertEventAvro` to side outputs
- Dynamic rules: compacted `rule-updates` topic -> Flink broadcast state -> `RuleSet` rebuilt per event
- Redis profile enrichment: Flink reads per-user blacklist and 7-day velocity from Redis before scoring

Full topology: `docs/architecture-diagram.md`

---

## Current Status

### Done

- Phase 1
  - API Gateway fast path: Redis blacklist + Lua rate limit
  - Kafka topics, Audit Writer, Risk Worker stub
  - Redis Materializer blacklist projection
  - PostgreSQL archivers for `raw_events` and `risk_decisions`
- Phase 2a
  - Avro schemas and Schema Registry added to all Kafka boundaries
- Phase 2b
  - Flink job skeleton implemented on Flink 1.20.x
  - Cross-user merchant burst rule implemented
  - Kafka sinks for `decision-audit`, `high-risk-events`, `alert-events`
- Phase 2c
  - `rule-updates` dynamic rule channel implemented
  - Broadcast state + `RuleSet.from(...)` working
  - E2E validated:
    - default large-amount rule -> `BLOCK / 80`
    - raised threshold -> `ALLOW / 0`
    - disabled override -> fallback to default -> `BLOCK / 80`
- Phase 3
  - Redis-backed `UserProfile` enrichment added to the Flink job
  - New rules:
    - `blacklisted_user` -> `+100`
    - `high_velocity_7d` -> `+40` by default
  - Redis lookup degrades to an empty profile on connection/runtime errors
  - Unit tests added for profile reads and profile-aware scoring
  - E2E validated:
    - `blacklist:user-e2e-04 = 1`
    - `evt-e2e-04e` -> `BLOCK / 100 / ["blacklisted_user"]`
    - `velocity:count:7d:user-e2e-04 = 125`
    - `evt-e2e-04f` -> `ALLOW / 40 / ["high_velocity_7d"]`
- Phase 4
  - New `alert-service/` Spring Boot consumer module added
  - Consumes `alert-events` with independent Kafka consumer group `alert-service`
  - Implements:
    - PostgreSQL-backed alert idempotency via `alert_log`
    - Redis-backed per-user per-severity notification rate limiting
    - config-driven routing
    - logging stub channels for email / sms / push
  - Verification:
    - `alert-service` unit tests pass
    - Kafka -> DB integration test exists and auto-skips when Docker is unavailable to the JVM
    - Local smoke test validated:
    - `alert-e2e-01`
    - `severity = HIGH`
    - `status = PROCESSED`
    - `channels_notified = {email,sms}`
    - Redis key `alert:ratelimit:user-alert-01:HIGH = 1`
- Phase 5
  - Added Docker build assets for:
    - API Gateway
    - alert-service
    - Flink job
  - Added Kustomize structure:
    - `k8s/base/`
    - `k8s/overlays/local/`
    - `k8s/overlays/prod/`
  - Added local MinIO deployment + PVC + bucket-init Job for Flink checkpoints
  - Added FlinkDeployment CRD manifest for the Flink K8s Operator
  - Added helper scripts:
    - `scripts/build-k8s-images.ps1`
    - `scripts/kind-load-images.ps1`
    - `scripts/install-flink-operator.ps1`
  - Updated Docker Compose Kafka with a K8s-friendly listener:
    - `host.docker.internal:19092`
  - Flink SA RBAC: Role + RoleBinding added (pods/services/configmaps, full verbs)
  - Validation:
    - `kubectl kustomize k8s/overlays/local` renders successfully
    - `kubectl kustomize k8s/overlays/prod` renders successfully
    - kind cluster apply succeeded; Flink Operator + TaskManagers up
    - MinIO checkpointing real: data visible under `realrisk-flink/checkpoints/`
    - E2E on K8s:
      - `evt-k8s-04` -> `BLOCK / 80 / ["large_amount"]` confirmed in `decision-audit`
      - `evt-k8s-alert-01` -> `BLOCK / 100 / ["blacklisted_user"]`
      - corresponding `alert_log` row persisted for `user-k8s-alert`
      - alert severity routed as `CRITICAL` with `{email,sms,push}`
- Phase 6
  - Added `k8s/base/kafka/`
    - Strimzi `Kafka` CR
    - KRaft `KafkaNodePool`
    - `KafkaTopic` CRs for `raw-events`, `raw-audit`, `decision-audit`, `high-risk-events`, `alert-events`, `rule-updates`
  - Added `k8s/base/schema-registry/` Deployment + Service
  - Added Helm values under:
    - `k8s/overlays/local/helm-values/`
    - `k8s/overlays/prod/helm-values/`
  - Added `scripts/install-infra.ps1` for:
    - Strimzi Kafka Operator
    - Bitnami Redis
    - Bitnami PostgreSQL
  - Strimzi chart version pinned in `install-infra.ps1`
  - Updated app and Flink ConfigMaps to use in-cluster DNS instead of `host.docker.internal`
  - Validation:
    - `kubectl kustomize k8s/overlays/local` renders successfully
    - `kubectl kustomize k8s/overlays/prod` renders successfully
    - in-cluster Kafka became `Ready` on KRaft
    - all `KafkaTopic` resources became `READY=True`
    - `evt-phase6-05` -> `BLOCK / 80 / ["large_amount"]` confirmed in in-cluster `decision-audit`
    - `evt-phase6-alert-01` -> alert row persisted in in-cluster PostgreSQL:
      - `user_id = user-phase6-alert`
      - `severity = CRITICAL`
      - `status = PROCESSED`
      - `channels_notified = {email,sms,push}`
      - `reason_summary = blacklisted_user`
- Tooling / docs
  - `scripts/run-api.ps1`
  - `scripts/send-rule-update.ps1`
  - README updated with Phase 2c and E2E walkthrough

### In Progress

- `scripts/register-schemas.ps1` still not implemented
- Current Phase 6 validation found a Strimzi startup compatibility issue on Kubernetes 1.35:
  - older operator startup can fail while parsing Kubernetes `VersionInfo`
  - `install-infra.ps1` now pins Strimzi `0.45.2` and sets `STRIMZI_KUBERNETES_VERSION`
    dynamically from `kubectl version`
- Strimzi in `strimzi-system` must watch the `realrisk` namespace; `install-infra.ps1`
  now enables `watchAnyNamespace=true` so the operator reconciles `Kafka` / `KafkaTopic`
  CRs created outside its own namespace
- Phase 6 local validation hit a single-node ZooKeeper runtime failure
  (`Leader.getDesignatedLeader -> NoSuchElementException`) even after Strimzi
  reconciliation issues were fixed
- Local Kafka manifests now switch to Strimzi KRaft mode with a single dual-role
  `KafkaNodePool` instead of ZooKeeper
- Phase 6 local validation also exposed a few recurring operational gotchas:
  - Strimzi operator in `strimzi-system` must watch `realrisk`
  - Schema Registry bootstrap must be plain `host:port`, not `PLAINTEXT://...`
  - after Kafka path changes, Flink should be restarted so pods consume the new ConfigMap
  - Phase 6 verification commands should run inside the Kubernetes Schema Registry pod,
    not the old Docker Compose container

### Next

1. Add schema registration helper
   - `scripts/register-schemas.ps1`
   - Register all `.avsc` files into local Schema Registry automatically
2. Phase 7 candidate
   - CloudNativePG or equivalent production-grade PostgreSQL operator
   - stronger production HA separation for Redis/PostgreSQL

---

## Repository Landmarks

### Flink

`flink-job/src/main/java/com/realrisk/flink/`

- `FlinkRiskJob.java`
  - Main wiring for Kafka sources, broadcast stream, side outputs, sinks
- `FlinkRiskJobConfig.java`
  - Env var config and defaults
- `MerchantBurstProcessFunction.java`
  - `KeyedBroadcastProcessFunction`
  - Broadcast state + side outputs
- `RiskRuleEngine.java`
  - Pure scoring logic, now profile-aware via `UserProfile`
- `RuleSet.java`
  - Immutable effective rule snapshot, including velocity thresholds
- `RedisUserProfileReader.java`
  - Reads `blacklist:<userId>` and `velocity:count:7d:<userId>` from Redis
- `UserProfile.java`
  - In-memory profile object passed into scoring
- `RiskEvaluation.java`
  - Internal evaluation record; should stay inside operator flow only
- `FlinkRiskMappers.java`
  - Internal evaluation -> Avro DTO mapping
- `AvroKafkaRecordSerializationSchema.java`
  - Kafka sink serializer wrapper

### Avro contracts

`src/main/avro/`

- `RiskEventAvro.avsc`
- `RiskDecisionAvro.avsc`
- `HighRiskEventAvro.avsc`
- `AlertEventAvro.avsc`
- `RuleUpdateAvro.avsc`

### Scripts

`scripts/`

- `run-api.ps1`
  - Starts Spring API with repo-local Maven and local infra defaults
- `send-rule-update.ps1`
  - Sends a `RuleUpdateAvro` message through the schema-registry container

### Alert Service

`alert-service/src/main/java/com/realrisk/alertservice/`

- `AlertServiceApplication.java`
  - Standalone Spring Boot entrypoint
- `service/AlertProcessor.java`
  - Idempotency, rate limiting, routing, and persistence flow
- `service/AlertEventListener.java`
  - Kafka consumer entrypoint for `AlertEventAvro`
- `notify/`
  - `NotificationRouter` plus stub channel implementations
- `rate/`
  - Redis-backed per-user severity limiter
- `persistence/AlertLogRepository.java`
  - `alert_log` insert/update/read access

### Infra

- `docker-compose.yml`
  - `realrisk-kafka`
  - `realrisk-schema-registry`
  - `realrisk-redis`
  - `realrisk-postgres`
- `k8s/`
  - `base/`
  - `overlays/local/`
  - `overlays/prod/`

---

## Runbook

### Start local infra

```powershell
docker compose up -d
```

For Phase 5 local Kubernetes tests, Docker Compose Kafka now also exposes:

- `host.docker.internal:19092`

### Build Flink job

```powershell
.\.tools\maven\bin\mvn.cmd -f .\flink-job\pom.xml -DskipTests package
```

### Start Flink job

```powershell
$env:RISK_WORKER_ENABLED = "false"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:SCHEMA_REGISTRY_URL = "http://localhost:8081"
$env:RULE_UPDATES_TOPIC = "rule-updates"
java -jar .\flink-job\target\realrisk-flink-job-0.1.0-SNAPSHOT.jar
```

### Seed Redis profile data manually

```powershell
docker exec realrisk-redis redis-cli SET blacklist:user-phase3 1
docker exec realrisk-redis redis-cli SET velocity:count:7d:user-phase3 125
docker exec realrisk-redis redis-cli DEL blacklist:user-phase3
docker exec realrisk-redis redis-cli DEL velocity:count:7d:user-phase3
```

### Start Spring API

```powershell
.\scripts\run-api.ps1 -DisableRiskWorker
```

### Build Kubernetes images

```powershell
.\scripts\build-k8s-images.ps1
```

### Load images into kind

```powershell
.\scripts\kind-load-images.ps1
```

### Install the Flink operator

```powershell
.\scripts\install-flink-operator.ps1
```

### Render the local Kubernetes overlay

```powershell
kubectl kustomize .\k8s\overlays\local
```

### Watch `decision-audit`

```bash
docker exec -it realrisk-schema-registry \
  kafka-avro-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic decision-audit \
    --from-beginning \
    --property schema.registry.url=http://schema-registry:8081
```

### Send a rule update

```powershell
.\scripts\send-rule-update.ps1 `
    -RuleId rule-large-amount-v1 `
    -RuleType large_amount `
    -Parameters @{ amount_cents = "3000000" }
```

### Send a raw event manually

Use the self-contained raw-event example in `README.md` under:

- `E2E Verification: Dynamic Rule Channel`

That section now includes the full inline `RiskEventAvro` schema and does not depend on a pre-existing `/tmp/schema.json`.

---

## Config Defaults

### FlinkRiskJobConfig

| Env var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL |
| `FLINK_LARGE_AMOUNT_CENTS` | `1_000_000` | Large-amount baseline |
| `FLINK_REVIEW_THRESHOLD` | `60` | `score >= 60` -> `REVIEW` |
| `FLINK_BLOCK_THRESHOLD` | `80` | `score >= 80` -> `BLOCK` |
| `FLINK_HIGH_RISK_THRESHOLD` | `85` | `score >= 85` -> emit high-risk event |
| `FLINK_ALERT_THRESHOLD` | `90` | `score >= 90` -> emit alert |
| `FLINK_MERCHANT_BURST_THRESHOLD` | `10` | Distinct users in window |
| `FLINK_VELOCITY_THRESHOLD_7D` | `100` | 7-day count for `high_velocity_7d` |
| `FLINK_HIGH_VELOCITY_SCORE` | `40` | Score added by `high_velocity_7d` |
| `FLINK_MERCHANT_BURST_WINDOW_MS` | `300_000` | 5 minutes |
| `FLINK_WATERMARK_SKEW_MS` | `5_000` | Allowed out-of-orderness |
| `FLINK_CHECKPOINTS_DIR` | `file:///tmp/realrisk-flink-checkpoints` | Local checkpoint path |
| `FLINK_PARALLELISM` | `2` | Local parallelism |
| `REDIS_HOST` | `localhost` | Redis host for Flink profile enrichment |
| `REDIS_PORT` | `6379` | Redis port for Flink profile enrichment |

### RuleSet parameters

| `ruleType` | Parameter keys |
|---|---|
| `large_amount` | `amount_cents`, `score_delta` |
| `withdrawal_without_device` | `score_delta` |
| `merchant_multi_user_burst` | `burst_threshold`, `score_delta` |
| `high_velocity_7d` | `velocity_threshold`, `score_delta` |
| `global` | `review_threshold`, `block_threshold` |

`enabled = false` means remove the rule from broadcast state and fall back to config defaults.

---

## Known Decisions

### Serialization and Flink operator boundaries

- `RiskEvaluation` should not be allowed to fan out across multiple downstream operators as a shared stream artifact
- Current design avoids that by emitting Avro outputs directly from `MerchantBurstProcessFunction`
- Broadcast rule state uses explicit Avro type information instead of bare class-based fallback

### Kafka / Schema Registry dependency alignment

- `flink-avro-confluent-registry:1.20.3` can pull an older `kafka-schema-registry-client`
- `kafka-avro-serializer:7.7.1` expects newer Confluent classes such as `RuleConditionException`
- `flink-job/pom.xml` explicitly pins:
  - `kafka-avro-serializer:7.7.1`
  - `kafka-schema-serializer:7.7.1`
  - `kafka-schema-registry-client:7.7.1`
- `schemaRegistryConfig()` also sets `rule.service.loader.enable=false`

### Offsets and replay behavior

- `raw-events` source starts from `latest()` on a fresh start
- checkpoint recovery restores offsets after restart
- `rule-updates` starts from `earliest()` because it must rebuild broadcast state

### Sink behavior

- Checkpoint interval is 30 seconds
- Kafka sinks are `AT_LEAST_ONCE`
- Local E2E latency of up to about 30 seconds is normal
- For local tests after a fresh Flink restart, `raw-events` must be produced after the
  new job is already running because the source starts from `latest()` on fresh groups

### Redis profile enrichment

- Flink uses synchronous Redis reads inside `MerchantBurstProcessFunction`
- This phase intentionally does not use `AsyncDataStream`
- Redis failures degrade to `UserProfile.empty()` instead of failing the job
- The current profile contract is:
  - `blacklist:<userId>` -> any string value means blacklisted
  - `velocity:count:7d:<userId>` -> integer count for the last 7 days

### Alert Service behavior

- `alert-service` uses its own Kafka consumer group: `alert-service`
- Routing is configuration-driven (via `AlertProperties`):
  - `MEDIUM` -> email
  - `HIGH` -> email + sms
  - `CRITICAL` -> email + sms + push
- Deduplication key is `alertId` (PostgreSQL `ON CONFLICT DO NOTHING`)
- `alert_log.status` tracks `PENDING` / `PROCESSED` / `RATE_LIMITED`
  - PENDING rows are retried on redelivery instead of being skipped as duplicates
- Redis key format for rate limiting: `alert:ratelimit:<userId>:<severity>`
- Consumer error handling: `DefaultErrorHandler` + `FixedBackOff(1s, 2 attempts)`
- Current channel implementations are logging stubs only
- DB migrations live in root `src/main/resources/db/migration/`; alert-service pulls them in via pom.xml resource include

### Phase 5 Kubernetes layout

- Kafka, Schema Registry, Redis, and PostgreSQL remain on Docker Compose in this phase
- Kubernetes workloads bridge back to those host services through:
  - Kafka: `host.docker.internal:19092`
  - Schema Registry: `http://host.docker.internal:8081`
  - Redis: `host.docker.internal:6379`
  - PostgreSQL: `host.docker.internal:55432`
- Flink checkpoints and savepoints are configured for:
  - `s3://realrisk-flink/checkpoints`
  - `s3://realrisk-flink/savepoints`
- Local MinIO provides the S3-compatible backend inside the cluster
- `kind` + `helm` local validation has been completed on this machine
- Compose Kafka must expose `host.docker.internal:19092` and may need a force-recreate after listener changes
- The `rule-updates` topic must exist before the K8s Flink job can stay healthy; after Kafka recreation it may need to be re-created

### Decision idempotency

- `decisionId` is deterministic from `eventId`
- Reusing the same `eventId` in manual tests will intentionally reuse the same `decisionId`

---

## Known Issues / Watch List

- Flink still logs `GenericType` info messages for some Avro fields such as `parameters` and `reasons`
  - not currently blocking
  - worth revisiting before production-hardening checkpoint/state guarantees
- Local Windows + Docker + Flink runs can be noisy; always focus on the first `Caused by:` line, not the later `CANCELING/CANCELED` cleanup logs
- Maven is not installed globally on this machine; prefer repo-local Maven:
  - `.\.tools\maven\bin\mvn.cmd`
- `kafka-avro-console-consumer --from-beginning --timeout-ms ...` timing out with
  `Processed a total of 0 messages` is a valid signal that the topic is actually empty,
  not necessarily a consumer bug
- `alert-service` integration test is written with Testcontainers but currently auto-skips in
  this environment because the JVM cannot see a valid Docker socket
- `alert-service` now has explicit consumer retry/backoff, but still has no dead-letter topic
  - acceptable for Phase 4, but should be tightened before K8s / production rollout
- `alert-service` packaging can fail if `alert-service-0.1.0-SNAPSHOT.jar` is locked by a running
  local process; stop the running jar before rebuilding the image layer
- Long-lived `kubectl exec -it ... kafka-avro-console-consumer` sessions can exit with `137`
  during local validation; prefer one-shot produce/consume checks when debugging Phase 6

---

## Session Notes

- This file is intended to reduce context loss across long AI-assisted sessions
- Keep it concise enough to scan in under a minute
- Prefer updating facts here over repeating them only in chat history
