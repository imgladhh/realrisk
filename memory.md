# RealRisk Session Memory

> Living context for AI-assisted development sessions.
> Update `Done`, `In Progress`, `Next`, and `Known Issues` at the end of each session.
> Last updated: 2026-05-20 (Phase 11 auth and HPA acceptance recorded)

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
- Phase 7
  - Micrometer business counters/timers added for API Gateway and Alert Service
  - Alert Service exposes approximate Kafka consumer lag gauge via `AdminClient`
  - Flink custom `DecisionMetrics` using flat counter names (`decision_allow/review/block`) via untagged groups
  - Flink Prometheus Reporter enabled in `FlinkDeployment` (`port: 9249`)
  - `ServiceMonitor` for api-gateway and alert-service; `PodMonitor` for Flink TM + JM
  - `scripts/install-monitoring.ps1` installs `kube-prometheus-stack` (chart pinned at 65.1.0)
  - Grafana dashboard ConfigMap with 6 panels under `k8s/base/monitoring/`
  - `management.metrics.tags.application` and `percentiles` configured in `application.yml`
  - API Gateway Latency panel uses `http_server_requests_seconds_max` (P99 histogram not supported by Spring Boot 3.3.x observation timer via YAML config)
  - Validation:
    - All 6 Grafana panels showing live data
    - Decision Distribution: ALLOW / REVIEW / BLOCK visible in piechart
    - Checkpoint Duration and Size: live data from JobManager PodMonitor
    - Alert Processing Rate: CRITICAL confirmed after velocity + large_amount forcing
    - API Gateway Latency: `http_server_requests_seconds_max{uri="/events"}` live
- Phase 8
  - Added GitHub Actions workflow under `.github/workflows/ci.yaml`
    - combined `build-and-push` job with Maven packaging before Docker builds
    - deploy step injects SHA-tagged images into the overlay before a single `kubectl apply`
  - Added `alert-events-dlq` Kafka topic CRD with 7-day retention
  - `alert-service` now consumes raw Kafka bytes, decodes Avro in-process, and forwards exhausted failures to DLQ
  - DLQ publisher adds headers:
    - `x-exception-message`
    - `x-retry-count`
    - `x-original-topic`
    - `x-failed-at`
  - Added `alert.dlq.published` Micrometer counter
  - Added PrometheusRule alerts:
    - `AlertServiceConsumerLagHigh`
    - `AlertServiceDown`
    - `FlinkCheckpointFailures`
    - `ApiGatewayHighErrorRate`
  - `install-monitoring.ps1` now creates placeholder Alertmanager receiver secrets directly in the `monitoring` namespace
  - Validation:
    - malformed payload published to `alert-events` was forwarded to `alert-events-dlq`
    - DLQ record verified with required headers and original payload
    - `alert_dlq_published_total{severity="unknown"} 1.0` confirmed in `alert-service` actuator metrics
    - `AlertServiceDown` fired after scaling `alert-service` to `0`
    - `alert_consumer_lag{namespace="realrisk"}` returned after scaling `alert-service` back to `1`
- Phase 9
  - Migrated DB credential source to CloudNativePG pattern:
    - `api-gateway` and `alert-service` read DB credentials from `realrisk-cluster-app` Secret
    - DB URL set to `jdbc:postgresql://realrisk-cluster-rw.realrisk.svc.cluster.local:5432/realrisk`
    - Validation (method A): temporary `realrisk-cluster-app` Secret + `realrisk-cluster-rw` ExternalName Service pointing back to Bitnami PostgreSQL; both pods recovered 1/1 Running ✅
  - Added `k8s/base/postgresql/cluster.yaml` (CloudNativePG `Cluster` CRD, 2 instances)
  - Switched Redis wiring to Sentinel-aware config:
    - Flink: `buildRedisUri()` in `MerchantBurstProcessFunction`, falls back to direct host/port when sentinel fields empty
    - alert-service / api-gateway: `spring.data.redis.sentinel.master/nodes` env vars
    - Bitnami Redis Helm values: `sentinel.enabled: true`, `quorum: 2`, `replicaCount: 2`
  - Added real notification channels:
    - `EmailNotificationChannel` (JavaMailSender, `NOTIFICATION_EMAIL_ENABLED`)
    - `PushNotificationChannel` (Slack webhook via RestClient, guards missing webhook URL)
    - `SmsNotificationChannel` (WARN stub only)
    - `realrisk-notification-secrets` holds SMTP/Slack credentials (created out-of-band)
  - Replaced broker JMX metrics path with Strimzi `kafkaExporter`:
    - `kafkaExporter.topicRegex` / `groupRegex` added to Kafka CR
    - `k8s/base/kafka/service-kafka-exporter.yaml` added manually (Strimzi 3.9.x does not auto-create Service)
    - `service-monitor-kafka-exporter.yaml` targets `strimzi.io/name: realrisk-kafka-kafka-exporter`
    - `AlertServiceConsumerLagHigh` uses `sum(kafka_consumergroup_lag{...})` (broker-side)
    - Validation: `kafka_consumergroup_lag` appeared after alert-service committed offsets on all partitions; `AlertServiceConsumerLagHigh` fired; `AlertServiceDown` fired then resolved ✅
  - Added `DlqReplayTool` + `replay-dlq.ps1`:
    - Tool runs in-cluster via `kubectl run --rm --attach` (not locally via Maven)
    - `pom.xml` root: `spring-boot-maven-plugin` now uses `<layout>ZIP</layout>` to enable `-Dloader.main` PropertiesLauncher override
    - `AlertDlqPublisher` now writes `severity` header so `--severity HIGH` filter works
    - `waitForAssignment` extended to 40 attempts (~14 s) for K8s coordinator latency
    - Dry-run confirmed in-cluster ✅; execute path confirmed (DLQ → alert-events → alert-service re-consumed) ✅
  - Validation (core paths):
    - `realrisk-cluster-app` Secret read path: ✅ (method A — temporary Secret + ExternalName Service)
    - `kafka_consumergroup_lag` appeared in exporter after committed offset baseline established ✅
    - `AlertServiceConsumerLagHigh` fired at lag > 10 ✅
    - `AlertServiceDown` fired and resolved on scale-to-0 / scale-to-1 cycle ✅
    - DLQ replay in-cluster: dry-run matched and printed DLQ records ✅
    - DLQ replay execute: message flowed DLQ → alert-events → alert-service re-consumed it ✅
  - Validation (infra acceptance):
    - CNPG primary+replica both Running; Flyway migrations ran successfully on CNPG ✅
    - Business smoke test: POST /events → ACCEPTED → raw_events row visible in CNPG ✅
    - CNPG failover: deleted primary realrisk-cluster-1 → realrisk-cluster-2 promoted; post-failover event landed in new primary ✅
    - Redis Sentinel: 3 nodes (1 master + 2 replicas), quorum 2, num-other-sentinels=2 ✅
    - Redis Sentinel failover: deleted master realrisk-redis-node-2 → new master elected; post-failover API request returned 202 ACCEPTED ✅
  - Not validated (deferred, non-blocking):
    - Real notification channel delivery (email/Slack): pending `realrisk-notification-secrets` with live credentials
    - DLQ replay → alert_log PROCESSED: deferred (replay tool path proven; alert-service processing proven in Phase 4/6/8)
- Phase 10
  - `/admin/rules` REST endpoint in api-gateway: POST (upsert), GET (list), DELETE (disable)
  - DB migration `V4__rules_outbox.sql`: `rules` table + `rule_outbox` table + unpublished index
  - `RuleService`: transactional upsert + disable writing atomically to both tables
  - `publishPendingOutbox()` @Scheduled poller: publishes to `rule-updates` Kafka topic, marks `published_at`; breaks on first failure to preserve ordering
  - `RuleUpdatePublisher`: wraps `KafkaTemplate` with 10 s blocking send
  - `Clock` injected via `@Bean` in `RealRiskApplication`; `RuleService` has single Spring-injectable constructor
  - `RiskProperties` extended with `RuleOutbox(pollIntervalMs, batchSize)`
  - `@EnableScheduling` added to `RealRiskApplication`
  - CI workflow: `packages: write` permission, GHCR image names, kubeconfig from `secrets.KUBECONFIG` base64-decoded, deploy job gated on `workflow_dispatch`
  - `install-infra.ps1`: parameterized notification secrets, local Helm binary fallback
  - K8s ConfigMaps migrated to standard Spring env var names for Redis Sentinel:
    - `SPRING_DATA_REDIS_SENTINEL_MASTER` / `SPRING_DATA_REDIS_SENTINEL_NODES`
    - applied to both `base/` configmaps and all overlays (local + prod)
  - Tests: `RuleServiceTest` (upsertRule, disableRule 404, publishPendingOutbox ordering break), `AdminRulesControllerTest` (POST 201, GET 200, DELETE 204)
  - Phase 10 validation notes:
    - local kind validation should use a unique image tag plus `kind load docker-image`; mutable reuse of an older tag can leave the node running stale application contents
    - after any `kubectl apply -k .\k8s\overlays\local`, re-check the deployment image because the overlay resets the api-gateway image back to its rendered tag and may overwrite a temporary validation image set via `kubectl set image`
  - Kubernetes validation (commit 01a5faa):
    - `POST /admin/rules` → 201, rule written to `rules` table ✅
    - `GET /admin/rules` → 200, rule listed with `enabled=true` ✅
    - `rule_outbox` enable record `published_at` filled within 2 s ✅
    - POST /events with `amountCents=600000` → `BLOCK / {large_amount}` in `risk_decisions` ✅
    - `DELETE /admin/rules/{ruleId}` → 204, `rules.enabled=false` ✅
    - `rule_outbox` disable record `published_at` filled ✅
    - POST /events with `amountCents=600000` after disable → `ALLOW / score=0 / {}` ✅
    - Full chain: rule persistence → outbox → Kafka `rule-updates` → Flink broadcast state → decision live/retracted ✅
- Phase 11
  - Added API key protection for `/admin/**`
    - `AdminApiKeyFilter`: constant-time `Authorization: Bearer <token>` comparison via `MessageDigest.isEqual`
    - `SecurityConfig`: stateless Spring Security chain, filter inserted before `AnonymousAuthenticationFilter`
    - `RealRiskApplication`: excludes `UserDetailsServiceAutoConfiguration` to avoid default generated-password noise
  - Added api-gateway HPA
    - `k8s/base/api-gateway/hpa.yaml`: autoscaling/v2, CPU target 60%, min 2 / max 8
    - local overlay patch: min 1 / max 3 / CPU 70 for kind
    - removed `spec.replicas` from the base Deployment so HPA owns replica count
  - Added admin API key secret wiring
    - `deployment.yaml` reads `ADMIN_API_KEY` from `realrisk-admin-api-key`
    - `scripts/install-infra.ps1` now supports `-AdminApiKey` and creates the Secret idempotently
  - Tests
    - `AdminApiKeyFilterTest` covers:
      - no token → 401
      - wrong token → 401
      - correct token → 200
      - `/events` and `/actuator/health` bypass auth
  - Kubernetes validation (local kind, commit `b38793d`)
    - `realrisk-admin-api-key` Secret created in `realrisk` namespace ✅
    - api-gateway rolled out on a unique image tag: `realrisk-api:phase11-admin-hpa` ✅
    - `POST /admin/rules` without token → `401` ✅
    - `POST /admin/rules` with `Authorization: Bearer dev-only-insecure` → `201` ✅
    - `rules` table contains `p11-test` with `enabled = true` ✅
    - `POST /events` without token → `202 ACCEPTED` ✅
    - `kubectl get hpa -n realrisk` shows `realrisk-api-gateway-hpa` with `MINPODS=1`, `MAXPODS=3`, `REPLICAS=1` ✅
- Tooling / docs
  - `scripts/run-api.ps1`
  - `scripts/send-rule-update.ps1`
  - `scripts/register-schemas.ps1` - registers all 5 Avro schemas into SR under the correct topic subjects
  - `scripts/install-monitoring.ps1` - installs kube-prometheus-stack into `monitoring` namespace
  - README updated with Phase 2c and E2E walkthrough

### In Progress

1. Notification delivery validation
   - update `realrisk-notification-secrets` with live SMTP and Slack webhook values
   - restart `realrisk-alert-service`
   - trigger a CRITICAL alert and confirm Slack + email receipt

### Next

1. Complete notification delivery validation
   - Populate `realrisk-notification-secrets` with live SMTP credentials and Slack webhook URL
   - Trigger a CRITICAL alert and verify email + Slack receipt end-to-end
   - Validate AlertManager receiver (PagerDuty / webhook) if routing rules are configured
2. Phase 11 closeout
   - record final notification acceptance evidence
   - decide whether to keep `ADMIN_API_KEY=dev-only-insecure` as local default or move to install-time-only override
3. Phase 12 (to be specced)
   - candidates: rate-limit visibility API, Flink savepoint automation, prod overlay hardening, alert-service autoscaling strategy

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
- `register-schemas.ps1`
  - Registers all `.avsc` files into Schema Registry under the correct topic subjects
  - Idempotent: safe to re-run; returns existing id for unchanged schemas
  - `-SchemaRegistryUrl` defaults to `http://localhost:8081`
- `install-monitoring.ps1`
  - Installs `kube-prometheus-stack` into the `monitoring` namespace
  - Creates placeholder `realrisk-alertmanager-secrets` in `monitoring`
  - Applies the RealRisk Grafana dashboard ConfigMap after the Helm install

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
- DLQ behavior:
  - topic name defaults to `alert-events-dlq`
  - exhausted failures are published as raw bytes with failure headers
  - `ALERT_MAX_RETRIES` and `ALERT_RETRY_BACKOFF_MS` are config-driven
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

### Phase 9 storage / HA direction

- PostgreSQL is moving from Bitnami Helm to CloudNativePG
  - base cluster resource: `realrisk-cluster`
  - application DB credentials come from `realrisk-cluster-app`
  - DB URL target should be `jdbc:postgresql://realrisk-cluster-rw.realrisk.svc.cluster.local:5432/realrisk`
- Redis is moving from single-node host/port wiring to Sentinel:
  - `REDIS_SENTINEL_MASTER=mymaster`
  - `REDIS_SENTINEL_NODES=<sentinel host list>`
  - applications should prefer Sentinel config over legacy `REDIS_HOST` / `REDIS_PORT`
- Notification secrets are intentionally created out-of-band by `scripts/install-infra.ps1`
  - secret name: `realrisk-notification-secrets`
  - namespace: `realrisk`
  - do not commit notification credentials to git

### Phase 9 broker-side lag metrics

- Do not use broker JMX / `metricsConfig` to implement consumer group lag alerts
- Strimzi `kafkaExporter` is the correct source for:
  - `kafka_consumergroup_lag`
  - related consumer group offset metrics
- `AlertServiceConsumerLagHigh` should evaluate broker-side lag via:
  - `kafka_consumergroup_lag{consumergroup="alert-service",topic="alert-events"}`
- `AlertServiceDown` remains separate and still covers the “consumer service disappeared” case

### Phase 9 DLQ replay approach

- `scripts/replay-dlq.ps1` is a thin PowerShell wrapper that runs `DlqReplayTool` **in-cluster** via `kubectl run --rm --attach --restart=Never`
  - Do NOT run DlqReplayTool locally via Maven: port-forward only reaches the bootstrap endpoint; Kafka metadata returns in-cluster `.svc` broker addresses that a local JVM cannot resolve
  - The in-cluster pod uses the `realrisk-api` image (contains the fat JAR)
  - `pom.xml` root must have `<layout>ZIP</layout>` in `spring-boot-maven-plugin` to enable PropertiesLauncher (`-Dloader.main`)
  - bootstrap address used in-cluster: `realrisk-kafka-kafka-bootstrap.realrisk.svc.cluster.local:9092`
- Replay uses raw Kafka `byte[]` consume / produce so Avro payload bytes are preserved exactly
- Default mode is dry-run; `-Execute` mode republishes to `alert-events` and adds `x-replayed-at` / `x-replayed-by` headers
- `AlertDlqPublisher` writes a `severity` header on every DLQ write (required for `--severity` filter to work)
- `kafka_consumergroup_lag` only appears in exporter after the consumer group has committed offsets on a partition; `-1` means no committed offset baseline (not zero lag)

### Phase 9 acceptance addendum (2026-05-20)

- Local Helm fallback was installed under `.tools/helm/windows-amd64/helm.exe`, and both `scripts/install-cnpg.ps1` and `scripts/install-infra.ps1` now use that fallback when `helm` is not present on PATH
- `install-infra.ps1 -Overlay local` succeeded, including:
  - CloudNativePG operator install in `cnpg-system`
  - Strimzi operator refresh
  - Redis Helm upgrade
  - placeholder `realrisk-notification-secrets` creation
- `kubectl apply -k .\k8s\overlays\local` succeeded after removing the temporary method-A `realrisk-cluster-rw` Service that blocked CNPG ownership
- CNPG validation completed:
  - `realrisk-cluster` reached `READY 2/2`
  - Flyway tables were present in CNPG (`alert_log`, `raw_events`, `risk_decisions`, etc.)
  - accepted event `req-phase9-cnpg-01` landed in CNPG `raw_events`
  - deleting `realrisk-cluster-1` promoted `realrisk-cluster-2`, and a post-failover event (`user-failover-01`) also landed in `raw_events`
- Redis Sentinel validation completed:
  - local Redis now runs as 1 master + 2 replicas with 3 sentinel sidecars (`replica.replicaCount: 3`)
  - `SENTINEL masters` reported `num-slaves=2` and `num-other-sentinels=2`
  - deleting the master pod promoted a new master, and a post-failover API request still returned `202 ACCEPTED`
- Kafka exporter / broker-side lag validation remains complete:
  - `kafka_consumergroup_lag` emitted after committed offset baseline was established
  - `AlertServiceConsumerLagHigh` fired
  - `AlertServiceDown` fired and later resolved
- DLQ replay validation remains complete at the core-path level:
  - in-cluster `DlqReplayTool` dry-run worked
  - replayed DLQ message flowed back to `alert-events` and was re-consumed by `alert-service`
  - a fresh `alert_log` `PROCESSED` row is still deferred because that requires cleaner replay sample data, not further replay-tool code changes

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
- `alert-service` integration test runs on GitHub Actions (Docker available) but auto-skips locally (no Docker socket)
- `AlertServiceIntegrationTest` requires explicit `@DynamicPropertySource` overrides for all Phase 9 new config: `spring.mail.*`, `NOTIFICATION_EMAIL_ENABLED=false`, `NOTIFICATION_SLACK_ENABLED=false`, `spring.data.redis.sentinel.*`; omitting these causes `Failed to load ApplicationContext` on CI runners (commit 56558db)
- GitHub CI: use `mvn` (system Maven), not `./.tools/maven/bin/mvn` — the `.tools` directory does not exist on GitHub runners
- Phase 10 kind image loading: `docker tag <old> <new>` only re-labels; always run `docker build` first, then `kind load docker-image <new-tag>`, then `kubectl set image`; same-tag images in kind nodes are not evicted automatically
- Phase 10 Redis Sentinel env var naming: apps expect standard Spring Boot names (`SPRING_DATA_REDIS_SENTINEL_MASTER` / `SPRING_DATA_REDIS_SENTINEL_NODES`); custom-named vars (e.g. `REDIS_SENTINEL_*`) are silently ignored and cause Spring to fall back to `localhost:6379`
- Phase 11 first-hit `/events` after api-gateway rollout may transiently return `503 redis_unavailable` while Lettuce warms up Redis Sentinel connections; a retry succeeded and the steady-state acceptance result remained `202`
- HPA existence is validated in local kind, but `ScalingActive=False` is expected when the cluster lacks `metrics-server` / `pods.metrics.k8s.io`; this does not invalidate the “HPA object present and wired” acceptance
- `alert-service` packaging can fail if `alert-service-0.1.0-SNAPSHOT.jar` is locked by a running
  local process; stop the running jar before rebuilding the image layer
- Long-lived `kubectl exec -it ... kafka-avro-console-consumer` sessions can exit with `137`
  during local validation; prefer one-shot produce/consume checks when debugging Phase 6
- For Phase 7 local monitoring bring-up, run `scripts/install-monitoring.ps1` before
  `kubectl apply -k .\k8s\overlays\local` so the ServiceMonitor and PodMonitor CRDs exist
- `AlertServiceConsumerLagHigh` and `AlertServiceDown` intentionally cover different cases:
  - lag high = service is alive but falling behind
  - absent metric = service is down / missing from scrape targets
- Strimzi `kafkaExporter` does NOT auto-create its own Kubernetes Service in Strimzi 3.9.x; `service-kafka-exporter.yaml` must be applied manually alongside the ServiceMonitor
- `kafka_consumergroup_lag` is emitted only for partitions with committed consumer group offsets; the `sum()` in `AlertServiceConsumerLagHigh` will be negative if some partitions return `-1`, so all partitions must have committed offsets before triggering the alert with a specific lag threshold
- `replay-dlq.ps1` must run in-cluster (see Phase 9 DLQ replay approach note above); local Maven + port-forward is NOT a valid execution model for this tool

---

## Session Notes

- This file is intended to reduce context loss across long AI-assisted sessions
- Keep it concise enough to scan in under a minute
- Prefer updating facts here over repeating them only in chat history
