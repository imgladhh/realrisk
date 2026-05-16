# RealRisk Session Memory

> Living context for AI-assisted development sessions.
> Update `Done`, `In Progress`, `Next`, and `Known Issues` at the end of each session.
> Last updated: 2026-05-15

---

## Project Summary

RealRisk is a two-tier payment risk engine:

- Fast path: Spring Boot API Gateway -> Redis blacklist check + Lua per-user rate limit -> publish `RiskEventAvro` to `raw-events`
- Slow path: Flink job consumes `raw-events`, evaluates rules, emits `RiskDecisionAvro` to `decision-audit`, and emits `HighRiskEventAvro` / `AlertEventAvro` to side outputs
- Dynamic rules: compacted `rule-updates` topic -> Flink broadcast state -> `RuleSet` rebuilt per event

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
- Tooling / docs
  - `scripts/run-api.ps1`
  - `scripts/send-rule-update.ps1`
  - README updated with Phase 2c and E2E walkthrough

### In Progress

- No active implementation branch at the moment
- Main focus has shifted from fixing Flink startup/runtime issues to planning the next phase cleanly

### Next

1. Phase 3: async Redis profile enrichment in Flink
   - Add async lookup before rule evaluation
   - Enrich with profile fields such as blacklist, velocity, lifetime amount
   - Extend `RiskRuleEngine` with profile-aware rules
2. Add schema registration helper
   - `scripts/register-schemas.ps1`
   - Register all `.avsc` files into local Schema Registry automatically
3. Phase 4: Alert Service
   - Consumer for `alert-events`
   - Start with stdout/log stub, then add real notification integration
4. Phase 5: K8s / Flink operator deployment path
   - FlinkDeployment CRD
   - MinIO or S3 checkpoint storage
   - Operator-based local/prod alignment

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
  - Pure scoring logic
- `RuleSet.java`
  - Immutable effective rule snapshot
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

### Infra

- `docker-compose.yml`
  - `realrisk-kafka`
  - `realrisk-schema-registry`
  - `realrisk-redis`
  - `realrisk-postgres`

---

## Runbook

### Start local infra

```powershell
docker compose up -d
```

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

### Start Spring API

```powershell
.\scripts\run-api.ps1 -DisableRiskWorker
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
| `FLINK_MERCHANT_BURST_WINDOW_MS` | `300_000` | 5 minutes |
| `FLINK_WATERMARK_SKEW_MS` | `5_000` | Allowed out-of-orderness |
| `FLINK_CHECKPOINTS_DIR` | `file:///tmp/realrisk-flink-checkpoints` | Local checkpoint path |
| `FLINK_PARALLELISM` | `2` | Local parallelism |

### RuleSet parameters

| `ruleType` | Parameter keys |
|---|---|
| `large_amount` | `amount_cents`, `score_delta` |
| `withdrawal_without_device` | `score_delta` |
| `merchant_multi_user_burst` | `burst_threshold`, `score_delta` |
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

---

## Session Notes

- This file is intended to reduce context loss across long AI-assisted sessions
- Keep it concise enough to scan in under a minute
- Prefer updating facts here over repeating them only in chat history
