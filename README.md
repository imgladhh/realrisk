# RealRisk

Phase 1 implements the vertical slice from `ARCHITECTURE.md`; Phase 2 has started with
Avro + Schema Registry on every Kafka boundary:

- API Gateway fast path: Redis blacklist check + Lua per-user rate limit
- Kafka topics: `raw-events`, `raw-audit`, `decision-audit`, `high-risk-events`, `alert-events`
- Audit writer: verbatim `raw-events` to `raw-audit`
- Risk worker: async rule stub to `decision-audit`
- Redis materializer: async blacklist projection from blocking decisions
- PostgreSQL archivers: `raw_events` and `risk_decisions`
- Avro contracts: `src/main/avro/RiskEventAvro.avsc`, `src/main/avro/RiskDecisionAvro.avsc`
- Schema Registry: Confluent-compatible registry at `http://localhost:8081`

## Local Run

```powershell
docker compose up -d
.\scripts\run-api.ps1
```

To run the API Gateway while a Flink job owns async decisions, disable the Phase 1
stub worker:

```powershell
.\scripts\run-api.ps1 -DisableRiskWorker
```

Kafka messages are Avro-encoded and require Schema Registry. Check local registry
connectivity with:

```powershell
Invoke-RestMethod http://localhost:8081/subjects
```

## Phase 2 Notes

Apache Flink 2.1.2 is the current stable Flink line, but the official Flink 2.1
Kafka connector documentation says there is no connector available yet for Flink
2.1. The Flink job should therefore be introduced on the Flink 1.20.x connector
line first, then upgraded once the 2.x Kafka connector catches up.

## Flink Job

The Phase 2 Flink job lives in [flink-job](E:/realRisk/flink-job) and consumes
`raw-events` directly from Kafka using `RiskEventAvro`.

Current behavior:

- Writes one `RiskDecisionAvro` record per input event to `decision-audit`
- Writes `HighRiskEventAvro` to `high-risk-events` for scores `>= 85`
- Writes `AlertEventAvro` to `alert-events` for scores `>= 90`
- Includes one cross-user sliding-window rule stub: the same merchant hit by
  `10` distinct users within `5` minutes
- On a first start without checkpoints, the Kafka source begins at `latest` to
  avoid replaying the entire retained `raw-events` backlog by accident
- Rebuilds live rule parameters from the compacted `rule-updates` topic, which
  is always replayed from `earliest` on a fresh start to repopulate broadcast state

Build and test:

```powershell
mvn -f .\flink-job\pom.xml test
mvn -f .\flink-job\pom.xml -DskipTests package
```

Run locally with the Spring stub disabled:

```powershell
$env:RISK_WORKER_ENABLED="false"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:SCHEMA_REGISTRY_URL="http://localhost:8081"
$env:RULE_UPDATES_TOPIC="rule-updates"
java -jar .\flink-job\target\realrisk-flink-job-0.1.0-SNAPSHOT.jar
```

The shaded jar includes the Flink runtime needed for local `java -jar` execution.

If Maven is not installed locally, run it through Docker:

```powershell
docker run --rm -v ${PWD}:/workspace -w /workspace maven:3.9-eclipse-temurin-21 mvn test
```

If Testcontainers cannot connect to Docker Desktop from the JVM, start Redis with Compose and
point the Redis tests at it:

```powershell
docker compose up -d redis
mvn -Drealrisk.test.redis.host=localhost -Drealrisk.test.redis.port=6379 test
```

To include the Redis Materializer audit-bans integration test, also start Postgres and pass
its JDBC URL:

```powershell
docker compose up -d redis postgres
mvn "-Drealrisk.test.redis.host=localhost" `
    "-Drealrisk.test.redis.port=6379" `
    "-Drealrisk.test.postgres.url=jdbc:postgresql://localhost:55432/realrisk" `
    "-Drealrisk.test.postgres.user=realrisk" `
    "-Drealrisk.test.postgres.password=realrisk" `
    test
```

## Phase 2c: Dynamic Rule Channel

Rules are managed through the compacted `rule-updates` Kafka topic. The Flink
job replays the topic from `earliest` on every fresh start to rebuild broadcast
state before processing any events. Changes take effect after the next Flink
checkpoint (about 30 s).

### Supported rule types

| `ruleType` | Parameters | Effect |
|---|---|---|
| `large_amount` | `amount_cents` (long), `score_delta` | Flags transactions above the threshold |
| `withdrawal_without_device` | `score_delta` | Flags deviceFp-less withdrawals |
| `merchant_multi_user_burst` | `burst_threshold`, `score_delta` | Flags merchants hit by N distinct users in the burst window |
| `global` | `review_threshold`, `block_threshold` | Overrides the decision boundaries |

### Sending a rule update

Use the helper script (requires `docker compose up -d` with
`realrisk-schema-registry` running):

```powershell
# Raise the large-amount threshold to USD 30 000
.\scripts\send-rule-update.ps1 `
    -RuleId rule-large-amount-v1 `
    -RuleType large_amount `
    -Parameters @{ amount_cents = "3000000" }

# Disable the rule - engine falls back to config default (USD 10 000)
.\scripts\send-rule-update.ps1 `
    -RuleId rule-large-amount-v1 `
    -RuleType large_amount `
    -Enabled $false
```

---

## E2E Verification: Dynamic Rule Channel

This three-scenario sequence proves the full broadcast state -> decision pipeline.
Run `docker compose up -d` and start the Flink job before beginning.

**Terminal A** - watch decisions arriving on `decision-audit`:

```bash
docker exec -it realrisk-schema-registry \
  kafka-avro-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic decision-audit \
    --from-beginning \
    --property schema.registry.url=http://schema-registry:8081
```

### Scenario 1 - Default rules -> BLOCK

Send an event with `amountCents=2000000` (above the 1 000 000-cent default):

```powershell
.\scripts\send-rule-update.ps1 `
    -RuleId rule-large-amount-v1 `
    -RuleType large_amount `
    -Enabled $false          # ensure default is active (no override)
```

```bash
# Terminal B - inside realrisk-schema-registry container
EVENT_SCHEMA='{"type":"record","name":"RiskEventAvro","namespace":"com.realrisk.avro","fields":[{"name":"eventId","type":"string"},{"name":"requestId","type":"string"},{"name":"userId","type":"string"},{"name":"eventType","type":"string"},{"name":"timestamp","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"amountCents","type":"long"},{"name":"currency","type":"string"},{"name":"ipAddress","type":["null","string"],"default":null},{"name":"deviceFp","type":["null","string"],"default":null},{"name":"merchantId","type":["null","string"],"default":null},{"name":"counterparty","type":["null","string"],"default":null},{"name":"source","type":"string"}]}'
echo '{"eventId":"evt-e2e-01","requestId":"req-e2e-01","userId":"user-e2e-01","eventType":"TRANSACTION","timestamp":1747224300000,"amountCents":2000000,"currency":"USD","ipAddress":null,"deviceFp":{"string":"device-e2e-01"},"merchantId":{"string":"merchant-e2e-01"},"counterparty":null,"source":"api-gateway"}' | \
kafka-avro-console-producer \
  --bootstrap-server kafka:29092 \
  --topic raw-events \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$EVENT_SCHEMA"
```

Expected (within 30 s): `"eventId":"evt-e2e-01","decision":"BLOCK","riskScore":80,"reasons":["large_amount"]`

### Scenario 2 - Dynamic threshold raise -> ALLOW

Raise the threshold to 3 000 000 cents, then send the same amount:

```powershell
.\scripts\send-rule-update.ps1 `
    -RuleId rule-large-amount-v1 `
    -RuleType large_amount `
    -Parameters @{ amount_cents = "3000000" }
```

Wait about 10 s, then send `evt-e2e-02` (same `amountCents=2000000`, new `eventId`).

Expected: `"eventId":"evt-e2e-02","decision":"ALLOW","riskScore":0,"reasons":[]`

### Scenario 3 - Disable rule -> BLOCK restored

```powershell
.\scripts\send-rule-update.ps1 `
    -RuleId rule-large-amount-v1 `
    -RuleType large_amount `
    -Enabled $false
```

Wait about 10 s, then send `evt-e2e-03`.

Expected: `"eventId":"evt-e2e-03","decision":"BLOCK","riskScore":80,"reasons":["large_amount"]`

---

## Ingest Example

```powershell
Invoke-RestMethod -Method Post http://localhost:8080/events `
  -ContentType "application/json" `
  -Body '{
    "requestId":"req-1",
    "userId":"user-123",
    "eventType":"TRANSACTION",
    "amountCents":1200,
    "currency":"USD",
    "source":"manual"
  }'
```
