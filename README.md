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
mvn spring-boot:run
```

To run the API Gateway while a Flink job owns async decisions, disable the Phase 1
stub worker:

```powershell
$env:RISK_WORKER_ENABLED="false"
mvn spring-boot:run
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
