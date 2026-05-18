---
name: realrisk-alert-e2e
description: Use when validating the RealRisk high-risk and alert path end to end. Covers how to force high-score events, check alert Kafka outputs, verify PostgreSQL alert_log rows, and avoid mixing Docker Compose and Kubernetes validation paths.
---

# RealRisk Alert E2E

Use this skill for RealRisk tasks that involve:

- validating `alert-events`
- checking `alert_log`
- forcing a `HIGH` or `CRITICAL` path for a test user
- troubleshooting whether Flink or `alert-service` is responsible for a missing alert

## Default workflow

1. Decide whether the target path is Phase 4/5 hybrid infra or Phase 6 in-cluster infra.
2. Decide whether the event should enter through API Gateway or be injected directly into Kafka. On this Windows workstation, prefer the API Gateway HTTP path when possible so Kafka Avro serialization happens inside the service instead of in ad hoc shell commands.
3. To force a reliable alert for the API Gateway path, prefer `velocity + large_amount` over Redis blacklist.
3. Verify in order:
   - event reached `raw-events`
   - decision reached `decision-audit`
   - alert-worthy event reached `alert-events` if needed
   - `alert_log` row exists in PostgreSQL

## Reliable forcing patterns

### Preferred forcing pattern for API Gateway validation

- Set `velocity:count:7d:<userId> = 200`
- Send a fresh HTTP `POST /events` with:
  - `amountCents > 1_000_000`
  - a non-blacklisted `userId`
  - required request fields including `source`
- Expect:
  - `decision = BLOCK`
  - `riskScore >= 90` (typically `120`)
  - reasons include `high_velocity_7d` and `large_amount`
  - the event is accepted by API Gateway and reaches `raw-events`

### Kafka-only forcing pattern

- Only use Redis blacklist when bypassing API Gateway and producing directly to Kafka.
- Set `blacklist:<userId> = 1`
- Send a fresh event for that user directly into Kafka
- Expect:
  - `decision = BLOCK`
  - `riskScore = 100`
  - `reason_summary` or `reasons` contains `blacklisted_user`

## Project-specific rules

- If the score is only `80` from `large_amount`, do not expect it in `alert-events` or `alert_log`.
- Do **not** use `blacklist:<userId>` when validating through API Gateway; the gateway checks the same key first and returns HTTP `403`, so the event never reaches Kafka or Flink.
- For API Gateway ingress tests, if you see HTTP `400`, check the request contract before blaming Kafka. `source` is required.
- For API Gateway ingress tests, if you see HTTP `403`, check for leftover blacklist keys before checking Flink.
- For API Gateway ingress tests, if you see HTTP `429`, vary `userId` or clear rate-limit state before treating it as a metrics or Kafka problem.
- Restart `alert-service` after Kafka/bootstrap path changes so it picks up the current config.
- For concrete commands and verification queries, read [references/alert-validation.md](references/alert-validation.md).
