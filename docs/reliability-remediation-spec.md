# RealRisk Correctness Remediation Specification

## Purpose

This document scopes the smallest set of changes that materially improves correctness and demonstrability for the RealRisk teaching/portfolio project. It intentionally does **not** attempt to make the system production-complete.

The priority order is based on whether the current implementation can silently lose an event, apply an incorrect rule, or allow a core regression to merge without detection.

## In scope

1. Do not acknowledge an accepted event before Kafka confirms the write.
2. Make transactional-outbox publication safe when the API Gateway has multiple replicas.
3. Prevent fresh Flink instances from evaluating events before rule state is usable.
4. Execute Flink tests in GitHub Actions.
5. Make local test behavior explicit when Docker is unavailable.
6. Document the deliberate Redis-enrichment degradation policy.

## Out of scope

The following are worthwhile production-hardening work but are not required for this remediation:

- multi-AZ Kafka, PostgreSQL, or Redis and replication-factor changes;
- OIDC/workload identity, API-key rotation, and full authorization/audit trails;
- exactly-once notification delivery or exactly-once DLQ replay;
- implementing the SMS provider;
- asynchronous Redis lookups, broad load testing, and global autoscaling design;
- automated backup/restore and multi-region disaster recovery.

## P0 — Ingest acknowledgment must reflect Kafka persistence

### Current behavior

`RiskEventPublisher.publishRawEvent` calls `KafkaTemplate.send(...)` asynchronously and only updates metrics from its completion callback. `IngestController` returns `202 Accepted` immediately afterwards. A broker, serialization, or timeout failure can therefore result in a successful HTTP response for an event that was never written to `raw-events`.

### Required change

- Change the publisher API to expose a completion result to the controller.
- Await the Kafka send for a bounded, configurable timeout on the request path.
- Return `202 Accepted` only after broker acknowledgement succeeds.
- Map a timeout or failed send to `503 Service Unavailable`; do not count it as an allowed ingest.
- Configure and document the producer acknowledgement/retry policy explicitly rather than relying on implicit defaults.

### Acceptance criteria

- A successful broker acknowledgement returns `202` and records latency through the existing
  `risk.kafka.publish` Micrometer timer.
- A failed or timed-out send returns `503`, records latency through `risk.kafka.publish` and an
  error through the existing `risk.kafka.publish.errors` counter, and does not return `202`.
- Preserve the existing `risk.*` Micrometer naming convention; do not introduce a parallel metric
  namespace or a redundant success counter.
- A controller/publisher test covers both branches with a failed future.

### Primary files

- `src/main/java/com/realrisk/api/IngestController.java`
- `src/main/java/com/realrisk/kafka/RiskEventPublisher.java`
- `src/main/resources/application.yml`

### Implementation status

Completed on 2026-09-11. The gateway now waits for a bounded broker acknowledgement, maps failed,
timed-out, interrupted, and immediate send failures to `503`, preserves the existing Micrometer
metrics, and explicitly configures `acks=all`, producer idempotence, retries, and Kafka delivery
timeouts. Publisher and controller tests cover acknowledgement, asynchronous failure, immediate
failure, timeout, and the rule that failed publishes are not counted as allowed ingress.

## P1 — Outbox publishing must have one active owner per row

### Current behavior

The API Gateway HPA can run multiple replicas, while every replica runs `RuleService.publishPendingOutbox()`. The query selects all unpublished rows without a claim or row lock. Multiple pods can publish the same record before one wins the conditional `published_at` update. Cross-pod producers also cannot guarantee a global order for updates to the same rule.

`RuleUpdatePublisher.publishBlocking()` already waits up to 10 seconds for Kafka acknowledgement.
P1 is therefore about selecting and owning outbox rows without coordination, not about the P0
raw-event acknowledgement defect. The two remediations are independent at the code level.

### Required change

Choose one of the following designs; option A is preferred for this portfolio project.

#### Option A: a dedicated single-replica outbox relay

- Move scheduled polling and Kafka publication into a named relay Deployment with one replica.
- Mechanically guarantee one active relay during rollout, for example with a `Recreate` deployment
  strategy or a database/Kubernetes leader lease; `replicas: 1` alone can briefly overlap during a
  rolling update.
- Keep rule writes in the API Gateway, but remove scheduled publishing from its replicas.
- Retain the current ordered, stop-on-first-failure processing behavior.
- Preserve the ordering contract: `rule-updates` remains one partition, every update is keyed by
  `ruleId`, and the single active relay publishes rows in ascending outbox `id` order.
- Kafka compaction supports eventual state reconstruction by retaining the latest value per
  `ruleId`, but it is not an ordering mechanism and must not be presented as protection against
  concurrently published stale updates.
- A monotonic rule version is optional only while the one-active-relay and one-partition contract is
  mechanically enforced. If relay overlap or repartitioning is allowed, add the version and make
  Flink reject an update older than the version already held in broadcast state.

#### Option B: database claims for horizontally scaled relay instances

- Add claim metadata such as `locked_at` and `locked_by` to `rule_outbox`.
- Claim rows in one short transaction using `FOR UPDATE SKIP LOCKED` in `id` order.
- Publish only claimed rows; mark a row published only after Kafka acknowledgement.
- Expire abandoned claims after a bounded lease and make duplicate delivery safe.
- Add a monotonic rule version and have Flink ignore an update older than the state it holds.

### Acceptance criteria

- Option A guarantees there cannot be two active relay publishers; Option B guarantees two
  concurrent publishers cannot publish the same unclaimed outbox row.
- Updates for the same rule cannot leave Flink with an older state after a newer state.
- Option A tests or validates the single-active-relay, single-partition, keyed, ascending-`id`
  ordering contract. If any part of that contract is relaxed, a monotonic-version test proves that
  Flink ignores stale updates.
- A publish failure leaves the row available for recovery and blocks later global ordering as intended.
- Tests cover concurrent claiming or the explicit single-relay deployment contract.

### Primary files

- `src/main/java/com/realrisk/rules/RuleService.java`
- `src/main/java/com/realrisk/rules/RuleUpdatePublisher.java`
- `src/main/resources/db/migration/V4__rules_outbox.sql`
- `k8s/base/api-gateway/hpa.yaml`

### Implementation status

Completed on 2026-09-11 with Option A. Scheduled polling now lives in a conditional
`RuleOutboxRelay`; Kubernetes disables it on API Gateway replicas and enables it only in the
dedicated one-replica `realrisk-rule-outbox-relay` Deployment. `Recreate` prevents rollout overlap.
The existing blocking publisher, ascending-ID query, stop-on-first-failure behavior, `ruleId` key,
and single-partition topic form the ordering contract. Unit tests cover relay delegation, disabling
the relay bean on API Gateway replicas, and the existing failure/order behavior; local and
production Kustomize overlays render successfully.

## P1 — Flink must not score against incomplete rule state at fresh startup

### Current behavior

`FlinkRiskJob` starts `raw-events` and `rule-updates` as independent Kafka sources. Starting the rules source at `earliest` does not establish an ordering barrier with raw-event processing. On a fresh job or a restore without prior broadcast state, an arriving raw event can be evaluated using fallback configuration before its persisted dynamic rule has been consumed.

### Required change

- Introduce an explicit rule-state readiness gate before `MerchantBurstProcessFunction` evaluates raw events on a fresh start.
- Define readiness precisely: for example, a bootstrap/snapshot marker emitted after the rule stream has been replayed, or an initial rule snapshot loaded before accepting raw events.
- Buffer, pause, or retry raw events while state is not ready; bound the buffer and expose a readiness metric/health signal.
- Preserve checkpoint recovery behavior: restored broadcast state may be marked ready without a full bootstrap replay only when its checkpoint is valid.

### Acceptance criteria

- A dynamic threshold present before job startup is applied to the first subsequently accepted raw event.
- No decision is emitted from fallback rules solely because rule bootstrap is still in progress.
- A focused integration or Flink harness test demonstrates the ordering scenario.
- Documentation no longer claims that calling the rule source first guarantees processing order.

### Primary files

- `flink-job/src/main/java/com/realrisk/flink/FlinkRiskJob.java`
- `flink-job/src/main/java/com/realrisk/flink/MerchantBurstProcessFunction.java`
- `flink-job/src/main/java/com/realrisk/flink/RuleSet.java`

### Implementation status

Completed on 2026-09-11. At submission the job captures the single rule partition's end offset and
preserves Kafka offsets in a rule-update envelope. Fresh operators buffer raw events in bounded
keyed state until that offset has been consumed, then mark broadcast readiness and flush the buffer.
Overflow fails the operator rather than scoring with incomplete rules. Valid checkpoint restores
skip a redundant bootstrap because rule state and source positions restore consistently. The
`realrisk.rule_bootstrap_ready` gauge exposes the gate state. Flink unit
tests and the shaded package build pass; a live Kubernetes rollout/E2E was not performed in this
implementation session.

## P1 — CI must execute all core rule-engine tests

### Current behavior

The CI `test` job verifies the root module and `alert-service`, but it does not run `flink-job` tests. The build job packages Flink with `-DskipTests`, which only gives compilation coverage for those tests.

### Required change

- Add `mvn -B -f ./flink-job/pom.xml verify` to the CI test job before image builds.
- Keep image packaging separate from test execution.

### Acceptance criteria

- A failing test in `flink-job/src/test` fails a pull-request workflow.
- Existing Flink unit tests run successfully in CI.

### Primary file

- `.github/workflows/ci.yaml`

### Implementation status

Completed on 2026-09-11. The CI test job now runs `flink-job` verification before image builds, and
the deploy job injects and waits for the dedicated relay image rollout.

## P2 — Make Docker-dependent test behavior predictable

### Current behavior

The root Redis tests start Testcontainers directly. On a machine without a Docker daemon, `mvn test` fails. Alert-service integration tests already use a Docker-aware skip policy, so test behavior is inconsistent between modules.

### Required change

Choose one clear convention:

- mark Docker-backed tests as disabled/skipped when Docker is unavailable; or
- move them to an explicit integration-test Maven profile that CI enables.

CI must continue to run these tests where Docker is available.

### Acceptance criteria

- A developer without Docker can run the default unit-test command without an infrastructure failure.
- CI still executes Redis integration coverage rather than silently removing it.

### Primary files

- `src/test/java/com/realrisk/redis/RedisTestSupport.java`
- `src/test/java/com/realrisk/redis/BlacklistServiceTest.java`
- `src/test/java/com/realrisk/redis/RateLimitServiceTest.java`
- `pom.xml`

## P2 — State the Redis enrichment failure policy truthfully

### Current behavior

When Redis cannot be read, `RedisUserProfileReader` returns `UserProfile.empty()`. This keeps the Flink pipeline live but can omit blacklist and high-velocity signals for events already in Kafka.

### Required change

- Keep the current availability-first behavior unless the project explicitly chooses a risk-first posture.
- Document it in the README/developer guide and expose or retain a metric/log signal sufficient to detect the degradation.
- In interviews, describe this as a deliberate false-negative tradeoff, not as guaranteed risk evaluation during a Redis outage.

### Acceptance criteria

- Repository documentation states the fallback and its risk implication.
- A unit test preserves the selected behavior on a Redis exception.

### Primary files

- `flink-job/src/main/java/com/realrisk/flink/RedisUserProfileReader.java`
- `flink-job/src/test/java/com/realrisk/flink/RedisUserProfileReaderTest.java`
- `README.md`
- `docs/developer-guide.md`

## Suggested implementation sequence

1. P0 Kafka acknowledgement and its controller test.
2. P1 outbox ownership model, schema migration, and concurrency test.
3. P1 Flink bootstrap-readiness design and integration/harness test.
4. P1 CI Flink verification.
5. P2 Docker-aware test convention.
6. P2 Redis-degradation documentation and metric/test confirmation.

## Explicitly deferred behavior

`alert_log.channels_notified` represents configured/attempted channels, not independently confirmed external delivery. The current channel implementations may catch provider failures and the processor can still mark an alert `PROCESSED`. This is an intentional known limitation for the teaching project and is not part of this remediation; do not describe it as confirmed delivery in documentation or interviews.
