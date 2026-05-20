---
name: realrisk-k8s-validation
description: Use when working on RealRisk Kubernetes validation, Kafka/Flink/Schema Registry troubleshooting, or Phase 5/6 end-to-end checks. Covers the project-specific in-cluster produce/consume workflow, Flink restart expectations, PostgreSQL/Redis access patterns, and known local Windows plus kubectl exec pitfalls.
---

# RealRisk K8s Validation

Use this skill for RealRisk tasks that involve:

- validating Phase 5 or Phase 6 flows
- checking whether a message made it from `raw-events` to `decision-audit` or `alert_log`
- restarting Flink after Kafka or ConfigMap changes
- producing or consuming Avro messages in the Kubernetes-only path
- debugging local kind + Strimzi + Schema Registry behavior on this repo

## Default workflow

1. Confirm the target path first:
   - Phase 5 uses hybrid infra (`host.docker.internal`, Compose-backed Kafka and DB)
   - Phase 6 uses Kubernetes-only infra (`realrisk-kafka-kafka-bootstrap:9092`, in-cluster SR/Redis/Postgres)
2. At the start of any validation turn, explicitly re-check the current pods, services, and whether old `port-forward` sessions are still trustworthy.
3. For Phase 6, always prefer commands run **inside the Kubernetes Schema Registry pod** unless the task specifically needs broker tooling or in-cluster HTTP.
4. Before blaming Kafka or Schema Registry, verify whether the event exists in:
   - `raw-events`
   - then `decision-audit`
   - then `alert-events` / `alert_log`
5. After Kafka bootstrap or ConfigMap changes, restart or recycle the Flink job pod and wait for:
   - `raw-events-source` running
   - `rule-updates-source` running
   - checkpoints completing

## Project-specific rules

- Prefer one-shot `kubectl exec ... sh -c "<command>"` or short-lived `bash` sessions over long-lived `kubectl exec -it ... kafka-avro-console-consumer` sessions.
- On this machine, long-lived `kubectl exec` console-consumer sessions can die with exit code `137`; treat that as a tooling nuisance, not immediate evidence of a Kafka failure.
- On Windows PowerShell, avoid embedding `$(cat /tmp/file)` in outer PowerShell strings unless you are sure PowerShell will not evaluate it locally.
- On Windows PowerShell, prefer the real HTTP ingress path (`Invoke-RestMethod` to a port-forwarded service) over hand-built Avro producer commands when either path is acceptable.
- When a local `port-forward` is flaky on Windows, prefer an **in-cluster probe pod** (`kubectl run ... --image=curlimages/curl` or another tiny utility image) over spending time fighting `Start-Process`, quoting, or stale local sockets.
- For in-cluster HTTP POST validation from PowerShell, the most reliable pattern is:
  1. build the JSON body locally
  2. base64-encode it
  3. decode it inside the probe pod to `/tmp/body.json`
  4. `curl --data-binary @/tmp/body.json`
  This avoids repeated PowerShell + shell quoting bugs that can turn valid JSON into malformed payloads.
- After any `kubectl rollout restart` or image rebuild that replaces pods, assume existing `kubectl port-forward` sessions are stale even if the terminal does not obviously fail. Re-resolve the pod name and re-run `port-forward`.
- When a metric seems "missing", verify it at the pod first with a pod-level port-forward to `/actuator/prometheus` or `/metrics` before blaming Prometheus or Grafana.
- For Phase 6 validation, do **not** use the old Docker Compose `realrisk-schema-registry` container to inspect topics that now live in Kubernetes.
- Different RealRisk pods have different Kafka tooling:
  - Schema Registry pod is the right place for `kafka-avro-console-producer` / `kafka-avro-console-consumer`
  - Kafka broker pod is the right place for raw `kafka-console-producer.sh` / `kafka-console-consumer.sh`
  Pick the pod by tool, not by habit.
- For CloudNativePG validation:
  - query the DB from the CNPG pod with `psql -h 127.0.0.1`, not peer auth defaults
  - if a `kubectl delete pod <primary>` call times out, immediately check `kubectl get cluster` and `kubectl describe cluster` before assuming the failover failed; CNPG often continues promoting a new primary while the delete command is still waiting on the old pod termination
  - temporary compatibility resources (for example a manually created `realrisk-cluster-rw` Service) can block CNPG ownership reconciliation and must be removed before the `Cluster` can become healthy
- For Bitnami Redis Sentinel on this repo:
  - `replica.replicaCount` controls how many redis+sentinel nodes exist; do not assume it means "number of replicas excluding master"
  - validate the topology with `redis-cli -p 26379 SENTINEL masters`, especially `num-slaves` and `num-other-sentinels`
- For broker-side lag validation:
  - Strimzi `kafkaExporter` is the source of `kafka_consumergroup_lag`, not broker JMX metrics
  - `kafka_consumergroup_lag` only appears after the consumer group has committed offsets on a partition
  - `-1` means "no committed offset baseline", not zero lag
  - if you need lag-based alerting to fire, establish committed offsets on all relevant partitions first, then stop the consumer, then add new messages
- For DLQ replay:
  - do not run the replay tool locally via Maven + Kafka port-forward
  - run it in-cluster so Kafka metadata can resolve `.svc` broker addresses

## Pod and service lookup

When commands need the current pod names, resolve them dynamically first:

```powershell
kubectl get pods -n realrisk | findstr schema-registry
kubectl get pods -n realrisk | findstr realrisk-flink-
kubectl get pods -n realrisk | findstr redis
kubectl get pods -n realrisk | findstr postgresql
```

## Validation patterns

- For current end-to-end commands and troubleshooting checkpoints, read [references/phase-validation.md](references/phase-validation.md).
- For observability checks, use this order:
  1. confirm the application pod exposes the expected metric locally
  2. confirm Prometheus can query the metric
  3. confirm Grafana panel logic and labels match the actual metric name
- For HA acceptance checks, use this order:
  1. prove the service works in the steady state
  2. trigger failover
  3. verify the control-plane state (`kubectl get`, `describe`, Sentinel state, etc.)
  4. immediately re-run a business request to prove the data path recovered
