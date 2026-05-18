# RealRisk Phase Validation Reference

## Phase 6 in-cluster Kafka path

Use these endpoints:

- Kafka bootstrap: `realrisk-kafka-kafka-bootstrap:9092`
- Schema Registry: `http://realrisk-schema-registry:8081`
- Redis pod access: `kubectl exec -n realrisk <redis-pod> -- redis-cli ...`
- PostgreSQL pod access:
  - `kubectl exec -n realrisk <postgres-pod> -- env PGPASSWORD=realrisk psql -U realrisk -d realrisk ...`

## Check that a produced event really landed

From inside the current Schema Registry pod:

```bash
kafka-avro-console-consumer \
  --bootstrap-server realrisk-kafka-kafka-bootstrap:9092 \
  --topic raw-events \
  --from-beginning \
  --timeout-ms 10000 \
  --property schema.registry.url=http://realrisk-schema-registry:8081 | grep <event-id>
```

If the event is not present in `raw-events`, stop there and debug produce-side behavior first.

## Check decision output

```bash
kafka-avro-console-consumer \
  --bootstrap-server realrisk-kafka-kafka-bootstrap:9092 \
  --topic decision-audit \
  --from-beginning \
  --timeout-ms 10000 \
  --property schema.registry.url=http://realrisk-schema-registry:8081 | grep <event-id>
```

## Check Flink health after infra changes

Use the active Flink job pod:

```powershell
kubectl logs -n realrisk <flink-job-pod> --tail=150
```

Healthy signs:

- `raw-events-source` discovered partitions
- `rule-updates-source` discovered partitions
- tasks switch to `RUNNING`
- checkpoints complete

## High-score / alert validation

To force a `CRITICAL` alert path, set a Redis blacklist key first:

```powershell
kubectl exec -n realrisk <redis-pod> -- redis-cli SET blacklist:<userId> 1
```

Then send a fresh event for that user and query PostgreSQL:

```powershell
kubectl exec -n realrisk <postgres-pod> -- env PGPASSWORD=realrisk psql -U realrisk -d realrisk -c "SELECT alert_id, user_id, severity, status, channels_notified, reason_summary FROM alert_log ORDER BY created_at DESC LIMIT 10;"
```

## Known local gotchas

- Strimzi on this machine needed:
  - a supported operator version
  - namespace watch enabled
  - KRaft instead of the earlier single-node ZooKeeper shape
- Schema Registry bootstrap for readiness must be plain `host:port`, not `PLAINTEXT://host:port`
- After Kafka path changes, restart Flink so it consumes the updated ConfigMap
