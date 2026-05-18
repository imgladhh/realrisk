# RealRisk Alert Validation Reference

## Preferred API Gateway forcing path

1. Seed Redis velocity:

```powershell
kubectl exec -n realrisk <redis-pod> -- redis-cli SET velocity:count:7d:<userId> 200
```

2. Port-forward API Gateway:

```powershell
kubectl port-forward svc/realrisk-api-gateway 18080:8080 -n realrisk
```

3. Send the event by HTTP so the gateway performs Avro serialization:

```powershell
$body = @{
  requestId   = "req-test-1"
  userId      = "<userId>"
  eventType   = "TRANSACTION"
  amountCents = 1500000
  currency    = "USD"
  merchantId  = "merchant-test-1"
  source      = "manual"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:18080/events" -Method Post -ContentType "application/json" -Body $body
```

4. Expect HTTP `202 ACCEPTED`, then verify `decision-audit`, `alert-events`, and `alert_processed_total`.

## In-cluster Redis blacklist

Only use this when bypassing API Gateway and producing directly to Kafka.

```powershell
kubectl exec -n realrisk <redis-pod> -- redis-cli SET blacklist:<userId> 1
```

## In-cluster PostgreSQL query

```powershell
kubectl exec -n realrisk <postgres-pod> -- env PGPASSWORD=realrisk psql -U realrisk -d realrisk -c "SELECT alert_id, user_id, severity, status, channels_notified, reason_summary FROM alert_log ORDER BY created_at DESC LIMIT 10;"
```

## What good looks like

Typical validated row shape:

- `severity = CRITICAL`
- `status = PROCESSED`
- `channels_notified = {email,sms,push}`
- `reason_summary = blacklisted_user` or a high-score combination such as `high_velocity_7d,large_amount`

## Debug order when alerts are missing

1. Confirm the event exists in `raw-events`
2. Confirm a `BLOCK / >=90` decision exists in `decision-audit`
3. Check whether Flink should have emitted `alert-events`
4. If `alert-events` exists but DB row is missing, inspect `alert-service` logs
