# RealRisk Schema Validation Reference

## Registration script

The repository script is:

- `scripts/register-schemas.ps1`

Expected behavior:

- waits for Schema Registry readiness
- registers all five `.avsc` files
- registers `RiskEventAvro` under both `raw-events-value` and `raw-audit-value`
- is safe to re-run
- exits non-zero on registration failures

## Compose validation

Default local Schema Registry URL:

- `http://localhost:8081`

Typical usage:

```powershell
.\scripts\register-schemas.ps1
```

## Kubernetes validation

Use the current in-cluster Schema Registry pod when validating topic contents, not the old Compose container.

If using a local port-forward:

```powershell
kubectl port-forward svc/realrisk-schema-registry 8081:8081 -n realrisk
.\scripts\register-schemas.ps1 -SchemaRegistryUrl http://localhost:8081
```

## Quick review checklist

- schema filename still matches the expected record role
- subject mapping still matches the topic names used by the app
- docs mention any new topic/schema relationship
- no mojibake / encoding damage in PowerShell help text

