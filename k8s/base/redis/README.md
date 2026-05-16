Redis is installed by Helm in Phase 6 via:

- `scripts/install-infra.ps1`
- values files under `k8s/overlays/*/helm-values/redis-values.yaml`

The application workloads reference the resulting service name:

- `realrisk-redis-master`
