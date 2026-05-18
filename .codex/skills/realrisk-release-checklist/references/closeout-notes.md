# RealRisk Closeout Notes

## What usually needs to move together

- implementation or infra manifests
- `README.md`
- `memory.md`
- sometimes scripts under `scripts/`

## Common mistakes

- marking a phase complete before E2E evidence exists
- leaving solved migration problems in `In Progress`
- forgetting to mention a new script in the `Scripts` section
- documenting the old Compose path after the K8s path became canonical

## Good summary shape

When summarizing progress, prefer:

- what changed
- what was validated
- what remains

Keep it short and concrete.
