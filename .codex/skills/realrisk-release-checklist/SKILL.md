---
name: realrisk-release-checklist
description: Use when wrapping up a RealRisk phase, preparing a commit, or checking whether docs, validation notes, and operational memory are consistent with the latest implementation. Covers the repo's expected closeout steps for README, memory.md, and phase validation evidence.
---

# RealRisk Release Checklist

Use this skill for RealRisk tasks that involve:

- phase closeout
- README and `memory.md` synchronization
- "are we done?" checks before committing
- summarizing validation results before a push

## Default workflow

1. Confirm what phase or feature batch actually changed.
2. Check whether there is concrete validation evidence, not just scaffolding.
3. Update docs and session memory to match reality.
4. Only then summarize and prepare the commit.

## Closeout checklist

- implementation files updated
- phase status reflects actual validated state
- `README.md` mentions important workflow or infrastructure changes
- `memory.md`:
  - `Done` reflects finished work
  - `In Progress` reflects current reality, not stale solved issues
  - `Next` points at genuine next work
  - `Scripts` section mentions any new operational tooling
- known gotchas are recorded somewhere durable if they are likely to recur

## Project-specific rules

- Treat "validated" as stronger than "scaffolded"; prefer documenting the stronger state only when we have evidence.
- Keep `memory.md` easy to scan. Move stale migration notes out of `In Progress`.
- If a new script was added, check whether it needs to be mentioned in both `Done` and `Scripts`.
- If a phase surfaced repeatable validation traps (PowerShell quoting, pod-specific tooling, failover timing, exporter prerequisites, etc.), write the durable parts back into the relevant RealRisk skill before calling the phase "wrapped up".
- Before pushing CI-related changes, sanity-check the workflow against the GitHub runner environment:
  - do not reference repo-local tools like `./.tools/maven/bin/mvn` unless they are actually committed
  - if CI uses `mvn -DskipTests package`, remember that Maven still compiles test sources, so stale test constructors can still break `build-and-push`
  - prefer `secrets.GITHUB_TOKEN` plus `permissions: packages: write` for GHCR login unless there is a specific reason to maintain a separate package token
  - if the configured `KUBE_CONFIG` points at a local cluster (`localhost`, kind, Docker Desktop), do not run deploy automatically on every push; gate deploy behind `workflow_dispatch` or another explicit manual trigger
- For Spring Boot integration tests that run in GitHub Actions but not reliably on the local Windows machine, expect to use one CI round to surface the real `Caused by:` and then make a focused fix. Do not trust the outer `Failed to load ApplicationContext` wrapper as the root cause.

## Reference

For the repo's current closeout expectations, read [references/closeout-notes.md](references/closeout-notes.md).
