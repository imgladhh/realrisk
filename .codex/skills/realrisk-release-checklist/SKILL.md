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

## Reference

For the repo's current closeout expectations, read [references/closeout-notes.md](references/closeout-notes.md).

