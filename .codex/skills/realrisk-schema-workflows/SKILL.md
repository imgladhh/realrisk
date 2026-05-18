---
name: realrisk-schema-workflows
description: Use when working on RealRisk Avro schemas, Schema Registry subjects, schema registration scripts, or topic-to-subject mapping validation. Covers the repo's five .avsc contracts, the expected subject names, and the preferred verification flow for Docker Compose and Kubernetes environments.
---

# RealRisk Schema Workflows

Use this skill for RealRisk tasks that involve:

- adding or changing `.avsc` files under `src/main/avro`
- checking whether Schema Registry subjects line up with topic usage
- maintaining `scripts/register-schemas.ps1`
- verifying that producers and consumers are using the expected subjects

## Current schema inventory

The project currently expects these contracts:

- `RiskEventAvro`
- `RiskDecisionAvro`
- `HighRiskEventAvro`
- `AlertEventAvro`
- `RuleUpdateAvro`

## Expected subject mapping

- `RiskEventAvro`
  - `raw-events-value`
  - `raw-audit-value`
- `RiskDecisionAvro`
  - `decision-audit-value`
- `HighRiskEventAvro`
  - `high-risk-events-value`
- `AlertEventAvro`
  - `alert-events-value`
- `RuleUpdateAvro`
  - `rule-updates-value`

## Default workflow

1. Check whether the task changes schema shape, subject mapping, or only docs/tooling.
2. Keep subject names aligned with the TopicNameStrategy convention: `<topic>-value`.
3. If schema registration tooling is touched, keep it safe to re-run and explicit about failures.
4. For verification commands and gotchas, read [references/schema-validation.md](references/schema-validation.md).

## Project-specific rules

- Prefer updating the registration script and mapping docs together.
- If a new topic needs Avro payloads, add the subject mapping in one place and mention it in docs.
- Avoid changing existing subject names casually; downstream Flink, Spring, and console tools all assume the current names.

