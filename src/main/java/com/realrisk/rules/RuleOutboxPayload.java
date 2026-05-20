package com.realrisk.rules;

import java.time.Instant;
import java.util.Map;

record RuleOutboxPayload(
    String ruleId,
    String ruleType,
    boolean enabled,
    Map<String, String> parameters,
    Instant updatedAt) {}
