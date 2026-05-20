package com.realrisk.model;

import java.time.Instant;
import java.util.Map;

public record RuleView(
    String ruleId,
    String ruleType,
    Map<String, String> parameters,
    boolean enabled,
    Instant createdAt,
    Instant updatedAt) {}
