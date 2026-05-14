package com.realrisk.model;

import java.time.Instant;
import java.util.List;

public record RiskDecision(
    String decisionId,
    String eventId,
    String requestId,
    String userId,
    String decision,
    int riskScore,
    List<String> reasons,
    Instant createdAt,
    String source) {}
