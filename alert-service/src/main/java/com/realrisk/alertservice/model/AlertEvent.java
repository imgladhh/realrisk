package com.realrisk.alertservice.model;

import java.time.Instant;

public record AlertEvent(
    String alertId,
    String decisionId,
    String eventId,
    String userId,
    int riskScore,
    String severity,
    String reasonSummary,
    Instant createdAt,
    String source) {}
