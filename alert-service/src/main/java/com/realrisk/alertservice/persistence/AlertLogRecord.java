package com.realrisk.alertservice.persistence;

import java.time.Instant;
import java.util.List;

public record AlertLogRecord(
    String alertId,
    String userId,
    String severity,
    String reasonSummary,
    AlertLogStatus status,
    List<String> channelsNotified,
    Instant createdAt,
    Instant processedAt) {}
