package com.realrisk.model;

import java.time.Instant;

public record RiskEvent(
    String eventId,
    String requestId,
    String userId,
    String eventType,
    Instant timestamp,
    long amountCents,
    String currency,
    String ipAddress,
    String deviceFp,
    String merchantId,
    String counterparty,
    String source) {}
