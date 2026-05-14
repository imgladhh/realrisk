package com.realrisk.model;

public record IngestResponse(
    String eventId,
    String requestId,
    String status,
    String reason,
    long rateLimitCount) {}
