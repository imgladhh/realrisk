package com.realrisk.model;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.time.Instant;

public record IngestRequest(
    String eventId,
    String requestId,
    @NotBlank String userId,
    @NotBlank String eventType,
    Instant timestamp,
    @Min(0) long amountCents,
    @NotBlank String currency,
    String ipAddress,
    String deviceFp,
    String merchantId,
    String counterparty,
    @NotBlank String source) {}
