package com.realrisk.api;

import com.realrisk.kafka.RiskEventPublisher;
import com.realrisk.metrics.ApiGatewayMetrics;
import com.realrisk.model.IngestRequest;
import com.realrisk.model.IngestResponse;
import com.realrisk.model.RateLimitResult;
import com.realrisk.model.RiskEvent;
import com.realrisk.redis.BlacklistService;
import com.realrisk.redis.RateLimitService;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/events")
public class IngestController {
  private static final Logger log = LoggerFactory.getLogger(IngestController.class);
  private static final Set<String> ALLOWED_EVENT_TYPES =
      Set.of("TRANSACTION", "LOGIN", "WITHDRAWAL", "TRANSFER", "BEHAVIOUR");

  private final BlacklistService blacklistService;
  private final RateLimitService rateLimitService;
  private final RiskEventPublisher publisher;
  private final ApiGatewayMetrics metrics;

  public IngestController(
      BlacklistService blacklistService,
      RateLimitService rateLimitService,
      RiskEventPublisher publisher,
      ApiGatewayMetrics metrics) {
    this.blacklistService = blacklistService;
    this.rateLimitService = rateLimitService;
    this.publisher = publisher;
    this.metrics = metrics;
  }

  @PostMapping
  public ResponseEntity<IngestResponse> ingest(@Valid @RequestBody IngestRequest request) {
    String eventType = normalizeEventType(request.eventType());
    if (!ALLOWED_EVENT_TYPES.contains(eventType)) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported event_type");
    }

    String eventId = valueOrUuid(request.eventId());
    String requestId = valueOrUuid(request.requestId());

    try {
      Timer.Sample blacklistSample = metrics.startSample();
      var blacklist = blacklistService.find(request.userId());
      metrics.recordBlacklistLookup(blacklistSample);
      if (blacklist.isPresent()) {
        metrics.incrementBlacklistHit();
        metrics.incrementIngressOutcome("BLOCK");
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
            .body(
                new IngestResponse(
                    eventId,
                    requestId,
                    "BLOCKED",
                    "blacklist:%s".formatted(blacklist.get().reason()),
                    0));
      }

      Timer.Sample rateLimitSample = metrics.startSample();
      RateLimitResult rateLimit = rateLimitService.check(request.userId(), requestId);
      metrics.recordRateLimitCheck(rateLimitSample);
      if (rateLimit.blocked()) {
        metrics.incrementRateLimitHit();
        metrics.incrementIngressOutcome("BLOCK");
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .body(new IngestResponse(eventId, requestId, "BLOCKED", "rate_limit", rateLimit.count()));
      }

      publisher.publishRawEvent(toEvent(request, eventId, requestId, eventType));
      metrics.incrementIngressOutcome("ALLOW");
      return ResponseEntity.accepted()
          .body(
              new IngestResponse(
                  eventId, requestId, "ACCEPTED", "passed_fast_path", rateLimit.count()));
    } catch (DataAccessException e) {
      log.error(
          "Redis unavailable for userId={}, requestId={}, failing closed",
          request.userId(),
          requestId,
          e);
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(new IngestResponse(eventId, requestId, "BLOCKED", "redis_unavailable", 0));
    }
  }

  private RiskEvent toEvent(IngestRequest request, String eventId, String requestId, String eventType) {
    return new RiskEvent(
        eventId,
        requestId,
        request.userId(),
        eventType,
        request.timestamp() == null ? Instant.now() : request.timestamp(),
        request.amountCents(),
        request.currency().toUpperCase(Locale.ROOT),
        request.ipAddress(),
        request.deviceFp(),
        request.merchantId(),
        request.counterparty(),
        request.source());
  }

  private String normalizeEventType(String value) {
    return value.toUpperCase(Locale.ROOT);
  }

  private String valueOrUuid(String value) {
    return value == null || value.isBlank() ? UUID.randomUUID().toString() : value;
  }
}
