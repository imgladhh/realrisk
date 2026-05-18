package com.realrisk.alertservice.service;

import com.realrisk.alertservice.model.AlertEvent;
import com.realrisk.alertservice.model.AlertProcessingResult;
import com.realrisk.alertservice.metrics.AlertMetrics;
import com.realrisk.alertservice.notify.NotificationChannel;
import com.realrisk.alertservice.notify.NotificationRouter;
import com.realrisk.alertservice.persistence.AlertLogRepository;
import com.realrisk.alertservice.persistence.AlertLogStatus;
import com.realrisk.alertservice.rate.AlertRateLimiter;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AlertProcessor {
  private static final Logger log = LoggerFactory.getLogger(AlertProcessor.class);

  private final AlertLogRepository alertLogRepository;
  private final AlertRateLimiter rateLimiter;
  private final NotificationRouter notificationRouter;
  private final AlertMetrics metrics;
  private final Clock clock;

  @Autowired
  public AlertProcessor(
      AlertLogRepository alertLogRepository,
      AlertRateLimiter rateLimiter,
      NotificationRouter notificationRouter,
      AlertMetrics metrics) {
    this(alertLogRepository, rateLimiter, notificationRouter, metrics, Clock.systemUTC());
  }

  AlertProcessor(
      AlertLogRepository alertLogRepository,
      AlertRateLimiter rateLimiter,
      NotificationRouter notificationRouter,
      AlertMetrics metrics,
      Clock clock) {
    this.alertLogRepository = alertLogRepository;
    this.rateLimiter = rateLimiter;
    this.notificationRouter = notificationRouter;
    this.metrics = metrics;
    this.clock = clock;
  }

  public AlertProcessingResult process(AlertEvent event) {
    boolean inserted = alertLogRepository.insertIfAbsent(event);
    if (!inserted) {
      var existing = alertLogRepository.findByAlertId(event.alertId());
      if (existing.isPresent() && existing.get().status() != AlertLogStatus.PENDING) {
        log.info("Skipping duplicate alertId={} status={}", event.alertId(), existing.get().status());
        metrics.incrementProcessed(event.severity(), "duplicate");
        return new AlertProcessingResult(event.alertId(), true, false, existing.get().channelsNotified());
      }
      log.info("Retrying pending alertId={}", event.alertId());
    }

    List<String> channelsNotified = List.of();
    boolean rateLimited = !rateLimiter.allow(event);
    AlertLogStatus status = AlertLogStatus.PROCESSED;
    if (!rateLimited) {
      List<NotificationChannel> channels = notificationRouter.route(event);
      for (NotificationChannel channel : channels) {
        channel.send(event);
      }
      channelsNotified = channels.stream().map(NotificationChannel::name).toList();
      metrics.incrementProcessed(event.severity(), AlertLogStatus.PROCESSED.name());
    } else {
      log.info("Rate limited alertId={} userId={} severity={}", event.alertId(), event.userId(), event.severity());
      status = AlertLogStatus.RATE_LIMITED;
      metrics.incrementRateLimitHit(event.severity());
      metrics.incrementProcessed(event.severity(), AlertLogStatus.RATE_LIMITED.name());
    }

    alertLogRepository.markProcessed(event.alertId(), status, channelsNotified, Instant.now(clock));
    return new AlertProcessingResult(event.alertId(), false, rateLimited, channelsNotified);
  }
}
