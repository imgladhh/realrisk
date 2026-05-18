package com.realrisk.alertservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.realrisk.alertservice.model.AlertEvent;
import com.realrisk.alertservice.metrics.AlertMetrics;
import com.realrisk.alertservice.notify.NotificationChannel;
import com.realrisk.alertservice.notify.NotificationRouter;
import com.realrisk.alertservice.persistence.AlertLogRepository;
import com.realrisk.alertservice.persistence.AlertLogStatus;
import com.realrisk.alertservice.rate.AlertRateLimiter;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AlertProcessorTest {
  @Test
  void happyPathSendsNotificationsAndPersistsChannelNames() {
    AlertLogRepository repository = Mockito.mock(AlertLogRepository.class);
    AlertRateLimiter rateLimiter = Mockito.mock(AlertRateLimiter.class);
    NotificationRouter router = Mockito.mock(NotificationRouter.class);
    AlertMetrics metrics = Mockito.mock(AlertMetrics.class);
    NotificationChannel email = Mockito.mock(NotificationChannel.class);
    NotificationChannel sms = Mockito.mock(NotificationChannel.class);
    when(repository.insertIfAbsent(any())).thenReturn(true);
    when(rateLimiter.allow(any())).thenReturn(true);
    when(router.route(any())).thenReturn(List.of(email, sms));
    when(email.name()).thenReturn("email");
    when(sms.name()).thenReturn("sms");

    AlertProcessor processor = new AlertProcessor(repository, rateLimiter, router, metrics, fixedClock());
    var result = processor.process(event());

    assertThat(result.duplicate()).isFalse();
    assertThat(result.rateLimited()).isFalse();
    assertThat(result.channelsNotified()).containsExactly("email", "sms");
    verify(email).send(any());
    verify(sms).send(any());
    verify(repository)
        .markProcessed(
            eq("alert-1"),
            eq(AlertLogStatus.PROCESSED),
            eq(List.of("email", "sms")),
            eq(Instant.parse("2026-05-16T16:00:00Z")));
  }

  @Test
  void duplicateAlertIdIsSkipped() {
    AlertLogRepository repository = Mockito.mock(AlertLogRepository.class);
    AlertRateLimiter rateLimiter = Mockito.mock(AlertRateLimiter.class);
    NotificationRouter router = Mockito.mock(NotificationRouter.class);
    AlertMetrics metrics = Mockito.mock(AlertMetrics.class);
    when(repository.insertIfAbsent(any())).thenReturn(false);
    when(repository.findByAlertId("alert-1"))
        .thenReturn(
            java.util.Optional.of(
                new com.realrisk.alertservice.persistence.AlertLogRecord(
                    "alert-1",
                    "user-1",
                    "CRITICAL",
                    "r1",
                    AlertLogStatus.PROCESSED,
                    List.of("email"),
                    Instant.parse("2026-05-16T15:59:00Z"),
                    Instant.parse("2026-05-16T16:00:00Z"))));

    AlertProcessor processor = new AlertProcessor(repository, rateLimiter, router, metrics, fixedClock());
    var result = processor.process(event());

    assertThat(result.duplicate()).isTrue();
    assertThat(result.channelsNotified()).containsExactly("email");
    verify(rateLimiter, never()).allow(any());
    verify(router, never()).route(any());
  }

  @Test
  void rateLimitedAlertSkipsNotificationsButPersistsProcessing() {
    AlertLogRepository repository = Mockito.mock(AlertLogRepository.class);
    AlertRateLimiter rateLimiter = Mockito.mock(AlertRateLimiter.class);
    NotificationRouter router = Mockito.mock(NotificationRouter.class);
    AlertMetrics metrics = Mockito.mock(AlertMetrics.class);
    when(repository.insertIfAbsent(any())).thenReturn(true);
    when(rateLimiter.allow(any())).thenReturn(false);

    AlertProcessor processor = new AlertProcessor(repository, rateLimiter, router, metrics, fixedClock());
    var result = processor.process(event());

    assertThat(result.rateLimited()).isTrue();
    assertThat(result.channelsNotified()).isEmpty();
    verify(router, never()).route(any());
    verify(repository)
        .markProcessed(
            eq("alert-1"),
            eq(AlertLogStatus.RATE_LIMITED),
            eq(List.of()),
            eq(Instant.parse("2026-05-16T16:00:00Z")));
  }

  @Test
  void pendingDuplicateIsRetriedInsteadOfSkipped() {
    AlertLogRepository repository = Mockito.mock(AlertLogRepository.class);
    AlertRateLimiter rateLimiter = Mockito.mock(AlertRateLimiter.class);
    NotificationRouter router = Mockito.mock(NotificationRouter.class);
    AlertMetrics metrics = Mockito.mock(AlertMetrics.class);
    NotificationChannel email = Mockito.mock(NotificationChannel.class);
    when(repository.insertIfAbsent(any())).thenReturn(false);
    when(repository.findByAlertId("alert-1"))
        .thenReturn(
            java.util.Optional.of(
                new com.realrisk.alertservice.persistence.AlertLogRecord(
                    "alert-1",
                    "user-1",
                    "CRITICAL",
                    "r1",
                    AlertLogStatus.PENDING,
                    List.of(),
                    Instant.parse("2026-05-16T15:59:00Z"),
                    null)));
    when(rateLimiter.allow(any())).thenReturn(true);
    when(router.route(any())).thenReturn(List.of(email));
    when(email.name()).thenReturn("email");

    AlertProcessor processor = new AlertProcessor(repository, rateLimiter, router, metrics, fixedClock());
    var result = processor.process(event());

    assertThat(result.duplicate()).isFalse();
    assertThat(result.channelsNotified()).containsExactly("email");
    verify(email).send(any());
  }

  private AlertEvent event() {
    return new AlertEvent("alert-1", "decision-1", "event-1", "user-1", 95, "CRITICAL", "r1", Instant.parse("2026-05-16T15:59:00Z"), "flink");
  }

  private Clock fixedClock() {
    return Clock.fixed(Instant.parse("2026-05-16T16:00:00Z"), ZoneOffset.UTC);
  }
}
