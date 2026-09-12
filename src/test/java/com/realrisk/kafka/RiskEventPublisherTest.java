package com.realrisk.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.realrisk.config.RiskProperties;
import com.realrisk.metrics.ApiGatewayMetrics;
import com.realrisk.model.RiskEvent;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@SuppressWarnings("unchecked")
class RiskEventPublisherTest {
  private final KafkaTemplate<String, Object> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
  private final ApiGatewayMetrics metrics = Mockito.mock(ApiGatewayMetrics.class);
  private RiskEventPublisher publisher;

  @BeforeEach
  void setUp() {
    var properties =
        new RiskProperties(
            new RiskProperties.Topics(
                "raw-events",
                "rule-updates",
                "raw-audit",
                "decision-audit",
                "high-risk-events",
                "alert-events"),
            new RiskProperties.RateLimit(60_000, 5),
            new RiskProperties.RuleOutbox(5_000, 100));
    publisher = new RiskEventPublisher(kafkaTemplate, properties, metrics, 100);
  }

  @Test
  void recordsSuccessOnlyAfterBrokerAcknowledgement() {
    when(kafkaTemplate.send(eq("raw-events"), eq("user-1"), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    publisher.publishRawEvent(event());

    verify(metrics).recordKafkaPublishSuccess(anyLong());
    verify(metrics, never()).recordKafkaPublishFailure(anyLong());
  }

  @Test
  void failedSendThrowsAndRecordsFailure() {
    CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("broker unavailable"));
    when(kafkaTemplate.send(eq("raw-events"), eq("user-1"), any())).thenReturn(failed);

    assertThatThrownBy(() -> publisher.publishRawEvent(event()))
        .isInstanceOf(KafkaPublishException.class)
        .hasMessage("Failed to publish raw event");

    verify(metrics).recordKafkaPublishFailure(anyLong());
    verify(metrics, never()).recordKafkaPublishSuccess(anyLong());
  }

  @Test
  void immediateSendFailureThrowsAndRecordsFailure() {
    when(kafkaTemplate.send(eq("raw-events"), eq("user-1"), any()))
        .thenThrow(new IllegalStateException("serialization failed"));

    assertThatThrownBy(() -> publisher.publishRawEvent(event()))
        .isInstanceOf(KafkaPublishException.class)
        .hasMessage("Failed to publish raw event");

    verify(metrics).recordKafkaPublishFailure(anyLong());
    verify(metrics, never()).recordKafkaPublishSuccess(anyLong());
  }

  @Test
  void timedOutSendThrowsAndRecordsFailure() {
    publisher = new RiskEventPublisher(kafkaTemplate, properties(), metrics, 1);
    when(kafkaTemplate.send(eq("raw-events"), eq("user-1"), any()))
        .thenReturn(new CompletableFuture<>());

    assertThatThrownBy(() -> publisher.publishRawEvent(event()))
        .isInstanceOf(KafkaPublishException.class)
        .hasMessage("Failed to publish raw event");

    verify(metrics).recordKafkaPublishFailure(anyLong());
    verify(metrics, never()).recordKafkaPublishSuccess(anyLong());
  }

  private RiskProperties properties() {
    return new RiskProperties(
        new RiskProperties.Topics(
            "raw-events",
            "rule-updates",
            "raw-audit",
            "decision-audit",
            "high-risk-events",
            "alert-events"),
        new RiskProperties.RateLimit(60_000, 5),
        new RiskProperties.RuleOutbox(5_000, 100));
  }

  private RiskEvent event() {
    return new RiskEvent(
        "event-1",
        "request-1",
        "user-1",
        "TRANSACTION",
        Instant.parse("2026-09-11T12:00:00Z"),
        1000,
        "USD",
        null,
        "device-1",
        "merchant-1",
        null,
        "test");
  }
}
