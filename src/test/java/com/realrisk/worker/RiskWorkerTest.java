package com.realrisk.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.realrisk.avro.RiskDecisionAvro;
import com.realrisk.config.RiskProperties;
import com.realrisk.model.RiskEvent;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class RiskWorkerTest {
  @Test
  void decisionIdIsDeterministicForSameEventId() {
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, Object> kafkaTemplate = org.mockito.Mockito.mock(KafkaTemplate.class);
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
    var worker = new RiskWorker(kafkaTemplate, properties);

    worker.evaluate(event("event-1"));
    worker.evaluate(event("event-1"));

    ArgumentCaptor<RiskDecisionAvro> captor = ArgumentCaptor.forClass(RiskDecisionAvro.class);
    verify(kafkaTemplate, org.mockito.Mockito.times(2))
        .send(eq("decision-audit"), eq("user-1"), captor.capture());

    assertThat(captor.getAllValues().get(0).getDecisionId())
        .isEqualTo(captor.getAllValues().get(1).getDecisionId());
  }

  private RiskEvent event(String eventId) {
    return new RiskEvent(
        eventId,
        "request-1",
        "user-1",
        "TRANSACTION",
        Instant.parse("2026-05-10T12:00:00Z"),
        2_000_000,
        "USD",
        null,
        "device-1",
        "merchant-1",
        null,
        "test");
  }
}
