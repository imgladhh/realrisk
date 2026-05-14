package com.realrisk.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.model.RiskDecision;
import com.realrisk.model.RiskEvent;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class AvroMapperTest {
  @Test
  void riskEventRoundTripsThroughAvroContract() {
    var event =
        new RiskEvent(
            "event-1",
            "request-1",
            "user-1",
            "TRANSACTION",
            Instant.parse("2026-05-10T12:00:00Z"),
            1_200,
            "USD",
            "203.0.113.1",
            "device-1",
            "merchant-1",
            null,
            "api-gateway");

    assertThat(AvroMapper.fromAvro(AvroMapper.toAvro(event))).isEqualTo(event);
  }

  @Test
  void riskEventWithNullOptionalFieldsRoundTrips() {
    var event =
        new RiskEvent(
            "event-2",
            "request-2",
            "user-2",
            "LOGIN",
            Instant.parse("2026-05-10T12:00:00Z"),
            0,
            "USD",
            null,
            null,
            null,
            null,
            "api-gateway");

    assertThat(AvroMapper.fromAvro(AvroMapper.toAvro(event))).isEqualTo(event);
  }

  @Test
  void riskDecisionRoundTripsThroughAvroContract() {
    var decision =
        new RiskDecision(
            "decision-1",
            "event-1",
            "request-1",
            "user-1",
            "BLOCK",
            90,
            List.of("large_amount", "merchant_burst:v2"),
            Instant.parse("2026-05-10T12:01:00Z"),
            "flink-risk-engine");

    assertThat(AvroMapper.fromAvro(AvroMapper.toAvro(decision))).isEqualTo(decision);
  }
}
