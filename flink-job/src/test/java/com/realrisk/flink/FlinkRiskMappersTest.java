package com.realrisk.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.avro.RiskEventAvro;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class FlinkRiskMappersTest {
  @Test
  void mappersProduceDeterministicIdsAndCarryCoreFields() {
    RiskEvaluation evaluation =
        new RiskEvaluation(
            "decision-1",
            RiskEventAvro.newBuilder()
                .setEventId("event-1")
                .setRequestId("request-1")
                .setUserId("user-1")
                .setEventType("TRANSACTION")
                .setTimestamp(Instant.parse("2026-05-10T12:00:00Z"))
                .setAmountCents(1_200)
                .setCurrency("USD")
                .setIpAddress(null)
                .setDeviceFp("device-1")
                .setMerchantId("merchant-1")
                .setCounterparty(null)
                .setSource("api-gateway")
                .build(),
            "BLOCK",
            95,
            List.of("large_amount", "merchant_multi_user_burst"),
            Instant.parse("2026-05-10T12:01:00Z"),
            "flink-risk-engine");

    var decision = FlinkRiskMappers.toDecisionAvro(evaluation);
    var highRisk = FlinkRiskMappers.toHighRiskEvent(evaluation);
    var alert = FlinkRiskMappers.toAlertEvent(evaluation);

    assertThat(decision.getDecisionId()).isEqualTo("decision-1");
    assertThat(highRisk.getDecisionId()).isEqualTo("decision-1");
    assertThat(alert.getDecisionId()).isEqualTo("decision-1");
    assertThat(highRisk.getHighRiskEventId()).isNotBlank();
    assertThat(alert.getAlertId()).isNotBlank();
    assertThat(alert.getSeverity()).isEqualTo("CRITICAL");
    assertThat(alert.getReasonSummary()).isEqualTo("large_amount,merchant_multi_user_burst");
  }

  @Test
  void allowDecisionProducesEmptyReasonSummary() {
    RiskEvaluation evaluation =
        new RiskEvaluation(
            "decision-allow",
            RiskEventAvro.newBuilder()
                .setEventId("event-2")
                .setRequestId("request-2")
                .setUserId("user-2")
                .setEventType("LOGIN")
                .setTimestamp(Instant.parse("2026-05-10T12:02:00Z"))
                .setAmountCents(0)
                .setCurrency("USD")
                .setIpAddress(null)
                .setDeviceFp("device-2")
                .setMerchantId(null)
                .setCounterparty(null)
                .setSource("api-gateway")
                .build(),
            "ALLOW",
            0,
            List.of(),
            Instant.parse("2026-05-10T12:02:01Z"),
            "flink-risk-engine");

    var alert = FlinkRiskMappers.toAlertEvent(evaluation);

    assertThat(alert.getReasonSummary()).isEmpty();
  }
}
