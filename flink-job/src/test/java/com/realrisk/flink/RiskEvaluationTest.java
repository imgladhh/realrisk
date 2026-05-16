package com.realrisk.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.avro.RiskEventAvro;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class RiskEvaluationTest {
  @Test
  void copiesReasonsIntoMutableArrayList() {
    List<String> immutableReasons = List.of("large_amount");

    RiskEvaluation evaluation =
        new RiskEvaluation(
            "decision-1",
            event(),
            "BLOCK",
            80,
            immutableReasons,
            Instant.parse("2026-05-10T12:01:00Z"),
            "flink-risk-engine");

    evaluation.reasons().add("merchant_multi_user_burst");

    assertThat(evaluation.reasons())
        .containsExactly("large_amount", "merchant_multi_user_burst");
    assertThat(immutableReasons).containsExactly("large_amount");
    assertThat(evaluation.reasons()).isNotSameAs(immutableReasons);
  }

  private RiskEventAvro event() {
    return RiskEventAvro.newBuilder()
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
        .build();
  }
}
