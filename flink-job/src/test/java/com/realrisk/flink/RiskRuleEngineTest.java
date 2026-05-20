package com.realrisk.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.avro.RiskEventAvro;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class RiskRuleEngineTest {

  private static final FlinkRiskJobConfig CONFIG =
      new FlinkRiskJobConfig(
          "localhost:9092",
          "http://localhost:8081",
          "localhost",
          6379,
          "",
          "",
          "raw-events",
          "rule-updates",
          "decision-audit",
          "high-risk-events",
          "alert-events",
          "file:///tmp/realrisk-flink-checkpoints",
          1,
          1_000_000L,
          60,
          80,
          85,
          90,
          10,
          100,
          40,
          Duration.ofMinutes(5),
          Duration.ofSeconds(5));

  private RiskRuleEngine engine() {
    return new RiskRuleEngine(RuleSet.defaultFrom(CONFIG));
  }

  @Test
  void largeAmountBecomesBlockingDecision() {
    RiskEvaluation evaluation =
        engine()
            .evaluate(
                event("TRANSACTION", 2_000_000L, "device-1", "merchant-1"),
                UserProfile.empty(),
                1,
                instant());

    assertThat(evaluation.decision()).isEqualTo("BLOCK");
    assertThat(evaluation.riskScore()).isEqualTo(80);
    assertThat(evaluation.reasons()).containsExactly("large_amount");
  }

  @Test
  void merchantBurstElevatesCrossUserPattern() {
    RiskEvaluation evaluation =
        engine()
            .evaluate(
                event("TRANSACTION", 10_000L, "device-1", "merchant-1"),
                UserProfile.empty(),
                10,
                instant());

    assertThat(evaluation.decision()).isEqualTo("REVIEW");
    assertThat(evaluation.riskScore()).isEqualTo(70);
    assertThat(evaluation.reasons()).containsExactly("merchant_multi_user_burst");
  }

  @Test
  void withdrawalWithoutDeviceStacksWithBurst() {
    RiskEvaluation evaluation =
        engine()
            .evaluate(
                event("WITHDRAWAL", 10_000L, null, "merchant-1"),
                UserProfile.empty(),
                10,
                instant());

    assertThat(evaluation.decision()).isEqualTo("BLOCK");
    assertThat(evaluation.riskScore()).isEqualTo(100);
    assertThat(evaluation.reasons())
        .containsExactly("withdrawal_without_device", "merchant_multi_user_burst");
  }

  @Test
  void allowDecisionKeepsReasonsEmpty() {
    RiskEvaluation evaluation =
        engine().evaluate(event("LOGIN", 0L, "device-1", null), UserProfile.empty(), 0, instant());

    assertThat(evaluation.decision()).isEqualTo("ALLOW");
    assertThat(evaluation.riskScore()).isZero();
    assertThat(evaluation.reasons()).isEmpty();
  }

  @Test
  void dynamicLargeAmountThresholdIsRespected() {
    RuleSet customRules = new RuleSet(500_000L, 90, 30, 10, 70, 100, 40, 60, 80);
    RiskRuleEngine customEngine = new RiskRuleEngine(customRules);

    RiskEvaluation evaluation =
        customEngine.evaluate(
            event("TRANSACTION", 600_000L, "device-1", "merchant-1"),
            UserProfile.empty(),
            0,
            instant());

    assertThat(evaluation.decision()).isEqualTo("BLOCK");
    assertThat(evaluation.riskScore()).isEqualTo(90);
    assertThat(evaluation.reasons()).containsExactly("large_amount");
  }

  @Test
  void dynamicBurstThresholdIsRespected() {
    RuleSet customRules = new RuleSet(1_000_000L, 80, 30, 3, 70, 100, 40, 60, 80);
    RiskRuleEngine customEngine = new RiskRuleEngine(customRules);

    RiskEvaluation evaluation =
        customEngine.evaluate(
            event("TRANSACTION", 10_000L, "device-1", "merchant-1"),
            UserProfile.empty(),
            5,
            instant());

    assertThat(evaluation.decision()).isEqualTo("REVIEW");
    assertThat(evaluation.riskScore()).isEqualTo(70);
    assertThat(evaluation.reasons()).containsExactly("merchant_multi_user_burst");
  }

  @Test
  void blacklistedUserBecomesBlockingDecision() {
    RiskEvaluation evaluation =
        engine()
            .evaluate(
                event("TRANSACTION", 10_000L, "device-1", "merchant-1"),
                new UserProfile(true, 0),
                0,
                instant());

    assertThat(evaluation.decision()).isEqualTo("BLOCK");
    assertThat(evaluation.riskScore()).isEqualTo(100);
    assertThat(evaluation.reasons()).containsExactly("blacklisted_user");
  }

  @Test
  void highVelocityAddsScore() {
    RiskEvaluation evaluation =
        engine()
            .evaluate(
                event("TRANSACTION", 10_000L, "device-1", "merchant-1"),
                new UserProfile(false, 100),
                0,
                instant());

    assertThat(evaluation.decision()).isEqualTo("ALLOW");
    assertThat(evaluation.riskScore()).isEqualTo(40);
    assertThat(evaluation.reasons()).containsExactly("high_velocity_7d");
  }

  private RiskEventAvro event(
      String eventType, long amountCents, String deviceFp, String merchantId) {
    return RiskEventAvro.newBuilder()
        .setEventId("event-1")
        .setRequestId("request-1")
        .setUserId("user-1")
        .setEventType(eventType)
        .setTimestamp(instant())
        .setAmountCents(amountCents)
        .setCurrency("USD")
        .setIpAddress(null)
        .setDeviceFp(deviceFp)
        .setMerchantId(merchantId)
        .setCounterparty(null)
        .setSource("api-gateway")
        .build();
  }

  private Instant instant() {
    return Instant.parse("2026-05-10T12:00:00Z");
  }
}
