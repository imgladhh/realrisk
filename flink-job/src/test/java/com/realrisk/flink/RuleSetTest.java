package com.realrisk.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.avro.RuleUpdateAvro;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RuleSetTest {

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
          "file:///tmp/checkpoints",
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

  @Test
  void defaultsMatchConfig() {
    RuleSet rs = RuleSet.defaultFrom(CONFIG);

    assertThat(rs.largeAmountCents()).isEqualTo(1_000_000L);
    assertThat(rs.largeAmountScore()).isEqualTo(80);
    assertThat(rs.withdrawalWithoutDeviceScore()).isEqualTo(30);
    assertThat(rs.merchantBurstThreshold()).isEqualTo(10);
    assertThat(rs.merchantBurstScore()).isEqualTo(70);
    assertThat(rs.velocityThreshold7d()).isEqualTo(100);
    assertThat(rs.highVelocityScore()).isEqualTo(40);
    assertThat(rs.reviewThreshold()).isEqualTo(60);
    assertThat(rs.blockThreshold()).isEqualTo(80);
  }

  @Test
  void largeAmountUpdateOverridesThresholdAndScore() {
    RuleSet rs =
        RuleSet.from(
            CONFIG,
            List.of(
                entry(
                    "rule-1",
                    "large_amount",
                    true,
                    Map.of("amount_cents", "500000", "score_delta", "85"))));

    assertThat(rs.largeAmountCents()).isEqualTo(500_000L);
    assertThat(rs.largeAmountScore()).isEqualTo(85);
    assertThat(rs.merchantBurstThreshold()).isEqualTo(10);
    assertThat(rs.withdrawalWithoutDeviceScore()).isEqualTo(30);
  }

  @Test
  void partialParametersLeaveOtherFieldsAtDefault() {
    RuleSet rs =
        RuleSet.from(
            CONFIG,
            List.of(entry("rule-1", "large_amount", true, Map.of("amount_cents", "200000"))));

    assertThat(rs.largeAmountCents()).isEqualTo(200_000L);
    assertThat(rs.largeAmountScore()).isEqualTo(80);
  }

  @Test
  void disabledRuleIsIgnored() {
    RuleSet rs =
        RuleSet.from(
            CONFIG, List.of(entry("rule-1", "large_amount", false, Map.of("amount_cents", "1"))));

    assertThat(rs.largeAmountCents()).isEqualTo(1_000_000L);
  }

  @Test
  void globalRuleUpdatesDecisionThresholds() {
    RuleSet rs =
        RuleSet.from(
            CONFIG,
            List.of(
                entry(
                    "rule-global",
                    "global",
                    true,
                    Map.of("review_threshold", "50", "block_threshold", "75"))));

    assertThat(rs.reviewThreshold()).isEqualTo(50);
    assertThat(rs.blockThreshold()).isEqualTo(75);
  }

  @Test
  void highVelocityRuleUpdatesThresholdAndScore() {
    RuleSet rs =
        RuleSet.from(
            CONFIG,
            List.of(
                entry(
                    "rule-velocity",
                    "high_velocity_7d",
                    true,
                    Map.of("velocity_threshold", "250", "score_delta", "55"))));

    assertThat(rs.velocityThreshold7d()).isEqualTo(250);
    assertThat(rs.highVelocityScore()).isEqualTo(55);
  }

  @Test
  void multipleRulesOfDifferentTypesAllApply() {
    RuleSet rs =
        RuleSet.from(
            CONFIG,
            List.of(
                entry("rule-1", "large_amount", true, Map.of("amount_cents", "300000")),
                entry(
                    "rule-2",
                    "merchant_multi_user_burst",
                    true,
                    Map.of("burst_threshold", "5")),
                entry("rule-3", "global", true, Map.of("block_threshold", "70"))));

    assertThat(rs.largeAmountCents()).isEqualTo(300_000L);
    assertThat(rs.merchantBurstThreshold()).isEqualTo(5);
    assertThat(rs.blockThreshold()).isEqualTo(70);
    assertThat(rs.velocityThreshold7d()).isEqualTo(100);
    assertThat(rs.withdrawalWithoutDeviceScore()).isEqualTo(30);
  }

  @Test
  void unknownRuleTypeIsIgnoredSafely() {
    RuleSet rs =
        RuleSet.from(
            CONFIG, List.of(entry("rule-x", "nonexistent_rule_type", true, Map.of("foo", "bar"))));

    assertThat(rs).isEqualTo(RuleSet.defaultFrom(CONFIG));
  }

  private static Map.Entry<String, RuleUpdateAvro> entry(
      String ruleId, String ruleType, boolean enabled, Map<String, String> parameters) {
    return Map.entry(
        ruleId,
        RuleUpdateAvro.newBuilder()
            .setRuleId(ruleId)
            .setRuleType(ruleType)
            .setEnabled(enabled)
            .setParameters(parameters)
            .setUpdatedAt(Instant.parse("2026-05-13T00:00:00Z"))
            .build());
  }
}
