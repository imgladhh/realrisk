package com.realrisk.flink;

import com.realrisk.avro.RiskEventAvro;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class RiskRuleEngine {
  private static final String SOURCE = "flink-risk-engine";

  private final RuleSet rules;

  public RiskRuleEngine(RuleSet rules) {
    this.rules = rules;
  }

  public RiskEvaluation evaluate(
      RiskEventAvro event,
      UserProfile userProfile,
      int merchantDistinctUsers,
      Instant createdAt) {
    var reasons = new ArrayList<String>();
    int score = 0;

    if (userProfile.blacklisted()) {
      reasons.add("blacklisted_user");
      score += 100;
    }

    if (userProfile.velocity7d() >= rules.velocityThreshold7d()) {
      reasons.add("high_velocity_7d");
      score += rules.highVelocityScore();
    }

    if (event.getAmountCents() >= rules.largeAmountCents()) {
      reasons.add("large_amount");
      score += rules.largeAmountScore();
    }

    if ("WITHDRAWAL".equals(event.getEventType()) && event.getDeviceFp() == null) {
      reasons.add("withdrawal_without_device");
      score += rules.withdrawalWithoutDeviceScore();
    }

    if (event.getMerchantId() != null
        && merchantDistinctUsers >= rules.merchantBurstThreshold()) {
      reasons.add("merchant_multi_user_burst");
      score += rules.merchantBurstScore();
    }

    return new RiskEvaluation(
        decisionId(event),
        event,
        decisionFor(score),
        Math.min(score, 100),
        new ArrayList<>(reasons),
        createdAt,
        SOURCE);
  }

  private String decisionFor(int score) {
    if (score >= rules.blockThreshold()) return "BLOCK";
    if (score >= rules.reviewThreshold()) return "REVIEW";
    return "ALLOW";
  }

  public static String decisionId(RiskEventAvro event) {
    return UUID.nameUUIDFromBytes(
            ("flink-risk-engine:" + event.getEventId()).getBytes(StandardCharsets.UTF_8))
        .toString();
  }
}
