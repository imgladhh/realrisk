package com.realrisk.flink;

import com.realrisk.avro.RuleUpdateAvro;
import java.io.Serializable;
import java.util.Map;

/**
 * Immutable snapshot of the currently active rule parameters.
 * Rebuilt from broadcast state on every processElement call.
 * Fields not covered by any active rule fall back to FlinkRiskJobConfig defaults.
 */
public record RuleSet(
    long largeAmountCents,
    int largeAmountScore,
    int withdrawalWithoutDeviceScore,
    int merchantBurstThreshold,
    int merchantBurstScore,
    int velocityThreshold7d,
    int highVelocityScore,
    int reviewThreshold,
    int blockThreshold)
    implements Serializable {

  /** Construct a RuleSet entirely from config defaults (no active rules). */
  public static RuleSet defaultFrom(FlinkRiskJobConfig config) {
    return new RuleSet(
        config.largeAmountCents(),
        80,
        30,
        config.merchantBurstThreshold(),
        70,
        config.velocityThreshold7d(),
        config.highVelocityScore(),
        config.reviewThreshold(),
        config.blockThreshold());
  }

  /**
   * Construct a RuleSet by applying all active rule entries on top of config defaults.
   *
   * @param activeRules iterable of broadcast state entries (ruleId -> RuleUpdateAvro). Disabled
   *     rules must already have been removed from broadcast state; any entry with enabled=false
   *     found here is silently skipped.
   */
  public static RuleSet from(
      FlinkRiskJobConfig config, Iterable<Map.Entry<String, RuleUpdateAvro>> activeRules) {

    long largeAmountCents = config.largeAmountCents();
    int largeAmountScore = 80;
    int withdrawalScore = 30;
    int burstThreshold = config.merchantBurstThreshold();
    int burstScore = 70;
    int velocityThreshold7d = config.velocityThreshold7d();
    int highVelocityScore = config.highVelocityScore();
    int reviewThreshold = config.reviewThreshold();
    int blockThreshold = config.blockThreshold();

    for (Map.Entry<String, RuleUpdateAvro> entry : activeRules) {
      RuleUpdateAvro rule = entry.getValue();
      if (!rule.getEnabled()) {
        continue;
      }
      Map<String, String> p = rule.getParameters();
      switch (rule.getRuleType()) {
        case "large_amount" -> {
          if (p.containsKey("amount_cents")) {
            largeAmountCents = Long.parseLong(p.get("amount_cents"));
          }
          if (p.containsKey("score_delta")) {
            largeAmountScore = Integer.parseInt(p.get("score_delta"));
          }
        }
        case "withdrawal_without_device" -> {
          if (p.containsKey("score_delta")) {
            withdrawalScore = Integer.parseInt(p.get("score_delta"));
          }
        }
        case "merchant_multi_user_burst" -> {
          if (p.containsKey("burst_threshold")) {
            burstThreshold = Integer.parseInt(p.get("burst_threshold"));
          }
          if (p.containsKey("score_delta")) {
            burstScore = Integer.parseInt(p.get("score_delta"));
          }
        }
        case "high_velocity_7d" -> {
          if (p.containsKey("velocity_threshold")) {
            velocityThreshold7d = Integer.parseInt(p.get("velocity_threshold"));
          }
          if (p.containsKey("score_delta")) {
            highVelocityScore = Integer.parseInt(p.get("score_delta"));
          }
        }
        case "global" -> {
          if (p.containsKey("review_threshold")) {
            reviewThreshold = Integer.parseInt(p.get("review_threshold"));
          }
          if (p.containsKey("block_threshold")) {
            blockThreshold = Integer.parseInt(p.get("block_threshold"));
          }
        }
        default -> {
          // Unknown rule types are ignored so newer producers do not break older jobs.
        }
      }
    }

    return new RuleSet(
        largeAmountCents,
        largeAmountScore,
        withdrawalScore,
        burstThreshold,
        burstScore,
        velocityThreshold7d,
        highVelocityScore,
        reviewThreshold,
        blockThreshold);
  }
}
