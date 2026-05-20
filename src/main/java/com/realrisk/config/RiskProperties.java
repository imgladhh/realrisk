package com.realrisk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "realrisk")
public record RiskProperties(Topics topics, RateLimit rateLimit, RuleOutbox ruleOutbox) {
  public record Topics(
      String rawEvents,
      String ruleUpdates,
      String rawAudit,
      String decisionAudit,
      String highRiskEvents,
      String alertEvents) {}

  public record RateLimit(long windowMs, long limit) {}

  public record RuleOutbox(long pollIntervalMs, int batchSize) {}
}
