package com.realrisk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "realrisk")
public record RiskProperties(Topics topics, RateLimit rateLimit) {
  public record Topics(
      String rawEvents,
      String rawAudit,
      String decisionAudit,
      String highRiskEvents,
      String alertEvents) {}

  public record RateLimit(long windowMs, long limit) {}
}
