package com.realrisk.rules;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
    name = "realrisk.rule-outbox.publisher-enabled",
    havingValue = "true",
    matchIfMissing = true)
public class RuleOutboxRelay {
  private final RuleService ruleService;

  public RuleOutboxRelay(RuleService ruleService) {
    this.ruleService = ruleService;
  }

  @Scheduled(fixedDelayString = "${realrisk.rule-outbox.poll-interval-ms:5000}")
  public void publishPendingOutbox() {
    ruleService.publishPendingOutbox();
  }
}
