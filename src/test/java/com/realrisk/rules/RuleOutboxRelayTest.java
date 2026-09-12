package com.realrisk.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RuleOutboxRelayTest {
  @Test
  void delegatesScheduledPublicationToRuleService() {
    RuleService ruleService = Mockito.mock(RuleService.class);

    new RuleOutboxRelay(ruleService).publishPendingOutbox();

    verify(ruleService).publishPendingOutbox();
  }

  @Test
  void publisherCanBeDisabledForApiGatewayReplicas() {
    RuleService ruleService = Mockito.mock(RuleService.class);

    new ApplicationContextRunner()
        .withBean(RuleService.class, () -> ruleService)
        .withUserConfiguration(RuleOutboxRelay.class)
        .withPropertyValues("realrisk.rule-outbox.publisher-enabled=false")
        .run(context -> assertThat(context).doesNotHaveBean(RuleOutboxRelay.class));
  }
}
