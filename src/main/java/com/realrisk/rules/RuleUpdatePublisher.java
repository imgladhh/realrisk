package com.realrisk.rules;

import com.realrisk.avro.RuleUpdateAvro;
import com.realrisk.config.RiskProperties;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RuleUpdatePublisher {
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final RiskProperties properties;

  public RuleUpdatePublisher(KafkaTemplate<String, Object> kafkaTemplate, RiskProperties properties) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
  }

  public void publishBlocking(RuleOutboxPayload payload) {
    var avro =
        RuleUpdateAvro.newBuilder()
            .setRuleId(payload.ruleId())
            .setRuleType(payload.ruleType())
            .setEnabled(payload.enabled())
            .setParameters(new HashMap<>(payload.parameters()))
            .setUpdatedAt(payload.updatedAt())
            .build();
    try {
      kafkaTemplate.send(properties.topics().ruleUpdates(), payload.ruleId(), avro).get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to publish rule update for ruleId=%s".formatted(payload.ruleId()), e);
    }
  }
}
