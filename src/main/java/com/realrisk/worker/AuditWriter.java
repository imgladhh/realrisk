package com.realrisk.worker;

import com.realrisk.avro.RiskEventAvro;
import com.realrisk.config.RiskProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AuditWriter {
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final RiskProperties properties;

  public AuditWriter(KafkaTemplate<String, Object> kafkaTemplate, RiskProperties properties) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
  }

  @KafkaListener(
      topics = "${realrisk.topics.raw-events}",
      groupId = "audit-writer",
      containerFactory = "riskEventKafkaListenerContainerFactory")
  public void copyRawEvent(RiskEventAvro event) {
    kafkaTemplate.send(properties.topics().rawAudit(), event.getUserId(), event);
  }
}
