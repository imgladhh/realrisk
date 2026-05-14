package com.realrisk.kafka;

import com.realrisk.config.RiskProperties;
import com.realrisk.model.RiskEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RiskEventPublisher {
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final RiskProperties properties;

  public RiskEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, RiskProperties properties) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
  }

  public void publishRawEvent(RiskEvent event) {
    kafkaTemplate.send(properties.topics().rawEvents(), event.userId(), AvroMapper.toAvro(event));
  }
}
