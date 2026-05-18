package com.realrisk.kafka;

import com.realrisk.config.RiskProperties;
import com.realrisk.metrics.ApiGatewayMetrics;
import com.realrisk.model.RiskEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RiskEventPublisher {
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final RiskProperties properties;
  private final ApiGatewayMetrics metrics;

  public RiskEventPublisher(
      KafkaTemplate<String, Object> kafkaTemplate,
      RiskProperties properties,
      ApiGatewayMetrics metrics) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
    this.metrics = metrics;
  }

  public void publishRawEvent(RiskEvent event) {
    long startedAt = System.nanoTime();
    kafkaTemplate
        .send(properties.topics().rawEvents(), event.userId(), AvroMapper.toAvro(event))
        .whenComplete(
            (result, error) -> {
              long elapsed = System.nanoTime() - startedAt;
              if (error == null) {
                metrics.recordKafkaPublishSuccess(elapsed);
              } else {
                metrics.recordKafkaPublishFailure(elapsed);
              }
            });
  }
}
