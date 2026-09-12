package com.realrisk.kafka;

import com.realrisk.config.RiskProperties;
import com.realrisk.metrics.ApiGatewayMetrics;
import com.realrisk.model.RiskEvent;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RiskEventPublisher {
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final RiskProperties properties;
  private final ApiGatewayMetrics metrics;
  private final long publishTimeoutMs;

  public RiskEventPublisher(
      KafkaTemplate<String, Object> kafkaTemplate,
      RiskProperties properties,
      ApiGatewayMetrics metrics,
      @Value("${realrisk.kafka.publish-timeout-ms:6000}") long publishTimeoutMs) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
    this.metrics = metrics;
    if (publishTimeoutMs <= 0) {
      throw new IllegalArgumentException("Kafka publish timeout must be positive");
    }
    this.publishTimeoutMs = publishTimeoutMs;
  }

  public void publishRawEvent(RiskEvent event) {
    long startedAt = System.nanoTime();
    try {
      kafkaTemplate
          .send(properties.topics().rawEvents(), event.userId(), AvroMapper.toAvro(event))
          .get(publishTimeoutMs, TimeUnit.MILLISECONDS);
      metrics.recordKafkaPublishSuccess(System.nanoTime() - startedAt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      metrics.recordKafkaPublishFailure(System.nanoTime() - startedAt);
      throw new KafkaPublishException("Interrupted while publishing raw event", e);
    } catch (ExecutionException | TimeoutException e) {
      metrics.recordKafkaPublishFailure(System.nanoTime() - startedAt);
      throw new KafkaPublishException("Failed to publish raw event", e);
    } catch (RuntimeException e) {
      metrics.recordKafkaPublishFailure(System.nanoTime() - startedAt);
      throw new KafkaPublishException("Failed to publish raw event", e);
    }
  }
}
