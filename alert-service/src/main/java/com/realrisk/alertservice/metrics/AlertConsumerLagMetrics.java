package com.realrisk.alertservice.metrics;

import com.realrisk.alertservice.config.AlertProperties;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AlertConsumerLagMetrics {
  private static final Logger log = LoggerFactory.getLogger(AlertConsumerLagMetrics.class);

  private final AlertProperties properties;
  private final AlertMetrics alertMetrics;
  private final AdminClient adminClient;

  public AlertConsumerLagMetrics(
      AlertProperties properties,
      AlertMetrics alertMetrics,
      @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
    this.properties = properties;
    this.alertMetrics = alertMetrics;
    this.adminClient = AdminClient.create(adminProps(bootstrapServers));
  }

  @Scheduled(fixedDelayString = "${realrisk.alert.lag-sample-interval-ms:30000}")
  public void sampleLag() {
    try {
      var committedOffsets =
          adminClient
              .listConsumerGroupOffsets(
                  properties.getConsumerGroup(),
                  new ListConsumerGroupOffsetsOptions().timeoutMs((int) Duration.ofSeconds(5).toMillis()))
              .partitionsToOffsetAndMetadata()
              .get();

      if (committedOffsets.isEmpty()) {
        alertMetrics.setConsumerLag(0);
        return;
      }

      Map<TopicPartition, OffsetSpec> latestOffsetsRequest =
          committedOffsets.keySet().stream()
              .collect(java.util.stream.Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

      var latestOffsets = adminClient.listOffsets(latestOffsetsRequest).all().get();

      long lag =
          committedOffsets.entrySet().stream()
              .mapToLong(
                  entry -> {
                    long latest = latestOffsets.get(entry.getKey()).offset();
                    long committed = entry.getValue().offset();
                    return Math.max(latest - committed, 0);
                  })
              .sum();

      alertMetrics.setConsumerLag(lag);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug("Interrupted while sampling alert consumer lag", e);
    } catch (ExecutionException | RuntimeException e) {
      log.debug("Unable to sample alert consumer lag", e);
    }
  }

  @PreDestroy
  public void close() {
    adminClient.close(Duration.ofSeconds(5));
  }

  private Properties adminProps(String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    return props;
  }
}
