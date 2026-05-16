package com.realrisk.alertservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.avro.AlertEventAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class AlertServiceIntegrationTest {
  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
          .withDatabaseName("realrisk")
          .withUsername("realrisk")
          .withPassword("realrisk");

  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

  @Autowired private JdbcTemplate jdbcTemplate;

  @DynamicPropertySource
  static void registerProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.kafka.properties.schema.registry.url", () -> "mock://alert-service-it");
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
  }

  @Test
  void consumesKafkaAlertAndWritesAlertLog() throws Exception {
    try (KafkaProducer<String, AlertEventAvro> producer = producer()) {
      producer
          .send(
              new ProducerRecord<>(
                  "alert-events",
                  "user-1",
                  AlertEventAvro.newBuilder()
                      .setAlertId("alert-it-1")
                      .setDecisionId("decision-it-1")
                      .setEventId("event-it-1")
                      .setUserId("user-1")
                      .setRiskScore(90)
                      .setSeverity("HIGH")
                      .setReasonSummary("large_amount")
                      .setCreatedAt(Instant.parse("2026-05-16T16:10:00Z"))
                      .setSource("flink-risk-engine")
                      .build()))
          .get();
    }

    long deadline = System.currentTimeMillis() + 15_000L;
    while (System.currentTimeMillis() < deadline) {
      Integer count =
          jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM alert_log WHERE alert_id = ?",
              Integer.class,
              "alert-it-1");
      if (count != null && count == 1) {
        var row =
            jdbcTemplate.queryForMap(
                "SELECT user_id, severity, reason_summary FROM alert_log WHERE alert_id = ?",
                "alert-it-1");
        assertThat(row.get("user_id")).isEqualTo("user-1");
        assertThat(row.get("severity")).isEqualTo("HIGH");
        assertThat(row.get("reason_summary")).isEqualTo("large_amount");
        return;
      }
      Thread.sleep(250L);
    }

    throw new AssertionError("Timed out waiting for alert_log row");
  }

  private KafkaProducer<String, AlertEventAvro> producer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA.getBootstrapServers());
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", KafkaAvroSerializer.class.getName());
    props.put("schema.registry.url", "mock://alert-service-it");
    props.put("rule.service.loader.enable", "false");
    return new KafkaProducer<>(props);
  }
}
