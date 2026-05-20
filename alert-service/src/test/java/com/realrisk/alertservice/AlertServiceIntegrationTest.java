package com.realrisk.alertservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.avro.AlertEventAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
    registry.add("spring.data.redis.sentinel.master", () -> "");
    registry.add("spring.data.redis.sentinel.nodes", () -> "");
    registry.add("spring.mail.host", () -> "localhost");
    registry.add("spring.mail.port", () -> 29025);
    registry.add("NOTIFICATION_EMAIL_ENABLED", () -> "false");
    registry.add("NOTIFICATION_SLACK_ENABLED", () -> "false");
    registry.add("realrisk.alert.max-retries", () -> 0L);
    registry.add("realrisk.alert.retry-backoff", () -> "10ms");
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

  @Test
  void malformedAlertPayloadIsPublishedToDlq() throws Exception {
    byte[] malformed = "malformed-alert".getBytes(StandardCharsets.UTF_8);
    try (KafkaProducer<String, byte[]> producer = rawProducer()) {
      producer.send(new ProducerRecord<>("alert-events", "user-dlq-1", malformed)).get();
    }

    try (Consumer<byte[], byte[]> consumer = dlqConsumer()) {
      consumer.subscribe(List.of("alert-events-dlq"));
      ConsumerRecord<byte[], byte[]> record = awaitRecord(consumer);
      assertThat(record.value()).isEqualTo(malformed);
      assertThat(headerValue(record, "x-original-topic")).isEqualTo("alert-events");
      assertThat(headerValue(record, "x-failed-at")).isNotBlank();
      assertThat(headerValue(record, "x-retry-count")).isEqualTo("1");
    }
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

  private KafkaProducer<String, byte[]> rawProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA.getBootstrapServers());
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private Consumer<byte[], byte[]> dlqConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-dlq-it-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

  private ConsumerRecord<byte[], byte[]> awaitRecord(Consumer<byte[], byte[]> consumer)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 15_000L;
    while (System.currentTimeMillis() < deadline) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(250));
      if (!records.isEmpty()) {
        return records.iterator().next();
      }
      Thread.sleep(100L);
    }
    throw new AssertionError("Timed out waiting for DLQ record");
  }

  private String headerValue(ConsumerRecord<byte[], byte[]> record, String key) {
    return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
  }
}
