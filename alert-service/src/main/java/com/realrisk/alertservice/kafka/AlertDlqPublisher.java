package com.realrisk.alertservice.kafka;

import com.realrisk.alertservice.config.AlertProperties;
import com.realrisk.alertservice.metrics.AlertMetrics;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Component
public class AlertDlqPublisher {
  private static final Logger log = LoggerFactory.getLogger(AlertDlqPublisher.class);
  private static final int MAX_EXCEPTION_MESSAGE_BYTES = 512;

  private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
  private final AlertMetrics metrics;
  private final AlertEventCodec codec;
  private final AlertProperties properties;
  private final Clock clock;

  @Autowired
  public AlertDlqPublisher(
      KafkaTemplate<byte[], byte[]> kafkaTemplate,
      AlertMetrics metrics,
      AlertEventCodec codec,
      AlertProperties properties) {
    this(kafkaTemplate, metrics, codec, properties, Clock.systemUTC());
  }

  AlertDlqPublisher(
      KafkaTemplate<byte[], byte[]> kafkaTemplate,
      AlertMetrics metrics,
      AlertEventCodec codec,
      AlertProperties properties,
      Clock clock) {
    this.kafkaTemplate = kafkaTemplate;
    this.metrics = metrics;
    this.codec = codec;
    this.properties = properties;
    this.clock = clock;
  }

  public void publish(ConsumerRecord<?, ?> record, Exception exception) {
    byte[] payload = extractValue(record.value());
    byte[] key = extractKey(record.key());
    Headers headers = copyHeaders(record.headers());
    String severity = resolveSeverity(headers, payload);
    addOrReplace(headers, "x-exception-message", truncate(exception.getMessage()));
    addOrReplace(headers, "x-retry-count", Integer.toString(resolveRetryCount(record.headers())));
    addOrReplace(headers, "x-original-topic", record.topic());
    addOrReplace(headers, "x-failed-at", Instant.now(clock).toString());
    addOrReplace(headers, "severity", severity);
    ProducerRecord<byte[], byte[]> dlqRecord =
        new ProducerRecord<>(properties.getDlqTopic(), null, key, payload, headers);

    try {
      kafkaTemplate.send(dlqRecord).get(10, TimeUnit.SECONDS);
      metrics.incrementDlqPublished(severity);
      log.warn(
          "Published alert failure to DLQ topic={} originalTopic={} partition={} offset={} severity={}",
          properties.getDlqTopic(),
          record.topic(),
          record.partition(),
          record.offset(),
          severity);
    } catch (Exception publishFailure) {
      throw new IllegalStateException("Failed to publish alert record to DLQ", publishFailure);
    }
  }

  private String resolveSeverity(Headers headers, byte[] payload) {
    if (payload == null || payload.length == 0) {
      return "unknown";
    }
    String severity = codec.tryResolveSeverity(headers, payload);
    return severity == null || severity.isBlank() ? "unknown" : severity;
  }

  private int resolveRetryCount(Headers headers) {
    Header attemptHeader = headers.lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
    if (attemptHeader == null || attemptHeader.value() == null || attemptHeader.value().length != 4) {
      return Math.toIntExact(properties.getMaxRetries() + 1);
    }
    return ByteBuffer.wrap(attemptHeader.value()).getInt();
  }

  private Headers copyHeaders(Headers source) {
    RecordHeaders headers = new RecordHeaders();
    for (Header header : source) {
      headers.add(header.key(), header.value());
    }
    return headers;
  }

  private void addOrReplace(Headers headers, String key, String value) {
    headers.remove(key);
    headers.add(key, value.getBytes(StandardCharsets.UTF_8));
  }

  private String truncate(String message) {
    if (message == null) {
      return "";
    }
    return message.length() <= MAX_EXCEPTION_MESSAGE_BYTES
        ? message
        : message.substring(0, MAX_EXCEPTION_MESSAGE_BYTES);
  }

  private byte[] extractValue(Object value) {
    if (value == null) {
      return new byte[0];
    }
    if (value instanceof byte[] bytes) {
      return bytes;
    }
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }

  private byte[] extractKey(Object key) {
    if (key == null) {
      return null;
    }
    if (key instanceof byte[] bytes) {
      return bytes;
    }
    return key.toString().getBytes(StandardCharsets.UTF_8);
  }
}
