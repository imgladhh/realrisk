package com.realrisk.alertservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.realrisk.alertservice.config.AlertProperties;
import com.realrisk.alertservice.metrics.AlertMetrics;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;

class AlertDlqPublisherTest {
  @Test
  void publishesOriginalPayloadWithFailureHeaders() {
    @SuppressWarnings("unchecked")
    KafkaTemplate<byte[], byte[]> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
    AlertMetrics metrics = Mockito.mock(AlertMetrics.class);
    AlertEventCodec codec = Mockito.mock(AlertEventCodec.class);
    AlertProperties properties = new AlertProperties();
    properties.setDlqTopic("alert-events-dlq");
    properties.setMaxRetries(2);
    Clock clock = Clock.fixed(Instant.parse("2026-05-17T23:45:00Z"), ZoneOffset.UTC);

    byte[] payload = "bad-payload".getBytes(StandardCharsets.UTF_8);
    byte[] key = "user-1".getBytes(StandardCharsets.UTF_8);
    RecordHeaders headers = new RecordHeaders();
    headers.add("existing", "value".getBytes(StandardCharsets.UTF_8));
    headers.add(
        org.springframework.kafka.support.KafkaHeaders.DELIVERY_ATTEMPT,
        ByteBuffer.allocate(4).putInt(3).array());
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("alert-events", 1, 42L, key, payload);
    headers.forEach(header -> record.headers().add(header));

    when(codec.tryResolveSeverity(any(), eq(payload))).thenReturn("CRITICAL");
    when(kafkaTemplate.send(Mockito.any(ProducerRecord.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    AlertDlqPublisher publisher =
        new AlertDlqPublisher(kafkaTemplate, metrics, codec, properties, clock);

    publisher.publish(record, new IllegalArgumentException("payload exploded"));

    ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafkaTemplate).send(captor.capture());
    ProducerRecord<byte[], byte[]> dlqRecord = captor.getValue();
    assertThat(dlqRecord.topic()).isEqualTo("alert-events-dlq");
    assertThat(dlqRecord.value()).isEqualTo(payload);
    assertThat(new String(dlqRecord.key(), StandardCharsets.UTF_8)).isEqualTo("user-1");
    assertThat(headerValue(dlqRecord, "x-original-topic")).isEqualTo("alert-events");
    assertThat(headerValue(dlqRecord, "x-failed-at")).isEqualTo("2026-05-17T23:45:00Z");
    assertThat(headerValue(dlqRecord, "x-retry-count")).isEqualTo("3");
    assertThat(headerValue(dlqRecord, "x-exception-message")).isEqualTo("payload exploded");
    verify(metrics).incrementDlqPublished("CRITICAL");
  }

  private String headerValue(ProducerRecord<byte[], byte[]> record, String key) {
    return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
  }
}
