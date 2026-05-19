package com.realrisk.alertservice.kafka;

import com.realrisk.avro.AlertEventAvro;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AlertEventCodec {
  private final KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();

  public AlertEventCodec(@Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    props.put("rule.service.loader.enable", "false");
    deserializer.configure(props, false);
  }

  public synchronized AlertEventAvro decode(Headers headers, byte[] payload) {
    Object decoded = deserializer.deserialize(null, headers, payload);
    if (decoded instanceof AlertEventAvro event) {
      return event;
    }
    throw new IllegalStateException("Decoded alert payload was not an AlertEventAvro");
  }

  public synchronized String tryResolveSeverity(Headers headers, byte[] payload) {
    try {
      return decode(headers, payload).getSeverity();
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  @PreDestroy
  void close() {
    deserializer.close();
  }
}
