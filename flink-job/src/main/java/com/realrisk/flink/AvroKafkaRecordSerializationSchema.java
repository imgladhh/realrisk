package com.realrisk.flink;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AvroKafkaRecordSerializationSchema<T extends SpecificRecord>
    implements KafkaRecordSerializationSchema<T> {
  private final String topic;
  private final Map<String, ?> schemaRegistryConfig;
  private final UserIdExtractor<T> userIdExtractor;
  private transient KafkaAvroSerializer valueSerializer;
  private transient StringSerializer keySerializer;

  public AvroKafkaRecordSerializationSchema(
      String topic, Map<String, ?> schemaRegistryConfig, UserIdExtractor<T> userIdExtractor) {
    this.topic = topic;
    this.schemaRegistryConfig = schemaRegistryConfig;
    this.userIdExtractor = userIdExtractor;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      T element, KafkaSinkContext context, Long timestamp) {
    ensureSerializers();
    byte[] key = keySerializer.serialize(topic, userIdExtractor.userId(element));
    byte[] value = valueSerializer.serialize(topic, element);
    return new ProducerRecord<>(topic, null, timestamp, key, value);
  }

  private void ensureSerializers() {
    if (valueSerializer == null) {
      valueSerializer = new KafkaAvroSerializer();
      valueSerializer.configure(schemaRegistryConfig, false);
    }
    if (keySerializer == null) {
      keySerializer = new StringSerializer();
    }
  }

  @FunctionalInterface
  public interface UserIdExtractor<T> extends java.io.Serializable {
    String userId(T event);
  }
}
