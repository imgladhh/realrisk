package com.realrisk.flink;

import com.realrisk.avro.RuleUpdateAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RuleUpdateKafkaDeserializationSchema
    implements KafkaRecordDeserializationSchema<RuleUpdateEnvelope> {
  private final String schemaRegistryUrl;
  private final Map<String, ?> schemaRegistryConfig;
  private transient KafkaAvroDeserializer deserializer;

  public RuleUpdateKafkaDeserializationSchema(
      String schemaRegistryUrl, Map<String, ?> schemaRegistryConfig) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.schemaRegistryConfig = Map.copyOf(schemaRegistryConfig);
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) {
    Map<String, Object> config = new HashMap<>(schemaRegistryConfig);
    config.put("schema.registry.url", schemaRegistryUrl);
    config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    deserializer = new KafkaAvroDeserializer();
    deserializer.configure(config, false);
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RuleUpdateEnvelope> out) {
    Object decoded = deserializer.deserialize(record.topic(), record.headers(), record.value());
    if (!(decoded instanceof RuleUpdateAvro update)) {
      throw new IllegalStateException("Decoded rule update was not a RuleUpdateAvro");
    }
    out.collect(new RuleUpdateEnvelope(update, record.topic(), record.partition(), record.offset()));
  }

  @Override
  public TypeInformation<RuleUpdateEnvelope> getProducedType() {
    return TypeInformation.of(new TypeHint<>() {});
  }
}
