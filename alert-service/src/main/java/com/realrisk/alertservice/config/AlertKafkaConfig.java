package com.realrisk.alertservice.config;

import com.realrisk.avro.AlertEventAvro;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class AlertKafkaConfig {
  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  @Bean
  NewTopic alertEventsTopic(AlertProperties properties) {
    return TopicBuilder.name(properties.getTopic()).partitions(4).replicas(1).build(); // dev only; production must be >=3
  }

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, AlertEventAvro>
      alertEventKafkaListenerContainerFactory(
          ConsumerFactory<String, AlertEventAvro> alertEventConsumerFactory,
          AlertProperties properties) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, AlertEventAvro>();
    factory.setConsumerFactory(alertEventConsumerFactory);
    factory.setConcurrency(Math.min(4, Runtime.getRuntime().availableProcessors()));
    factory.setCommonErrorHandler(alertKafkaErrorHandler(properties));
    return factory;
  }

  @Bean
  ConsumerFactory<String, AlertEventAvro> alertEventConsumerFactory(AlertProperties properties) {
    return new DefaultKafkaConsumerFactory<>(consumerProps(properties));
  }

  private Map<String, Object> consumerProps(AlertProperties properties) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumerGroup());
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    props.put("rule.service.loader.enable", "false");
    return props;
  }

  @Bean
  DefaultErrorHandler alertKafkaErrorHandler(AlertProperties properties) {
    return new DefaultErrorHandler(
        new FixedBackOff(
            properties.getConsumerRetryBackoff().toMillis(),
            properties.getConsumerRetryAttempts()));
  }
}
