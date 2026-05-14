package com.realrisk.config;

import com.realrisk.avro.RiskDecisionAvro;
import com.realrisk.avro.RiskEventAvro;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class KafkaConfig {
  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.properties.schema.registry.url}")
  private String schemaRegistryUrl;

  @Bean
  NewTopic rawEventsTopic(RiskProperties properties) {
    return TopicBuilder.name(properties.topics().rawEvents()).partitions(16).replicas(1).build(); // dev only; production must be >=3
  }

  @Bean
  NewTopic rawAuditTopic(RiskProperties properties) {
    return TopicBuilder.name(properties.topics().rawAudit()).partitions(8).replicas(1).build(); // dev only; production must be >=3
  }

  @Bean
  NewTopic decisionAuditTopic(RiskProperties properties) {
    return TopicBuilder.name(properties.topics().decisionAudit()).partitions(8).replicas(1).build(); // dev only; production must be >=3
  }

  @Bean
  NewTopic highRiskEventsTopic(RiskProperties properties) {
    return TopicBuilder.name(properties.topics().highRiskEvents()).partitions(4).replicas(1).build(); // dev only; production must be >=3
  }

  @Bean
  NewTopic alertEventsTopic(RiskProperties properties) {
    return TopicBuilder.name(properties.topics().alertEvents()).partitions(4).replicas(1).build(); // dev only; production must be >=3
  }

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, RiskEventAvro> riskEventKafkaListenerContainerFactory(
      @Qualifier("riskEventConsumerFactory") ConsumerFactory<String, RiskEventAvro> consumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, RiskEventAvro>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(Math.min(16, Runtime.getRuntime().availableProcessors()));
    return factory;
  }

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, RiskDecisionAvro> riskDecisionKafkaListenerContainerFactory(
      @Qualifier("riskDecisionConsumerFactory")
          ConsumerFactory<String, RiskDecisionAvro> consumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, RiskDecisionAvro>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(Math.min(8, Runtime.getRuntime().availableProcessors()));
    return factory;
  }

  @Bean
  ConsumerFactory<String, RiskEventAvro> riskEventConsumerFactory() {
    return avroConsumerFactory();
  }

  @Bean
  ConsumerFactory<String, RiskDecisionAvro> riskDecisionConsumerFactory() {
    return avroConsumerFactory();
  }

  private <T> ConsumerFactory<String, T> avroConsumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerProps());
  }

  private Map<String, Object> consumerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    return props;
  }
}
