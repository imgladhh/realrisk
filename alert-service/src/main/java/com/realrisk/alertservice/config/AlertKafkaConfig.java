package com.realrisk.alertservice.config;

import com.realrisk.alertservice.kafka.AlertDlqPublisher;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class AlertKafkaConfig {
  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, byte[]>
      alertEventKafkaListenerContainerFactory(
          ConsumerFactory<String, byte[]> alertEventConsumerFactory,
          DefaultErrorHandler alertKafkaErrorHandler) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
    factory.setConsumerFactory(alertEventConsumerFactory);
    factory.setConcurrency(Math.min(4, Runtime.getRuntime().availableProcessors()));
    factory.getContainerProperties().setDeliveryAttemptHeader(true);
    factory.setCommonErrorHandler(alertKafkaErrorHandler);
    return factory;
  }

  @Bean
  ConsumerFactory<String, byte[]> alertEventConsumerFactory(AlertProperties properties) {
    return new DefaultKafkaConsumerFactory<>(consumerProps(properties));
  }

  private Map<String, Object> consumerProps(AlertProperties properties) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumerGroup());
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

  @Bean
  ProducerFactory<byte[], byte[]> alertDlqProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class);
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class);
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  KafkaTemplate<byte[], byte[]> alertDlqKafkaTemplate() {
    return new KafkaTemplate<>(alertDlqProducerFactory());
  }

  @Bean
  DefaultErrorHandler alertKafkaErrorHandler(
      AlertProperties properties, AlertDlqPublisher alertDlqPublisher) {
    DefaultErrorHandler handler =
        new DefaultErrorHandler(
            alertDlqPublisher::publish,
            new FixedBackOff(properties.getRetryBackoff().toMillis(), properties.getMaxRetries()));
    handler.setCommitRecovered(true);
    return handler;
  }
}
