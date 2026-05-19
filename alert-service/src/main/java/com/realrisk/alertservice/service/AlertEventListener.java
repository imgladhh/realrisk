package com.realrisk.alertservice.service;

import com.realrisk.alertservice.kafka.AlertEventCodec;
import com.realrisk.alertservice.kafka.AlertEventMapper;
import com.realrisk.avro.AlertEventAvro;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AlertEventListener {
  private final AlertProcessor alertProcessor;
  private final AlertEventCodec alertEventCodec;

  public AlertEventListener(AlertProcessor alertProcessor, AlertEventCodec alertEventCodec) {
    this.alertProcessor = alertProcessor;
    this.alertEventCodec = alertEventCodec;
  }

  @KafkaListener(
      topics = "${realrisk.alert.topic}",
      groupId = "${realrisk.alert.consumer-group}",
      containerFactory = "alertEventKafkaListenerContainerFactory")
  public void onAlert(ConsumerRecord<String, byte[]> record) {
    AlertEventAvro event = alertEventCodec.decode(record.headers(), record.value());
    alertProcessor.process(AlertEventMapper.fromAvro(event));
  }
}
