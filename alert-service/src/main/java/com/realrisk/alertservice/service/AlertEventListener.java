package com.realrisk.alertservice.service;

import com.realrisk.alertservice.config.AlertProperties;
import com.realrisk.alertservice.kafka.AlertEventMapper;
import com.realrisk.avro.AlertEventAvro;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AlertEventListener {
  private final AlertProcessor alertProcessor;

  public AlertEventListener(AlertProcessor alertProcessor) {
    this.alertProcessor = alertProcessor;
  }

  @KafkaListener(
      topics = "${realrisk.alert.topic}",
      groupId = "${realrisk.alert.consumer-group}",
      containerFactory = "alertEventKafkaListenerContainerFactory")
  public void onAlert(AlertEventAvro event) {
    alertProcessor.process(AlertEventMapper.fromAvro(event));
  }
}
