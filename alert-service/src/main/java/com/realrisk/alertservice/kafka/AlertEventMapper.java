package com.realrisk.alertservice.kafka;

import com.realrisk.alertservice.model.AlertEvent;
import com.realrisk.avro.AlertEventAvro;

public final class AlertEventMapper {
  private AlertEventMapper() {}

  public static AlertEvent fromAvro(AlertEventAvro event) {
    return new AlertEvent(
        event.getAlertId(),
        event.getDecisionId(),
        event.getEventId(),
        event.getUserId(),
        event.getRiskScore(),
        event.getSeverity(),
        event.getReasonSummary(),
        event.getCreatedAt(),
        event.getSource());
  }
}
