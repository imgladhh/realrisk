package com.realrisk.kafka;

import com.realrisk.avro.RiskDecisionAvro;
import com.realrisk.avro.RiskEventAvro;
import com.realrisk.model.RiskDecision;
import com.realrisk.model.RiskEvent;
import java.util.List;

public final class AvroMapper {
  private AvroMapper() {}

  public static RiskEventAvro toAvro(RiskEvent event) {
    return RiskEventAvro.newBuilder()
        .setEventId(event.eventId())
        .setRequestId(event.requestId())
        .setUserId(event.userId())
        .setEventType(event.eventType())
        .setTimestamp(event.timestamp())
        .setAmountCents(event.amountCents())
        .setCurrency(event.currency())
        .setIpAddress(event.ipAddress())
        .setDeviceFp(event.deviceFp())
        .setMerchantId(event.merchantId())
        .setCounterparty(event.counterparty())
        .setSource(event.source())
        .build();
  }

  public static RiskEvent fromAvro(RiskEventAvro event) {
    return new RiskEvent(
        event.getEventId(),
        event.getRequestId(),
        event.getUserId(),
        event.getEventType(),
        event.getTimestamp(),
        event.getAmountCents(),
        event.getCurrency(),
        event.getIpAddress(),
        event.getDeviceFp(),
        event.getMerchantId(),
        event.getCounterparty(),
        event.getSource());
  }

  public static RiskDecisionAvro toAvro(RiskDecision decision) {
    return RiskDecisionAvro.newBuilder()
        .setDecisionId(decision.decisionId())
        .setEventId(decision.eventId())
        .setRequestId(decision.requestId())
        .setUserId(decision.userId())
        .setDecision(decision.decision())
        .setRiskScore(decision.riskScore())
        .setReasons(decision.reasons())
        .setCreatedAt(decision.createdAt())
        .setSource(decision.source())
        .build();
  }

  public static RiskDecision fromAvro(RiskDecisionAvro decision) {
    return new RiskDecision(
        decision.getDecisionId(),
        decision.getEventId(),
        decision.getRequestId(),
        decision.getUserId(),
        decision.getDecision(),
        decision.getRiskScore(),
        List.copyOf(decision.getReasons()),
        decision.getCreatedAt(),
        decision.getSource());
  }
}
