package com.realrisk.flink;

import com.realrisk.avro.AlertEventAvro;
import com.realrisk.avro.HighRiskEventAvro;
import com.realrisk.avro.RiskDecisionAvro;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;
import java.util.UUID;

public final class FlinkRiskMappers {
  private FlinkRiskMappers() {}

  public static RiskDecisionAvro toDecisionAvro(RiskEvaluation evaluation) {
    return RiskDecisionAvro.newBuilder()
        .setDecisionId(evaluation.decisionId())
        .setEventId(evaluation.event().getEventId())
        .setRequestId(evaluation.event().getRequestId())
        .setUserId(evaluation.event().getUserId())
        .setDecision(evaluation.decision())
        .setRiskScore(evaluation.riskScore())
        .setReasons(evaluation.reasons())
        .setCreatedAt(evaluation.createdAt())
        .setSource(evaluation.source())
        .build();
  }

  public static HighRiskEventAvro toHighRiskEvent(RiskEvaluation evaluation) {
    return HighRiskEventAvro.newBuilder()
        .setHighRiskEventId(stableId("high-risk", evaluation.decisionId()))
        .setDecisionId(evaluation.decisionId())
        .setEventId(evaluation.event().getEventId())
        .setRequestId(evaluation.event().getRequestId())
        .setUserId(evaluation.event().getUserId())
        .setMerchantId(evaluation.event().getMerchantId())
        .setRiskScore(evaluation.riskScore())
        .setReasons(evaluation.reasons())
        .setCreatedAt(evaluation.createdAt())
        .setSource(evaluation.source())
        .build();
  }

  public static AlertEventAvro toAlertEvent(RiskEvaluation evaluation) {
    return AlertEventAvro.newBuilder()
        .setAlertId(stableId("alert", evaluation.decisionId()))
        .setDecisionId(evaluation.decisionId())
        .setEventId(evaluation.event().getEventId())
        .setUserId(evaluation.event().getUserId())
        .setRiskScore(evaluation.riskScore())
        .setSeverity(alertSeverity(evaluation.riskScore()))
        .setReasonSummary(reasonSummary(evaluation))
        .setCreatedAt(evaluation.createdAt())
        .setSource(evaluation.source())
        .build();
  }

  private static String stableId(String prefix, String seed) {
    return UUID.nameUUIDFromBytes((prefix + ":" + seed).getBytes(StandardCharsets.UTF_8))
        .toString();
  }

  private static String alertSeverity(int riskScore) {
    if (riskScore >= 95) {
      return "CRITICAL";
    }
    if (riskScore >= 90) {
      return "HIGH";
    }
    return "MEDIUM";
  }

  private static String reasonSummary(RiskEvaluation evaluation) {
    var joiner = new StringJoiner(",");
    evaluation.reasons().forEach(joiner::add);
    return joiner.toString();
  }
}
