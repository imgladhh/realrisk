package com.realrisk.flink;

import com.realrisk.avro.RiskEventAvro;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public record RiskEvaluation(
    String decisionId,
    RiskEventAvro event,
    String decision,
    int riskScore,
    List<String> reasons,
    Instant createdAt,
    String source) {
  public RiskEvaluation {
    reasons = new ArrayList<>(reasons);
  }
}
