package com.realrisk.flink;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

final class DecisionMetrics {
  private final Counter allowCounter;
  private final Counter reviewCounter;
  private final Counter blockCounter;

  DecisionMetrics(MetricGroup metricGroup) {
    MetricGroup root = metricGroup.addGroup("realrisk");
    allowCounter = root.counter("decision_allow");
    reviewCounter = root.counter("decision_review");
    blockCounter = root.counter("decision_block");
  }

  void recordDecision(String decision) {
    switch (decision) {
      case "ALLOW" -> allowCounter.inc();
      case "REVIEW" -> reviewCounter.inc();
      case "BLOCK" -> blockCounter.inc();
      default -> {
        // Ignore unknown decision values so the evaluator keeps processing.
      }
    }
  }
}
