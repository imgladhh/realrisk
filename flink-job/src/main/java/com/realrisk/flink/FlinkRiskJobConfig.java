package com.realrisk.flink;

import java.io.Serializable;
import java.time.Duration;

public record FlinkRiskJobConfig(
    String bootstrapServers,
    String schemaRegistryUrl,
    String rawEventsTopic,
    String ruleUpdatesTopic,
    String decisionAuditTopic,
    String highRiskEventsTopic,
    String alertEventsTopic,
    String checkpointDir,
    int parallelism,
    long largeAmountCents,
    int reviewThreshold,
    int blockThreshold,
    int highRiskThreshold,
    int alertThreshold,
    int merchantBurstThreshold,
    Duration merchantBurstWindow,
    Duration watermarkSkew)
    implements Serializable {

  public static FlinkRiskJobConfig fromEnv() {
    return new FlinkRiskJobConfig(
        env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        env("RAW_EVENTS_TOPIC", "raw-events"),
        env("RULE_UPDATES_TOPIC", "rule-updates"),
        env("DECISION_AUDIT_TOPIC", "decision-audit"),
        env("HIGH_RISK_EVENTS_TOPIC", "high-risk-events"),
        env("ALERT_EVENTS_TOPIC", "alert-events"),
        env("FLINK_CHECKPOINTS_DIR", "file:///tmp/realrisk-flink-checkpoints"),
        envInt("FLINK_PARALLELISM", 2),
        envLong("FLINK_LARGE_AMOUNT_CENTS", 1_000_000L),
        envInt("FLINK_REVIEW_THRESHOLD", 60),
        envInt("FLINK_BLOCK_THRESHOLD", 80),
        envInt("FLINK_HIGH_RISK_THRESHOLD", 85),
        envInt("FLINK_ALERT_THRESHOLD", 90),
        envInt("FLINK_MERCHANT_BURST_THRESHOLD", 10),
        Duration.ofMillis(envLong("FLINK_MERCHANT_BURST_WINDOW_MS", 300_000L)),
        Duration.ofMillis(envLong("FLINK_WATERMARK_SKEW_MS", 5_000L)));
  }

  private static String env(String key, String defaultValue) {
    return System.getenv().getOrDefault(key, defaultValue);
  }

  private static int envInt(String key, int defaultValue) {
    return Integer.parseInt(env(key, Integer.toString(defaultValue)));
  }

  private static long envLong(String key, long defaultValue) {
    return Long.parseLong(env(key, Long.toString(defaultValue)));
  }
}
