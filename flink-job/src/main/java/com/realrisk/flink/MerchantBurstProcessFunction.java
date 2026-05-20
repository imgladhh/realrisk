package com.realrisk.flink;

import com.realrisk.avro.HighRiskEventAvro;
import com.realrisk.avro.RiskDecisionAvro;
import com.realrisk.avro.RiskEventAvro;
import com.realrisk.avro.RuleUpdateAvro;
import com.realrisk.avro.AlertEventAvro;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Instant;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;

public class MerchantBurstProcessFunction
    extends KeyedBroadcastProcessFunction<String, RiskEventAvro, RuleUpdateAvro, RiskDecisionAvro> {
  /** Broadcast state descriptor shared between this class and FlinkRiskJob. */
  public static final MapStateDescriptor<String, RuleUpdateAvro> RULE_STATE_DESCRIPTOR =
      new MapStateDescriptor<>(
          "active-rules",
          BasicTypeInfo.STRING_TYPE_INFO,
          new AvroTypeInfo<>(RuleUpdateAvro.class));

  public static final OutputTag<HighRiskEventAvro> HIGH_RISK_OUTPUT_TAG =
      new OutputTag<>("high-risk-events", new AvroTypeInfo<>(HighRiskEventAvro.class)) {};

  public static final OutputTag<AlertEventAvro> ALERT_OUTPUT_TAG =
      new OutputTag<>("alert-events", new AvroTypeInfo<>(AlertEventAvro.class)) {};

  private final FlinkRiskJobConfig config;
  // Keyed state: userId -> latest event timestamp (epoch ms) within the burst window
  private transient MapState<String, Long> userSeenAtState;
  private transient RedisClient redisClient;
  private transient StatefulRedisConnection<String, String> redisConnection;
  private transient RedisUserProfileReader userProfileReader;
  private transient DecisionMetrics decisionMetrics;

  public MerchantBurstProcessFunction(FlinkRiskJobConfig config) {
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) {
    userSeenAtState =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>("merchant-user-seen-at", String.class, Long.class));

    try {
      redisClient = RedisClient.create(buildRedisUri());
      redisConnection = redisClient.connect();
      userProfileReader = new RedisUserProfileReader(redisConnection.sync());
    } catch (RuntimeException e) {
      userProfileReader = new RedisUserProfileReader(null);
      closeRedisResources();
    }

    decisionMetrics = new DecisionMetrics(getRuntimeContext().getMetricGroup());
  }

  @Override
  public void processElement(
      RiskEventAvro event, ReadOnlyContext ctx, Collector<RiskDecisionAvro> out)
      throws Exception {
    long eventTimestamp = event.getTimestamp().toEpochMilli();
    pruneExpiredUsers(ctx.timerService().currentWatermark());

    if (event.getMerchantId() != null) {
      userSeenAtState.put(event.getUserId(), eventTimestamp);
    }

    ReadOnlyBroadcastState<String, RuleUpdateAvro> broadcastState =
        ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
    RuleSet rules = RuleSet.from(config, broadcastState.immutableEntries());
    UserProfile userProfile = userProfileReader.read(event.getUserId());

    RiskEvaluation evaluation =
        new RiskRuleEngine(rules)
            .evaluate(
                event, userProfile, distinctUsersInWindow(), Instant.ofEpochMilli(eventTimestamp));

    decisionMetrics.recordDecision(evaluation.decision());
    out.collect(FlinkRiskMappers.toDecisionAvro(evaluation));
    if (evaluation.riskScore() >= config.highRiskThreshold()) {
      ctx.output(HIGH_RISK_OUTPUT_TAG, FlinkRiskMappers.toHighRiskEvent(evaluation));
    }
    if (evaluation.riskScore() >= config.alertThreshold()) {
      ctx.output(ALERT_OUTPUT_TAG, FlinkRiskMappers.toAlertEvent(evaluation));
    }
  }

  @Override
  public void processBroadcastElement(
      RuleUpdateAvro update, Context ctx, Collector<RiskDecisionAvro> out) throws Exception {
    BroadcastState<String, RuleUpdateAvro> state = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
    if (update.getEnabled()) {
      state.put(update.getRuleId(), update);
    } else {
      // Disabled rule: remove so it stops affecting RuleSet.from()
      state.remove(update.getRuleId());
    }
  }

  private void pruneExpiredUsers(long watermark) throws Exception {
    if (watermark == Long.MIN_VALUE) return;
    long cutoff = watermark - config.merchantBurstWindow().toMillis();
    var iterator = userSeenAtState.entries().iterator();
    while (iterator.hasNext()) {
      var entry = iterator.next();
      if (entry.getValue() < cutoff) {
        iterator.remove();
      }
    }
  }

  private int distinctUsersInWindow() throws Exception {
    int count = 0;
    for (var ignored : userSeenAtState.keys()) {
      count++;
    }
    return count;
  }

  private RedisURI buildRedisUri() {
    if (config.redisSentinelNodes() == null
        || config.redisSentinelNodes().isBlank()
        || config.redisSentinelMaster() == null
        || config.redisSentinelMaster().isBlank()) {
      return RedisURI.builder().withHost(config.redisHost()).withPort(config.redisPort()).build();
    }

    RedisURI.Builder builder = RedisURI.builder().withSentinelMasterId(config.redisSentinelMaster());
    for (String rawNode : config.redisSentinelNodes().split(",")) {
      String node = rawNode.trim();
      if (node.isBlank()) {
        continue;
      }
      String[] parts = node.split(":");
      String host = parts[0].trim();
      int port = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 26379;
      builder.withSentinel(host, port);
    }
    return builder.build();
  }

  @Override
  public void close() throws Exception {
    closeRedisResources();
    super.close();
  }

  private void closeRedisResources() {
    if (redisConnection != null) {
      redisConnection.close();
      redisConnection = null;
    }
    if (redisClient != null) {
      redisClient.shutdown();
      redisClient = null;
    }
  }
}
