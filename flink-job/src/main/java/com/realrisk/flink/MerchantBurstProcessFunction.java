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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;

public class MerchantBurstProcessFunction
    extends KeyedBroadcastProcessFunction<String, RiskEventAvro, RuleUpdateEnvelope, RiskDecisionAvro>
    implements CheckpointedFunction {
  private static final String READY_KEY = "rule-bootstrap-ready";
  private static final int MAX_PENDING_EVENTS_PER_KEY = 100;
  /** Broadcast state descriptor shared between this class and FlinkRiskJob. */
  public static final MapStateDescriptor<String, RuleUpdateAvro> RULE_STATE_DESCRIPTOR =
      new MapStateDescriptor<>(
          "active-rules",
          BasicTypeInfo.STRING_TYPE_INFO,
          new AvroTypeInfo<>(RuleUpdateAvro.class));

  public static final MapStateDescriptor<String, Long> RULE_READINESS_DESCRIPTOR =
      new MapStateDescriptor<>("rule-bootstrap-readiness", Types.STRING, Types.LONG);

  private static final ListStateDescriptor<RiskEventAvro> PENDING_EVENTS_DESCRIPTOR =
      new ListStateDescriptor<>("rule-bootstrap-pending-events", new AvroTypeInfo<>(RiskEventAvro.class));

  public static final OutputTag<HighRiskEventAvro> HIGH_RISK_OUTPUT_TAG =
      new OutputTag<>("high-risk-events", new AvroTypeInfo<>(HighRiskEventAvro.class)) {};

  public static final OutputTag<AlertEventAvro> ALERT_OUTPUT_TAG =
      new OutputTag<>("alert-events", new AvroTypeInfo<>(AlertEventAvro.class)) {};

  private final FlinkRiskJobConfig config;
  private final long ruleBootstrapEndOffset;
  // Keyed state: userId -> latest event timestamp (epoch ms) within the burst window
  private transient MapState<String, Long> userSeenAtState;
  private transient RedisClient redisClient;
  private transient StatefulRedisConnection<String, String> redisConnection;
  private transient RedisUserProfileReader userProfileReader;
  private transient DecisionMetrics decisionMetrics;
  private transient ListState<RiskEventAvro> pendingEventsState;
  private transient boolean restoredFromCheckpoint;
  private transient AtomicInteger ruleBootstrapReadyGauge;

  public MerchantBurstProcessFunction(FlinkRiskJobConfig config) {
    this(config, 0L);
  }

  public MerchantBurstProcessFunction(FlinkRiskJobConfig config, long ruleBootstrapEndOffset) {
    this.config = config;
    this.ruleBootstrapEndOffset = ruleBootstrapEndOffset;
  }

  @Override
  public void open(Configuration parameters) {
    userSeenAtState =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>("merchant-user-seen-at", String.class, Long.class));
    pendingEventsState = getRuntimeContext().getListState(PENDING_EVENTS_DESCRIPTOR);
    ruleBootstrapReadyGauge =
        new AtomicInteger(restoredFromCheckpoint || ruleBootstrapEndOffset == 0L ? 1 : 0);
    getRuntimeContext()
        .getMetricGroup()
        .addGroup("realrisk")
        .gauge("rule_bootstrap_ready", ruleBootstrapReadyGauge::get);
    decisionMetrics = new DecisionMetrics(getRuntimeContext().getMetricGroup());

    try {
      redisClient = RedisClient.create(buildRedisUri());
      redisConnection = redisClient.connect();
      userProfileReader =
          new RedisUserProfileReader(
              redisConnection.sync(), decisionMetrics::recordRedisProfileFallback);
    } catch (RuntimeException e) {
      userProfileReader =
          new RedisUserProfileReader(null, decisionMetrics::recordRedisProfileFallback);
      closeRedisResources();
    }
  }

  @Override
  public void processElement(
      RiskEventAvro event, ReadOnlyContext ctx, Collector<RiskDecisionAvro> out)
      throws Exception {
    ReadOnlyBroadcastState<String, Long> readinessState =
        ctx.getBroadcastState(RULE_READINESS_DESCRIPTOR);
    if (!isRuleStateReady(readinessState)) {
      bufferPendingEvent(event);
      return;
    }

    evaluateAndEmit(event, ctx.timerService().currentWatermark(), ctx, out);
  }

  private void evaluateAndEmit(
      RiskEventAvro event,
      long watermark,
      ReadOnlyContext ctx,
      Collector<RiskDecisionAvro> out)
      throws Exception {
    long eventTimestamp = event.getTimestamp().toEpochMilli();
    pruneExpiredUsers(watermark);

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
      RuleUpdateEnvelope envelope, Context ctx, Collector<RiskDecisionAvro> out) throws Exception {
    RuleUpdateAvro update = envelope.update();
    BroadcastState<String, RuleUpdateAvro> state = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
    if (update.getEnabled()) {
      state.put(update.getRuleId(), update);
    } else {
      // Disabled rule: remove so it stops affecting RuleSet.from()
      state.remove(update.getRuleId());
    }

    if (completesBootstrap(envelope.offset(), ruleBootstrapEndOffset)) {
      BroadcastState<String, Long> readinessState =
          ctx.getBroadcastState(RULE_READINESS_DESCRIPTOR);
      if (!readinessState.contains(READY_KEY)) {
        readinessState.put(READY_KEY, envelope.offset());
        ruleBootstrapReadyGauge.set(1);
        flushPendingEvents(ctx, out, state);
      }
    }
  }

  private void flushPendingEvents(
      Context ctx,
      Collector<RiskDecisionAvro> out,
      BroadcastState<String, RuleUpdateAvro> ruleState)
      throws Exception {
    ctx.applyToKeyedState(
        PENDING_EVENTS_DESCRIPTOR,
        (key, pendingState) -> {
          for (RiskEventAvro event : pendingState.get()) {
            long eventTimestamp = event.getTimestamp().toEpochMilli();
            pruneExpiredUsers(ctx.currentWatermark());
            if (event.getMerchantId() != null) {
              userSeenAtState.put(event.getUserId(), eventTimestamp);
            }
            RiskEvaluation evaluation =
                new RiskRuleEngine(RuleSet.from(config, ruleState.immutableEntries()))
                    .evaluate(
                        event,
                        userProfileReader.read(event.getUserId()),
                        distinctUsersInWindow(),
                        Instant.ofEpochMilli(eventTimestamp));
            decisionMetrics.recordDecision(evaluation.decision());
            out.collect(FlinkRiskMappers.toDecisionAvro(evaluation));
            if (evaluation.riskScore() >= config.highRiskThreshold()) {
              ctx.output(HIGH_RISK_OUTPUT_TAG, FlinkRiskMappers.toHighRiskEvent(evaluation));
            }
            if (evaluation.riskScore() >= config.alertThreshold()) {
              ctx.output(ALERT_OUTPUT_TAG, FlinkRiskMappers.toAlertEvent(evaluation));
            }
          }
          pendingState.clear();
        });
  }

  private boolean isRuleStateReady(ReadOnlyBroadcastState<String, Long> readinessState)
      throws Exception {
    return restoredFromCheckpoint
        || ruleBootstrapEndOffset == 0L
        || readinessState.contains(READY_KEY);
  }

  private void bufferPendingEvent(RiskEventAvro event) throws Exception {
    int pendingCount = 0;
    for (RiskEventAvro ignored : pendingEventsState.get()) {
      pendingCount++;
    }
    if (pendingCount >= MAX_PENDING_EVENTS_PER_KEY) {
      throw new IllegalStateException(
          "Rule bootstrap pending-event limit exceeded for the current key");
    }
    pendingEventsState.add(event);
  }

  static boolean completesBootstrap(long recordOffset, long bootstrapEndOffset) {
    return bootstrapEndOffset > 0L && recordOffset >= bootstrapEndOffset - 1L;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    restoredFromCheckpoint = context.isRestored();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    // Keyed pending events and broadcast readiness are managed Flink state.
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
