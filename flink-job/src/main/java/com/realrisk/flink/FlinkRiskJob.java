package com.realrisk.flink;

import com.realrisk.avro.AlertEventAvro;
import com.realrisk.avro.HighRiskEventAvro;
import com.realrisk.avro.RiskDecisionAvro;
import com.realrisk.avro.RiskEventAvro;
import com.realrisk.avro.RuleUpdateAvro;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class FlinkRiskJob {
  private FlinkRiskJob() {}

  public static void main(String[] args) throws Exception {
    FlinkRiskJobConfig config = FlinkRiskJobConfig.fromEnv();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureEnvironment(env, config);

    DataStream<RiskEventAvro> rawEvents =
        env.fromSource(rawEventsSource(config), watermarkStrategy(config), "raw-events-source");

    // rule-updates is a compact topic: always replay from earliest so broadcast state is
    // fully populated before any raw-events are processed on fresh start.
    BroadcastStream<RuleUpdateAvro> broadcastRules =
        env.fromSource(
                ruleUpdatesSource(config),
                WatermarkStrategy.noWatermarks(),
                "rule-updates-source")
            .broadcast(MerchantBurstProcessFunction.RULE_STATE_DESCRIPTOR);

    SingleOutputStreamOperator<RiskDecisionAvro> decisionAuditEvents =
        rawEvents
            .keyBy(FlinkRiskJob::merchantKey)
            .connect(broadcastRules)
            .process(new MerchantBurstProcessFunction(config))
            .returns(new AvroTypeInfo<>(RiskDecisionAvro.class))
            .name("merchant-burst-evaluator");

    DataStream<HighRiskEventAvro> highRiskEvents =
        decisionAuditEvents.getSideOutput(MerchantBurstProcessFunction.HIGH_RISK_OUTPUT_TAG);
    DataStream<AlertEventAvro> alertEvents =
        decisionAuditEvents.getSideOutput(MerchantBurstProcessFunction.ALERT_OUTPUT_TAG);

    decisionAuditEvents.sinkTo(decisionAuditSink(config)).name("decision-audit-sink");

    highRiskEvents
        .sinkTo(highRiskSink(config))
        .name("high-risk-events-sink");

    alertEvents
        .sinkTo(alertSink(config))
        .name("alert-events-sink");

    env.execute("realrisk-flink-risk-engine");
  }

  private static void configureEnvironment(
      StreamExecutionEnvironment env, FlinkRiskJobConfig config) {
    env.setParallelism(config.parallelism());
    env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000L);
    env.getCheckpointConfig().setCheckpointTimeout(60_000L);
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
    env.getCheckpointConfig()
        .setCheckpointStorage(new FileSystemCheckpointStorage(config.checkpointDir()));
  }

  private static KafkaSource<RiskEventAvro> rawEventsSource(FlinkRiskJobConfig config) {
    return KafkaSource.<RiskEventAvro>builder()
        .setBootstrapServers(config.bootstrapServers())
        .setTopics(config.rawEventsTopic())
        .setGroupId("flink-risk-engine")
        // On the very first deployment without checkpoints, start from the end to avoid
        // replaying a long raw-events backlog by accident. Checkpoint recovery keeps offsets.
        .setStartingOffsets(OffsetsInitializer.latest())
        .setProperty("isolation.level", "read_committed")
        .setDeserializer(
            KafkaRecordDeserializationSchema.valueOnly(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                    RiskEventAvro.class,
                    config.schemaRegistryUrl(),
                    schemaRegistryConfig(config))))
        .build();
  }

  private static KafkaSource<RuleUpdateAvro> ruleUpdatesSource(FlinkRiskJobConfig config) {
    return KafkaSource.<RuleUpdateAvro>builder()
        .setBootstrapServers(config.bootstrapServers())
        .setTopics(config.ruleUpdatesTopic())
        .setGroupId("flink-risk-engine-rules")
        // Compact topic: always replay from earliest to rebuild broadcast state on (re)start.
        // Flink stores broadcast state in checkpoints so this only matters on fresh start
        // or when restoring from a savepoint without a prior checkpoint.
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setProperty("isolation.level", "read_committed")
        .setDeserializer(
            KafkaRecordDeserializationSchema.valueOnly(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                    RuleUpdateAvro.class,
                    config.schemaRegistryUrl(),
                    schemaRegistryConfig(config))))
        .build();
  }

  private static WatermarkStrategy<RiskEventAvro> watermarkStrategy(FlinkRiskJobConfig config) {
    return WatermarkStrategy.<RiskEventAvro>forBoundedOutOfOrderness(config.watermarkSkew())
        .withTimestampAssigner((event, previousTimestamp) -> event.getTimestamp().toEpochMilli());
  }

  private static KafkaSink<RiskDecisionAvro> decisionAuditSink(FlinkRiskJobConfig config) {
    return KafkaSink.<RiskDecisionAvro>builder()
        .setBootstrapServers(config.bootstrapServers())
        .setRecordSerializer(
            new AvroKafkaRecordSerializationSchema<>(
                config.decisionAuditTopic(), schemaRegistryConfig(config), RiskDecisionAvro::getUserId))
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  private static KafkaSink<HighRiskEventAvro> highRiskSink(FlinkRiskJobConfig config) {
    return KafkaSink.<HighRiskEventAvro>builder()
        .setBootstrapServers(config.bootstrapServers())
        .setRecordSerializer(
            new AvroKafkaRecordSerializationSchema<>(
                config.highRiskEventsTopic(),
                schemaRegistryConfig(config),
                HighRiskEventAvro::getUserId))
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  private static KafkaSink<AlertEventAvro> alertSink(FlinkRiskJobConfig config) {
    return KafkaSink.<AlertEventAvro>builder()
        .setBootstrapServers(config.bootstrapServers())
        .setRecordSerializer(
            new AvroKafkaRecordSerializationSchema<>(
                config.alertEventsTopic(), schemaRegistryConfig(config), AlertEventAvro::getUserId))
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  private static Map<String, ?> schemaRegistryConfig(FlinkRiskJobConfig config) {
    return Map.of(
        "schema.registry.url", config.schemaRegistryUrl(),
        "rule.service.loader.enable", "false");
  }

  private static String merchantKey(RiskEventAvro event) {
    if (event.getMerchantId() != null && !event.getMerchantId().isBlank()) {
      return event.getMerchantId();
    }
    return "__no_merchant__:" + event.getEventId();
  }
}
