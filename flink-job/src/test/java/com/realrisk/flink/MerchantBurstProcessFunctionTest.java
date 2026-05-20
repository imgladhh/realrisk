package com.realrisk.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.api.common.state.MapState;
import org.junit.jupiter.api.Test;

class MerchantBurstProcessFunctionTest {
  private static final FlinkRiskJobConfig CONFIG =
      new FlinkRiskJobConfig(
          "localhost:9092",
          "http://localhost:8081",
          "localhost",
          6379,
          "",
          "",
          "raw-events",
          "rule-updates",
          "decision-audit",
          "high-risk-events",
          "alert-events",
          "file:///tmp/realrisk-flink-checkpoints",
          1,
          1_000_000L,
          60,
          80,
          85,
          90,
          10,
          100,
          40,
          Duration.ofMinutes(5),
          Duration.ofSeconds(5));

  @Test
  void minWatermarkReturnsBeforeTouchingState() throws Exception {
    MerchantBurstProcessFunction function = new MerchantBurstProcessFunction(CONFIG);
    AtomicBoolean entriesCalled = new AtomicBoolean(false);

    MapState<String, Long> state =
        (MapState<String, Long>)
            Proxy.newProxyInstance(
                MapState.class.getClassLoader(),
                new Class<?>[] {MapState.class},
                (proxy, method, args) -> {
                  if ("entries".equals(method.getName())) {
                    entriesCalled.set(true);
                    throw new AssertionError("entries() should not be called for Long.MIN_VALUE");
                  }
                  return null;
                });

    Field field = MerchantBurstProcessFunction.class.getDeclaredField("userSeenAtState");
    field.setAccessible(true);
    field.set(function, state);

    Method prune =
        MerchantBurstProcessFunction.class.getDeclaredMethod("pruneExpiredUsers", long.class);
    prune.setAccessible(true);

    assertThatCode(() -> prune.invoke(function, Long.MIN_VALUE)).doesNotThrowAnyException();
    assertThat(entriesCalled.get()).isFalse();
  }

  @Test
  void ruleBroadcastStateUsesAvroTypeInformation() {
    MerchantBurstProcessFunction.RULE_STATE_DESCRIPTOR.initializeSerializerUnlessSet(
        new ExecutionConfig());
    assertThat(MerchantBurstProcessFunction.RULE_STATE_DESCRIPTOR.getValueSerializer())
        .isInstanceOf(AvroSerializer.class);
  }
}
