package com.realrisk.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

@Component
public class ApiGatewayMetrics {
  private final MeterRegistry meterRegistry;
  private final Timer blacklistLookupTimer;
  private final Timer rateLimitTimer;
  private final Timer kafkaPublishTimer;
  private final Counter kafkaPublishErrors;

  public ApiGatewayMetrics(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    this.blacklistLookupTimer =
        Timer.builder("risk.blacklist.lookup")
            .description("Latency of Redis blacklist lookups on the fast path")
            .register(meterRegistry);
    this.rateLimitTimer =
        Timer.builder("risk.ratelimit.check")
            .description("Latency of Redis-backed rate limit checks on the fast path")
            .register(meterRegistry);
    this.kafkaPublishTimer =
        Timer.builder("risk.kafka.publish")
            .description("Latency of publishing raw-events to Kafka")
            .register(meterRegistry);
    this.kafkaPublishErrors =
        Counter.builder("risk.kafka.publish.errors")
            .description("Kafka raw-events publish failures")
            .register(meterRegistry);
  }

  public Timer.Sample startSample() {
    return Timer.start(meterRegistry);
  }

  public void recordBlacklistLookup(Timer.Sample sample) {
    sample.stop(blacklistLookupTimer);
  }

  public void recordRateLimitCheck(Timer.Sample sample) {
    sample.stop(rateLimitTimer);
  }

  public void incrementBlacklistHit() {
    meterRegistry.counter("risk.blacklist.hit").increment();
  }

  public void incrementRateLimitHit() {
    meterRegistry.counter("risk.ratelimit.hit").increment();
  }

  public void incrementIngressOutcome(String decision) {
    meterRegistry.counter("risk.event.decision", "decision", decision).increment();
  }

  public void recordKafkaPublishSuccess(long nanos) {
    kafkaPublishTimer.record(nanos, TimeUnit.NANOSECONDS);
  }

  public void recordKafkaPublishFailure(long nanos) {
    kafkaPublishTimer.record(nanos, TimeUnit.NANOSECONDS);
    kafkaPublishErrors.increment();
  }
}
