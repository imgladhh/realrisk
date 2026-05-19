package com.realrisk.alertservice.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

@Component
public class AlertMetrics {
  private final MeterRegistry meterRegistry;
  private final AtomicLong consumerLag = new AtomicLong(0);

  public AlertMetrics(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    Gauge.builder("alert.consumer.lag", consumerLag, AtomicLong::get)
        .description("Approximate lag for the alert-events consumer group")
        .register(meterRegistry);
  }

  public void incrementProcessed(String severity, String status) {
    meterRegistry.counter("alert.processed", "severity", severity, "status", status).increment();
  }

  public void incrementRateLimitHit(String severity) {
    meterRegistry.counter("alert.ratelimit.hit", "severity", severity).increment();
  }

  public void incrementDlqPublished(String severity) {
    meterRegistry.counter("alert.dlq.published", "severity", severity).increment();
  }

  public void setConsumerLag(long lag) {
    consumerLag.set(Math.max(lag, 0));
  }
}
