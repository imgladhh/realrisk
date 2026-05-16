package com.realrisk.alertservice.rate;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.alertservice.config.AlertProperties;
import com.realrisk.alertservice.model.AlertEvent;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AlertRateLimiterTest {
  @Test
  void blocksSecondAlertForSameUserAndSeverityWithinWindow() {
    AlertRateLimiter rateLimiter = new AlertRateLimiter(new InMemoryStateStore(), new AlertProperties());

    assertThat(rateLimiter.allow(event("user-1", "HIGH"))).isTrue();
    assertThat(rateLimiter.allow(event("user-1", "HIGH"))).isFalse();
    assertThat(rateLimiter.allow(event("user-1", "CRITICAL"))).isTrue();
  }

  private AlertEvent event(String userId, String severity) {
    return new AlertEvent("alert-" + userId + severity, "decision-1", "event-1", userId, 90, severity, "x", Instant.now(), "flink");
  }

  private static final class InMemoryStateStore implements AlertRateLimitStateStore {
    private final Set<String> keys = new HashSet<>();

    @Override
    public boolean acquire(String key, java.time.Duration ttl) {
      return keys.add(key);
    }
  }
}
