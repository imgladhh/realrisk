package com.realrisk.alertservice.rate;

import com.realrisk.alertservice.config.AlertProperties;
import com.realrisk.alertservice.model.AlertEvent;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AlertRateLimiter {
  private static final Logger log = LoggerFactory.getLogger(AlertRateLimiter.class);
  private final AlertRateLimitStateStore stateStore;
  private final AlertProperties properties;

  public AlertRateLimiter(AlertRateLimitStateStore stateStore, AlertProperties properties) {
    this.stateStore = stateStore;
    this.properties = properties;
  }

  public boolean allow(AlertEvent event) {
    String severity = event.severity().toUpperCase(Locale.ROOT);
    String key = "alert:ratelimit:" + event.userId() + ":" + severity;
    try {
      return stateStore.acquire(key, properties.getRateLimitWindow());
    } catch (RuntimeException e) {
      log.warn("Redis alert rate limiter unavailable for alertId={}, failing open", event.alertId(), e);
      return true;
    }
  }
}
