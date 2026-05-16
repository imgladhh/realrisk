package com.realrisk.alertservice.rate;

import java.time.Duration;

public interface AlertRateLimitStateStore {
  boolean acquire(String key, Duration ttl);
}
