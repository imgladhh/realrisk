package com.realrisk.alertservice.rate;

import java.time.Duration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class RedisAlertRateLimitStateStore implements AlertRateLimitStateStore {
  private final StringRedisTemplate redisTemplate;

  public RedisAlertRateLimitStateStore(StringRedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  @Override
  public boolean acquire(String key, Duration ttl) {
    Boolean acquired = redisTemplate.opsForValue().setIfAbsent(key, "1", ttl);
    return Boolean.TRUE.equals(acquired);
  }
}
