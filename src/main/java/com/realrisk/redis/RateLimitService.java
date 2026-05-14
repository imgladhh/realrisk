package com.realrisk.redis;

import com.realrisk.config.RiskProperties;
import com.realrisk.model.RateLimitResult;
import java.time.Clock;
import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

@Service
public class RateLimitService {
  private final StringRedisTemplate redis;
  private final RiskProperties properties;
  private final Clock clock;
  private final DefaultRedisScript<List> riskCheckScript;

  public RateLimitService(StringRedisTemplate redis, RiskProperties properties) {
    this(redis, properties, Clock.systemUTC());
  }

  RateLimitService(StringRedisTemplate redis, RiskProperties properties, Clock clock) {
    this.redis = redis;
    this.properties = properties;
    this.clock = clock;
    this.riskCheckScript = new DefaultRedisScript<>();
    this.riskCheckScript.setResultType(List.class);
    this.riskCheckScript.setScriptSource(
        new ResourceScriptSource(new ClassPathResource("redis/risk_check.lua")));
  }

  public RateLimitResult check(String userId, String requestId) {
    long now = clock.millis();
    List<?> result =
        redis.execute(
            riskCheckScript,
            List.of("risk:txn:%s".formatted(userId)),
            Long.toString(now),
            Long.toString(properties.rateLimit().windowMs()),
            Long.toString(properties.rateLimit().limit()),
            requestId);

    if (result == null || result.size() < 2) {
      throw new IllegalStateException("Redis rate-limit script returned no result");
    }
    boolean blocked = asLong(result.get(0)) == 1L;
    long count = asLong(result.get(1));
    return new RateLimitResult(blocked, count);
  }

  private long asLong(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    return Long.parseLong(value.toString());
  }
}
