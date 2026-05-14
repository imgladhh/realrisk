package com.realrisk.redis;

import com.realrisk.model.BlacklistEntry;
import java.util.List;
import java.util.Optional;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

@Service
public class BlacklistService {
  private final StringRedisTemplate redis;
  private final DefaultRedisScript<Long> setBlacklistScript;

  public BlacklistService(StringRedisTemplate redis) {
    this.redis = redis;
    this.setBlacklistScript = new DefaultRedisScript<>();
    this.setBlacklistScript.setResultType(Long.class);
    this.setBlacklistScript.setScriptSource(
        new ResourceScriptSource(new ClassPathResource("redis/set_blacklist.lua")));
  }

  public Optional<BlacklistEntry> find(String userId) {
    return Optional.ofNullable(redis.opsForValue().get(key(userId))).map(BlacklistEntry::parse);
  }

  public boolean set(String userId, String reason, int severity, long ttlSeconds) {
    Long result =
        redis.execute(
            setBlacklistScript,
            List.of(key(userId)),
            reason,
            Integer.toString(severity),
            Long.toString(ttlSeconds));
    return Long.valueOf(1).equals(result);
  }

  private String key(String userId) {
    return "blacklist:%s".formatted(userId);
  }
}
