package com.realrisk.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.api.sync.RedisCommands;
import java.lang.reflect.Proxy;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RedisUserProfileReaderTest {
  @Test
  void readsBlacklistedAndVelocityProfile() {
    RedisUserProfileReader reader =
        new RedisUserProfileReader(
            commands(Map.of("blacklist:user-1", 1L), Map.of("velocity:count:7d:user-1", "123")));

    UserProfile profile = reader.read("user-1");

    assertThat(profile.blacklisted()).isTrue();
    assertThat(profile.velocity7d()).isEqualTo(123);
  }

  @Test
  void missingKeysReturnEmptyProfileFields() {
    RedisUserProfileReader reader = new RedisUserProfileReader(commands(Map.of(), Map.of()));

    UserProfile profile = reader.read("user-2");

    assertThat(profile.blacklisted()).isFalse();
    assertThat(profile.velocity7d()).isZero();
  }

  @Test
  void redisErrorsDegradeToEmptyProfile() {
    @SuppressWarnings("unchecked")
    RedisCommands<String, String> failingCommands =
        (RedisCommands<String, String>)
            Proxy.newProxyInstance(
                RedisCommands.class.getClassLoader(),
                new Class<?>[] {RedisCommands.class},
                (proxy, method, args) -> {
                  throw new RuntimeException("redis unavailable");
                });

    UserProfile profile = new RedisUserProfileReader(failingCommands).read("user-3");

    assertThat(profile).isEqualTo(UserProfile.empty());
  }

  @SuppressWarnings("unchecked")
  private RedisCommands<String, String> commands(
      Map<String, Long> existsResults, Map<String, String> getResults) {
    return (RedisCommands<String, String>)
        Proxy.newProxyInstance(
            RedisCommands.class.getClassLoader(),
            new Class<?>[] {RedisCommands.class},
            (proxy, method, args) -> {
              String key = firstKey(args);
              return switch (method.getName()) {
                case "exists" -> existsResults.getOrDefault(key, 0L);
                case "get" -> getResults.get(key);
                default -> null;
              };
            });
  }

  private String firstKey(Object[] args) {
    if (args == null || args.length == 0 || args[0] == null) {
      return "";
    }
    Object firstArg = args[0];
    if (firstArg instanceof Object[] values && values.length > 0) {
      return String.valueOf(values[0]);
    }
    return String.valueOf(firstArg);
  }
}
