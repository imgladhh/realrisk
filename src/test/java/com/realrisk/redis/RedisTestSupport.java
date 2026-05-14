package com.realrisk.redis;

import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

final class RedisTestSupport {
  private static GenericContainer<?> redis;

  private RedisTestSupport() {}

  static StringRedisTemplate createTemplate() {
    String host = System.getProperty("realrisk.test.redis.host");
    int port = Integer.getInteger("realrisk.test.redis.port", 6379);

    if (host == null || host.isBlank()) {
      redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
      redis.start();
      host = redis.getHost();
      port = redis.getMappedPort(6379);
    }

    var connectionFactory = new LettuceConnectionFactory(host, port);
    connectionFactory.afterPropertiesSet();
    var template = new StringRedisTemplate(connectionFactory);
    template.afterPropertiesSet();
    template.getConnectionFactory().getConnection().serverCommands().flushDb();
    return template;
  }

  static void stopContainer() {
    if (redis != null) {
      redis.stop();
      redis = null;
    }
  }
}
