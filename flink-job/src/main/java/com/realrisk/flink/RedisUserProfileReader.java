package com.realrisk.flink;

import io.lettuce.core.api.sync.RedisCommands;

public class RedisUserProfileReader {
  private final RedisCommands<String, String> commands;
  private final Runnable fallbackRecorder;

  public RedisUserProfileReader(RedisCommands<String, String> commands) {
    this(commands, () -> {});
  }

  RedisUserProfileReader(RedisCommands<String, String> commands, Runnable fallbackRecorder) {
    this.commands = commands;
    this.fallbackRecorder = fallbackRecorder;
  }

  public UserProfile read(String userId) {
    if (commands == null) {
      fallbackRecorder.run();
      return UserProfile.empty();
    }

    try {
      boolean blacklisted = commands.exists("blacklist:" + userId) > 0;
      int velocity7d = parseVelocity(commands.get("velocity:count:7d:" + userId));
      return new UserProfile(blacklisted, velocity7d);
    } catch (RuntimeException e) {
      fallbackRecorder.run();
      return UserProfile.empty();
    }
  }

  private int parseVelocity(String rawValue) {
    if (rawValue == null || rawValue.isBlank()) {
      return 0;
    }
    try {
      return Integer.parseInt(rawValue);
    } catch (NumberFormatException e) {
      return 0;
    }
  }
}
