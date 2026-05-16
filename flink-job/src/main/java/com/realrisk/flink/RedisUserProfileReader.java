package com.realrisk.flink;

import io.lettuce.core.api.sync.RedisCommands;

public class RedisUserProfileReader {
  private final RedisCommands<String, String> commands;

  public RedisUserProfileReader(RedisCommands<String, String> commands) {
    this.commands = commands;
  }

  public UserProfile read(String userId) {
    if (commands == null) {
      return UserProfile.empty();
    }

    try {
      boolean blacklisted = commands.exists("blacklist:" + userId) > 0;
      int velocity7d = parseVelocity(commands.get("velocity:count:7d:" + userId));
      return new UserProfile(blacklisted, velocity7d);
    } catch (RuntimeException e) {
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
