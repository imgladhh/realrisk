package com.realrisk.flink;

public record UserProfile(boolean blacklisted, int velocity7d) {
  private static final UserProfile EMPTY = new UserProfile(false, 0);

  public static UserProfile empty() {
    return EMPTY;
  }
}
