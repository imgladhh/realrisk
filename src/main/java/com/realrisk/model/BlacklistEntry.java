package com.realrisk.model;

public record BlacklistEntry(String reason, int severity) {
  public static BlacklistEntry parse(String value) {
    int idx = value.lastIndexOf(':');
    if (idx < 1 || idx == value.length() - 1) {
      return new BlacklistEntry(value, 0);
    }
    return new BlacklistEntry(value.substring(0, idx), Integer.parseInt(value.substring(idx + 1)));
  }
}
