package com.realrisk.redis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BlacklistServiceTest {
  private static BlacklistService blacklistService;

  @BeforeAll
  static void setUp() {
    var template = RedisTestSupport.createTemplate();
    blacklistService = new BlacklistService(template);
  }

  @AfterAll
  static void tearDown() {
    RedisTestSupport.stopContainer();
  }

  @Test
  void moreSevereBanOverwritesLessSevereBan() {
    assertThat(blacklistService.set("user-severity", "RATE_LIMIT", 1, 60)).isTrue();
    assertThat(blacklistService.set("user-severity", "FRAUD_CONFIRMED", 4, 3600)).isTrue();

    var entry = blacklistService.find("user-severity");
    assertThat(entry).isPresent();
    assertThat(entry.get().reason()).isEqualTo("FRAUD_CONFIRMED");
    assertThat(entry.get().severity()).isEqualTo(4);
  }

  @Test
  void lessSevereBanDoesNotOverwriteMoreSevereBan() {
    assertThat(blacklistService.set("user-no-downgrade", "FRAUD_CONFIRMED", 4, 3600)).isTrue();
    assertThat(blacklistService.set("user-no-downgrade", "RATE_LIMIT", 1, 60)).isFalse();

    var entry = blacklistService.find("user-no-downgrade");
    assertThat(entry).isPresent();
    assertThat(entry.get().reason()).isEqualTo("FRAUD_CONFIRMED");
    assertThat(entry.get().severity()).isEqualTo(4);
  }
}
