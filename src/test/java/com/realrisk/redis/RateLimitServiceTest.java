package com.realrisk.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.config.RiskProperties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RateLimitServiceTest {
  private static RateLimitService rateLimitService;

  @BeforeAll
  static void setUp() {
    var template = RedisTestSupport.createTemplate();
    var properties =
        new RiskProperties(
            new RiskProperties.Topics(
                "raw-events", "raw-audit", "decision-audit", "high-risk-events", "alert-events"),
            new RiskProperties.RateLimit(2_000, 5));
    rateLimitService = new RateLimitService(template, properties);
  }

  @AfterAll
  static void tearDown() {
    RedisTestSupport.stopContainer();
  }

  @Test
  void concurrentRequestsOnlyFirstFiveAllowed() throws Exception {
    int threads = 10;
    var latch = new CountDownLatch(1);
    var futures =
        IntStream.range(0, threads)
            .mapToObj(
                i ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          try {
                            latch.await();
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                          }
                          return rateLimitService.check("user-concurrent", "req-" + i);
                        }))
            .toList();

    latch.countDown();
    var results = futures.stream().map(CompletableFuture::join).toList();

    long allowed = results.stream().filter(result -> !result.blocked()).count();
    long blocked = results.stream().filter(result -> result.blocked()).count();
    assertThat(allowed).isEqualTo(5);
    assertThat(blocked).isEqualTo(5);
  }

  @Test
  void sameRequestIdDoesNotDoubleCountProducerRetry() {
    var first = rateLimitService.check("user-retry", "same-request");
    var retry = rateLimitService.check("user-retry", "same-request");

    assertThat(first.blocked()).isFalse();
    assertThat(first.count()).isEqualTo(1);
    assertThat(retry.blocked()).isFalse();
    assertThat(retry.count()).isEqualTo(1);
  }

  @Test
  void windowExpiryAllowsNewRequests() throws InterruptedException {
    for (int i = 0; i < 5; i++) {
      assertThat(rateLimitService.check("user-expiry", "req-" + i).blocked()).isFalse();
    }
    assertThat(rateLimitService.check("user-expiry", "req-blocked").blocked()).isTrue();

    Thread.sleep(2_500);

    var afterExpiry = rateLimitService.check("user-expiry", "req-after-expiry");
    assertThat(afterExpiry.blocked()).isFalse();
    assertThat(afterExpiry.count()).isEqualTo(1);
  }
}
