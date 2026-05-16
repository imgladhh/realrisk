package com.realrisk.alertservice.notify;

import static org.assertj.core.api.Assertions.assertThat;

import com.realrisk.alertservice.config.AlertProperties;
import com.realrisk.alertservice.model.AlertEvent;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class NotificationRouterTest {
  private final NotificationRouter router =
      new NotificationRouter(
          new AlertProperties(),
          List.of(new StubChannel("email"), new StubChannel("sms"), new StubChannel("push")));

  @Test
  void routesMediumToEmailOnly() {
    assertThat(router.route(event("MEDIUM")).stream().map(NotificationChannel::name).toList())
        .containsExactly("email");
  }

  @Test
  void routesHighToEmailAndSms() {
    assertThat(router.route(event("HIGH")).stream().map(NotificationChannel::name).toList())
        .containsExactly("email", "sms");
  }

  @Test
  void routesCriticalToAllChannels() {
    assertThat(router.route(event("CRITICAL")).stream().map(NotificationChannel::name).toList())
        .containsExactly("email", "sms", "push");
  }

  private AlertEvent event(String severity) {
    return new AlertEvent("alert-1", "decision-1", "event-1", "user-1", 95, severity, "x", Instant.now(), "flink");
  }

  private record StubChannel(String name) implements NotificationChannel {
    @Override
    public void send(AlertEvent event) {}
  }
}
