package com.realrisk.alertservice.notify;

import com.realrisk.alertservice.model.AlertEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PushNotificationChannel implements NotificationChannel {
  private static final Logger log = LoggerFactory.getLogger(PushNotificationChannel.class);

  @Override
  public String name() {
    return "push";
  }

  @Override
  public void send(AlertEvent event) {
    log.info("PUSH alertId={} userId={} severity={} reasons={}", event.alertId(), event.userId(), event.severity(), event.reasonSummary());
  }
}
