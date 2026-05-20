package com.realrisk.alertservice.notify;

import com.realrisk.alertservice.model.AlertEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SmsNotificationChannel implements NotificationChannel {
  private static final Logger log = LoggerFactory.getLogger(SmsNotificationChannel.class);

  @Override
  public String name() {
    return "sms";
  }

  @Override
  public void send(AlertEvent event) {
    log.warn(
        "SMS channel not yet implemented alertId={} userId={} severity={} reasons={}",
        event.alertId(),
        event.userId(),
        event.severity(),
        event.reasonSummary());
  }
}
