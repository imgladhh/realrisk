package com.realrisk.alertservice.notify;

import com.realrisk.alertservice.model.AlertEvent;
import com.realrisk.alertservice.metrics.AlertMetrics;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.springframework.stereotype.Component;

@Component
public class PushNotificationChannel implements NotificationChannel {
  private static final Logger log = LoggerFactory.getLogger(PushNotificationChannel.class);

  private final RestClient restClient;
  private final AlertMetrics metrics;
  private final boolean enabled;
  private final String webhookUrl;

  public PushNotificationChannel(
      RestClient.Builder restClientBuilder,
      AlertMetrics metrics,
      @Value("${NOTIFICATION_SLACK_ENABLED:true}") boolean enabled,
      @Value("${SLACK_WEBHOOK_URL:}") String webhookUrl) {
    this.restClient = restClientBuilder.build();
    this.metrics = metrics;
    this.enabled = enabled;
    this.webhookUrl = webhookUrl;
  }

  @Override
  public String name() {
    return "push";
  }

  @Override
  public void send(AlertEvent event) {
    if (!enabled) {
      log.info("PUSH disabled alertId={} userId={}", event.alertId(), event.userId());
      return;
    }
    if (webhookUrl == null || webhookUrl.isBlank()) {
      metrics.incrementNotificationFailed(name());
      log.warn(
          "PUSH delivery skipped alertId={} userId={} severity={} reason=missing_webhook",
          event.alertId(),
          event.userId(),
          event.severity());
      return;
    }

    try {
      restClient
          .post()
          .uri(webhookUrl)
          .body(
              Map.of(
                  "text",
                  "RealRisk "
                      + event.severity()
                      + " alert\nuserId="
                      + event.userId()
                      + "\nreasonSummary="
                      + event.reasonSummary()
                      + "\nalertId="
                      + event.alertId()))
          .retrieve()
          .toBodilessEntity();
      log.info(
          "PUSH alertId={} userId={} severity={} reasons={}",
          event.alertId(),
          event.userId(),
          event.severity(),
          event.reasonSummary());
    } catch (RuntimeException e) {
      metrics.incrementNotificationFailed(name());
      log.warn(
          "PUSH delivery failed alertId={} userId={} severity={} message={}",
          event.alertId(),
          event.userId(),
          event.severity(),
          e.getMessage());
    }
  }
}
