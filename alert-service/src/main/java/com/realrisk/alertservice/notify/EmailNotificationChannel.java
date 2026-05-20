package com.realrisk.alertservice.notify;

import com.realrisk.alertservice.model.AlertEvent;
import com.realrisk.alertservice.metrics.AlertMetrics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EmailNotificationChannel implements NotificationChannel {
  private static final Logger log = LoggerFactory.getLogger(EmailNotificationChannel.class);

  private final JavaMailSender mailSender;
  private final AlertMetrics metrics;
  private final boolean enabled;
  private final String fromAddress;

  public EmailNotificationChannel(
      JavaMailSender mailSender,
      AlertMetrics metrics,
      @Value("${NOTIFICATION_EMAIL_ENABLED:true}") boolean enabled,
      @Value("${SMTP_FROM:realrisk@example.com}") String fromAddress) {
    this.mailSender = mailSender;
    this.metrics = metrics;
    this.enabled = enabled;
    this.fromAddress = fromAddress;
  }

  @Override
  public String name() {
    return "email";
  }

  @Override
  public void send(AlertEvent event) {
    if (!enabled) {
      log.info("EMAIL disabled alertId={} userId={}", event.alertId(), event.userId());
      return;
    }

    try {
      SimpleMailMessage message = new SimpleMailMessage();
      message.setFrom(fromAddress);
      message.setTo(fromAddress);
      message.setSubject("RealRisk " + event.severity() + " alert for " + event.userId());
      message.setText(
          "alertId="
              + event.alertId()
              + "\nuserId="
              + event.userId()
              + "\nseverity="
              + event.severity()
              + "\nriskScore="
              + event.riskScore()
              + "\nreasonSummary="
              + event.reasonSummary()
              + "\nsource="
              + event.source());
      mailSender.send(message);
      log.info(
          "EMAIL alertId={} userId={} severity={} reasons={}",
          event.alertId(),
          event.userId(),
          event.severity(),
          event.reasonSummary());
    } catch (RuntimeException e) {
      metrics.incrementNotificationFailed(name());
      log.warn(
          "EMAIL delivery failed alertId={} userId={} severity={} message={}",
          event.alertId(),
          event.userId(),
          event.severity(),
          e.getMessage());
    }
  }
}
