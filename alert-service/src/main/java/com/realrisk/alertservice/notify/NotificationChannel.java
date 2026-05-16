package com.realrisk.alertservice.notify;

import com.realrisk.alertservice.model.AlertEvent;

public interface NotificationChannel {
  String name();

  void send(AlertEvent event);
}
