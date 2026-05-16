package com.realrisk.alertservice.notify;

import com.realrisk.alertservice.config.AlertProperties;
import com.realrisk.alertservice.model.AlertEvent;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class NotificationRouter {
  private final AlertProperties properties;
  private final Map<String, NotificationChannel> channelsByName;

  public NotificationRouter(AlertProperties properties, List<NotificationChannel> channels) {
    this.properties = properties;
    this.channelsByName =
        channels.stream().collect(Collectors.toMap(NotificationChannel::name, channel -> channel));
  }

  public List<NotificationChannel> route(AlertEvent event) {
    List<String> channelNames =
        properties
            .getRouting()
            .getOrDefault(event.severity().toUpperCase(Locale.ROOT), List.of());
    return channelNames.stream()
        .map(this::requiredChannel)
        .toList();
  }

  private NotificationChannel requiredChannel(String channelName) {
    NotificationChannel channel = channelsByName.get(channelName);
    if (channel == null) {
      throw new IllegalStateException("No notification channel bean registered for " + channelName);
    }
    return channel;
  }
}
