package com.realrisk.alertservice.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "realrisk.alert")
public class AlertProperties {
  private String topic = "alert-events";
  private String consumerGroup = "alert-service";
  private Duration rateLimitWindow = Duration.ofMinutes(15);
  private Duration consumerRetryBackoff = Duration.ofSeconds(1);
  private long consumerRetryAttempts = 2;
  private Map<String, List<String>> routing = defaultRouting();

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public void setConsumerGroup(String consumerGroup) {
    this.consumerGroup = consumerGroup;
  }

  public Duration getRateLimitWindow() {
    return rateLimitWindow;
  }

  public void setRateLimitWindow(Duration rateLimitWindow) {
    this.rateLimitWindow = rateLimitWindow;
  }

  public Duration getConsumerRetryBackoff() {
    return consumerRetryBackoff;
  }

  public void setConsumerRetryBackoff(Duration consumerRetryBackoff) {
    this.consumerRetryBackoff = consumerRetryBackoff;
  }

  public long getConsumerRetryAttempts() {
    return consumerRetryAttempts;
  }

  public void setConsumerRetryAttempts(long consumerRetryAttempts) {
    this.consumerRetryAttempts = consumerRetryAttempts;
  }

  public Map<String, List<String>> getRouting() {
    return routing;
  }

  public void setRouting(Map<String, List<String>> routing) {
    this.routing = routing;
  }

  private static Map<String, List<String>> defaultRouting() {
    Map<String, List<String>> defaults = new LinkedHashMap<>();
    defaults.put("MEDIUM", List.of("email"));
    defaults.put("HIGH", List.of("email", "sms"));
    defaults.put("CRITICAL", List.of("email", "sms", "push"));
    return defaults;
  }
}
