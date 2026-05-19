package com.realrisk.alertservice.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "realrisk.alert")
public class AlertProperties {
  private String topic = "alert-events";
  private String dlqTopic = "alert-events-dlq";
  private String consumerGroup = "alert-service";
  private Duration rateLimitWindow = Duration.ofMinutes(15);
  private Duration retryBackoff = Duration.ofSeconds(1);
  private long maxRetries = 2;
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

  public String getDlqTopic() {
    return dlqTopic;
  }

  public void setDlqTopic(String dlqTopic) {
    this.dlqTopic = dlqTopic;
  }

  public Duration getRateLimitWindow() {
    return rateLimitWindow;
  }

  public void setRateLimitWindow(Duration rateLimitWindow) {
    this.rateLimitWindow = rateLimitWindow;
  }

  public Duration getRetryBackoff() {
    return retryBackoff;
  }

  public void setRetryBackoff(Duration retryBackoff) {
    this.retryBackoff = retryBackoff;
  }

  public long getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(long maxRetries) {
    this.maxRetries = maxRetries;
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
