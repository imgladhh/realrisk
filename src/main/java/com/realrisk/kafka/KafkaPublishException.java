package com.realrisk.kafka;

public class KafkaPublishException extends RuntimeException {
  public KafkaPublishException(String message, Throwable cause) {
    super(message, cause);
  }
}
