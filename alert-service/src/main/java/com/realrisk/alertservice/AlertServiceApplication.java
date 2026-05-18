package com.realrisk.alertservice;

import com.realrisk.alertservice.config.AlertProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(AlertProperties.class)
public class AlertServiceApplication {
  public static void main(String[] args) {
    SpringApplication.run(AlertServiceApplication.class, args);
  }
}
