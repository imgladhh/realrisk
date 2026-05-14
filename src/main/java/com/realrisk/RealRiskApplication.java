package com.realrisk;

import com.realrisk.config.RiskProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(RiskProperties.class)
public class RealRiskApplication {
  public static void main(String[] args) {
    SpringApplication.run(RealRiskApplication.class, args);
  }
}
