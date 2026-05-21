package com.realrisk;

import com.realrisk.config.RiskProperties;
import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = UserDetailsServiceAutoConfiguration.class)
@EnableConfigurationProperties(RiskProperties.class)
@EnableScheduling
public class RealRiskApplication {
  public static void main(String[] args) {
    SpringApplication.run(RealRiskApplication.class, args);
  }

  @Bean
  Clock systemClock() {
    return Clock.systemUTC();
  }
}
