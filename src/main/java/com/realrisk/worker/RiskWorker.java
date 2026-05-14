package com.realrisk.worker;

import com.realrisk.avro.RiskEventAvro;
import com.realrisk.config.RiskProperties;
import com.realrisk.kafka.AvroMapper;
import com.realrisk.model.RiskDecision;
import com.realrisk.model.RiskEvent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "realrisk.risk-worker.enabled", havingValue = "true", matchIfMissing = true)
public class RiskWorker {
  private static final long LARGE_AMOUNT_CENTS = 1_000_000L;

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final RiskProperties properties;

  public RiskWorker(KafkaTemplate<String, Object> kafkaTemplate, RiskProperties properties) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
  }

  @KafkaListener(
      topics = "${realrisk.topics.raw-events}",
      groupId = "risk-worker",
      containerFactory = "riskEventKafkaListenerContainerFactory")
  public void evaluate(RiskEventAvro event) {
    evaluate(AvroMapper.fromAvro(event));
  }

  public void evaluate(RiskEvent event) {
    var reasons = new ArrayList<String>();
    int score = 0;

    // Phase 1 async-rule stub. Flink will replace this worker in Phase 2.
    if (event.amountCents() >= LARGE_AMOUNT_CENTS) {
      reasons.add("large_amount");
      score += 80;
    }
    if ("WITHDRAWAL".equals(event.eventType()) && event.deviceFp() == null) {
      reasons.add("withdrawal_without_device");
      score += 30;
    }

    String decision = score >= 80 ? "BLOCK" : "ALLOW";
    if (reasons.isEmpty()) {
      reasons.add("no_async_rule_hit");
    }

    var riskDecision =
        new RiskDecision(
            decisionId(event),
            event.eventId(),
            event.requestId(),
            event.userId(),
            decision,
            Math.min(score, 100),
            List.copyOf(reasons),
            Instant.now(),
            "risk-worker");

    kafkaTemplate.send(properties.topics().decisionAudit(), event.userId(), AvroMapper.toAvro(riskDecision));
  }

  private String decisionId(RiskEvent event) {
    return UUID.nameUUIDFromBytes(("risk-worker:" + event.eventId()).getBytes(StandardCharsets.UTF_8))
        .toString();
  }
}
