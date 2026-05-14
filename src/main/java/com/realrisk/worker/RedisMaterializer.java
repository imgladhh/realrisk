package com.realrisk.worker;

import com.realrisk.avro.RiskDecisionAvro;
import com.realrisk.kafka.AvroMapper;
import com.realrisk.model.RiskDecision;
import com.realrisk.redis.BlacklistService;
import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RedisMaterializer {
  private static final int ASYNC_BLOCK_SEVERITY = 3;
  private static final long ASYNC_BLOCK_TTL_SECONDS = 86_400;

  private final BlacklistService blacklistService;
  private final JdbcTemplate jdbcTemplate;

  public RedisMaterializer(BlacklistService blacklistService, JdbcTemplate jdbcTemplate) {
    this.blacklistService = blacklistService;
    this.jdbcTemplate = jdbcTemplate;
  }

  @KafkaListener(
      topics = "${realrisk.topics.decision-audit}",
      groupId = "redis-materializer",
      containerFactory = "riskDecisionKafkaListenerContainerFactory")
  public void materialize(RiskDecisionAvro decision) {
    materialize(AvroMapper.fromAvro(decision));
  }

  public void materialize(RiskDecision decision) {
    if (!"BLOCK".equals(decision.decision())) {
      return;
    }
    String reason = "ASYNC_RULE:%s".formatted(decision.decisionId());
    blacklistService.set(
        decision.userId(),
        reason,
        ASYNC_BLOCK_SEVERITY,
        ASYNC_BLOCK_TTL_SECONDS);
    auditBan(decision, reason);
  }

  private void auditBan(RiskDecision decision, String reason) {
    Instant expiresAt = Instant.now().plusSeconds(ASYNC_BLOCK_TTL_SECONDS);
    jdbcTemplate.update(
        """
        INSERT INTO audit_bans (decision_id, user_id, reason, severity, expires_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (decision_id) DO NOTHING
        """,
        decision.decisionId(),
        decision.userId(),
        reason,
        ASYNC_BLOCK_SEVERITY,
        Timestamp.from(expiresAt));
  }
}
