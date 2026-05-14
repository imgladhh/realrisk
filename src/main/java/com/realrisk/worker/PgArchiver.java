package com.realrisk.worker;

import com.realrisk.avro.RiskDecisionAvro;
import com.realrisk.avro.RiskEventAvro;
import com.realrisk.kafka.AvroMapper;
import com.realrisk.model.RiskDecision;
import com.realrisk.model.RiskEvent;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PgArchiver {
  private final JdbcTemplate jdbcTemplate;

  public PgArchiver(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @KafkaListener(
      topics = "${realrisk.topics.raw-audit}",
      groupId = "pg-archiver-raw",
      containerFactory = "riskEventKafkaListenerContainerFactory")
  public void archiveRawEvent(RiskEventAvro event) {
    archiveRawEvent(AvroMapper.fromAvro(event));
  }

  public void archiveRawEvent(RiskEvent event) {
    jdbcTemplate.update(
        """
        INSERT INTO raw_events (
          event_id, request_id, user_id, event_type, occurred_at, amount_cents,
          currency, ip_address, device_fp, merchant_id, counterparty, source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (event_id) DO NOTHING
        """,
        event.eventId(),
        event.requestId(),
        event.userId(),
        event.eventType(),
        Timestamp.from(event.timestamp()),
        event.amountCents(),
        event.currency(),
        event.ipAddress(),
        event.deviceFp(),
        event.merchantId(),
        event.counterparty(),
        event.source());
  }

  @KafkaListener(
      topics = "${realrisk.topics.decision-audit}",
      groupId = "pg-archiver-decisions",
      containerFactory = "riskDecisionKafkaListenerContainerFactory")
  public void archiveDecision(RiskDecisionAvro decision) {
    archiveDecision(AvroMapper.fromAvro(decision));
  }

  public void archiveDecision(RiskDecision decision) {
    jdbcTemplate.update(
        connection -> {
          PreparedStatement ps =
              connection.prepareStatement(
                  """
                  INSERT INTO risk_decisions (
                    decision_id, event_id, request_id, user_id, decision, risk_score, reasons, created_at
                  )
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                  ON CONFLICT (decision_id) DO NOTHING
                  """);
          ps.setString(1, decision.decisionId());
          ps.setString(2, decision.eventId());
          ps.setString(3, decision.requestId());
          ps.setString(4, decision.userId());
          ps.setString(5, decision.decision());
          ps.setInt(6, decision.riskScore());
          ps.setArray(7, reasonsArray(ps, decision));
          ps.setTimestamp(8, Timestamp.from(decision.createdAt()));
          return ps;
        });
  }

  private Array reasonsArray(PreparedStatement ps, RiskDecision decision) throws SQLException {
    return ps.getConnection().createArrayOf("text", decision.reasons().toArray(String[]::new));
  }
}
