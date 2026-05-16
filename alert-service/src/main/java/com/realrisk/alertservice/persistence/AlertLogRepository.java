package com.realrisk.alertservice.persistence;

import com.realrisk.alertservice.model.AlertEvent;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class AlertLogRepository {
  private static final RowMapper<AlertLogRecord> ROW_MAPPER =
      (rs, rowNum) ->
          new AlertLogRecord(
              rs.getString("alert_id"),
              rs.getString("user_id"),
              rs.getString("severity"),
              rs.getString("reason_summary"),
              AlertLogStatus.valueOf(rs.getString("status")),
              List.of((String[]) rs.getArray("channels_notified").getArray()),
              rs.getTimestamp("created_at").toInstant(),
              Optional.ofNullable(rs.getTimestamp("processed_at")).map(Timestamp::toInstant).orElse(null));

  private final JdbcTemplate jdbcTemplate;

  public AlertLogRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public boolean insertIfAbsent(AlertEvent event) {
    return jdbcTemplate.update(
            connection -> {
              PreparedStatement statement =
                  connection.prepareStatement(
                      """
                      INSERT INTO alert_log (
                        alert_id, user_id, severity, reason_summary, status, channels_notified, created_at, processed_at
                      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                      ON CONFLICT (alert_id) DO NOTHING
                      """);
              statement.setString(1, event.alertId());
              statement.setString(2, event.userId());
              statement.setString(3, event.severity());
              statement.setString(4, event.reasonSummary());
              statement.setString(5, AlertLogStatus.PENDING.name());
              Array channelsArray = connection.createArrayOf("text", new String[0]);
              statement.setArray(6, channelsArray);
              statement.setTimestamp(7, Timestamp.from(event.createdAt()));
              statement.setTimestamp(8, null);
              return statement;
            })
        > 0;
  }

  public void markProcessed(
      String alertId, AlertLogStatus status, List<String> channelsNotified, Instant processedAt) {
    jdbcTemplate.update(
        connection -> {
          PreparedStatement statement =
              connection.prepareStatement(
                  """
                  UPDATE alert_log
                  SET status = ?, channels_notified = ?, processed_at = ?
                  WHERE alert_id = ?
                  """);
          statement.setString(1, status.name());
          Array channelsArray = connection.createArrayOf("text", channelsNotified.toArray(String[]::new));
          statement.setArray(2, channelsArray);
          statement.setTimestamp(3, Timestamp.from(processedAt));
          statement.setString(4, alertId);
          return statement;
        });
  }

  public Optional<AlertLogRecord> findByAlertId(String alertId) {
    return jdbcTemplate.query(
            "SELECT * FROM alert_log WHERE alert_id = ?",
            ROW_MAPPER,
            alertId)
        .stream()
        .findFirst();
  }
}
