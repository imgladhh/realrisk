package com.realrisk.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.realrisk.model.RiskDecision;
import com.realrisk.redis.BlacklistService;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

@EnabledIfSystemProperty(named = "realrisk.test.postgres.url", matches = ".+")
class RedisMaterializerTest {
  private JdbcTemplate jdbcTemplate;
  private BlacklistService blacklistService;
  private RedisMaterializer materializer;

  @BeforeEach
  void setUp() {
    jdbcTemplate = new JdbcTemplate(dataSource());
    jdbcTemplate.execute(
        """
        DROP TABLE IF EXISTS audit_bans
        """);
    jdbcTemplate.execute(
        """
        CREATE TABLE audit_bans (
          id BIGSERIAL PRIMARY KEY,
          decision_id TEXT NOT NULL UNIQUE,
          user_id TEXT NOT NULL,
          reason TEXT NOT NULL,
          severity INTEGER NOT NULL,
          expires_at TIMESTAMPTZ,
          cleared_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """);
    blacklistService = mock(BlacklistService.class);
    when(blacklistService.set("user-1", "ASYNC_RULE:decision-1", 3, 86_400)).thenReturn(true);
    materializer = new RedisMaterializer(blacklistService, jdbcTemplate);
  }

  @Test
  void blockingDecisionWritesAuditBanIdempotently() {
    var decision = blockingDecision();

    materializer.materialize(decision);
    materializer.materialize(decision);

    Integer count =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM audit_bans WHERE decision_id = 'decision-1'", Integer.class);
    assertThat(count).isEqualTo(1);

    var row =
        jdbcTemplate.queryForMap(
            "SELECT user_id, reason, severity, expires_at FROM audit_bans WHERE decision_id = 'decision-1'");
    assertThat(row.get("USER_ID")).isEqualTo("user-1");
    assertThat(row.get("REASON")).isEqualTo("ASYNC_RULE:decision-1");
    assertThat(row.get("SEVERITY")).isEqualTo(3);
    assertThat(row.get("EXPIRES_AT")).isNotNull();
  }

  private RiskDecision blockingDecision() {
    return new RiskDecision(
        "decision-1",
        "event-1",
        "request-1",
        "user-1",
        "BLOCK",
        90,
        List.of("large_amount"),
        Instant.parse("2026-05-10T12:00:00Z"),
        "risk-worker");
  }

  private DataSource dataSource() {
    var dataSource = new DriverManagerDataSource();
    dataSource.setUrl(System.getProperty("realrisk.test.postgres.url"));
    dataSource.setUsername(System.getProperty("realrisk.test.postgres.user", "realrisk"));
    dataSource.setPassword(System.getProperty("realrisk.test.postgres.password", "realrisk"));
    return dataSource;
  }
}
