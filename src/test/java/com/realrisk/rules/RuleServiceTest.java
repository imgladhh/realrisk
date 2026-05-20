package com.realrisk.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.realrisk.config.RiskProperties;
import com.realrisk.model.AdminRuleRequest;
import com.realrisk.model.RuleView;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.server.ResponseStatusException;

class RuleServiceTest {
  private final JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
  private final RuleUpdatePublisher publisher = Mockito.mock(RuleUpdatePublisher.class);
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
  private final Clock clock = Clock.fixed(Instant.parse("2026-05-20T01:00:00Z"), ZoneOffset.UTC);
  private RuleService service;

  @BeforeEach
  void setUp() {
    var properties =
        new RiskProperties(
            new RiskProperties.Topics(
                "raw-events",
                "rule-updates",
                "raw-audit",
                "decision-audit",
                "high-risk-events",
                "alert-events"),
            new RiskProperties.RateLimit(60_000, 5),
            new RiskProperties.RuleOutbox(5_000, 100));
    service = new RuleService(jdbcTemplate, objectMapper, publisher, properties, clock);
  }

  @Test
  void upsertRuleNormalizesRuleTypeAndReturnsStoredRule() {
    RuleView expected =
        new RuleView(
            "rule-1",
            "large_amount",
            Map.of("amount_cents", "500000"),
            true,
            Instant.parse("2026-05-20T01:00:00Z"),
            Instant.parse("2026-05-20T01:00:00Z"));
    stubFindRule("rule-1", expected);

    RuleView actual =
        service.upsertRule(
            new AdminRuleRequest("rule-1", "LARGE_AMOUNT", Map.of("amount_cents", "500000")));

    assertThat(actual).isEqualTo(expected);
    verify(jdbcTemplate)
        .update(
            anyString(),
            eq("rule-1"),
            eq("large_amount"),
            anyString(),
            eq(true),
            eq(Timestamp.from(clock.instant())),
            eq(Timestamp.from(clock.instant())));
  }

  @Test
  void disableRuleThrowsNotFoundWhenRuleMissing() {
    stubFindRule("missing", null);

    assertThatThrownBy(() -> service.disableRule("missing"))
        .isInstanceOf(ResponseStatusException.class)
        .satisfies(
            ex ->
                assertThat(((ResponseStatusException) ex).getStatusCode())
                    .isEqualTo(HttpStatus.NOT_FOUND));
  }

  @Test
  void publishPendingOutboxStopsAtFirstFailureToPreserveOrdering() {
    RuleOutboxPayload first =
        new RuleOutboxPayload(
            "rule-1", "large_amount", true, Map.of("amount_cents", "500000"), clock.instant());
    RuleOutboxPayload second =
        new RuleOutboxPayload(
            "rule-1", "large_amount", false, Map.of("amount_cents", "500000"), clock.instant());

    when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(RowMapper.class)))
        .thenReturn(List.of(new RuleOutboxEntry(1L, first), new RuleOutboxEntry(2L, second)));
    doThrow(new IllegalStateException("boom")).when(publisher).publishBlocking(first);

    service.publishPendingOutbox();

    verify(publisher).publishBlocking(first);
    verify(publisher, never()).publishBlocking(second);
    verify(jdbcTemplate, never())
        .update("UPDATE rule_outbox SET published_at = now() WHERE id = ? AND published_at IS NULL", 2L);
  }

  @SuppressWarnings("unchecked")
  private void stubFindRule(String ruleId, RuleView ruleView) {
    when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(RowMapper.class)))
        .thenAnswer(
            invocation -> {
              String sql = invocation.getArgument(0, String.class);
              if (!sql.contains("FROM rules")) {
                return List.of();
              }
              return ruleView == null ? List.of() : List.of(ruleView);
            });
  }
}
