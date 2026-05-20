package com.realrisk.rules;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.realrisk.config.RiskProperties;
import com.realrisk.model.AdminRuleRequest;
import com.realrisk.model.RuleView;
import java.time.Clock;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

@Service
public class RuleService {
  private static final Logger log = LoggerFactory.getLogger(RuleService.class);
  private static final Set<String> ALLOWED_RULE_TYPES =
      Set.of(
          "large_amount",
          "withdrawal_without_device",
          "merchant_multi_user_burst",
          "high_velocity_7d",
          "global");
  private static final TypeReference<Map<String, String>> PARAMETERS_TYPE = new TypeReference<>() {};

  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;
  private final RuleUpdatePublisher publisher;
  private final RiskProperties properties;
  private final Clock clock;

  public RuleService(
      JdbcTemplate jdbcTemplate,
      ObjectMapper objectMapper,
      RuleUpdatePublisher publisher,
      RiskProperties properties) {
    this(jdbcTemplate, objectMapper, publisher, properties, Clock.systemUTC());
  }

  RuleService(
      JdbcTemplate jdbcTemplate,
      ObjectMapper objectMapper,
      RuleUpdatePublisher publisher,
      RiskProperties properties,
      Clock clock) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
    this.publisher = publisher;
    this.properties = properties;
    this.clock = clock;
  }

  @Transactional
  public RuleView upsertRule(AdminRuleRequest request) {
    String ruleType = normalizeRuleType(request.ruleType());
    Map<String, String> parameters = sanitizeParameters(request.parameters());
    Instant now = Instant.now(clock);
    RuleOutboxPayload payload =
        new RuleOutboxPayload(request.ruleId(), ruleType, true, parameters, now);
    String parametersJson = toJson(parameters);
    String payloadJson = toJson(payload);

    jdbcTemplate.update(
        """
        INSERT INTO rules (rule_id, rule_type, parameters, enabled, created_at, updated_at)
        VALUES (?, ?, CAST(? AS jsonb), ?, ?, ?)
        ON CONFLICT (rule_id) DO UPDATE SET
          rule_type = EXCLUDED.rule_type,
          parameters = EXCLUDED.parameters,
          enabled = EXCLUDED.enabled,
          updated_at = EXCLUDED.updated_at
        """,
        request.ruleId(),
        ruleType,
        parametersJson,
        true,
        Timestamp.from(now),
        Timestamp.from(now));

    jdbcTemplate.update(
        """
        INSERT INTO rule_outbox (rule_id, payload, created_at)
        VALUES (?, CAST(? AS jsonb), ?)
        """,
        request.ruleId(),
        payloadJson,
        Timestamp.from(now));

    return findRule(request.ruleId());
  }

  @Transactional
  public RuleView disableRule(String ruleId) {
    RuleView existing = findOptionalRule(ruleId);
    if (existing == null) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Rule not found");
    }

    Instant now = Instant.now(clock);
    RuleOutboxPayload payload =
        new RuleOutboxPayload(
            existing.ruleId(), existing.ruleType(), false, existing.parameters(), now);
    String payloadJson = toJson(payload);

    jdbcTemplate.update(
        "UPDATE rules SET enabled = ?, updated_at = ? WHERE rule_id = ?",
        false,
        Timestamp.from(now),
        ruleId);

    jdbcTemplate.update(
        "INSERT INTO rule_outbox (rule_id, payload, created_at) VALUES (?, CAST(? AS jsonb), ?)",
        ruleId,
        payloadJson,
        Timestamp.from(now));

    return findRule(ruleId);
  }

  public List<RuleView> listRules() {
    return jdbcTemplate.query(
        """
        SELECT rule_id, rule_type, parameters::text AS parameters_json, enabled, created_at, updated_at
        FROM rules
        ORDER BY updated_at DESC, rule_id ASC
        """,
        (rs, rowNum) -> mapRule(rs));
  }

  @Scheduled(fixedDelayString = "${realrisk.rule-outbox.poll-interval-ms:5000}")
  public void publishPendingOutbox() {
    List<RuleOutboxEntry> pending =
        jdbcTemplate.query(
            """
            SELECT id, payload::text AS payload_json
            FROM rule_outbox
            WHERE published_at IS NULL
            ORDER BY id ASC
            LIMIT ?
            """,
            ps -> ps.setInt(1, properties.ruleOutbox().batchSize()),
            (rs, rowNum) -> new RuleOutboxEntry(rs.getLong("id"), fromJson(rs.getString("payload_json"))));

    for (RuleOutboxEntry entry : pending) {
      try {
        publisher.publishBlocking(entry.payload());
        jdbcTemplate.update(
            "UPDATE rule_outbox SET published_at = now() WHERE id = ? AND published_at IS NULL",
            entry.id());
      } catch (RuntimeException e) {
        log.warn(
            "Failed to publish rule outbox entry id={} ruleId={}, will retry",
            entry.id(),
            entry.payload().ruleId(),
            e);
        break;
      }
    }
  }

  private RuleView findRule(String ruleId) {
    RuleView rule = findOptionalRule(ruleId);
    if (rule == null) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Rule not found");
    }
    return rule;
  }

  private RuleView findOptionalRule(String ruleId) {
    List<RuleView> rows =
        jdbcTemplate.query(
            """
            SELECT rule_id, rule_type, parameters::text AS parameters_json, enabled, created_at, updated_at
            FROM rules
            WHERE rule_id = ?
            """,
            ps -> ps.setString(1, ruleId),
            (rs, rowNum) -> mapRule(rs));
    return rows.isEmpty() ? null : rows.getFirst();
  }

  private RuleView mapRule(ResultSet rs) throws SQLException {
    return new RuleView(
        rs.getString("rule_id"),
        rs.getString("rule_type"),
        readParameters(rs.getString("parameters_json")),
        rs.getBoolean("enabled"),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("updated_at").toInstant());
  }

  private Map<String, String> sanitizeParameters(Map<String, String> parameters) {
    return parameters == null ? Map.of() : Map.copyOf(parameters);
  }

  private String normalizeRuleType(String ruleType) {
    String normalized = ruleType.toLowerCase(Locale.ROOT);
    if (!ALLOWED_RULE_TYPES.contains(normalized)) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported rule_type");
    }
    return normalized;
  }

  private String toJson(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to serialize rule payload", e);
    }
  }

  private RuleOutboxPayload fromJson(String payloadJson) {
    try {
      return objectMapper.readValue(payloadJson, RuleOutboxPayload.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to deserialize rule outbox payload", e);
    }
  }

  private Map<String, String> readParameters(String parametersJson) {
    try {
      return objectMapper.readValue(parametersJson, PARAMETERS_TYPE);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to deserialize rule parameters", e);
    }
  }
}
