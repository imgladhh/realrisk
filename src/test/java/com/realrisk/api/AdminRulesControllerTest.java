package com.realrisk.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.realrisk.model.RuleView;
import com.realrisk.rules.RuleService;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class AdminRulesControllerTest {
  private final RuleService ruleService = Mockito.mock(RuleService.class);
  private MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    mockMvc = MockMvcBuilders.standaloneSetup(new AdminRulesController(ruleService)).build();
  }

  @Test
  void postReturnsCreated() throws Exception {
    when(ruleService.upsertRule(any()))
        .thenReturn(
            new RuleView(
                "rule-1",
                "large_amount",
                Map.of("amount_cents", "500000"),
                true,
                Instant.parse("2026-05-20T01:00:00Z"),
                Instant.parse("2026-05-20T01:00:00Z")));

    mockMvc
        .perform(
            post("/admin/rules")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    """
                    {
                      "ruleId":"rule-1",
                      "ruleType":"large_amount",
                      "parameters":{"amount_cents":"500000"}
                    }
                    """))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.ruleId").value("rule-1"));
  }

  @Test
  void getReturnsRules() throws Exception {
    when(ruleService.listRules())
        .thenReturn(
            List.of(
                new RuleView(
                    "rule-1",
                    "large_amount",
                    Map.of("amount_cents", "500000"),
                    true,
                    Instant.parse("2026-05-20T01:00:00Z"),
                    Instant.parse("2026-05-20T01:00:00Z"))));

    mockMvc
        .perform(get("/admin/rules"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].ruleType").value("large_amount"));
  }

  @Test
  void deleteReturnsNoContent() throws Exception {
    when(ruleService.disableRule("rule-1"))
        .thenReturn(
            new RuleView(
                "rule-1",
                "large_amount",
                Map.of("amount_cents", "500000"),
                false,
                Instant.parse("2026-05-20T01:00:00Z"),
                Instant.parse("2026-05-20T01:00:00Z")));

    mockMvc.perform(delete("/admin/rules/rule-1")).andExpect(status().isNoContent());

    verify(ruleService).disableRule("rule-1");
  }
}
