package com.realrisk.security;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

class AdminApiKeyFilterTest {
  private MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    mockMvc =
        MockMvcBuilders.standaloneSetup(new DummyController())
            .addFilters(new AdminApiKeyFilter("dev-only-insecure"))
            .build();
  }

  @Test
  void adminRequestWithoutTokenReturnsUnauthorized() throws Exception {
    mockMvc
        .perform(
            post("/admin/rules")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"ruleId\":\"rule-1\"}"))
        .andExpect(status().isUnauthorized())
        .andExpect(content().string(""));
  }

  @Test
  void adminRequestWithWrongTokenReturnsUnauthorized() throws Exception {
    mockMvc
        .perform(
            post("/admin/rules")
                .header("Authorization", "Bearer wrong-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"ruleId\":\"rule-1\"}"))
        .andExpect(status().isUnauthorized());
  }

  @Test
  void adminRequestWithCorrectTokenSucceeds() throws Exception {
    mockMvc
        .perform(
            post("/admin/rules")
                .header("Authorization", "Bearer dev-only-insecure")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"ruleId\":\"rule-1\"}"))
        .andExpect(status().isOk())
        .andExpect(content().string("admin-ok"));
  }

  @Test
  void eventsRequestRemainsUnauthenticated() throws Exception {
    mockMvc
        .perform(
            post("/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventId\":\"evt-1\"}"))
        .andExpect(status().isOk())
        .andExpect(content().string("events-ok"));
  }

  @Test
  void healthRequestRemainsUnauthenticated() throws Exception {
    mockMvc.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(content().string("healthy"));
  }

  @RestController
  static class DummyController {
    @PostMapping("/admin/rules")
    String admin(@RequestBody String ignored) {
      return "admin-ok";
    }

    @PostMapping("/events")
    String events(@RequestBody String ignored) {
      return "events-ok";
    }

    @GetMapping("/actuator/health")
    String health() {
      return "healthy";
    }
  }
}
