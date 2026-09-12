package com.realrisk.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.realrisk.kafka.KafkaPublishException;
import com.realrisk.kafka.RiskEventPublisher;
import com.realrisk.metrics.ApiGatewayMetrics;
import com.realrisk.model.RateLimitResult;
import com.realrisk.redis.BlacklistService;
import com.realrisk.redis.RateLimitService;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class IngestControllerTest {
  private final BlacklistService blacklistService = Mockito.mock(BlacklistService.class);
  private final RateLimitService rateLimitService = Mockito.mock(RateLimitService.class);
  private final RiskEventPublisher publisher = Mockito.mock(RiskEventPublisher.class);
  private final ApiGatewayMetrics metrics = Mockito.mock(ApiGatewayMetrics.class);
  private MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    when(blacklistService.find("user-1")).thenReturn(Optional.empty());
    when(rateLimitService.check("user-1", "request-1")).thenReturn(new RateLimitResult(false, 1));
    mockMvc =
        MockMvcBuilders.standaloneSetup(
                new IngestController(blacklistService, rateLimitService, publisher, metrics))
            .build();
  }

  @Test
  void brokerAcknowledgementReturnsAccepted() throws Exception {
    mockMvc
        .perform(postEvent())
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.status").value("ACCEPTED"));

    verify(publisher).publishRawEvent(any());
    verify(metrics).incrementIngressOutcome("ALLOW");
  }

  @Test
  void kafkaFailureReturnsServiceUnavailableAndIsNotCountedAsAllowed() throws Exception {
    Mockito.doThrow(new KafkaPublishException("failed", new IllegalStateException("broker down")))
        .when(publisher)
        .publishRawEvent(any());

    mockMvc
        .perform(postEvent())
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.status").value("UNAVAILABLE"))
        .andExpect(jsonPath("$.reason").value("kafka_publish_failed"));

    verify(metrics, never()).incrementIngressOutcome("ALLOW");
  }

  private org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder postEvent() {
    return post("/events")
        .contentType(MediaType.APPLICATION_JSON)
        .content(
            """
            {
              "eventId":"event-1",
              "requestId":"request-1",
              "userId":"user-1",
              "eventType":"TRANSACTION",
              "amountCents":1000,
              "currency":"USD",
              "deviceFp":"device-1",
              "merchantId":"merchant-1",
              "source":"test"
            }
            """);
  }
}
