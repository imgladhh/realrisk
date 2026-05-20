package com.realrisk.model;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public record AdminRuleRequest(
    @NotBlank String ruleId, @NotBlank String ruleType, Map<String, String> parameters) {}
