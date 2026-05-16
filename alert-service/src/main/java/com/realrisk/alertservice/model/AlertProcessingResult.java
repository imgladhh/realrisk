package com.realrisk.alertservice.model;

import java.util.List;

public record AlertProcessingResult(
    String alertId,
    boolean duplicate,
    boolean rateLimited,
    List<String> channelsNotified) {}
