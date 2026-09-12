package com.realrisk.flink;

import com.realrisk.avro.RuleUpdateAvro;
import java.io.Serializable;

public record RuleUpdateEnvelope(
    RuleUpdateAvro update, String topic, int partition, long offset) implements Serializable {}
