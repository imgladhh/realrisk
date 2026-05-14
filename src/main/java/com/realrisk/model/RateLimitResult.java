package com.realrisk.model;

public record RateLimitResult(boolean blocked, long count) {}
