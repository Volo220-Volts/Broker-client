package com.volo.broker.client.internal;

public final class ClientValidators {
    private ClientValidators() {
    }

    public static void requireQueuePath(String queuePath) {
        if (queuePath == null || queuePath.isBlank()) {
            throw new IllegalArgumentException("queuePath is required");
        }

        if (queuePath.contains("*") || queuePath.contains("#")) {
            throw new IllegalArgumentException("queuePath must not contain wildcards");
        }

        validateSegments(queuePath, "queuePath");
    }

    public static void requireRoutingKey(String routingKey) {
        if (routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("routingKey is required");
        }

        validateSegments(routingKey, "routingKey");
    }

    private static void validateSegments(String value, String fieldName) {
        String[] segments = value.trim().split("\\.", -1);

        for (String segment : segments) {
            if (segment.isBlank()) {
                throw new IllegalArgumentException(fieldName + " must not contain empty segments");
            }
        }
    }
}