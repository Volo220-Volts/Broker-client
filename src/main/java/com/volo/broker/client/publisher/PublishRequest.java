package com.volo.broker.client.publisher;

import java.util.Map;

public record PublishRequest(
        String routingKey,
        byte[] payload,
        Map<String, String> headers
) {
    public PublishRequest {
        if (routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("routingKey is required");
        }

        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("payload is required");
        }

        payload = payload.clone();
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public static PublishRequest of(String routingKey, byte[] payload) {
        return new PublishRequest(routingKey, payload, Map.of());
    }

    public static PublishRequest of(
            String routingKey,
            byte[] payload,
            Map<String, String> headers
    ) {
        return new PublishRequest(routingKey, payload, headers);
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}