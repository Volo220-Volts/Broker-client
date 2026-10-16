package com.volo.broker.client.publisher;

import com.volo.broker.client.internal.ClientValidators;

import java.util.Map;

public record PublishRequest(
        String routingKey,
        byte[] payload,
        Map<String, String> headers
) {
    public PublishRequest {
        ClientValidators.requireRoutingKey(routingKey);

        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null (use byte[0] for empty payload)");
        }

        payload = payload.clone();
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public PublishRequest(String routingKey, byte[] payload) {
        this(routingKey, payload, Map.of());
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

    public com.google.protobuf.ByteString toByteString() {
        return com.google.protobuf.ByteString.copyFrom(payload);
    }
}