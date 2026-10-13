package com.volo.broker.client.consumer;

import com.volo.broker.client.connection.ReconnectPolicy;

import java.util.Objects;

public record ReconnectingConsumerOptions(
        ReconnectPolicy reconnectPolicy
) {
    public ReconnectingConsumerOptions {
        reconnectPolicy = Objects.requireNonNullElse(
                reconnectPolicy,
                ReconnectPolicy.unlimited()
        );
    }

    public static ReconnectingConsumerOptions defaults() {
        return new ReconnectingConsumerOptions(
                ReconnectPolicy.unlimited()
        );
    }
}