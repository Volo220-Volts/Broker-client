package com.volo.broker.client.publisher;

import com.volo.broker.client.connection.ReconnectPolicy;

import java.util.Objects;

public record ReconnectingPublisherOptions(
        ReconnectPolicy reconnectPolicy,
        boolean retryPublishWhenResultUnknown
) {
    public ReconnectingPublisherOptions {
        reconnectPolicy = Objects.requireNonNullElse(
                reconnectPolicy,
                ReconnectPolicy.unlimited()
        );
    }

    public static ReconnectingPublisherOptions defaults() {
        return new ReconnectingPublisherOptions(
                ReconnectPolicy.unlimited(),
                false
        );
    }

    public static ReconnectingPublisherOptions retryUnknownPublishes() {
        return new ReconnectingPublisherOptions(
                ReconnectPolicy.unlimited(),
                true
        );
    }
}