package com.volo.broker.client.publisher;

import java.time.Duration;

public record PublishOptions(
        boolean waitForAck,
        Duration timeout
) {
    public PublishOptions {
        if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    public static PublishOptions waitForAck(Duration timeout) {
        return new PublishOptions(true, timeout);
    }

    public static PublishOptions fireAndForget() {
        return new PublishOptions(false, null);
    }
}