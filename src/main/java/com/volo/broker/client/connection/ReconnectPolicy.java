package com.volo.broker.client.connection;

import java.time.Duration;
import java.util.Objects;

public record ReconnectPolicy(
        boolean enabled,
        int maxAttempts,
        Duration initialDelay,
        Duration maxDelay
) {
    public ReconnectPolicy {
        if (maxAttempts < 0) {
            throw new IllegalArgumentException("maxAttempts must not be negative");
        }

        initialDelay = requirePositive(initialDelay, "initialDelay");
        maxDelay = requirePositive(maxDelay, "maxDelay");

        if (maxDelay.compareTo(initialDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must be greater than or equal to initialDelay");
        }
    }

    public static ReconnectPolicy disabled() {
        return new ReconnectPolicy(
                false,
                0,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );
    }

    public static ReconnectPolicy unlimited() {
        return new ReconnectPolicy(
                true,
                0,
                Duration.ofSeconds(1),
                Duration.ofSeconds(30)
        );
    }

    public static ReconnectPolicy limited(int maxAttempts) {
        return new ReconnectPolicy(
                true,
                maxAttempts,
                Duration.ofSeconds(1),
                Duration.ofSeconds(30)
        );
    }

    public boolean shouldRetry(int failedAttempts) {
        if (!enabled) {
            return false;
        }

        return maxAttempts == 0 || failedAttempts < maxAttempts;
    }

    public Duration delayForAttempt(int failedAttempts) {
        long multiplier = Math.max(1, failedAttempts);
        long delayMs = initialDelay.toMillis() * multiplier;

        return Duration.ofMillis(Math.min(delayMs, maxDelay.toMillis()));
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name + " is required");

        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }

        return value;
    }
}