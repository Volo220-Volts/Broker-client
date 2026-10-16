package com.volo.broker.simulation.core;

public record PublishingOptions(
        int maxConcurrentPublishers,
        int maxInFlightPerPublisher
) {
    private static final int DEFAULT_CONCURRENT_PUBLISHERS = 64;
    private static final int DEFAULT_IN_FLIGHT_PER_PUBLISHER = 32;

    public PublishingOptions {
        if (maxConcurrentPublishers <= 0) {
            maxConcurrentPublishers = DEFAULT_CONCURRENT_PUBLISHERS;
        }

        if (maxInFlightPerPublisher <= 0) {
            maxInFlightPerPublisher = DEFAULT_IN_FLIGHT_PER_PUBLISHER;
        }
    }

    public static PublishingOptions defaults() {
        return new PublishingOptions(0, 0);
    }
}

