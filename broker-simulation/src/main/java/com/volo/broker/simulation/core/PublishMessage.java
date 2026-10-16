package com.volo.broker.simulation.core;

public record PublishMessage(
        String queueName,
        byte[] payload,
        long publishedAtNs,
        int publisherIndex,
        int messageIndex
) {
}

