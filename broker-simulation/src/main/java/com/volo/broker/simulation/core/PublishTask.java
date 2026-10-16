package com.volo.broker.simulation.core;

public record PublishTask(
        String queueName,
        int publisherIndex,
        int messageCount
) {
}

