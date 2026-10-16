package com.volo.broker.simulation.journal.model;

public record QueueSecondSnapshot(
        String queueName,
        long publishedDelta,
        long publishFailedDelta,
        long publishedBytesDelta,
        long consumedDelta,
        long consumeFailedDelta,
        long consumedBytesDelta,
        long cumulativePublished,
        long cumulativeConsumed,
        long backlog,
        double publishAckAvgMs,
        double publishAckMaxMs,
        double endToEndAvgMs,
        double endToEndMaxMs
) {
}

