package com.volo.broker.simulation.journal.model;

import java.time.Instant;
import java.util.List;

public record SecondSnapshot(
        int second,
        Instant timestamp,
        double elapsedSeconds,
        // Window deltas (rates over last 1 second)
        long publishedDelta,
        long publishFailedDelta,
        long publishedBytesDelta,
        long consumedDelta,
        long consumeFailedDelta,
        long consumedBytesDelta,
        long throughputBalance,
        // Cumulative totals
        long cumulativePublished,
        long cumulativePublishFailed,
        long cumulativePublishedBytes,
        long cumulativeConsumed,
        long cumulativeConsumeFailed,
        long cumulativeConsumedBytes,
        long backlog,
        double completionPercent,
        // Window latencies
        double windowPublishAckAvgMs,
        double windowPublishAckMaxMs,
        double windowPublishAckMinMs,
        double windowEndToEndAvgMs,
        double windowEndToEndMaxMs,
        double windowEndToEndMinMs,
        // Cumulative latencies
        double cumulativePublishAckAvgMs,
        double cumulativePublishAckMaxMs,
        double cumulativeEndToEndAvgMs,
        double cumulativeEndToEndMaxMs,
        // System & JVM resources
        long heapUsedBytes,
        long heapMaxBytes,
        double heapUsagePercent,
        int liveThreads,
        double cpuLoadPercent,
        // Per-queue breakdown
        List<QueueSecondSnapshot> queues
) {
    public SecondSnapshot {
        queues = queues == null ? List.of() : List.copyOf(queues);
    }
}

