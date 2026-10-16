package com.volo.broker.simulation.journal.storage;

import com.volo.broker.simulation.config.BrokerType;

import java.nio.file.Path;
import java.time.Instant;

public record SimulationRunInfo(
        String runId,
        BrokerType brokerType,
        Path directory,
        Instant lastModified,
        boolean hasJournal,
        boolean hasSummary,
        boolean hasTimeline,
        boolean hasConfig
) {
}

