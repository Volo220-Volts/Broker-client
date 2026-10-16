package com.volo.broker.simulation.journal.model;

import java.time.Instant;
import java.util.List;

public record SimulationSummaryReport(
        String simulationId,
        String status,
        Instant finishedAt,
        double durationSeconds,
        long expectedMessages,
        long totalPublished,
        long totalConsumed,
        long totalPublishFailed,
        long totalConsumeFailed,
        long totalPublishedBytes,
        long totalConsumedBytes,
        double avgPublishMsgSec,
        double peakPublishMsgSec,
        double avgConsumeMsgSec,
        double peakConsumeMsgSec,
        double avgPublishMBs,
        double avgConsumeMBs,
        double avgPublishAckMs,
        double maxPublishAckMs,
        double avgEndToEndMs,
        double maxEndToEndMs,
        List<DiagnosticFinding> findings,
        String healthSummary
) {
    public SimulationSummaryReport {
        findings = findings == null ? List.of() : List.copyOf(findings);
    }
}

