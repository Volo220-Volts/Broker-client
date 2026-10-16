package com.volo.broker.simulation.journal.diagnostics;

import com.volo.broker.simulation.core.SimulationStats;
import com.volo.broker.simulation.journal.model.DiagnosticFinding;
import com.volo.broker.simulation.journal.model.SecondSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class SimulationDiagnostics {
    private SimulationDiagnostics() {
    }

    public record AnalysisResult(
            List<DiagnosticFinding> findings,
            String healthSummary
    ) {
    }

    public static AnalysisResult analyze(SimulationStats stats, List<SecondSnapshot> snapshots, long totalExpected) {
        List<DiagnosticFinding> findings = new ArrayList<>();

        // 1. Check failures
        if (stats.publishFailedCount() > 0) {
            findings.add(new DiagnosticFinding(
                    "CRITICAL",
                    "FAILURE",
                    findFirstFailureSecond(snapshots, true),
                    "Detected " + stats.publishFailedCount() + " failed message publish attempts!"
            ));
        }

        if (stats.consumeFailedCount() > 0) {
            findings.add(new DiagnosticFinding(
                    "CRITICAL",
                    "FAILURE",
                    findFirstFailureSecond(snapshots, false),
                    "Detected " + stats.consumeFailedCount() + " consumer message delivery errors!"
            ));
        }

        // 2. Check latency spikes across seconds
        int maxAckSecond = 0;
        double maxAckValue = 0;
        int maxE2eSecond = 0;
        double maxE2eValue = 0;

        int consecutiveStalls = 0;
        int stallStartSecond = 0;

        for (SecondSnapshot s : snapshots) {
            if (s.windowPublishAckMaxMs() > maxAckValue) {
                maxAckValue = s.windowPublishAckMaxMs();
                maxAckSecond = s.second();
            }
            if (s.windowEndToEndMaxMs() > maxE2eValue) {
                maxE2eValue = s.windowEndToEndMaxMs();
                maxE2eSecond = s.second();
            }

            // High memory
            if (s.heapUsagePercent() > 85.0) {
                findings.add(new DiagnosticFinding(
                        "WARNING",
                        "RESOURCE",
                        s.second(),
                        String.format(Locale.ROOT, "High JVM Heap memory usage: %.1f%% (%d MB)",
                                s.heapUsagePercent(), s.heapUsedBytes() / (1024 * 1024))
                ));
            }

            // Check consumer stall
            if (s.publishedDelta() > 0 && s.consumedDelta() == 0) {
                consecutiveStalls++;
                if (consecutiveStalls == 1) {
                    stallStartSecond = s.second();
                }
            } else {
                if (consecutiveStalls >= 3) {
                    findings.add(new DiagnosticFinding(
                            "WARNING",
                            "BACKLOG",
                            stallStartSecond,
                            String.format(Locale.ROOT, "Consumer read stall: no messages received for %d consecutive seconds (starting at sec %d)",
                                    consecutiveStalls, stallStartSecond)
                    ));
                }
                consecutiveStalls = 0;
            }
        }

        if (maxAckValue > 100.0) {
            findings.add(new DiagnosticFinding(
                    "WARNING",
                    "LATENCY",
                    maxAckSecond,
                    String.format(Locale.ROOT, "Publish ACK latency spike: %.2f ms at second %d (possible socket or broker queue backlog)",
                            maxAckValue, maxAckSecond)
            ));
        }

        if (maxE2eValue > 500.0) {
            findings.add(new DiagnosticFinding(
                    "WARNING",
                    "LATENCY",
                    maxE2eSecond,
                    String.format(Locale.ROOT, "End-to-End latency spike: %.2f ms at second %d (consumer delivery delay)",
                            maxE2eValue, maxE2eSecond)
            ));
        }

        // Summary generation
        boolean hasCritical = findings.stream().anyMatch(f -> "CRITICAL".equalsIgnoreCase(f.severity()));
        boolean hasWarning = findings.stream().anyMatch(f -> "WARNING".equalsIgnoreCase(f.severity()));

        String summary;
        if (hasCritical) {
            summary = "CRITICAL: Serious transmission failures or dropped messages detected during simulation!";
        } else if (hasWarning) {
            summary = "Simulation completed, but temporary bottlenecks or latency spikes were detected.";
        } else {
            summary = "Simulation completed successfully: no errors, balanced throughput, and stable latencies.";
        }

        return new AnalysisResult(findings, summary);
    }

    private static int findFirstFailureSecond(List<SecondSnapshot> snapshots, boolean publish) {
        for (SecondSnapshot s : snapshots) {
            if (publish && s.publishFailedDelta() > 0) {
                return s.second();
            }
            if (!publish && s.consumeFailedDelta() > 0) {
                return s.second();
            }
        }
        return 0;
    }
}
