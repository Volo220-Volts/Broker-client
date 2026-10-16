package com.volo.broker.simulation.journal.storage;

import com.volo.broker.simulation.journal.model.DiagnosticFinding;
import com.volo.broker.simulation.journal.model.QueueConfigSnapshot;
import com.volo.broker.simulation.journal.model.SimulationRunMetadata;
import com.volo.broker.simulation.journal.model.SimulationSummaryReport;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public final class SummaryTextWriter {
    private SummaryTextWriter() {
    }

    public static void writeToFile(
            Path filePath,
            SimulationRunMetadata metadata,
            SimulationSummaryReport summary
    ) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8)) {
            writer.write("========================================================================================\n");
            writer.write(String.format("                   BROKER SIMULATION REPORT: %s (%s)\n",
                    metadata.simulationId(), metadata.activeBroker()));
            writer.write("========================================================================================\n\n");

            writer.write(String.format("Start time:           %s\n", metadata.startedAt()));
            writer.write(String.format("Finish time:          %s\n", summary.finishedAt()));
            writer.write(String.format(Locale.ROOT, "Duration:             %.2f sec\n", summary.durationSeconds()));
            writer.write(String.format("Status:               %s\n", summary.status()));
            writer.write(String.format(Locale.ROOT, "Expected messages:    %,d\n\n", summary.expectedMessages()));

            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write("BROKER CONNECTION PARAMETERS:\n");
            writer.write("----------------------------------------------------------------------------------------\n");
            metadata.brokerConnection().forEach((key, val) -> {
                try {
                    writer.write(String.format("  %-30s : %s\n", key, val));
                } catch (IOException ignored) {
                }
            });
            writer.write("\n");

            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write("QUEUE CONFIGURATIONS:\n");
            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write(String.format("%-25s | %5s | %5s | %8s | %10s | %8s | %11s | %15s\n",
                    "Queue Name", "Pubs", "Cons", "Msg/Pub", "Total Msg", "Prefetch", "StorageMode", "AckMode"));
            writer.write("----------------------------------------------------------------------------------------------------------\n");
            for (QueueConfigSnapshot q : metadata.queues()) {
                writer.write(String.format(Locale.ROOT, "%-25s | %5d | %5d | %8d | %,10d | %8d | %11s | %15s\n",
                        q.queueName(),
                        q.publishers(),
                        q.consumers(),
                        q.messagesPerPublisher(),
                        q.expectedMessages(),
                        q.consumerPrefetch(),
                        q.storageMode(),
                        q.ackMode()));
            }
            writer.write("\n");

            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write("OVERALL THROUGHPUT AND DATA METRICS:\n");
            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write(String.format(Locale.ROOT, "Published messages:        %,d (failed: %,d)\n",
                    summary.totalPublished(), summary.totalPublishFailed()));
            writer.write(String.format(Locale.ROOT, "Consumed messages:         %,d (failed: %,d)\n",
                    summary.totalConsumed(), summary.totalConsumeFailed()));
            writer.write(String.format(Locale.ROOT, "Transferred payload (Pub): %.2f MB (avg rate: %.2f MB/s)\n",
                    summary.totalPublishedBytes() / (1024.0 * 1024.0), summary.avgPublishMBs()));
            writer.write(String.format(Locale.ROOT, "Received payload (Cons):    %.2f MB (avg rate: %.2f MB/s)\n",
                    summary.totalConsumedBytes() / (1024.0 * 1024.0), summary.avgConsumeMBs()));
            writer.write(String.format(Locale.ROOT, "Publish throughput:        avg %,.1f msg/s (peak: %,.1f msg/s)\n",
                    summary.avgPublishMsgSec(), summary.peakPublishMsgSec()));
            writer.write(String.format(Locale.ROOT, "Consume throughput:        avg %,.1f msg/s (peak: %,.1f msg/s)\n\n",
                    summary.avgConsumeMsgSec(), summary.peakConsumeMsgSec()));

            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write("LATENCIES:\n");
            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write(String.format(Locale.ROOT, "Publish ACK Latency:       avg %.2f ms | max %.2f ms\n",
                    summary.avgPublishAckMs(), summary.maxPublishAckMs()));
            writer.write(String.format(Locale.ROOT, "End-to-End Latency:        avg %.2f ms | max %.2f ms\n\n",
                    summary.avgEndToEndMs(), summary.maxEndToEndMs()));

            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write("DIAGNOSTICS & BOTTLENECK ANALYSIS:\n");
            writer.write("----------------------------------------------------------------------------------------\n");
            writer.write("Health Summary: " + summary.healthSummary() + "\n");
            if (summary.findings().isEmpty()) {
                writer.write("No anomalies or transfer issues detected.\n");
            } else {
                for (DiagnosticFinding finding : summary.findings()) {
                    writer.write(String.format("  [%s] (sec %d, %s): %s\n",
                            finding.severity(),
                            finding.secondDetected(),
                            finding.category(),
                            finding.description()));
                }
            }
            writer.write("========================================================================================\n");
        }
    }
}
