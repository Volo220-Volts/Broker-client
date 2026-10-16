package com.volo.broker.simulation.report;

import com.volo.broker.simulation.core.SimulationStats;

public final class SimulationStatsPrinter {
    private static final SimulationReportRenderer DEFAULT_RENDERER = new TableSimulationReportRenderer();

    private SimulationStatsPrinter() {
    }

    public static void print(SimulationStats stats, long expectedMessages) {
        print(stats, expectedMessages, DEFAULT_RENDERER);
    }

    public static void print(
            SimulationStats stats,
            long expectedMessages,
            SimulationReportRenderer renderer
    ) {
        System.out.println(renderer.render(stats, expectedMessages));
    }

    public static void printSummary(SimulationStats stats, long expectedMessages) {
        printSummary(stats, null, expectedMessages);
    }

    public static void printSummary(SimulationStats stats, String simulationName, long expectedMessages) {
        double durationSec = Math.max(0.001, stats.duration().toNanos() / 1_000_000_000.0);
        long pubTotal = stats.publishedCount();
        long consTotal = stats.consumedCount();
        long pubErrors = stats.publishFailedCount();
        long consErrors = stats.consumeFailedCount();

        double pubRate = stats.publishThroughputPerSecond();
        double consRate = stats.consumeThroughputPerSecond();
        String pubBandwidth = DataSizeFormatter.bytesPerSecond(stats.publishBytesPerSecond());
        String consBandwidth = DataSizeFormatter.bytesPerSecond(stats.consumeBytesPerSecond());

        StringBuilder sb = new StringBuilder();
        sb.append(System.lineSeparator());
        sb.append("============================= SIMULATION SUMMARY =============================").append(System.lineSeparator());
        if (simulationName != null && !simulationName.isBlank()) {
            sb.append(String.format(" Simulation:  %s%n", simulationName));
        }
        sb.append(String.format(" Duration:    %.2f s | Expected messages: %,d%n", durationSec, expectedMessages));
        sb.append(String.format(" Published:   %,d msgs (%,.1f msg/s | %s | errors: %,d)%n", pubTotal, pubRate, pubBandwidth, pubErrors));
        sb.append(String.format(" Consumed:    %,d msgs (%,.1f msg/s | %s | errors: %,d)%n", consTotal, consRate, consBandwidth, consErrors));
        sb.append(String.format(" Latency Pub: avg: %.2f ms | max: %.2f ms%n", stats.averagePublishAckLatencyMs(), stats.maxPublishAckLatencyMs()));
        sb.append(String.format(" Latency E2E: avg: %.2f ms | max: %.2f ms%n", stats.averageEndToEndLatencyMs(), stats.maxEndToEndLatencyMs()));
        sb.append("==============================================================================").append(System.lineSeparator());

        System.out.println(sb);
    }
}

