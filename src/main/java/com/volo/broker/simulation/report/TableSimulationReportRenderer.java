package com.volo.broker.simulation.report;

import com.volo.broker.simulation.core.SimulationStats;
import com.volo.broker.simulation.core.SimulationStats.QueueStats;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class TableSimulationReportRenderer implements SimulationReportRenderer {
    @Override
    public String render(SimulationStats stats, long expectedMessages) {
        StringBuilder out = new StringBuilder();

        out.append(summaryTable(stats, expectedMessages).render());

        List<Map.Entry<String, QueueStats>> queues = sortedQueues(stats);

        if (!queues.isEmpty()) {
            double seconds = stats.duration().toNanos() / 1_000_000_000.0;

            out.append(System.lineSeparator());
            out.append(volumeTable(queues).render());
            out.append(System.lineSeparator());
            out.append(performanceTable(queues, seconds).render());
        }

        return out.toString();
    }

    private ConsoleTable summaryTable(SimulationStats stats, long expectedMessages) {
        ConsoleTable table = new ConsoleTable("Simulation summary", "Metric", "Publish", "Consume");

        table.addRow("Messages", number(stats.publishedCount()), number(stats.consumedCount()));
        table.addRow("Failed", number(stats.publishFailedCount()), number(stats.consumeFailedCount()));
        table.addRow("Expected", number(expectedMessages), number(expectedMessages));
        table.addRow("Completion", percent(stats.publishedCount(), expectedMessages),
                percent(stats.consumedCount(), expectedMessages));
        table.addRow("Data", DataSizeFormatter.bytes(stats.publishedBytes()),
                DataSizeFormatter.bytes(stats.consumedBytes()));
        table.addRow("Throughput (msg/s)", decimal(stats.publishThroughputPerSecond()),
                decimal(stats.consumeThroughputPerSecond()));
        table.addRow("Throughput (data/s)", DataSizeFormatter.bytesPerSecond(stats.publishBytesPerSecond()),
                DataSizeFormatter.bytesPerSecond(stats.consumeBytesPerSecond()));
        table.addRow("Avg latency (ms)", decimal(stats.averagePublishAckLatencyMs()),
                decimal(stats.averageEndToEndLatencyMs()));
        table.addRow("Max latency (ms)", decimal(stats.maxPublishAckLatencyMs()),
                decimal(stats.maxEndToEndLatencyMs()));
        table.addRow("Duration", stats.duration().toMillis() + " ms", "");

        return table;
    }

    private ConsoleTable volumeTable(List<Map.Entry<String, QueueStats>> queues) {
        ConsoleTable table = new ConsoleTable(
                "Per-queue volume",
                "Queue", "Published", "Pub failed", "Sent", "Consumed", "Cons failed", "Received"
        );

        for (Map.Entry<String, QueueStats> entry : queues) {
            QueueStats q = entry.getValue();

            table.addRow(
                    entry.getKey(),
                    number(q.published()),
                    number(q.publishFailed()),
                    DataSizeFormatter.bytes(q.publishedBytes()),
                    number(q.consumed()),
                    number(q.consumeFailed()),
                    DataSizeFormatter.bytes(q.consumedBytes())
            );
        }

        return table;
    }

    private ConsoleTable performanceTable(List<Map.Entry<String, QueueStats>> queues, double seconds) {
        ConsoleTable table = new ConsoleTable(
                "Per-queue throughput and latency",
                "Queue", "Pub msg/s", "Pub data/s", "Cons msg/s", "Cons data/s",
                "Avg ACK ms", "Max ACK ms", "Avg E2E ms", "Max E2E ms"
        );

        for (Map.Entry<String, QueueStats> entry : queues) {
            QueueStats q = entry.getValue();

            table.addRow(
                    entry.getKey(),
                    decimal(rate(q.published(), seconds)),
                    DataSizeFormatter.bytesPerSecond(rate(q.publishedBytes(), seconds)),
                    decimal(rate(q.consumed(), seconds)),
                    DataSizeFormatter.bytesPerSecond(rate(q.consumedBytes(), seconds)),
                    decimal(q.averagePublishAckLatencyMs()),
                    decimal(q.maxPublishAckLatencyMs()),
                    decimal(q.averageEndToEndLatencyMs()),
                    decimal(q.maxEndToEndLatencyMs())
            );
        }

        return table;
    }

    private List<Map.Entry<String, QueueStats>> sortedQueues(SimulationStats stats) {
        return stats.perQueueStats().entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(
                        TableSimulationReportRenderer::lastNumber,
                        Comparator.nullsLast(Comparator.naturalOrder())
                ).thenComparing(Comparator.naturalOrder())))
                .toList();
    }

    private static Integer lastNumber(String queuePath) {
        int dot = queuePath.lastIndexOf('.');

        if (dot < 0 || dot == queuePath.length() - 1) {
            return null;
        }

        try {
            return Integer.parseInt(queuePath.substring(dot + 1));
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static double rate(long value, double seconds) {
        return seconds <= 0 ? 0 : value / seconds;
    }

    private static String number(long value) {
        return String.format(Locale.ROOT, "%,d", value);
    }

    private static String decimal(double value) {
        return String.format(Locale.ROOT, "%,.2f", value);
    }

    private static String percent(long value, long total) {
        return total <= 0 ? "n/a" : String.format(Locale.ROOT, "%.1f%%", value * 100.0 / total);
    }
}

