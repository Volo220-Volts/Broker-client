package com.volo.broker.simulation.journal.storage;

import com.volo.broker.simulation.journal.model.SecondSnapshot;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

public final class CsvTimelineWriter {
    private static final String HEADER = "second,timestamp,elapsed_s,published_msg_s,consumed_msg_s," +
            "published_mb_s,consumed_mb_s,net_balance,cum_published,cum_consumed,backlog,completion_pct," +
            "ack_avg_ms,ack_max_ms,e2e_avg_ms,e2e_max_ms,heap_used_mb,heap_usage_pct,live_threads,cpu_load_pct";

    private CsvTimelineWriter() {
    }

    public static void writeToFile(Path filePath, List<SecondSnapshot> snapshots) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8)) {
            writer.write(HEADER);
            writer.newLine();

            for (SecondSnapshot s : snapshots) {
                double pubMb = s.publishedBytesDelta() / (1024.0 * 1024.0);
                double consMb = s.consumedBytesDelta() / (1024.0 * 1024.0);
                double heapMb = s.heapUsedBytes() / (1024.0 * 1024.0);

                String line = String.format(Locale.ROOT,
                        "%d,%s,%.2f,%d,%d,%.2f,%.2f,%d,%d,%d,%d,%.1f,%.2f,%.2f,%.2f,%.2f,%.1f,%.1f,%d,%.1f",
                        s.second(),
                        s.timestamp().toString(),
                        s.elapsedSeconds(),
                        s.publishedDelta(),
                        s.consumedDelta(),
                        pubMb,
                        consMb,
                        s.throughputBalance(),
                        s.cumulativePublished(),
                        s.cumulativeConsumed(),
                        s.backlog(),
                        s.completionPercent(),
                        s.windowPublishAckAvgMs(),
                        s.windowPublishAckMaxMs(),
                        s.windowEndToEndAvgMs(),
                        s.windowEndToEndMaxMs(),
                        heapMb,
                        s.heapUsagePercent(),
                        s.liveThreads(),
                        s.cpuLoadPercent()
                );
                writer.write(line);
                writer.newLine();
            }
        }
    }
}

