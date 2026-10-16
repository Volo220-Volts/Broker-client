package com.volo.broker.simulation.journal;

import com.volo.broker.simulation.config.BrokerType;
import com.volo.broker.simulation.config.SimulationEnvironmentConfig;
import com.volo.broker.simulation.core.QueueTarget;
import com.volo.broker.simulation.core.SimulationConfig;
import com.volo.broker.simulation.core.SimulationStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SimulationJournalTest {

    @Test
    void shouldRecordAndSaveCompleteJournal(@TempDir Path tempDir) throws IOException {
        SimulationConfig config = SimulationConfig.builder()
                .addQueue(QueueTarget.builder()
                        .queueName("test.queue")
                        .publishers(2)
                        .consumers(2)
                        .messagesPerPublisher(50)
                        .consumerPrefetch(20)
                        .durable(false)
                        .build())
                .payloadSizeBytes(128)
                .journalDirectory(tempDir.toString())
                .enableJournal(true)
                .build();

        SimulationEnvironmentConfig envConfig = SimulationEnvironmentConfig.builder()
                .activeBroker(BrokerType.CUSTOM)
                .build();

        SimulationStats stats = new SimulationStats();
        stats.markStarted();

        SimulationJournal journal = new SimulationJournal(config, envConfig, stats);
        journal.start();

        // Simulate activity
        stats.published("test.queue", 2_000_000, 128);
        stats.consumed("test.queue", 4_000_000, 128);
        stats.markFinished();

        journal.finish(true);

        Path runDir = journal.runDirectoryPath();

        journal.close();
        assertThat(runDir).isNotNull();
        assertThat(runDir).isDirectory();
        String simId = runDir.getFileName().toString();
        assertThat(simId).startsWith("sim_");

        // Verify config.json
        Path configFile = runDir.resolve("config.json");
        assertThat(configFile).exists();
        String configContent = Files.readString(configFile);
        assertThat(configContent).contains("\"simulationId\": \"" + simId + "\"");
        assertThat(configContent).contains("\"test.queue\"");

        // Verify timeline.csv
        Path csvFile = runDir.resolve("timeline.csv");
        assertThat(csvFile).exists();
        String csvContent = Files.readString(csvFile);
        assertThat(csvContent).contains("second,timestamp");

        // Verify summary.txt
        Path summaryFile = runDir.resolve("summary.txt");
        assertThat(summaryFile).exists();
        String summaryContent = Files.readString(summaryFile);
        assertThat(summaryContent).contains("BROKER SIMULATION REPORT");
        assertThat(summaryContent).contains("test.queue");
        assertThat(summaryContent).doesNotContain("avg 0.0 msg/s");

        // Verify journal.json
        Path journalFile = runDir.resolve("journal.json");
        assertThat(journalFile).exists();
        String journalContent = Files.readString(journalFile);
        assertThat(journalContent).contains("\"metadata\"");
        assertThat(journalContent).contains("\"timeline\"");
        assertThat(journalContent).contains("\"summary\"");
    }

    @Test
    void shouldRecordCustomNamedSimulation(@TempDir Path tempDir) {
        SimulationConfig config = SimulationConfig.builder()
                .addQueue(QueueTarget.builder()
                        .queueName("orders.queue")
                        .publishers(1)
                        .consumers(1)
                        .messagesPerPublisher(10)
                        .build())
                .journalDirectory(tempDir.toString())
                .enableJournal(true)
                .build();

        SimulationEnvironmentConfig envConfig = SimulationEnvironmentConfig.builder()
                .activeBroker(BrokerType.CUSTOM)
                .build();

        SimulationStats stats = new SimulationStats();
        stats.markStarted();

        SimulationJournal journal = new SimulationJournal(config, envConfig, stats, "S1_PingPong");
        journal.start();

        stats.published("orders.queue", 1_000_000, 256);
        stats.consumed("orders.queue", 2_000_000, 256);
        stats.markFinished();

        journal.finish(true);

        Path runDir = journal.runDirectoryPath();
        journal.close();
        assertThat(runDir).isNotNull();
        assertThat(runDir.getFileName().toString()).isEqualTo("S1_PingPong");
        assertThat(runDir.resolve("summary.txt")).exists();
        assertThat(runDir.resolve("journal.json")).exists();
    }
}

