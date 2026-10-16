package com.volo.broker.simulation.journal.storage;

import com.volo.broker.simulation.config.BrokerType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JournalStorageManagerTest {

    @Test
    void shouldAllocateTimestampRunDirectoriesByDefault(@TempDir Path tempDir) {
        JournalStorageManager manager = new JournalStorageManager(tempDir);

        JournalStorageManager.RunDirectory run1 = manager.allocateNextRunDirectory(BrokerType.CUSTOM);
        assertThat(run1.simulationId()).startsWith("sim_");
        assertThat(run1.directory()).isDirectory();
        assertThat(run1.directory()).hasParent(tempDir.resolve("custom"));

        // If another run is allocated in the same second, it shouldn't overwrite but add suffix
        JournalStorageManager.RunDirectory run2 = manager.allocateNextRunDirectory(BrokerType.CUSTOM);
        assertThat(run2.directory()).isDirectory();
        assertThat(run2.directory()).isNotEqualTo(run1.directory());
    }

    @Test
    void shouldIncrementallyAllocateSequentialRunDirectoriesWhenConfigured(@TempDir Path tempDir) throws IOException {
        JournalStorageManager manager = new JournalStorageManager(tempDir, JournalNamingStrategy.SEQUENTIAL);

        JournalStorageManager.RunDirectory run1 = manager.allocateNextRunDirectory(BrokerType.CUSTOM);
        assertThat(run1.simulationId()).isEqualTo("sim.1");
        assertThat(run1.directory()).isDirectory();
        assertThat(run1.directory()).hasParent(tempDir.resolve("custom"));

        // Also test detecting existing sim2 without dot
        Files.createDirectories(tempDir.resolve("custom").resolve("sim2"));

        JournalStorageManager.RunDirectory run3 = manager.allocateNextRunDirectory(BrokerType.CUSTOM);
        assertThat(run3.simulationId()).isEqualTo("sim.3");
        assertThat(run3.directory()).isDirectory();
    }

    @Test
    void shouldListExistingSimulationRuns(@TempDir Path tempDir) throws IOException {
        JournalStorageManager manager = new JournalStorageManager(tempDir);

        Path rabbitDir = tempDir.resolve("rabbitmq");
        Path runDir = rabbitDir.resolve("sim_2026-10-14_12-00-00");
        Files.createDirectories(runDir);
        Files.writeString(runDir.resolve("journal.json"), "{}");
        Files.writeString(runDir.resolve("summary.txt"), "summary");

        List<SimulationRunInfo> runs = manager.listExistingRuns(BrokerType.RABBITMQ);
        assertThat(runs).hasSize(1);
        SimulationRunInfo info = runs.get(0);
        assertThat(info.runId()).isEqualTo("sim_2026-10-14_12-00-00");
        assertThat(info.brokerType()).isEqualTo(BrokerType.RABBITMQ);
        assertThat(info.hasJournal()).isTrue();
        assertThat(info.hasSummary()).isTrue();
        assertThat(info.hasTimeline()).isFalse();
    }

    @Test
    void shouldAllocateCustomRunDirectoriesWithCollisionProtection(@TempDir Path tempDir) throws IOException {
        JournalStorageManager manager = new JournalStorageManager(tempDir);

        JournalStorageManager.RunDirectory run1 = manager.allocateCustomRunDirectory(BrokerType.RABBITMQ, "S1_PingPong");
        assertThat(run1.simulationId()).isEqualTo("S1_PingPong");
        assertThat(run1.directory()).isDirectory();
        assertThat(run1.directory()).hasParent(tempDir.resolve("rabbitmq"));

        // Second allocation with same name should append _2
        JournalStorageManager.RunDirectory run2 = manager.allocateCustomRunDirectory(BrokerType.RABBITMQ, "S1_PingPong");
        assertThat(run2.simulationId()).isEqualTo("S1_PingPong_2");
        assertThat(run2.directory()).isDirectory();

        // Write marker file and check listExistingRuns finds custom-named directory
        Files.writeString(run1.directory().resolve("summary.txt"), "report");
        List<SimulationRunInfo> runs = manager.listExistingRuns(BrokerType.RABBITMQ);
        assertThat(runs).extracting(SimulationRunInfo::runId).contains("S1_PingPong");
    }
}
