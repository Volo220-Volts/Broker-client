package com.volo.broker.simulation.journal;

import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.simulation.adapter.rabbitmq.RabbitMqConfig;
import com.volo.broker.simulation.config.BrokerType;
import com.volo.broker.simulation.config.SimulationEnvironmentConfig;
import com.volo.broker.simulation.core.QueueTarget;
import com.volo.broker.simulation.core.SimulationConfig;
import com.volo.broker.simulation.core.SimulationStats;
import com.volo.broker.simulation.journal.diagnostics.SimulationDiagnostics;
import com.volo.broker.simulation.journal.model.QueueConfigSnapshot;
import com.volo.broker.simulation.journal.model.SecondSnapshot;
import com.volo.broker.simulation.journal.model.SimulationRunMetadata;
import com.volo.broker.simulation.journal.model.SimulationSummaryReport;
import com.volo.broker.simulation.journal.recorder.SecondSnapshotRecorder;
import com.volo.broker.simulation.journal.storage.CsvTimelineWriter;
import com.volo.broker.simulation.journal.storage.JournalStorageManager;
import com.volo.broker.simulation.journal.storage.SimpleJsonWriter;
import com.volo.broker.simulation.journal.storage.SummaryTextWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

public final class SimulationJournal implements AutoCloseable {
    private final SimulationConfig config;
    private final SimulationEnvironmentConfig envConfig;
    private final SimulationStats stats;
    private final JournalStorageManager storageManager;
    private final String customSimulationName;

    private JournalStorageManager.RunDirectory runDirectory;
    private SimulationRunMetadata metadata;
    private SecondSnapshotRecorder recorder;
    private boolean finished = false;

    public SimulationJournal(
            SimulationConfig config,
            SimulationEnvironmentConfig envConfig,
            SimulationStats stats
    ) {
        this(config, envConfig, stats, null);
    }

    public SimulationJournal(
            SimulationConfig config,
            SimulationEnvironmentConfig envConfig,
            SimulationStats stats,
            String customSimulationName
    ) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.envConfig = envConfig;
        this.stats = Objects.requireNonNull(stats, "stats is required");
        this.customSimulationName = resolveSimulationName(customSimulationName, config);
        this.storageManager = new JournalStorageManager(config.journalDirectory(), config.journalNamingStrategy());
    }

    private static String resolveSimulationName(String constructorName, SimulationConfig config) {
        if (constructorName != null && !constructorName.isBlank()) {
            return constructorName.trim();
        }
        if (config != null && config.simulationName() != null && !config.simulationName().isBlank()) {
            return config.simulationName().trim();
        }
        return null;
    }

    public void start() {
        BrokerType brokerType = envConfig != null ? envConfig.activeBroker() : BrokerType.CUSTOM;
        if (customSimulationName != null && !customSimulationName.isBlank()) {
            this.runDirectory = storageManager.allocateCustomRunDirectory(brokerType, customSimulationName);
        } else {
            this.runDirectory = storageManager.allocateNextRunDirectory(brokerType);
        }
        this.metadata = buildMetadata(runDirectory.simulationId(), brokerType);

        this.recorder = new SecondSnapshotRecorder(
                stats,
                config.expectedMessages(),
                config.queues()
        );
        this.recorder.start();
    }

    public void finish(boolean completed) {
        if (finished || recorder == null) {
            return;
        }
        finished = true;

        recorder.stop();
        List<SecondSnapshot> snapshots = recorder.snapshots();

        Instant finishedAt = Instant.now();
        double durationSeconds = stats.duration().toNanos() / 1_000_000_000.0;
        if (durationSeconds <= 0 && !snapshots.isEmpty()) {
            durationSeconds = snapshots.getLast().elapsedSeconds();
        }

        double peakPublishRate = snapshots.stream()
                .mapToDouble(SecondSnapshot::publishedDelta)
                .max()
                .orElse(0.0);

        double peakConsumeRate = snapshots.stream()
                .mapToDouble(SecondSnapshot::consumedDelta)
                .max()
                .orElse(0.0);

        SimulationDiagnostics.AnalysisResult diagnostics = SimulationDiagnostics.analyze(
                stats,
                snapshots,
                config.expectedMessages()
        );

        String status = completed ? "COMPLETED" : "TIMEOUT";
        if (stats.publishFailedCount() > 0 || stats.consumeFailedCount() > 0) {
            status = "FAILED_WITH_ERRORS";
        }

        double avgPublishRate = durationSeconds > 0
                ? stats.publishedCount() / durationSeconds
                : stats.publishThroughputPerSecond();
        double avgConsumeRate = durationSeconds > 0
                ? stats.consumedCount() / durationSeconds
                : stats.consumeThroughputPerSecond();
        double avgPublishMBs = durationSeconds > 0
                ? (stats.publishedBytes() / (1024.0 * 1024.0)) / durationSeconds
                : (stats.publishBytesPerSecond() / (1024.0 * 1024.0));
        double avgConsumeMBs = durationSeconds > 0
                ? (stats.consumedBytes() / (1024.0 * 1024.0)) / durationSeconds
                : (stats.consumeBytesPerSecond() / (1024.0 * 1024.0));

        SimulationSummaryReport summary = new SimulationSummaryReport(
                runDirectory.simulationId(),
                status,
                finishedAt,
                durationSeconds,
                config.expectedMessages(),
                stats.publishedCount(),
                stats.consumedCount(),
                stats.publishFailedCount(),
                stats.consumeFailedCount(),
                stats.publishedBytes(),
                stats.consumedBytes(),
                avgPublishRate,
                peakPublishRate,
                avgConsumeRate,
                peakConsumeRate,
                avgPublishMBs,
                avgConsumeMBs,
                stats.averagePublishAckLatencyMs(),
                stats.maxPublishAckLatencyMs(),
                stats.averageEndToEndLatencyMs(),
                stats.maxEndToEndLatencyMs(),
                diagnostics.findings(),
                diagnostics.healthSummary()
        );

        saveToDisk(snapshots, summary);
    }

    private void saveToDisk(List<SecondSnapshot> snapshots, SimulationSummaryReport summary) {
        Path dir = runDirectory.directory();

        try {
            // 1. config.json
            Path configFile = dir.resolve("config.json");
            Files.writeString(configFile, SimpleJsonWriter.toJson(metadata), StandardCharsets.UTF_8);

            // 2. timeline.csv
            Path csvFile = dir.resolve("timeline.csv");
            CsvTimelineWriter.writeToFile(csvFile, snapshots);

            // 3. summary.txt
            Path summaryFile = dir.resolve("summary.txt");
            SummaryTextWriter.writeToFile(summaryFile, metadata, summary);

            // 4. journal.json (All-in-one unified document)
            Map<String, Object> unifiedJournal = new LinkedHashMap<>();
            unifiedJournal.put("metadata", metadata);
            unifiedJournal.put("timeline", snapshots);
            unifiedJournal.put("summary", summary);

            Path journalFile = dir.resolve("journal.json");
            Files.writeString(journalFile, SimpleJsonWriter.toJson(unifiedJournal), StandardCharsets.UTF_8);

            System.out.printf("[Journal] Simulation data saved to: %s\n", dir.toAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write simulation journal files to " + dir, e);
        }
    }

    private SimulationRunMetadata buildMetadata(String simId, BrokerType brokerType) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("osName", System.getProperty("os.name"));
        env.put("osVersion", System.getProperty("os.version"));
        env.put("osArch", System.getProperty("os.arch"));
        env.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        env.put("javaVersion", System.getProperty("java.version"));
        env.put("jvmName", System.getProperty("java.vm.name"));
        env.put("maxHeapBytes", Runtime.getRuntime().maxMemory());

        Map<String, Object> brokerDetails = new LinkedHashMap<>();
        brokerDetails.put("brokerType", brokerType.name());

        if (envConfig != null) {
            if (brokerType == BrokerType.CUSTOM && envConfig.customBrokerConfig() != null) {
                BrokerClientConfig c = envConfig.customBrokerConfig();
                brokerDetails.put("host", c.host());
                brokerDetails.put("port", c.port());
                brokerDetails.put("plaintext", c.plaintext());
                brokerDetails.put("keepAliveIntervalMs", c.keepAliveInterval().toMillis());
                brokerDetails.put("publishResponseTimeoutMs", c.publishResponseTimeout().toMillis());
                brokerDetails.put("shutdownTimeoutMs", c.shutdownTimeout().toMillis());
                brokerDetails.put("maxInboundMessageSizeBytes", c.maxInboundMessageSizeBytes());
                brokerDetails.put("keepAliveThreads", c.keepAliveThreads());
                brokerDetails.put("handlerThreads", c.handlerThreads());
                brokerDetails.put("reconnectThreads", c.reconnectThreads());
            } else if (brokerType == BrokerType.RABBITMQ && envConfig.rabbitMqConfig() != null) {
                RabbitMqConfig r = envConfig.rabbitMqConfig();
                brokerDetails.put("host", r.host());
                brokerDetails.put("port", r.port());
                brokerDetails.put("virtualHost", r.virtualHost());
                brokerDetails.put("username", r.username());
                brokerDetails.put("automaticRecovery", true);
            }
        }

        Map<String, Object> simParams = new LinkedHashMap<>();
        simParams.put("payloadSizeBytes", config.payloadSizeBytes());
        simParams.put("totalExpectedMessages", config.expectedMessages());
        simParams.put("totalExpectedBytes", config.expectedMessages() * (long) config.payloadSizeBytes());
        simParams.put("consumerStartupDelayMs", config.consumerStartupDelay().toMillis());
        simParams.put("consumptionTimeoutMs", config.consumptionTimeout().toMillis());
        simParams.put("maxConcurrentPublishers", config.publishingOptions().maxConcurrentPublishers());
        simParams.put("maxInFlightPerPublisher", config.publishingOptions().maxInFlightPerPublisher());

        List<QueueConfigSnapshot> queues = new ArrayList<>();
        List<QueueTarget> targets = config.queues();
        for (int i = 0; i < targets.size(); i++) {
            QueueTarget target = targets.get(i);
            queues.add(new QueueConfigSnapshot(
                    i,
                    target.queueName(),
                    target.publishers(),
                    target.consumers(),
                    target.messagesPerPublisher(),
                    target.expectedMessages(),
                    target.expectedMessages() * (long) config.payloadSizeBytes(),
                    target.consumerPrefetch(),
                    target.durable(),
                    target.durable() ? "PERSISTENT" : "MEMORY_FAST",
                    target.waitForPublishAck(),
                    target.waitForConsumerAck()
            ));
        }

        return new SimulationRunMetadata(
                simId,
                brokerType,
                Instant.now(),
                env,
                brokerDetails,
                simParams,
                queues
        );
    }

    public Path runDirectoryPath() {
        return runDirectory != null ? runDirectory.directory() : null;
    }

    @Override
    public void close() {
        if (!finished) {
            finish(false);
        }
        if (recorder != null) {
            recorder.close();
        }
    }
}

