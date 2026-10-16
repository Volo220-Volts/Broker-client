package com.volo.broker.simulation.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.volo.broker.simulation.config.SimulationEnvironmentConfig;
import com.volo.broker.simulation.journal.SimulationJournal;

public final class BrokerSimulation {
    private final SimulationConfig config;
    private final BrokerSimulationAdapter adapter;
    private final SimulationEnvironmentConfig envConfig;
    private final String simulationName;
    private final SimulationStats stats = new SimulationStats();

    public BrokerSimulation(SimulationConfig config, BrokerSimulationAdapter adapter) {
        this(config, adapter, null, null);
    }

    public BrokerSimulation(SimulationConfig config, BrokerSimulationAdapter adapter, String simulationName) {
        this(config, adapter, null, simulationName);
    }

    public BrokerSimulation(SimulationConfig config, SimulationEnvironmentConfig envConfig) {
        this(config, Objects.requireNonNull(envConfig, "envConfig is required").createAdapter(), envConfig, null);
    }

    public BrokerSimulation(SimulationConfig config, SimulationEnvironmentConfig envConfig, String simulationName) {
        this(config, Objects.requireNonNull(envConfig, "envConfig is required").createAdapter(), envConfig, simulationName);
    }

    public BrokerSimulation(
            SimulationConfig config,
            BrokerSimulationAdapter adapter,
            SimulationEnvironmentConfig envConfig
    ) {
        this(config, adapter, envConfig, null);
    }

    public BrokerSimulation(
            SimulationConfig config,
            BrokerSimulationAdapter adapter,
            SimulationEnvironmentConfig envConfig,
            String simulationName
    ) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.adapter = Objects.requireNonNull(adapter, "adapter is required");
        this.envConfig = envConfig;
        this.simulationName = resolveSimulationName(simulationName, config);
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

    public SimulationStats run() {
        List<AutoCloseable> consumers = new ArrayList<>();
        boolean hasConsumers = config.queues().stream().anyMatch(q -> q.consumers() > 0);
        com.volo.broker.simulation.report.SimulationProgressBar progressBar = config.showProgressBar()
                ? new com.volo.broker.simulation.report.SimulationProgressBar(stats, config.expectedMessages(), hasConsumers)
                : null;
        SimulationJournal journal = config.enableJournal()
                ? new SimulationJournal(config, envConfig, stats, simulationName)
                : null;

        try (adapter;
             AsyncPublishingEngine engine = createPublishingEngine();
             progressBar;
             journal) {

            adapter.initialize();
            adapter.declareQueues(config.queues());

            startConsumers(consumers);

            sleep(config.consumerStartupDelay());

            // Benchmark timing strictly measures message transfer (publish to consume completion)
            stats.markStarted();

            if (progressBar != null) {
                progressBar.start();
            }
            if (journal != null) {
                journal.start();
            }

            engine.publishAll(createTasks()).join();
            boolean completed = awaitConsumption();
            stats.markFinished();

            if (journal != null) {
                journal.finish(completed);
            }

            return stats;
        } finally {
            if (progressBar != null) {
                progressBar.finish();
            }
            stats.markFinished();
            closeConsumers(consumers);
        }
    }

    private AsyncPublishingEngine createPublishingEngine() {
        return new AsyncPublishingEngine(
                adapter::createPublisher,
                new MessagePayloadFactory(config.payloadSizeBytes()),
                stats,
                config.publishingOptions()
        );
    }

    private List<PublishTask> createTasks() {
        List<PublishTask> tasks = new ArrayList<>();
        for (QueueTarget target : config.queues()) {
            for (int publisherIndex = 0; publisherIndex < target.publishers(); publisherIndex++) {
                tasks.add(new PublishTask(
                        target.queueName(),
                        publisherIndex,
                        target.messagesPerPublisher()
                ));
            }
        }
        return tasks;
    }

    private void startConsumers(List<AutoCloseable> consumers) {
        for (QueueTarget target : config.queues()) {
            for (int i = 0; i < target.consumers(); i++) {
                AutoCloseable consumer = adapter.startConsumer(
                        target.queueName(),
                        target.consumerPrefetch(),
                        (latencyNs, payloadBytes) -> stats.consumed(target.queueName(), latencyNs, payloadBytes)
                );
                consumers.add(consumer);
            }
        }
    }

    private boolean awaitConsumption() {
        long expectedMessages = config.expectedMessages();
        long deadlineNs = System.nanoTime() + config.consumptionTimeout().toNanos();

        while (System.nanoTime() < deadlineNs) {
            if (stats.consumedExpectedMessages(expectedMessages)) {
                return true;
            }
            sleep(Duration.ofMillis(100));
        }
        return stats.consumedExpectedMessages(expectedMessages);
    }

    private void closeConsumers(List<AutoCloseable> consumers) {
        for (AutoCloseable consumer : consumers) {
            try {
                consumer.close();
            } catch (Throwable ignored) {
            }
        }
    }

    private void sleep(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            return;
        }
        try {
            TimeUnit.MILLISECONDS.sleep(duration.toMillis());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    public SimulationStats stats() {
        return stats;
    }
}

