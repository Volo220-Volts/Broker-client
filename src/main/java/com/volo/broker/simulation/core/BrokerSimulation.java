package com.volo.broker.simulation.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class BrokerSimulation {
    private final SimulationConfig config;
    private final BrokerSimulationAdapter adapter;
    private final SimulationStats stats = new SimulationStats();

    public BrokerSimulation(SimulationConfig config, BrokerSimulationAdapter adapter) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.adapter = Objects.requireNonNull(adapter, "adapter is required");
    }

    public SimulationStats run() {
        stats.markStarted();

        List<AutoCloseable> consumers = new ArrayList<>();

        try (adapter;
             AsyncPublishingEngine engine = createPublishingEngine()) {

            adapter.initialize();
            adapter.declareQueues(config.queues());

            startConsumers(consumers);

            sleep(config.consumerStartupDelay());

            engine.publishAll(createTasks()).join();
            awaitConsumption();

            return stats;
        } finally {
            closeConsumers(consumers);
            stats.markFinished();
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

    private void awaitConsumption() {
        long expectedMessages = config.expectedMessages();
        long deadlineNs = System.nanoTime() + config.consumptionTimeout().toNanos();

        while (System.nanoTime() < deadlineNs) {
            if (stats.consumedExpectedMessages(expectedMessages)) {
                return;
            }
            sleep(Duration.ofMillis(100));
        }
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

