package com.volo.broker.simulation.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.volo.broker.simulation.journal.storage.JournalNamingStrategy;

public record SimulationConfig(
        List<QueueTarget> queues,
        int payloadSizeBytes,
        PublishingOptions publishingOptions,
        Duration consumerStartupDelay,
        Duration consumptionTimeout,
        boolean showProgressBar,
        boolean enableJournal,
        String journalDirectory,
        JournalNamingStrategy journalNamingStrategy,
        String simulationName
) {
    public SimulationConfig {
        if (queues == null || queues.isEmpty()) {
            throw new IllegalArgumentException("at least one queue target is required");
        }
        queues = List.copyOf(queues);

        if (payloadSizeBytes <= 0) {
            throw new IllegalArgumentException("payloadSizeBytes must be greater than zero");
        }

        publishingOptions = publishingOptions == null
                ? PublishingOptions.defaults()
                : publishingOptions;

        consumerStartupDelay = consumerStartupDelay == null
                ? Duration.ofSeconds(1)
                : consumerStartupDelay;

        consumptionTimeout = consumptionTimeout == null
                ? Duration.ofSeconds(60)
                : consumptionTimeout;

        if (consumerStartupDelay.isNegative()) {
            throw new IllegalArgumentException("consumerStartupDelay must not be negative");
        }

        if (consumptionTimeout.isZero() || consumptionTimeout.isNegative()) {
            throw new IllegalArgumentException("consumptionTimeout must be positive");
        }

        journalDirectory = (journalDirectory == null || journalDirectory.isBlank())
                ? "simulation"
                : journalDirectory;

        journalNamingStrategy = journalNamingStrategy == null
                ? JournalNamingStrategy.TIMESTAMP
                : journalNamingStrategy;
    }

    public SimulationConfig(
            List<QueueTarget> queues,
            int payloadSizeBytes,
            PublishingOptions publishingOptions,
            Duration consumerStartupDelay,
            Duration consumptionTimeout
    ) {
        this(queues, payloadSizeBytes, publishingOptions, consumerStartupDelay, consumptionTimeout, true, true, "simulation", JournalNamingStrategy.TIMESTAMP);
    }

    public SimulationConfig(
            List<QueueTarget> queues,
            int payloadSizeBytes,
            PublishingOptions publishingOptions,
            Duration consumerStartupDelay,
            Duration consumptionTimeout,
            boolean showProgressBar
    ) {
        this(queues, payloadSizeBytes, publishingOptions, consumerStartupDelay, consumptionTimeout, showProgressBar, true, "simulation", JournalNamingStrategy.TIMESTAMP);
    }

    public SimulationConfig(
            List<QueueTarget> queues,
            int payloadSizeBytes,
            PublishingOptions publishingOptions,
            Duration consumerStartupDelay,
            Duration consumptionTimeout,
            boolean showProgressBar,
            boolean enableJournal,
            String journalDirectory,
            JournalNamingStrategy journalNamingStrategy
    ) {
        this(queues, payloadSizeBytes, publishingOptions, consumerStartupDelay, consumptionTimeout, showProgressBar, enableJournal, journalDirectory, journalNamingStrategy, null);
    }

    public long expectedMessages() {
        return queues.stream().mapToLong(QueueTarget::expectedMessages).sum();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<QueueTarget> queues = new ArrayList<>();
        private int payloadSizeBytes = 200;
        private PublishingOptions publishingOptions = PublishingOptions.defaults();
        private Duration consumerStartupDelay = Duration.ofSeconds(1);
        private Duration consumptionTimeout = Duration.ofSeconds(60);
        private boolean showProgressBar = true;
        private boolean enableJournal = true;
        private String journalDirectory = "simulation";
        private JournalNamingStrategy journalNamingStrategy = JournalNamingStrategy.TIMESTAMP;
        private String simulationName = null;

        public Builder addQueue(QueueTarget queue) {
            this.queues.add(Objects.requireNonNull(queue, "queue is required"));
            return this;
        }

        public Builder queues(List<QueueTarget> queues) {
            this.queues.clear();
            if (queues != null) {
                this.queues.addAll(queues);
            }
            return this;
        }

        public Builder payloadSizeBytes(int payloadSizeBytes) {
            this.payloadSizeBytes = payloadSizeBytes;
            return this;
        }

        public Builder publishingOptions(PublishingOptions publishingOptions) {
            this.publishingOptions = publishingOptions;
            return this;
        }

        public Builder consumerStartupDelay(Duration consumerStartupDelay) {
            this.consumerStartupDelay = consumerStartupDelay;
            return this;
        }

        public Builder consumptionTimeout(Duration consumptionTimeout) {
            this.consumptionTimeout = consumptionTimeout;
            return this;
        }

        public Builder showProgressBar(boolean showProgressBar) {
            this.showProgressBar = showProgressBar;
            return this;
        }

        public Builder enableJournal(boolean enableJournal) {
            this.enableJournal = enableJournal;
            return this;
        }

        public Builder journalDirectory(String journalDirectory) {
            this.journalDirectory = journalDirectory;
            return this;
        }

        public Builder journalNamingStrategy(JournalNamingStrategy journalNamingStrategy) {
            this.journalNamingStrategy = journalNamingStrategy;
            return this;
        }

        public Builder simulationName(String simulationName) {
            this.simulationName = simulationName;
            return this;
        }

        public SimulationConfig build() {
            return new SimulationConfig(
                    queues,
                    payloadSizeBytes,
                    publishingOptions,
                    consumerStartupDelay,
                    consumptionTimeout,
                    showProgressBar,
                    enableJournal,
                    journalDirectory,
                    journalNamingStrategy,
                    simulationName
            );
        }
    }
}

