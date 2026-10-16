package com.volo.broker.simulation.core;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public final class AsyncPublishingEngine implements AutoCloseable {
    private final Supplier<SimulatedPublisher> publisherFactory;
    private final MessagePayloadFactory payloadFactory;
    private final PublishMetrics metrics;
    private final PublishingOptions options;
    private final ExecutorService lifecycleExecutor;

    public AsyncPublishingEngine(
            Supplier<SimulatedPublisher> publisherFactory,
            MessagePayloadFactory payloadFactory,
            PublishMetrics metrics,
            PublishingOptions options
    ) {
        this.publisherFactory = publisherFactory;
        this.payloadFactory = payloadFactory;
        this.metrics = metrics;
        this.options = options;
        this.lifecycleExecutor = Executors.newFixedThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors()),
                runnable -> {
                    Thread t = new Thread(runnable, "simulation-publisher-lifecycle");
                    t.setDaemon(true);
                    return t;
                }
        );
    }

    public CompletableFuture<Void> publishAll(List<PublishTask> tasks) {
        CompletableFuture<Void> all = new CompletableFuture<>();

        if (tasks.isEmpty()) {
            all.complete(null);
            return all;
        }

        Queue<PublishTask> pending = new ConcurrentLinkedQueue<>(tasks);
        AtomicInteger remaining = new AtomicInteger(tasks.size());
        int slots = Math.min(options.maxConcurrentPublishers(), tasks.size());

        for (int i = 0; i < slots; i++) {
            launchNext(pending, remaining, all);
        }

        return all;
    }

    private void launchNext(
            Queue<PublishTask> pending,
            AtomicInteger remaining,
            CompletableFuture<Void> all
    ) {
        PublishTask task = pending.poll();

        if (task == null) {
            return;
        }

        CompletableFuture
                .supplyAsync(() -> runTask(task), lifecycleExecutor)
                .thenCompose(future -> future)
                .whenComplete((ignored, throwable) -> {
                    if (remaining.decrementAndGet() == 0) {
                        all.complete(null);
                        return;
                    }

                    launchNext(pending, remaining, all);
                });
    }

    private CompletableFuture<Void> runTask(PublishTask task) {
        SimulatedPublisher publisher;

        try {
            publisher = publisherFactory.get();
        } catch (Throwable throwable) {
            for (int i = 0; i < task.messageCount(); i++) {
                metrics.publishFailed(task.queueName());
            }

            return CompletableFuture.completedFuture(null);
        }

        return new AsyncPublisherSession(
                task,
                publisher,
                payloadFactory,
                metrics,
                options.maxInFlightPerPublisher()
        ).start().handleAsync((ignored, throwable) -> {
            publisher.close();
            return null;
        }, lifecycleExecutor);
    }

    @Override
    public void close() {
        lifecycleExecutor.shutdownNow();
    }
}

