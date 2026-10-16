package com.volo.broker.simulation.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

final class AsyncPublisherSession {
    private final PublishTask task;
    private final SimulatedPublisher publisher;
    private final MessagePayloadFactory payloadFactory;
    private final PublishMetrics metrics;
    private final int maxInFlight;

    private final AtomicInteger nextMessageIndex = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger completed = new AtomicInteger();
    private final AtomicInteger pumpRequests = new AtomicInteger();
    private final CompletableFuture<Void> done = new CompletableFuture<>();

    AsyncPublisherSession(
            PublishTask task,
            SimulatedPublisher publisher,
            MessagePayloadFactory payloadFactory,
            PublishMetrics metrics,
            int maxInFlight
    ) {
        this.task = task;
        this.publisher = publisher;
        this.payloadFactory = payloadFactory;
        this.metrics = metrics;
        this.maxInFlight = maxInFlight;
    }

    CompletableFuture<Void> start() {
        if (task.messageCount() <= 0) {
            done.complete(null);
            return done;
        }

        pump();

        return done;
    }

    private void pump() {
        if (pumpRequests.getAndIncrement() != 0) {
            return;
        }

        do {
            fillWindow();
        } while (pumpRequests.decrementAndGet() != 0);
    }

    private void fillWindow() {
        while (tryReserveSlot()) {
            int messageIndex = nextMessageIndex.getAndIncrement();

            if (messageIndex >= task.messageCount()) {
                inFlight.decrementAndGet();
                return;
            }

            send(messageIndex);
        }
    }

    private boolean tryReserveSlot() {
        int current = inFlight.get();

        while (current < maxInFlight) {
            if (inFlight.compareAndSet(current, current + 1)) {
                return true;
            }

            current = inFlight.get();
        }

        return false;
    }

    private void send(int messageIndex) {
        long startedAtNs = System.nanoTime();
        byte[] payload = payloadFactory.createPayload(task.queueName(), task.publisherIndex(), messageIndex);
        int payloadBytes = payload.length;

        PublishMessage message = new PublishMessage(
                task.queueName(),
                payload,
                startedAtNs,
                task.publisherIndex(),
                messageIndex
        );

        CompletableFuture<Void> result;

        try {
            result = publisher.publish(message);
        } catch (Throwable throwable) {
            result = CompletableFuture.failedFuture(throwable);
        }

        result.whenComplete((receipt, throwable) -> onComplete(startedAtNs, payloadBytes, throwable));
    }

    private void onComplete(long startedAtNs, int payloadBytes, Throwable throwable) {
        if (throwable == null) {
            metrics.published(task.queueName(), System.nanoTime() - startedAtNs, payloadBytes);
        } else {
            metrics.publishFailed(task.queueName());
        }

        inFlight.decrementAndGet();

        if (completed.incrementAndGet() >= task.messageCount()) {
            done.complete(null);
            return;
        }

        pump();
    }
}

