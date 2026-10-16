package com.volo.broker.client.consumer;

import java.util.concurrent.CompletableFuture;

/**
 * Public abstraction for consuming messages from the broker.
 * Implements {@link AutoCloseable} for resource management.
 */
public interface MessageConsumer extends AutoCloseable {

    /**
     * Starts consuming messages from the configured queue.
     */
    void start();

    /**
     * Checks if the consumer is closed.
     *
     * @return {@code true} if closed, {@code false} otherwise
     */
    boolean isClosed();

    /**
     * Returns a {@link CompletableFuture} that completes when this consumer has finished closing.
     *
     * @return future that completes on closure
     */
    CompletableFuture<Void> closedFuture();

    /**
     * Closes the consumer and unsubscribes from the queue.
     */
    @Override
    void close();
}

