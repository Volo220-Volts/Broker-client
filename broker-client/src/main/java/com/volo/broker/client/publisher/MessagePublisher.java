package com.volo.broker.client.publisher;

import java.util.concurrent.CompletableFuture;

/**
 * Public abstraction for publishing messages to the broker.
 * Implements {@link AutoCloseable} for resource management.
 */
public interface MessagePublisher extends AutoCloseable {

    /**
     * Publishes a message and returns a {@link CompletableFuture} that completes when the broker
     * acknowledges receipt or rejects the message.
     *
     * @param request the publish request containing routing key, payload, headers, etc.
     * @return future with the {@link PublishReceipt}
     */
    CompletableFuture<PublishReceipt> publish(PublishRequest request);

    /**
     * Sends a message in fire-and-forget mode without waiting for broker acknowledgment.
     *
     * @param request the publish request
     * @return the client-assigned publish ID
     */
    long publishFireAndForget(PublishRequest request);

    /**
     * Checks if the publisher is closed.
     *
     * @return {@code true} if closed, {@code false} otherwise
     */
    boolean isClosed();

    /**
     * Closes the publisher and releases associated streaming resources.
     */
    @Override
    void close();
}

