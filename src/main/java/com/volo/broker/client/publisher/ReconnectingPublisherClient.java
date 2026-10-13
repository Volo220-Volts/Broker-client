package com.volo.broker.client.publisher;

import com.volo.broker.client.BrokerClient;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.BrokerClientException;
import com.volo.broker.client.ClientResources;
import com.volo.broker.client.connection.ReconnectPolicy;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ReconnectingPublisherClient implements AutoCloseable {
    private final BrokerClientConfig config;
    private final ClientResources resources;
    private final ReconnectingPublisherOptions options;

    private final Object lifecycleLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private BrokerClient brokerClient;
    private PublisherClient publisherClient;

    public ReconnectingPublisherClient(
            BrokerClientConfig config,
            ClientResources resources,
            ReconnectingPublisherOptions options
    ) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.resources = Objects.requireNonNull(resources, "resources is required");
        this.options = options == null
                ? ReconnectingPublisherOptions.defaults()
                : options;

        reconnect();
    }

    public CompletableFuture<PublishReceipt> publish(PublishRequest request) {
        Objects.requireNonNull(request, "request is required");
        ensureOpen();

        CompletableFuture<PublishReceipt> result = new CompletableFuture<>();

        publishAttempt(request, result, 0);

        return result;
    }

    public long publishFireAndForget(PublishRequest request) {
        Objects.requireNonNull(request, "request is required");
        ensureOpen();

        try {
            return currentPublisher().publishFireAndForget(request);
        } catch (Throwable throwable) {
            reconnect();

            return currentPublisher().publishFireAndForget(request);
        }
    }

    private void publishAttempt(
            PublishRequest request,
            CompletableFuture<PublishReceipt> result,
            int failedAttempts
    ) {
        PublisherClient publisher = currentPublisher();

        publisher.publish(request).whenComplete((receipt, throwable) -> {
            if (throwable == null) {
                result.complete(receipt);
                return;
            }

            if (!shouldRetryPublish(failedAttempts)) {
                result.completeExceptionally(throwable);
                return;
            }

            scheduleRetry(request, result, failedAttempts + 1);
        });
    }

    private boolean shouldRetryPublish(int failedAttempts) {
        if (!options.retryPublishWhenResultUnknown()) {
            return false;
        }

        ReconnectPolicy reconnectPolicy = options.reconnectPolicy();

        return reconnectPolicy.shouldRetry(failedAttempts);
    }

    private PublisherClient currentPublisher() {
        ensureOpen();

        synchronized (lifecycleLock) {
            if (publisherClient == null || publisherClient.isClosed()) {
                reconnect();
            }

            return publisherClient;
        }
    }

    private void reconnect() {
        ensureOpen();

        synchronized (lifecycleLock) {
            closeCurrentResources();

            brokerClient = BrokerClient.connect(config, resources);
            publisherClient = brokerClient.openPublisher();
        }
    }

    private void closeCurrentResources() {
        if (publisherClient != null) {
            try {
                publisherClient.close();
            } catch (Throwable ignored) {
            }
        }

        if (brokerClient != null) {
            try {
                brokerClient.close();
            } catch (Throwable ignored) {
            }
        }
    }

    private void scheduleRetry(
            PublishRequest request,
            CompletableFuture<PublishReceipt> result,
            int attempt
    ) {
        long delayMs = options.reconnectPolicy().delayForAttempt(attempt).toMillis();

        try {
            resources.scheduler().schedule(
                    () -> dispatchRetry(request, result, attempt),
                    delayMs,
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException exception) {
            result.completeExceptionally(exception);
        }
    }

    private void dispatchRetry(
            PublishRequest request,
            CompletableFuture<PublishReceipt> result,
            int attempt
    ) {
        try {
            resources.reconnectExecutor().execute(() -> retry(request, result, attempt));
        } catch (RejectedExecutionException exception) {
            result.completeExceptionally(exception);
        }
    }

    private void retry(
            PublishRequest request,
            CompletableFuture<PublishReceipt> result,
            int attempt
    ) {
        try {
            reconnect();
            publishAttempt(request, result, attempt);
        } catch (Throwable throwable) {
            result.completeExceptionally(throwable);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new BrokerClientException("ReconnectingPublisherClient is closed");
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        synchronized (lifecycleLock) {
            closeCurrentResources();
        }
    }
}