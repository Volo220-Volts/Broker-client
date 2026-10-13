package com.volo.broker.client.consumer;

import com.volo.broker.client.BrokerClient;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.BrokerClientException;
import com.volo.broker.client.ClientResources;
import com.volo.broker.client.connection.ReconnectPolicy;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class ReconnectingConsumerClient implements AutoCloseable {
    private final BrokerClientConfig config;
    private final ClientResources resources;
    private final ConsumerOptions consumerOptions;
    private final ReconnectingConsumerOptions reconnectOptions;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger failedAttempts = new AtomicInteger();

    private final Object lifecycleLock = new Object();

    private BrokerClient brokerClient;
    private ConsumerClient consumerClient;

    public ReconnectingConsumerClient(
            BrokerClientConfig config,
            ClientResources resources,
            ConsumerOptions consumerOptions,
            ReconnectingConsumerOptions reconnectOptions
    ) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.resources = Objects.requireNonNull(resources, "resources is required");
        this.consumerOptions = Objects.requireNonNull(consumerOptions, "consumerOptions is required");
        this.reconnectOptions = reconnectOptions == null
                ? ReconnectingConsumerOptions.defaults()
                : reconnectOptions;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new BrokerClientException("ReconnectingConsumerClient already started");
        }

        submit(this::connect);
    }

    private void connect() {
        if (closed.get()) {
            return;
        }

        try {
            ConsumerClient consumer = openConsumer();

            if (consumer == null) {
                return;
            }

            failedAttempts.set(0);
            consumer.closedFuture().thenRun(() -> submit(this::reconnect));
        } catch (Throwable throwable) {
            handleConnectFailure();
        }
    }

    private void reconnect() {
        if (closed.get()) {
            return;
        }

        closeCurrentResources();
        connect();
    }

    private ConsumerClient openConsumer() {
        synchronized (lifecycleLock) {
            if (closed.get()) {
                return null;
            }

            closeCurrentResources();

            brokerClient = BrokerClient.connect(config, resources);
            consumerClient = brokerClient.openConsumer(consumerOptions);

            return consumerClient;
        }
    }

    private void handleConnectFailure() {
        closeCurrentResources();

        int attempts = failedAttempts.incrementAndGet();
        ReconnectPolicy policy = reconnectOptions.reconnectPolicy();

        if (!policy.shouldRetry(attempts)) {
            closed.set(true);
            return;
        }

        Duration delay = policy.delayForAttempt(attempts);

        try {
            resources.scheduler().schedule(
                    () -> submit(this::connect),
                    delay.toMillis(),
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException exception) {
            closed.set(true);
        }
    }

    private void submit(Runnable task) {
        try {
            resources.reconnectExecutor().execute(task);
        } catch (RejectedExecutionException ignored) {
        }
    }

    private void closeCurrentResources() {
        synchronized (lifecycleLock) {
            if (consumerClient != null) {
                try {
                    consumerClient.close();
                } catch (Throwable ignored) {
                }

                consumerClient = null;
            }

            if (brokerClient != null) {
                try {
                    brokerClient.close();
                } catch (Throwable ignored) {
                }

                brokerClient = null;
            }
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

        closeCurrentResources();
    }
}