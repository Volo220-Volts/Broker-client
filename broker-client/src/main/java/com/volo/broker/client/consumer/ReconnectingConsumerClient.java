package com.volo.broker.client.consumer;

import com.custombroker.grpc.ConsumerServiceGrpc;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.ClientResources;
import com.volo.broker.client.connection.ReconnectPolicy;
import com.volo.broker.client.internal.ClientState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class ReconnectingConsumerClient implements MessageConsumer {
    private static final Logger log = LoggerFactory.getLogger(ReconnectingConsumerClient.class);

    private final ManagedChannel channel;
    private final boolean ownsChannel;
    private final BrokerClientConfig config;
    private final ClientResources resources;
    private final ConsumerOptions consumerOptions;
    private final ReconnectingConsumerOptions reconnectOptions;

    private final ClientState state = new ClientState();
    private final AtomicInteger failedAttempts = new AtomicInteger();
    private final CompletableFuture<Void> closedFuture = new CompletableFuture<>();

    private final Object lifecycleLock = new Object();

    private ConsumerClient consumerClient;

    private static final long STABILIZATION_DELAY_MS = 5000;

    private volatile ScheduledFuture<?> pendingReconnectTask;
    private volatile ScheduledFuture<?> stabilizationTask;

    public ReconnectingConsumerClient(
            BrokerClientConfig config,
            ClientResources resources,
            ConsumerOptions consumerOptions,
            ReconnectingConsumerOptions reconnectOptions
    ) {
        this(createChannel(config), true, config, resources, consumerOptions, reconnectOptions);
    }

    public ReconnectingConsumerClient(
            ManagedChannel channel,
            BrokerClientConfig config,
            ClientResources resources,
            ConsumerOptions consumerOptions,
            ReconnectingConsumerOptions reconnectOptions
    ) {
        this(channel, false, config, resources, consumerOptions, reconnectOptions);
    }

    private ReconnectingConsumerClient(
            ManagedChannel channel,
            boolean ownsChannel,
            BrokerClientConfig config,
            ClientResources resources,
            ConsumerOptions consumerOptions,
            ReconnectingConsumerOptions reconnectOptions
    ) {
        this.channel = Objects.requireNonNull(channel, "channel is required");
        this.ownsChannel = ownsChannel;
        this.config = Objects.requireNonNull(config, "config is required");
        this.resources = Objects.requireNonNull(resources, "resources is required");
        this.consumerOptions = Objects.requireNonNull(consumerOptions, "consumerOptions is required");
        this.reconnectOptions = reconnectOptions == null
                ? ReconnectingConsumerOptions.defaults()
                : reconnectOptions;
    }

    private static ManagedChannel createChannel(BrokerClientConfig config) {
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder
                .forAddress(config.host(), config.port())
                .maxInboundMessageSize(config.maxInboundMessageSizeBytes())
                .keepAliveTime(config.keepAliveInterval().toMillis(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true);

        if (config.plaintext()) {
            builder.usePlaintext();
        }

        return builder.build();
    }

    @Override
    public void start() {
        state.markStarted();
        submit(this::connect);
    }

    private void connect() {
        if (state.isClosed()) {
            return;
        }

        try {
            log.info("Connecting consumer stream for queue '{}'", consumerOptions.queuePath());
            ConsumerClient consumer = openConsumer();

            if (consumer == null) {
                return;
            }

            cancelStabilization();
            stabilizationTask = resources.scheduler().schedule(
                    () -> {
                        log.debug("Consumer connection stabilized for queue '{}', resetting failed attempts",
                                consumerOptions.queuePath());
                        failedAttempts.set(0);
                    },
                    STABILIZATION_DELAY_MS,
                    TimeUnit.MILLISECONDS
            );

            consumer.closedFuture().thenRun(() -> {
                cancelStabilization();
                submit(this::handleDisconnect);
            });
        } catch (Throwable throwable) {
            handleConnectFailure();
        }
    }

    private void handleDisconnect() {
        if (state.isClosed()) {
            return;
        }

        handleConnectFailure();
    }

    private void cancelStabilization() {
        ScheduledFuture<?> task = stabilizationTask;
        if (task != null) {
            task.cancel(false);
            stabilizationTask = null;
        }
    }

    private ConsumerClient openConsumer() {
        synchronized (lifecycleLock) {
            if (state.isClosed()) {
                return null;
            }

            closeCurrentConsumer();

            consumerClient = new ConsumerClient(
                    ConsumerServiceGrpc.newStub(channel),
                    config,
                    consumerOptions,
                    resources.handlerExecutor(),
                    resources.scheduler()
            );
            consumerClient.start();

            return consumerClient;
        }
    }

    private void handleConnectFailure() {
        closeCurrentConsumer();

        if (state.isClosed()) {
            return;
        }

        int attempts = failedAttempts.incrementAndGet();
        ReconnectPolicy policy = reconnectOptions.reconnectPolicy();

        if (!policy.shouldRetry(attempts)) {
            log.error("Consumer for queue '{}' reached max reconnect attempts ({}), terminating",
                    consumerOptions.queuePath(), attempts);
            close();
            return;
        }

        Duration delay = policy.delayForAttempt(attempts);
        log.warn("Consumer disconnected from queue '{}'. Scheduling reconnect attempt #{} in {} ms",
                consumerOptions.queuePath(), attempts, delay.toMillis());

        try {
            pendingReconnectTask = resources.scheduler().schedule(
                    () -> submit(this::connect),
                    delay.toMillis(),
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException exception) {
            close();
        }
    }

    private void submit(Runnable task) {
        try {
            resources.reconnectExecutor().execute(task);
        } catch (RejectedExecutionException ignored) {
        }
    }

    private void closeCurrentConsumer() {
        synchronized (lifecycleLock) {
            if (consumerClient != null) {
                try {
                    consumerClient.close();
                } catch (Throwable ignored) {
                }
                consumerClient = null;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return state.isClosed();
    }

    @Override
    public CompletableFuture<Void> closedFuture() {
        return closedFuture;
    }

    @Override
    public void close() {
        if (!state.closeOnce()) {
            return;
        }

        log.info("Closing reconnecting consumer for queue '{}'", consumerOptions.queuePath());
        cancelStabilization();
        ScheduledFuture<?> reconnectTask = pendingReconnectTask;
        if (reconnectTask != null) {
            reconnectTask.cancel(false);
            pendingReconnectTask = null;
        }

        synchronized (lifecycleLock) {
            closeCurrentConsumer();

            if (ownsChannel) {
                channel.shutdown();
                try {
                    boolean terminated = channel.awaitTermination(
                            config.shutdownTimeout().toMillis(),
                            TimeUnit.MILLISECONDS
                    );
                    if (!terminated) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    channel.shutdownNow();
                }
            }
        }

        closedFuture.complete(null);
    }
}