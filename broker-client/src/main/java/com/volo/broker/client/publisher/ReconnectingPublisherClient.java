package com.volo.broker.client.publisher;

import com.custombroker.grpc.PublisherServiceGrpc;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.ClientResources;
import com.volo.broker.client.connection.ReconnectPolicy;
import com.volo.broker.client.internal.ClientState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public final class ReconnectingPublisherClient implements MessagePublisher {
    private static final Logger log = LoggerFactory.getLogger(ReconnectingPublisherClient.class);

    private final ManagedChannel channel;
    private final boolean ownsChannel;
    private final BrokerClientConfig config;
    private final ClientResources resources;
    private final ReconnectingPublisherOptions options;

    private final Object lifecycleLock = new Object();
    private final ClientState state = new ClientState();

    private PublisherClient publisherClient;

    public ReconnectingPublisherClient(
            BrokerClientConfig config,
            ClientResources resources,
            ReconnectingPublisherOptions options
    ) {
        this(createChannel(config), true, config, resources, options);
    }

    public ReconnectingPublisherClient(
            ManagedChannel channel,
            BrokerClientConfig config,
            ClientResources resources,
            ReconnectingPublisherOptions options
    ) {
        this(channel, false, config, resources, options);
    }

    private ReconnectingPublisherClient(
            ManagedChannel channel,
            boolean ownsChannel,
            BrokerClientConfig config,
            ClientResources resources,
            ReconnectingPublisherOptions options
    ) {
        this.channel = Objects.requireNonNull(channel, "channel is required");
        this.ownsChannel = ownsChannel;
        this.config = Objects.requireNonNull(config, "config is required");
        this.resources = Objects.requireNonNull(resources, "resources is required");
        this.options = options == null
                ? ReconnectingPublisherOptions.defaults()
                : options;

        reconnectStream();
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
    public CompletableFuture<PublishReceipt> publish(PublishRequest request) {
        Objects.requireNonNull(request, "request is required");
        ensureOpen();

        CompletableFuture<PublishReceipt> result = new CompletableFuture<>();
        publishAttempt(request, result, 0);

        return result;
    }

    @Override
    public long publishFireAndForget(PublishRequest request) {
        Objects.requireNonNull(request, "request is required");
        ensureOpen();

        try {
            return currentPublisher().publishFireAndForget(request);
        } catch (Throwable throwable) {
            log.warn("Fire-and-forget publish failed, reconnecting stream: {}", throwable.getMessage());
            reconnectStream();
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
                log.error("Publish attempt #{} failed and retry exhausted or disabled: {}",
                        failedAttempts + 1, throwable.getMessage());
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
                reconnectStream();
            }

            return publisherClient;
        }
    }

    private void reconnectStream() {
        ensureOpen();

        synchronized (lifecycleLock) {
            closeCurrentStream();

            log.info("Reconnecting publisher stream over shared channel");
            publisherClient = new PublisherClient(
                    PublisherServiceGrpc.newStub(channel),
                    config,
                    resources.scheduler(),
                    PublisherOptions.defaults()
            );
        }
    }

    private void closeCurrentStream() {
        if (publisherClient != null) {
            try {
                publisherClient.close();
            } catch (Throwable ignored) {
            }
            publisherClient = null;
        }
    }

    private void scheduleRetry(
            PublishRequest request,
            CompletableFuture<PublishReceipt> result,
            int attempt
    ) {
        long delayMs = options.reconnectPolicy().delayForAttempt(attempt).toMillis();
        log.warn("Publish failed, scheduling retry #{} in {} ms", attempt, delayMs);

        try {
            resources.scheduler().schedule(
                    () -> {
                        try {
                            resources.reconnectExecutor().execute(() -> retry(request, result, attempt));
                        } catch (RejectedExecutionException ex) {
                            result.completeExceptionally(ex);
                        }
                    },
                    delayMs,
                    TimeUnit.MILLISECONDS
            );
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
            reconnectStream();
            publishAttempt(request, result, attempt);
        } catch (Throwable throwable) {
            result.completeExceptionally(throwable);
        }
    }

    private void ensureOpen() {
        state.ensureOpen("ReconnectingPublisherClient");
    }

    @Override
    public boolean isClosed() {
        return state.isClosed();
    }

    @Override
    public void close() {
        if (!state.closeOnce()) {
            return;
        }

        synchronized (lifecycleLock) {
            closeCurrentStream();

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
    }
}