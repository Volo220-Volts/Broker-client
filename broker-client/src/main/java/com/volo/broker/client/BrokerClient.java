package com.volo.broker.client;

import com.custombroker.grpc.ConsumerServiceGrpc;
import com.custombroker.grpc.PublisherServiceGrpc;
import com.custombroker.grpc.QueueServiceGrpc;
import com.volo.broker.client.consumer.ConsumerClient;
import com.volo.broker.client.consumer.ConsumerOptions;
import com.volo.broker.client.consumer.MessageConsumer;
import com.volo.broker.client.consumer.ReconnectingConsumerClient;
import com.volo.broker.client.consumer.ReconnectingConsumerOptions;
import com.volo.broker.client.internal.ClientState;
import com.volo.broker.client.publisher.MessagePublisher;
import com.volo.broker.client.publisher.PublisherClient;
import com.volo.broker.client.publisher.ReconnectingPublisherClient;
import com.volo.broker.client.publisher.ReconnectingPublisherOptions;
import com.volo.broker.client.queue.QueueAdminClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class BrokerClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerClient.class);

    private final ManagedChannel channel;
    private final BrokerClientConfig config;
    private final ClientResources resources;
    private final boolean ownsResources;
    private final ClientState state = new ClientState();

    private final QueueAdminClient queueAdminClient;

    private BrokerClient(
            ManagedChannel channel,
            BrokerClientConfig config,
            ClientResources resources,
            boolean ownsResources) {
        this.channel = Objects.requireNonNull(channel, "channel is required");
        this.config = Objects.requireNonNull(config, "config is required");
        this.resources = Objects.requireNonNull(resources, "resources is required");
        this.ownsResources = ownsResources;

        this.queueAdminClient = new QueueAdminClient(
                QueueServiceGrpc.newBlockingStub(channel)
        );
    }

    public static BrokerClient connect(BrokerClientConfig config) {
        Objects.requireNonNull(config, "config is required");

        return create(config, ClientResources.create(config), true);
    }

    public static BrokerClient connect(BrokerClientConfig config, ClientResources resources) {
        Objects.requireNonNull(config, "config is required");
        Objects.requireNonNull(resources, "resources is required");

        return create(config, resources, false);
    }

    private static BrokerClient create(
            BrokerClientConfig config,
            ClientResources resources,
            boolean ownsResources
    ) {
        log.info("Connecting to broker at {}:{} (plaintext={})", config.host(), config.port(), config.plaintext());

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder
                .forAddress(config.host(), config.port())
                .maxInboundMessageSize(config.maxInboundMessageSizeBytes())
                .keepAliveTime(config.keepAliveInterval().toMillis(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true);

        if (config.plaintext()) {
            builder.usePlaintext();
        }

        return new BrokerClient(builder.build(), config, resources, ownsResources);
    }

    public QueueAdminClient queues() {
        ensureOpen();
        return queueAdminClient;
    }

    public PublisherClient openPublisher() {
        return openPublisher(com.volo.broker.client.publisher.PublisherOptions.defaults());
    }

    public PublisherClient openPublisher(com.volo.broker.client.publisher.PublisherOptions options) {
        ensureOpen();

        return new PublisherClient(
                PublisherServiceGrpc.newStub(channel),
                config,
                resources.scheduler(),
                options
        );
    }

    public ConsumerClient openConsumer(ConsumerOptions options) {
        ensureOpen();

        ConsumerClient consumerClient = new ConsumerClient(
                ConsumerServiceGrpc.newStub(channel),
                config,
                options,
                resources.handlerExecutor(),
                resources.scheduler()
        );

        consumerClient.start();

        return consumerClient;
    }

    public boolean isClosed() {
        return state.isClosed();
    }

    private void ensureOpen() {
        state.ensureOpen("BrokerClient");
    }

    @Override
    public void close() {
        if (!state.closeOnce()) {
            return;
        }

        log.info("Closing BrokerClient and terminating channel");

        try {
            shutdownChannel();
        } finally {
            if (ownsResources) {
                resources.close();
            }
        }
    }

    private void shutdownChannel() {
        channel.shutdown();

        try {
            boolean terminated = channel.awaitTermination(
                    config.shutdownTimeout().toMillis(),
                    TimeUnit.MILLISECONDS
            );

            if (!terminated) {
                channel.shutdownNow();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }

    public ReconnectingPublisherClient openReconnectingPublisher() {
        return openReconnectingPublisher(ReconnectingPublisherOptions.defaults());
    }

    public ReconnectingPublisherClient openReconnectingPublisher(
            ReconnectingPublisherOptions options) {
        ensureOpen();

        return new ReconnectingPublisherClient(
                channel,
                config,
                resources,
                options
        );
    }

    public ReconnectingConsumerClient openReconnectingConsumer(
            ConsumerOptions consumerOptions) {
        return openReconnectingConsumer(
                consumerOptions,
                ReconnectingConsumerOptions.defaults()
        );
    }

    public ReconnectingConsumerClient openReconnectingConsumer(
            ConsumerOptions consumerOptions,
            ReconnectingConsumerOptions reconnectOptions) {
        ensureOpen();

        ReconnectingConsumerClient client = new ReconnectingConsumerClient(
                channel,
                config,
                resources,
                consumerOptions,
                reconnectOptions
        );

        client.start();

        return client;
    }

    public MessagePublisher publisher() {
        return openReconnectingPublisher();
    }

    public MessageConsumer consumer(ConsumerOptions consumerOptions) {
        return openReconnectingConsumer(consumerOptions);
    }
}