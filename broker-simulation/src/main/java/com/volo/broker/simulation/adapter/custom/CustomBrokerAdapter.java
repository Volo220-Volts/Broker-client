package com.volo.broker.simulation.adapter.custom;

import com.volo.broker.client.BrokerClient;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.ClientResources;
import com.volo.broker.client.consumer.ConsumerOptions;
import com.volo.broker.client.publisher.PublishRequest;
import com.volo.broker.client.queue.QueueDeclarationOptions;
import com.volo.broker.client.queue.QueueSettingsBuilder;
import com.volo.broker.simulation.core.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public final class CustomBrokerAdapter implements BrokerSimulationAdapter {
    private final BrokerClientConfig brokerConfig;
    private ClientResources resources;
    private BrokerClient adminClient;

    public CustomBrokerAdapter(BrokerClientConfig brokerConfig) {
        this.brokerConfig = Objects.requireNonNull(brokerConfig, "brokerConfig is required");
    }

    @Override
    public void initialize() {
        this.resources = ClientResources.create(brokerConfig);
        this.adminClient = BrokerClient.connect(brokerConfig, resources);
    }

    private final java.util.Map<String, Boolean> publishAckByQueue = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void declareQueues(List<QueueTarget> queues) {
        for (QueueTarget target : queues) {
            publishAckByQueue.put(target.queueName(), target.waitForPublishAck());

            var builder = target.durable()
                    ? QueueSettingsBuilder.builder().persistent().redelivery(true, 5).durable(true)
                    : QueueSettingsBuilder.builder().memoryOnly().redelivery(false, 1).durable(false);

            if (target.waitForPublishAck()) {
                builder.publishAckEnabled();
            } else {
                builder.publishAckNone();
            }

            if (target.waitForConsumerAck()) {
                builder.consumerAckEnabled();
            } else {
                builder.consumerAckNone();
            }

            if (target.flowControlEnabled()) {
                builder.flowControl(
                        target.highWatermarkMessages(),
                        target.lowWatermarkMessages(),
                        target.maxBlockMs()
                );
            }

            try {
                adminClient.queues().deleteQueue(target.queueName());
            } catch (Exception ignored) {
                // Ignore if queue does not exist yet
            }

            adminClient.queues().declareQueue(
                    target.queueName(),
                    builder.build(),
                    QueueDeclarationOptions.useExisting()
            );
        }
    }

    @Override
    public AutoCloseable startConsumer(String queueName, int prefetchCount, SimulatedMessageHandler handler) {
        return adminClient.openReconnectingConsumer(
                ConsumerOptions.builder()
                        .queuePath(queueName)
                        .prefetchCount(prefetchCount)
                        .autoAck()
                        .handler(delivery -> {
                            long latencyNs = 0;
                            String publishedAt = delivery.headers().get(MessagePayloadFactory.HEADER_PUBLISHED_AT_NS);
                            if (publishedAt != null && !publishedAt.isBlank()) {
                                try {
                                    latencyNs = System.nanoTime() - Long.parseLong(publishedAt);
                                } catch (NumberFormatException ignored) {
                                }
                            }
                            handler.handle(latencyNs, delivery.payloadSize());
                        })
                        .build()
        );
    }

    @Override
    public SimulatedPublisher createPublisher() {
        var client = adminClient.openReconnectingPublisher();
        return new SimulatedPublisher() {
            @Override
            public CompletableFuture<Void> publish(PublishMessage message) {
                PublishRequest request = new PublishRequest(
                        message.queueName(),
                        message.payload(),
                        Map.of(
                                MessagePayloadFactory.HEADER_PUBLISHED_AT_NS, Long.toString(message.publishedAtNs()),
                                MessagePayloadFactory.HEADER_QUEUE_PATH, message.queueName(),
                                MessagePayloadFactory.HEADER_PUBLISHER_INDEX, Integer.toString(message.publisherIndex()),
                                MessagePayloadFactory.HEADER_MESSAGE_INDEX, Integer.toString(message.messageIndex())
                        )
                );

                boolean trackAck = publishAckByQueue.getOrDefault(message.queueName(), true);
                if (trackAck) {
                    return client.publish(request).thenApply(receipt -> null);
                } else {
                    client.publishFireAndForget(request);
                    return CompletableFuture.completedFuture(null);
                }
            }

            @Override
            public void close() {
                client.close();
            }
        };
    }

    @Override
    public void close() {
        if (adminClient != null) {
            try {
                adminClient.close();
            } catch (Throwable ignored) {
            }
        }
        if (resources != null) {
            try {
                resources.close();
            } catch (Throwable ignored) {
            }
        }
    }
}

