package com.volo.broker.simulation.adapter.rabbitmq;

import com.rabbitmq.client.*;
import com.volo.broker.simulation.core.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RabbitMqAdapter implements BrokerSimulationAdapter {
    private final RabbitMqConfig config;
    private final ConnectionFactory connectionFactory;
    private Connection sharedConnection;

    public RabbitMqAdapter(RabbitMqConfig config) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.connectionFactory = new ConnectionFactory();
        this.connectionFactory.setHost(config.host());
        this.connectionFactory.setPort(config.port());
        this.connectionFactory.setVirtualHost(config.virtualHost());
        this.connectionFactory.setUsername(config.username());
        this.connectionFactory.setPassword(config.password());
        this.connectionFactory.setAutomaticRecoveryEnabled(true);
    }

    @Override
    public void initialize() {
        try {
            this.sharedConnection = connectionFactory.newConnection();
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Failed to establish RabbitMQ connection", e);
        }
    }

    @Override
    public void declareQueues(List<QueueTarget> queues) {
        try (Channel channel = sharedConnection.createChannel()) {
            for (QueueTarget target : queues) {
                boolean durable = true;
                channel.queueDeclare(target.queueName(), durable, false, false, null);
            }
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Failed to declare RabbitMQ queues", e);
        }
    }

    @Override
    public AutoCloseable startConsumer(String queueName, int prefetchCount, SimulatedMessageHandler handler) {
        try {
            Channel channel = sharedConnection.createChannel();
            channel.basicQos(prefetchCount);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                long latencyNs = 0;
                BasicProperties props = delivery.getProperties();
                if (props.getHeaders() != null) {
                    Object publishedAt = props.getHeaders().get(MessagePayloadFactory.HEADER_PUBLISHED_AT_NS);
                    if (publishedAt != null) {
                        try {
                            latencyNs = System.nanoTime() - Long.parseLong(publishedAt.toString());
                        } catch (NumberFormatException ignored) {
                        }
                    }
                }
                int payloadLength = delivery.getBody() != null ? delivery.getBody().length : 0;
                handler.handle(latencyNs, payloadLength);
            };

            String consumerTag = channel.basicConsume(queueName, true, deliverCallback, consumerTagIgnored -> {});

            return () -> {
                try {
                    channel.basicCancel(consumerTag);
                } catch (Throwable ignored) {
                }
                try {
                    channel.close();
                } catch (Throwable ignored) {
                }
            };
        } catch (IOException e) {
            throw new RuntimeException("Failed to start RabbitMQ consumer for queue " + queueName, e);
        }
    }

    @Override
    public SimulatedPublisher createPublisher() {
        try {
            Channel channel = sharedConnection.createChannel();
            channel.confirmSelect();

            ConcurrentNavigableMap<Long, CompletableFuture<Void>> outstandingConfirms = new ConcurrentSkipListMap<>();
            AtomicBoolean closed = new AtomicBoolean(false);

            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) {
                    if (multiple) {
                        ConcurrentNavigableMap<Long, CompletableFuture<Void>> headMap = outstandingConfirms.headMap(deliveryTag, true);
                        for (CompletableFuture<Void> future : headMap.values()) {
                            future.complete(null);
                        }
                        headMap.clear();
                    } else {
                        CompletableFuture<Void> future = outstandingConfirms.remove(deliveryTag);
                        if (future != null) {
                            future.complete(null);
                        }
                    }
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) {
                    Throwable nackEx = new RuntimeException("RabbitMQ broker NACK for deliveryTag " + deliveryTag);
                    if (multiple) {
                        ConcurrentNavigableMap<Long, CompletableFuture<Void>> headMap = outstandingConfirms.headMap(deliveryTag, true);
                        for (CompletableFuture<Void> future : headMap.values()) {
                            future.completeExceptionally(nackEx);
                        }
                        headMap.clear();
                    } else {
                        CompletableFuture<Void> future = outstandingConfirms.remove(deliveryTag);
                        if (future != null) {
                            future.completeExceptionally(nackEx);
                        }
                    }
                }
            });

            return new SimulatedPublisher() {
                @Override
                public CompletableFuture<Void> publish(PublishMessage message) {
                    if (closed.get()) {
                        return CompletableFuture.failedFuture(new IllegalStateException("Publisher closed"));
                    }

                    CompletableFuture<Void> future = new CompletableFuture<>();
                    try {
                        synchronized (channel) {
                            long seqNo = channel.getNextPublishSeqNo();
                            outstandingConfirms.put(seqNo, future);

                            Map<String, Object> headers = Map.of(
                                    MessagePayloadFactory.HEADER_PUBLISHED_AT_NS, Long.toString(message.publishedAtNs()),
                                    MessagePayloadFactory.HEADER_QUEUE_PATH, message.queueName(),
                                    MessagePayloadFactory.HEADER_PUBLISHER_INDEX, message.publisherIndex(),
                                    MessagePayloadFactory.HEADER_MESSAGE_INDEX, message.messageIndex()
                            );

                            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                                    .headers(headers)
                                    .build();

                            channel.basicPublish("", message.queueName(), props, message.payload());
                        }
                    } catch (Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                    return future;
                }

                @Override
                public void close() {
                    if (closed.compareAndSet(false, true)) {
                        try {
                            channel.close();
                        } catch (Throwable ignored) {
                        }
                        for (CompletableFuture<Void> pending : outstandingConfirms.values()) {
                            pending.completeExceptionally(new IllegalStateException("Channel closed"));
                        }
                        outstandingConfirms.clear();
                    }
                }
            };
        } catch (IOException e) {
            throw new RuntimeException("Failed to create RabbitMQ channel", e);
        }
    }

    @Override
    public void close() {
        if (sharedConnection != null) {
            try {
                sharedConnection.close();
            } catch (Throwable ignored) {
            }
        }
    }
}

