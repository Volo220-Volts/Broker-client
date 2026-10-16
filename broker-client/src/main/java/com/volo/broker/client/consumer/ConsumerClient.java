package com.volo.broker.client.consumer;

import com.custombroker.grpc.*;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.internal.ClientState;
import com.volo.broker.client.internal.GrpcErrorMapper;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.*;

public final class ConsumerClient implements MessageConsumer {
    private static final Logger log = LoggerFactory.getLogger(ConsumerClient.class);

    private final ConsumerServiceGrpc.ConsumerServiceStub stub;
    private final BrokerClientConfig config;
    private final ConsumerOptions options;

    private final ExecutorService handlerExecutor;
    private final ScheduledExecutorService keepAliveExecutor;

    private final ClientState state = new ClientState();
    private final CompletableFuture<Void> closedFuture = new CompletableFuture<>();

    private volatile com.volo.broker.client.internal.NonBlockingStreamSender<ConsumerRequest> sender;
    private volatile ScheduledFuture<?> keepAliveTask;

    public ConsumerClient(
            ConsumerServiceGrpc.ConsumerServiceStub stub,
            BrokerClientConfig config,
            ConsumerOptions options,
            ExecutorService handlerExecutor,
            ScheduledExecutorService keepAliveExecutor
    ) {
        this.stub = Objects.requireNonNull(stub, "stub is required");
        this.config = Objects.requireNonNull(config, "config is required");
        this.options = Objects.requireNonNull(options, "options is required");
        this.handlerExecutor = Objects.requireNonNull(handlerExecutor, "handlerExecutor is required");
        this.keepAliveExecutor = Objects.requireNonNull(keepAliveExecutor, "keepAliveExecutor is required");
    }

    @Override
    public void start() {
        state.markStarted();
        log.info("Starting consumer client on queue: {}", options.queuePath());

        StreamObserver<ConsumerRequest> observer = stub.openChannel(new StreamObserver<>() {
            @Override
            public void onNext(BrokerResponse response) {
                handleResponse(response);
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn("Consumer stream error for queue '{}': {}", options.queuePath(), throwable.getMessage());
                markClosed();
            }

            @Override
            public void onCompleted() {
                log.info("Consumer stream completed for queue '{}'", options.queuePath());
                markClosed();
            }
        });

        this.sender = new com.volo.broker.client.internal.NonBlockingStreamSender<>(observer);

        subscribe();
        startKeepAlive();

        try {
            options.eventListener().onStarted(options.queuePath());
        } catch (Throwable ignored) {
        }
    }

    @Override
    public boolean isClosed() {
        return state.isClosed();
    }

    private void subscribe() {
        send(ConsumerRequest.newBuilder()
                .setSubscribe(SubscribeAction.newBuilder()
                        .setQueueName(options.queuePath())
                        .setPrefetchCount(options.prefetchCount())
                        .build())
                .build());
    }

    private void handleResponse(BrokerResponse response) {
        if (response.hasDelivery()) {
            submitDelivery(response.getDelivery());
            return;
        }

        if (response.hasPong()) {
            return;
        }

        if (response.hasError()) {
            throw GrpcErrorMapper.toException(response.getError());
        }
    }

    private void submitDelivery(MessageDelivery grpcDelivery) {
        Delivery delivery = new Delivery(
                grpcDelivery.getDeliveryTag(),
                grpcDelivery.getMessage(),
                grpcDelivery.getRedelivered(),
                grpcDelivery.getDeliveryAttempt(),
                options.ackMode(),
                new Delivery.AckOperations() {
                    @Override
                    public void ack(long deliveryTag, long messageId) {
                        sendAck(deliveryTag, messageId);
                    }

                    @Override
                    public void nack(long deliveryTag, long messageId, boolean requeue) {
                        sendNack(deliveryTag, messageId, requeue);
                    }
                }
        );

        try {
            options.eventListener().onDelivery(delivery);
        } catch (Throwable ignored) {
        }

        handlerExecutor.submit(() -> handleDeliverySafely(delivery));
    }

    private void handleDeliverySafely(Delivery delivery) {
        if (state.isClosed()) {
            return;
        }

        try {
            options.handler().handle(delivery);

            if (options.ackMode() == ConsumerAckMode.AUTO && !delivery.completed()) {
                delivery.ack();
            }

            try {
                options.eventListener().onHandled(delivery);
            } catch (Throwable ignored) {
            }
        } catch (Throwable throwable) {
            log.error("Failed to process message [tag={}, id={}, queue={}]: {}",
                    delivery.deliveryTag(), delivery.messageId(), options.queuePath(), throwable.getMessage(), throwable);

            try {
                options.eventListener().onHandlerError(delivery, throwable);
            } catch (Throwable ignored) {
            }

            if (options.ackMode() != ConsumerAckMode.NONE && !delivery.completed()) {
                try {
                    boolean shouldRequeue = options.requeueOnHandlerError()
                            && delivery.deliveryAttempt() < options.maxDeliveryAttempts();

                    if (!shouldRequeue && options.requeueOnHandlerError()) {
                        log.warn("Message [tag={}, id={}] exceeded max delivery attempts ({}), discarding/sending to DLQ",
                                delivery.deliveryTag(), delivery.messageId(), options.maxDeliveryAttempts());
                    }

                    delivery.nack(shouldRequeue);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    private void sendAck(long deliveryTag, long messageId) {
        send(ConsumerRequest.newBuilder()
                .setAck(AckAction.newBuilder()
                        .setDeliveryTag(deliveryTag)
                        .setMessageId(messageId)
                        .build())
                .build());
    }

    private void sendNack(long deliveryTag, long messageId, boolean requeue) {
        send(ConsumerRequest.newBuilder()
                .setNack(NackAction.newBuilder()
                        .setDeliveryTag(deliveryTag)
                        .setMessageId(messageId)
                        .setRequeue(requeue)
                        .build())
                .build());
    }

    private void startKeepAlive() {
        ScheduledFuture<?> task = keepAliveExecutor.scheduleWithFixedDelay(
                this::sendPingSafely,
                config.keepAliveInterval().toMillis(),
                config.keepAliveInterval().toMillis(),
                TimeUnit.MILLISECONDS
        );

        keepAliveTask = task;

        if (state.isClosed()) {
            task.cancel(false);
        }
    }

    private void sendPingSafely() {
        if (state.isClosed()) {
            return;
        }

        try {
            send(ConsumerRequest.newBuilder()
                    .setPing(KeepAlive.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .build())
                    .build());
        } catch (Throwable throwable) {
            close();
        }
    }

    private void send(ConsumerRequest request) {
        ensureOpen();
        com.volo.broker.client.internal.NonBlockingStreamSender<ConsumerRequest> currentSender = sender;
        if (currentSender != null) {
            currentSender.send(request);
        }
    }

    private void ensureOpen() {
        state.ensureOpen("ConsumerClient");
    }

    @Override
    public CompletableFuture<Void> closedFuture() {
        return closedFuture;
    }

    private boolean markClosed() {
        if (!state.closeOnce()) {
            return false;
        }

        ScheduledFuture<?> task = keepAliveTask;

        if (task != null) {
            task.cancel(false);
        }

        closedFuture.complete(null);

        try {
            options.eventListener().onClosed();
        } catch (Throwable ignored) {
        }

        return true;
    }

    @Override
    public void close() {
        if (!markClosed()) {
            return;
        }

        log.info("Consumer client closed on queue: {}", options.queuePath());
        com.volo.broker.client.internal.NonBlockingStreamSender<ConsumerRequest> currentSender = sender;
        if (currentSender != null) {
            currentSender.complete();
        }
    }
}