package com.volo.broker.client.consumer;

import com.custombroker.grpc.*;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.BrokerClientException;
import com.volo.broker.client.internal.GrpcErrorMapper;
import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ConsumerClient implements AutoCloseable {
    private final ConsumerServiceGrpc.ConsumerServiceStub stub;
    private final BrokerClientConfig config;
    private final ConsumerOptions options;

    private final ExecutorService handlerExecutor;
    private final ScheduledExecutorService keepAliveExecutor;

    private final Object sendLock = new Object();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CompletableFuture<Void> closedFuture = new CompletableFuture<>();

    private volatile StreamObserver<ConsumerRequest> requestObserver;
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

    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new BrokerClientException("ConsumerClient already started");
        }

        requestObserver = stub.openChannel(new StreamObserver<>() {
            @Override
            public void onNext(BrokerResponse response) {
                handleResponse(response);
            }

            @Override
            public void onError(Throwable throwable) {
                markClosed();
            }

            @Override
            public void onCompleted() {
                markClosed();
            }
        });

        subscribe();
        startKeepAlive();
    }

    public boolean isClosed() {
        return closed.get();
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

        handlerExecutor.submit(() -> handleDeliverySafely(delivery));
    }

    private void handleDeliverySafely(Delivery delivery) {
        if (closed.get()) {
            return;
        }

        try {
            options.handler().handle(delivery);

            if (options.ackMode() == ConsumerAckMode.AUTO && !delivery.completed()) {
                delivery.ack();
            }
        } catch (Throwable throwable) {
            if (options.ackMode() != ConsumerAckMode.NONE && !delivery.completed()) {
                try {
                    delivery.nack(options.requeueOnHandlerError());
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

        if (closed.get()) {
            task.cancel(false);
        }
    }

    private void sendPingSafely() {
        if (closed.get()) {
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

        synchronized (sendLock) {
            requestObserver.onNext(request);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new BrokerClientException("ConsumerClient is closed");
        }
    }

    public CompletableFuture<Void> closedFuture() {
        return closedFuture;
    }

    private boolean markClosed() {
        if (!closed.compareAndSet(false, true)) {
            return false;
        }

        ScheduledFuture<?> task = keepAliveTask;

        if (task != null) {
            task.cancel(false);
        }

        closedFuture.complete(null);

        return true;
    }

    @Override
    public void close() {
        if (!markClosed()) {
            return;
        }

        synchronized (sendLock) {
            try {
                requestObserver.onCompleted();
            } catch (Throwable ignored) {
            }
        }
    }
}