package com.volo.broker.client.publisher;

import com.custombroker.grpc.*;
import com.google.protobuf.ByteString;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.BrokerClientException;
import com.volo.broker.client.internal.GrpcErrorMapper;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class PublisherClient implements AutoCloseable {
    private final PublisherServiceGrpc.PublisherServiceStub stub;
    private final BrokerClientConfig config;
    private final ClientPublishIdGenerator publishIdGenerator = new ClientPublishIdGenerator();

    private final ScheduledExecutorService keepAliveExecutor;

    private final ConcurrentMap<Long, CompletableFuture<PublishReceipt>> pendingPublishes =
            new ConcurrentHashMap<>();

    private final Object sendLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile StreamObserver<PublisherRequest> requestObserver;
    private volatile ScheduledFuture<?> keepAliveTask;

    public PublisherClient(
            PublisherServiceGrpc.PublisherServiceStub stub,
            BrokerClientConfig config,
            ScheduledExecutorService keepAliveExecutor
    ) {
        this.stub = Objects.requireNonNull(stub, "stub is required");
        this.config = Objects.requireNonNull(config, "config is required");
        this.keepAliveExecutor = Objects.requireNonNull(keepAliveExecutor, "keepAliveExecutor is required");

        openStream();
        startKeepAlive();
    }

    public CompletableFuture<PublishReceipt> publish(PublishRequest request) {
        Objects.requireNonNull(request, "request is required");
        ensureOpen();

        long clientPublishId = publishIdGenerator.nextId();

        CompletableFuture<PublishReceipt> result = new CompletableFuture<>();
        pendingPublishes.put(clientPublishId, result);

        sendPublish(clientPublishId, request, result);

        return result.orTimeout(
                config.publishResponseTimeout().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    public long publishFireAndForget(PublishRequest request) {
        Objects.requireNonNull(request, "request is required");
        ensureOpen();

        long clientPublishId = publishIdGenerator.nextId();

        PublisherRequest grpcRequest = toGrpcPublishRequest(clientPublishId, request);

        send(grpcRequest);

        return clientPublishId;
    }

    public int pendingPublishes() {
        return pendingPublishes.size();
    }

    public boolean isClosed() {
        return closed.get();
    }

    private void openStream() {
        requestObserver = stub.openPublisherChannel(new StreamObserver<>() {
            @Override
            public void onNext(PublisherResponse response) {
                handleResponse(response);
            }

            @Override
            public void onError(Throwable throwable) {
                failAllPending(new BrokerClientException("Publisher stream failed", throwable));
                closed.set(true);
                cancelKeepAlive();
            }

            @Override
            public void onCompleted() {
                failAllPending(new BrokerClientException("Publisher stream completed"));
                closed.set(true);
                cancelKeepAlive();
            }
        });
    }

    private void sendPublish(
            long clientPublishId,
            PublishRequest request,
            CompletableFuture<PublishReceipt> result
    ) {
        try {
            send(toGrpcPublishRequest(clientPublishId, request));
        } catch (Throwable throwable) {
            pendingPublishes.remove(clientPublishId);
            result.completeExceptionally(
                    new BrokerClientException("Failed to send publish request", throwable)
            );
        }
    }

    private PublisherRequest toGrpcPublishRequest(
            long clientPublishId,
            PublishRequest request
    ) {
        return PublisherRequest.newBuilder()
                .setPublish(PublishAction.newBuilder()
                        .setClientPublishId(clientPublishId)
                        .setRoutingKey(request.routingKey())
                        .setPayload(ByteString.copyFrom(request.payload()))
                        .putAllHeaders(request.headers())
                        .build())
                .build();
    }

    private void handleResponse(PublisherResponse response) {
        if (response.hasAck()) {
            handleAck(response.getAck());
            return;
        }

        if (response.hasNack()) {
            handleNack(response.getNack());
            return;
        }

        if (response.hasPong()) {
            return;
        }
    }

    private void handleAck(PublishAck ack) {
        CompletableFuture<PublishReceipt> result =
                pendingPublishes.remove(ack.getClientPublishId());

        if (result == null) {
            return;
        }

        result.complete(new PublishReceipt(
                ack.getClientPublishId(),
                ack.getRoutedQueueCount()
        ));
    }

    private void handleNack(PublishNack nack) {
        CompletableFuture<PublishReceipt> result =
                pendingPublishes.remove(nack.getClientPublishId());

        if (result == null) {
            return;
        }

        result.completeExceptionally(
                GrpcErrorMapper.toException(nack.getError())
        );
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

    private void cancelKeepAlive() {
        ScheduledFuture<?> task = keepAliveTask;

        if (task != null) {
            task.cancel(false);
        }
    }

    private void sendPingSafely() {
        if (closed.get()) {
            return;
        }

        try {
            send(PublisherRequest.newBuilder()
                    .setPing(KeepAlive.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .build())
                    .build());
        } catch (Throwable throwable) {
            close();
        }
    }

    private void send(PublisherRequest request) {
        ensureOpen();

        synchronized (sendLock) {
            requestObserver.onNext(request);
        }
    }

    private void failAllPending(Throwable throwable) {
        for (Map.Entry<Long, CompletableFuture<PublishReceipt>> entry : pendingPublishes.entrySet()) {
            entry.getValue().completeExceptionally(throwable);
        }

        pendingPublishes.clear();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new BrokerClientException("PublisherClient is closed");
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        cancelKeepAlive();

        synchronized (sendLock) {
            try {
                requestObserver.onNext(PublisherRequest.newBuilder()
                        .setClose(ClosePublisherAction.newBuilder()
                                .setReason("client closed")
                                .build())
                        .build());
            } catch (Throwable ignored) {
            }

            try {
                requestObserver.onCompleted();
            } catch (Throwable ignored) {
            }
        }

        failAllPending(new BrokerClientException("PublisherClient closed"));
    }
}