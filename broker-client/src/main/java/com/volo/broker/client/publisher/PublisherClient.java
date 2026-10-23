package com.volo.broker.client.publisher;

import com.custombroker.grpc.*;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.BrokerClientException;
import com.volo.broker.client.internal.ClientState;
import com.volo.broker.client.internal.GrpcErrorMapper;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.*;

public final class PublisherClient implements MessagePublisher {
    private static final Logger log = LoggerFactory.getLogger(PublisherClient.class);

    private final PublisherServiceGrpc.PublisherServiceStub stub;
    private final BrokerClientConfig config;
    private final ClientPublishIdGenerator publishIdGenerator = new ClientPublishIdGenerator();

    private final ScheduledExecutorService keepAliveExecutor;
    private final PublisherOptions options;

    private final PendingPublishRegistry pendingPublishes = new PendingPublishRegistry();
    private final ClientState state = new ClientState();

    private final com.volo.broker.client.internal.NonBlockingStreamSender<PublisherRequest> sender;
    private volatile ScheduledFuture<?> keepAliveTask;
    private volatile boolean flowBlocked;

    public PublisherClient(
            PublisherServiceGrpc.PublisherServiceStub stub,
            BrokerClientConfig config,
            ScheduledExecutorService keepAliveExecutor
    ) {
        this(stub, config, keepAliveExecutor, PublisherOptions.defaults());
    }

    public PublisherClient(
            PublisherServiceGrpc.PublisherServiceStub stub,
            BrokerClientConfig config,
            ScheduledExecutorService keepAliveExecutor,
            PublisherOptions options
    ) {
        this.stub = Objects.requireNonNull(stub, "stub is required");
        this.config = Objects.requireNonNull(config, "config is required");
        this.keepAliveExecutor = Objects.requireNonNull(keepAliveExecutor, "keepAliveExecutor is required");
        this.options = options == null ? PublisherOptions.defaults() : options;

        this.sender = openStream();
        startKeepAlive();
    }

    @Override
    public CompletableFuture<PublishReceipt> publish(PublishRequest request) {
        return publish(request, null);
    }

    public CompletableFuture<PublishReceipt> publish(PublishRequest request, PublishOptions publishOptions) {
        Objects.requireNonNull(request, "request is required");
        ensureOpen();

        if (publishOptions != null && !publishOptions.waitForAck()) {
            long clientPublishId = publishFireAndForget(request);
            return CompletableFuture.completedFuture(new PublishReceipt(clientPublishId, 0));
        }

        long clientPublishId = publishIdGenerator.nextId();
        CompletableFuture<PublishReceipt> result = pendingPublishes.register(clientPublishId);

        sendPublish(clientPublishId, request, result);

        java.time.Duration timeout = (publishOptions != null && publishOptions.timeout() != null)
                ? publishOptions.timeout()
                : config.publishResponseTimeout();

        return result.orTimeout(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    @Override
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

    public boolean isFlowBlocked() {
        return flowBlocked;
    }

    @Override
    public boolean isClosed() {
        return state.isClosed();
    }

    private com.volo.broker.client.internal.NonBlockingStreamSender<PublisherRequest> openStream() {
        StreamObserver<PublisherRequest> requestObserver = stub.openPublisherChannel(new StreamObserver<>() {
            @Override
            public void onNext(PublisherResponse response) {
                handleResponse(response);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Publisher stream error: {}", throwable.getMessage());
                failAllPending(new BrokerClientException("Publisher stream failed", throwable));
                state.closeOnce();
                cancelKeepAlive();
                try {
                    options.eventListener().onStreamClosed(throwable);
                } catch (Throwable ignored) {
                }
            }

            @Override
            public void onCompleted() {
                log.info("Publisher stream completed by broker");
                failAllPending(new BrokerClientException("Publisher stream completed"));
                state.closeOnce();
                cancelKeepAlive();
                try {
                    options.eventListener().onStreamClosed(null);
                } catch (Throwable ignored) {
                }
            }
        });

        return new com.volo.broker.client.internal.NonBlockingStreamSender<>(requestObserver);
    }

    private void sendPublish(
            long clientPublishId,
            PublishRequest request,
            CompletableFuture<PublishReceipt> result
    ) {
        try {
            send(toGrpcPublishRequest(clientPublishId, request));
            try {
                options.eventListener().onPublishSent(clientPublishId, request);
            } catch (Throwable ignored) {
            }
        } catch (Throwable throwable) {
            pendingPublishes.fail(
                    clientPublishId,
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
                        .setPayload(request.toByteString())
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

        if (response.hasFlow()) {
            handleFlowControl(response.getFlow());
            return;
        }

        if (response.hasPong()) {
            return;
        }
    }

    private void handleFlowControl(FlowControlNotice flow) {
        this.flowBlocked = flow.getBlocked();
        if (flow.getBlocked()) {
            log.warn("[FLOW CONTROL] Publisher PAUSED by broker. Reason: {}", flow.getReason());
        } else {
            log.info("[FLOW CONTROL] Publisher RESUMED by broker. Reason: {}", flow.getReason());
        }
        try {
            options.eventListener().onFlowControl(flow.getBlocked(), flow.getReason(), flow.getTimestamp());
        } catch (Throwable ignored) {
        }
    }

    private void handleAck(PublishAck ack) {
        PublishReceipt receipt = new PublishReceipt(
                ack.getClientPublishId(),
                ack.getRoutedQueueCount()
        );
        pendingPublishes.complete(receipt);
        try {
            options.eventListener().onPublishAck(receipt);
        } catch (Throwable ignored) {
        }
    }

    private void handleNack(PublishNack nack) {
        com.volo.broker.client.BrokerRemoteException exception = GrpcErrorMapper.toException(nack.getError());
        pendingPublishes.fail(nack.getClientPublishId(), exception);
        try {
            options.eventListener().onPublishNack(nack.getClientPublishId(), exception);
        } catch (Throwable ignored) {
        }
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

    private void cancelKeepAlive() {
        ScheduledFuture<?> task = keepAliveTask;

        if (task != null) {
            task.cancel(false);
        }
    }

    private void sendPingSafely() {
        if (state.isClosed()) {
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
        sender.send(request);
    }

    private void failAllPending(Throwable throwable) {
        pendingPublishes.failAll(throwable);
    }

    private void ensureOpen() {
        state.ensureOpen("PublisherClient");
    }

    @Override
    public void close() {
        if (!state.closeOnce()) {
            return;
        }

        cancelKeepAlive();

        try {
            sender.send(PublisherRequest.newBuilder()
                    .setClose(ClosePublisherAction.newBuilder()
                            .setReason("client closed")
                            .build())
                    .build());
            sender.complete();
        } catch (Throwable ignored) {
        }

        failAllPending(new BrokerClientException("PublisherClient closed"));
    }
}