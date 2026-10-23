package com.volo.broker.client.publisher;

import com.custombroker.grpc.*;
import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.client.BrokerRemoteException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PublisherClientFlowControlTest {

    private PublisherServiceGrpc.PublisherServiceStub stub;
    private ScheduledExecutorService scheduler;
    private StreamObserver<PublisherResponse> responseObserver;
    private AtomicBoolean flowBlockedEvent;
    private AtomicReference<String> flowReasonEvent;
    private PublisherClient client;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        stub = mock(PublisherServiceGrpc.PublisherServiceStub.class);
        scheduler = Executors.newSingleThreadScheduledExecutor();

        StreamObserver<PublisherRequest> requestObserver = mock(StreamObserver.class);
        ArgumentCaptor<StreamObserver<PublisherResponse>> captor = ArgumentCaptor.forClass(StreamObserver.class);
        when(stub.openPublisherChannel(captor.capture())).thenReturn(requestObserver);

        flowBlockedEvent = new AtomicBoolean(false);
        flowReasonEvent = new AtomicReference<>();

        PublisherEventListener listener = new PublisherEventListener() {
            @Override
            public void onFlowControl(boolean blocked, String reason, long timestamp) {
                flowBlockedEvent.set(blocked);
                flowReasonEvent.set(reason);
            }
        };

        PublisherOptions options = new PublisherOptions(listener);
        BrokerClientConfig config = BrokerClientConfig.builder().host("localhost").port(9090).build();

        client = new PublisherClient(stub, config, scheduler, options);
        responseObserver = captor.getValue();
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Test
    void shouldHandleFlowControlNoticeBlockedAndResumed() {
        assertThat(client.isFlowBlocked()).isFalse();

        // 1. Simulate Broker sending FlowControlNotice (blocked = true)
        responseObserver.onNext(PublisherResponse.newBuilder()
                .setFlow(FlowControlNotice.newBuilder()
                        .setBlocked(true)
                        .setReason("QUEUE_WATERMARK:[orders.created]")
                        .setTimestamp(1000L)
                        .build())
                .build());

        assertThat(client.isFlowBlocked()).isTrue();
        assertThat(flowBlockedEvent.get()).isTrue();
        assertThat(flowReasonEvent.get()).isEqualTo("QUEUE_WATERMARK:[orders.created]");

        // 2. Simulate Broker sending FlowControlNotice (blocked = false / resumed)
        responseObserver.onNext(PublisherResponse.newBuilder()
                .setFlow(FlowControlNotice.newBuilder()
                        .setBlocked(false)
                        .setReason("OPEN")
                        .setTimestamp(2000L)
                        .build())
                .build());

        assertThat(client.isFlowBlocked()).isFalse();
        assertThat(flowBlockedEvent.get()).isFalse();
        assertThat(flowReasonEvent.get()).isEqualTo("OPEN");
    }

    @Test
    void shouldHandleNackWithResourceExhausted() {
        CompletableFuture<PublishReceipt> future = client.publish(
                PublishRequest.of("orders.created", new byte[]{1, 2, 3})
        );

        // Simulate Broker rejecting with RESOURCE_EXHAUSTED
        responseObserver.onNext(PublisherResponse.newBuilder()
                .setNack(PublishNack.newBuilder()
                        .setClientPublishId(1L)
                        .setError(ErrorInfo.newBuilder()
                                .setCode(ErrorCode.RESOURCE_EXHAUSTED)
                                .setMessage("Queue blocked timeout exceeded")
                                .build())
                        .build())
                .build());

        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(BrokerRemoteException.class)
                .satisfies(thrown -> {
                    BrokerRemoteException bre = (BrokerRemoteException) thrown.getCause();
                    assertThat(bre.isResourceExhausted()).isTrue();
                    assertThat(bre.errorCode()).isEqualTo(ErrorCode.RESOURCE_EXHAUSTED);
                });
    }
}
