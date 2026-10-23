package com.volo.broker.client;

import com.custombroker.grpc.ErrorCode;
import com.custombroker.grpc.ErrorInfo;
import com.volo.broker.client.internal.GrpcErrorMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerRemoteExceptionTest {

    @Test
    void shouldRecognizeResourceExhaustedErrorCode() {
        BrokerRemoteException ex = new BrokerRemoteException(
                ErrorCode.RESOURCE_EXHAUSTED,
                "Queue orders blocked too long",
                Map.of("queue", "orders")
        );

        assertThat(ex.errorCode()).isEqualTo(ErrorCode.RESOURCE_EXHAUSTED);
        assertThat(ex.isResourceExhausted()).isTrue();
        assertThat(ex.getMessage()).isEqualTo("Queue orders blocked too long");
        assertThat(ex.details()).containsEntry("queue", "orders");
    }

    @Test
    void shouldMapErrorInfoWithResourceExhausted() {
        ErrorInfo errorInfo = ErrorInfo.newBuilder()
                .setCode(ErrorCode.RESOURCE_EXHAUSTED)
                .setMessage("GlobalMemoryGuard active")
                .putDetails("reason", "heap_limit")
                .build();

        BrokerRemoteException ex = GrpcErrorMapper.toException(errorInfo);

        assertThat(ex.errorCode()).isEqualTo(ErrorCode.RESOURCE_EXHAUSTED);
        assertThat(ex.isResourceExhausted()).isTrue();
        assertThat(ex.details()).containsEntry("reason", "heap_limit");
    }

    @Test
    void shouldReturnFalseForOtherErrorCodes() {
        BrokerRemoteException ex = new BrokerRemoteException(
                ErrorCode.QUEUE_OVERFLOW,
                "Queue full",
                Map.of()
        );

        assertThat(ex.isResourceExhausted()).isFalse();
    }
}

