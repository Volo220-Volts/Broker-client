package com.volo.broker.client.publisher;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublishRequestTest {

    @Test
    void shouldAllowEmptyPayloadForSignalEvents() {
        PublishRequest request = new PublishRequest("signals.heartbeat", new byte[0]);

        assertThat(request.routingKey()).isEqualTo("signals.heartbeat");
        assertThat(request.payload()).isEmpty();
        assertThat(request.toByteString().isEmpty()).isTrue();
    }

    @Test
    void shouldValidateRoutingKey() {
        assertThatThrownBy(() -> new PublishRequest("", new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new PublishRequest(null, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNullPayload() {
        assertThatThrownBy(() -> new PublishRequest("orders", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload must not be null");
    }

    @Test
    void shouldPreserveHeadersAndByteString() {
        byte[] bytes = "test-payload".getBytes(StandardCharsets.UTF_8);
        PublishRequest request = new PublishRequest(
                "orders.create",
                bytes,
                Map.of("X-Correlation-Id", "abc-123")
        );

        assertThat(request.headers()).containsEntry("X-Correlation-Id", "abc-123");
        assertThat(request.toByteString().toByteArray()).isEqualTo(bytes);
    }
}

