package com.volo.broker.client.consumer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsumerOptionsTest {

    @Test
    void shouldBuildWithDefaults() {
        ConsumerOptions options = ConsumerOptions.builder()
                .queuePath("orders.inbox")
                .handler(delivery -> {})
                .build();

        assertThat(options.queuePath()).isEqualTo("orders.inbox");
        assertThat(options.prefetchCount()).isEqualTo(10);
        assertThat(options.ackMode()).isEqualTo(ConsumerAckMode.MANUAL);
        assertThat(options.requeueOnHandlerError()).isTrue();
        assertThat(options.maxDeliveryAttempts()).isEqualTo(3);
        assertThat(options.handlerThreads()).isEqualTo(1);
        assertThat(options.eventListener()).isNotNull();
    }

    @Test
    void shouldCustomiseOptionsViaBuilder() {
        ConsumerEventListener listener = new ConsumerEventListener() {};
        ConsumerOptions options = ConsumerOptions.builder()
                .queuePath("events.audit")
                .prefetchCount(50)
                .autoAck()
                .requeueOnHandlerError(false)
                .maxDeliveryAttempts(5)
                .handlerThreads(4)
                .eventListener(listener)
                .handler(delivery -> {})
                .build();

        assertThat(options.prefetchCount()).isEqualTo(50);
        assertThat(options.ackMode()).isEqualTo(ConsumerAckMode.AUTO);
        assertThat(options.requeueOnHandlerError()).isFalse();
        assertThat(options.maxDeliveryAttempts()).isEqualTo(5);
        assertThat(options.handlerThreads()).isEqualTo(4);
        assertThat(options.eventListener()).isSameAs(listener);
    }

    @Test
    void shouldEnforceRequiredFields() {
        assertThatThrownBy(() -> ConsumerOptions.builder().handler(delivery -> {}).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queuePath is required");

        assertThatThrownBy(() -> ConsumerOptions.builder().queuePath("orders").build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("handler is required");
    }
}

