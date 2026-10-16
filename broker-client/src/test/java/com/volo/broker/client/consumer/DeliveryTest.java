package com.volo.broker.client.consumer;

import com.custombroker.grpc.Message;
import com.google.protobuf.ByteString;
import com.volo.broker.client.BrokerClientException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryTest {

    @Test
    void shouldReadZeroCopyPayloadAndMetadata() {
        byte[] rawBytes = "hello-world-payload".getBytes(StandardCharsets.UTF_8);
        Message message = Message.newBuilder()
                .setId(12345L)
                .setRoutingKey("orders.create")
                .setPayload(ByteString.copyFrom(rawBytes))
                .putHeaders("trace-id", "xyz-999")
                .build();

        Delivery.AckOperations ackOps = new Delivery.AckOperations() {
            @Override
            public void ack(long deliveryTag, long messageId) {}

            @Override
            public void nack(long deliveryTag, long messageId, boolean requeue) {}
        };

        Delivery delivery = new Delivery(
                1L,
                message,
                false,
                1,
                ConsumerAckMode.MANUAL,
                ackOps
        );

        assertThat(delivery.deliveryTag()).isEqualTo(1L);
        assertThat(delivery.messageId()).isEqualTo(12345L);
        assertThat(delivery.routingKey()).isEqualTo("orders.create");
        assertThat(delivery.headers()).containsEntry("trace-id", "xyz-999");
        assertThat(delivery.payloadSize()).isEqualTo(rawBytes.length);
        assertThat(delivery.payload()).isEqualTo(rawBytes);
        assertThat(delivery.payloadBytes().toByteArray()).isEqualTo(rawBytes);
        assertThat(delivery.completed()).isFalse();

        // Verify toString contains byte length, not byte array hashcode
        assertThat(delivery.toString()).contains("payloadSize=" + rawBytes.length + " bytes");
    }

    @Test
    void shouldAckSuccessfullyAndRejectSecondAck() {
        Message message = Message.newBuilder()
                .setId(10L)
                .setRoutingKey("test")
                .setPayload(ByteString.EMPTY)
                .build();

        AtomicBoolean ackCalled = new AtomicBoolean(false);
        Delivery.AckOperations ackOps = new Delivery.AckOperations() {
            @Override
            public void ack(long deliveryTag, long messageId) {
                ackCalled.set(true);
            }

            @Override
            public void nack(long deliveryTag, long messageId, boolean requeue) {}
        };

        Delivery delivery = new Delivery(1L, message, false, 1, ConsumerAckMode.MANUAL, ackOps);
        delivery.ack();

        assertThat(ackCalled.get()).isTrue();
        assertThat(delivery.completed()).isTrue();

        assertThatThrownBy(delivery::ack)
                .isInstanceOf(BrokerClientException.class)
                .hasMessageContaining("Delivery already completed");
    }

    @Test
    void shouldNackSuccessfullyAndRejectSubsequentNack() {
        Message message = Message.newBuilder()
                .setId(11L)
                .setRoutingKey("test")
                .setPayload(ByteString.EMPTY)
                .build();

        AtomicBoolean nackCalled = new AtomicBoolean(false);
        Delivery.AckOperations ackOps = new Delivery.AckOperations() {
            @Override
            public void ack(long deliveryTag, long messageId) {}

            @Override
            public void nack(long deliveryTag, long messageId, boolean requeue) {
                nackCalled.set(true);
            }
        };

        Delivery delivery = new Delivery(2L, message, false, 1, ConsumerAckMode.MANUAL, ackOps);
        delivery.nack(true);

        assertThat(nackCalled.get()).isTrue();
        assertThat(delivery.completed()).isTrue();

        assertThatThrownBy(() -> delivery.nack(false))
                .isInstanceOf(BrokerClientException.class)
                .hasMessageContaining("Delivery already completed");
    }
}

