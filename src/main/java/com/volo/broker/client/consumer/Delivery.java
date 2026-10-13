package com.volo.broker.client.consumer;

import com.custombroker.grpc.Message;
import com.volo.broker.client.BrokerClientException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Delivery {
    private final long deliveryTag;
    private final long messageId;
    private final byte[] payload;
    private final String routingKey;
    private final Map<String, String> headers;
    private final boolean redelivered;
    private final int deliveryAttempt;
    private final AckOperations ackOperations;
    private final ConsumerAckMode ackMode;

    private final AtomicBoolean completed = new AtomicBoolean(false);

    public Delivery(
            long deliveryTag,
            Message message,
            boolean redelivered,
            int deliveryAttempt,
            ConsumerAckMode ackMode,
            AckOperations ackOperations
    ) {
        this.deliveryTag = deliveryTag;
        this.messageId = message.getId();
        this.payload = message.getPayload().toByteArray();
        this.routingKey = message.getRoutingKey();
        this.headers = Map.copyOf(message.getHeadersMap());
        this.redelivered = redelivered;
        this.deliveryAttempt = deliveryAttempt;
        this.ackMode = ackMode == null ? ConsumerAckMode.MANUAL : ackMode;
        this.ackOperations = Objects.requireNonNull(ackOperations, "ackOperations is required");
    }

    public void ack() {
        ensureAckAllowed();

        if (!completed.compareAndSet(false, true)) {
            throw new BrokerClientException("Delivery already completed");
        }

        ackOperations.ack(deliveryTag, messageId);
    }

    public void nack(boolean requeue) {
        ensureAckAllowed();

        if (!completed.compareAndSet(false, true)) {
            throw new BrokerClientException("Delivery already completed");
        }

        ackOperations.nack(deliveryTag, messageId, requeue);
    }

    public boolean completed() {
        return completed.get();
    }

    public long deliveryTag() {
        return deliveryTag;
    }

    public long messageId() {
        return messageId;
    }

    public byte[] payload() {
        return payload.clone();
    }

    public int payloadSize() {
        return payload.length;
    }

    public String routingKey() {
        return routingKey;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public boolean redelivered() {
        return redelivered;
    }

    public int deliveryAttempt() {
        return deliveryAttempt;
    }

    private void ensureAckAllowed() {
        if (ackMode == ConsumerAckMode.NONE) {
            throw new BrokerClientException("Ack/Nack is disabled for this consumer");
        }
    }

    public interface AckOperations {
        void ack(long deliveryTag, long messageId);

        void nack(long deliveryTag, long messageId, boolean requeue);
    }

    @Override
    public String toString() {
        return "Delivery [deliveryTag=" + deliveryTag + ", messageId=" + messageId + ", payload="
                + payload.toString() + ", routingKey=" + routingKey + ", headers=" + headers + ", redelivered="
                + redelivered + ", deliveryAttempt=" + deliveryAttempt + ", ackOperations=" + ackOperations
                + ", ackMode=" + ackMode + ", completed=" + completed + "]";
    }
}