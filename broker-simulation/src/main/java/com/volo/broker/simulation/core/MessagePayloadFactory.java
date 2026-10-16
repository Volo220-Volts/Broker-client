package com.volo.broker.simulation.core;

import java.nio.ByteBuffer;

public final class MessagePayloadFactory {
    public static final String HEADER_PUBLISHED_AT_NS = "x-published-at-ns";
    public static final String HEADER_QUEUE_PATH = "x-queue-path";
    public static final String HEADER_PUBLISHER_INDEX = "x-publisher-index";
    public static final String HEADER_MESSAGE_INDEX = "x-message-index";

    private final int payloadSizeBytes;

    public MessagePayloadFactory(int payloadSizeBytes) {
        if (payloadSizeBytes <= 0) {
            throw new IllegalArgumentException("payloadSizeBytes must be greater than zero");
        }
        this.payloadSizeBytes = payloadSizeBytes;
    }

    public byte[] createPayload(String queueName, int publisherIndex, int messageIndex) {
        byte[] payload = new byte[payloadSizeBytes];

        if (payload.length >= Integer.BYTES * 3) {
            ByteBuffer.wrap(payload)
                    .putInt(queueName.hashCode())
                    .putInt(publisherIndex)
                    .putInt(messageIndex);
        }

        return payload;
    }

    public int payloadSizeBytes() {
        return payloadSizeBytes;
    }
}

