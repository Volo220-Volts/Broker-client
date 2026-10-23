package com.volo.broker.simulation.journal.model;

public record QueueConfigSnapshot(
        int queueIndex,
        String queueName,
        int publishers,
        int consumers,
        int messagesPerPublisher,
        long expectedMessages,
        long expectedBytes,
        int consumerPrefetch,
        boolean durable,
        String storageMode,
        boolean waitForPublishAck,
        boolean waitForConsumerAck,
        boolean flowControlEnabled,
        int highWatermarkMessages,
        int lowWatermarkMessages,
        long maxBlockMs
) {
    public QueueConfigSnapshot(
            int queueIndex,
            String queueName,
            int publishers,
            int consumers,
            int messagesPerPublisher,
            long expectedMessages,
            long expectedBytes,
            int consumerPrefetch,
            boolean durable,
            String storageMode,
            boolean waitForPublishAck,
            boolean waitForConsumerAck
    ) {
        this(queueIndex, queueName, publishers, consumers, messagesPerPublisher,
                expectedMessages, expectedBytes, consumerPrefetch, durable, storageMode,
                waitForPublishAck, waitForConsumerAck, false, 0, 0, 0);
    }

    public QueueConfigSnapshot(
            int queueIndex,
            String queueName,
            int publishers,
            int consumers,
            int messagesPerPublisher,
            long expectedMessages,
            long expectedBytes,
            int consumerPrefetch,
            boolean durable,
            String storageMode
    ) {
        this(queueIndex, queueName, publishers, consumers, messagesPerPublisher,
                expectedMessages, expectedBytes, consumerPrefetch, durable, storageMode,
                true, true, false, 0, 0, 0);
    }

    public String ackMode() {
        if (!waitForPublishAck && !waitForConsumerAck) {
            return "FIRE_AND_FORGET";
        }
        if (!waitForPublishAck) {
            return "PUB_NO_ACK";
        }
        if (!waitForConsumerAck) {
            return "CONS_NO_ACK";
        }
        return "TRACKED";
    }
}
