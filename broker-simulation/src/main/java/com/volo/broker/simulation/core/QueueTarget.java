package com.volo.broker.simulation.core;


public record QueueTarget(
        String queueName,
        int publishers,
        int consumers,
        int messagesPerPublisher,
        int consumerPrefetch,
        boolean durable,
        boolean waitForPublishAck,
        boolean waitForConsumerAck
) {
    public QueueTarget {
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("queueName is required");
        }
        if (publishers < 0) {
            throw new IllegalArgumentException("publishers must not be negative");
        }
        if (consumers < 0) {
            throw new IllegalArgumentException("consumers must not be negative");
        }
        if (messagesPerPublisher <= 0) {
            throw new IllegalArgumentException("messagesPerPublisher must be greater than zero");
        }
        if (consumerPrefetch <= 0) {
            consumerPrefetch = 50;
        }
    }

    public QueueTarget(
            String queueName,
            int publishers,
            int consumers,
            int messagesPerPublisher,
            int consumerPrefetch,
            boolean durable
    ) {
        this(queueName, publishers, consumers, messagesPerPublisher, consumerPrefetch, durable, true, true);
    }

    public long expectedMessages() {
        return (long) publishers * messagesPerPublisher;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String queueName;
        private int publishers = 1;
        private int consumers = 1;
        private int messagesPerPublisher = 1000;
        private int consumerPrefetch = 50;
        private boolean durable = true;
        private boolean waitForPublishAck = true;
        private boolean waitForConsumerAck = true;

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder publishers(int publishers) {
            this.publishers = publishers;
            return this;
        }

        public Builder consumers(int consumers) {
            this.consumers = consumers;
            return this;
        }

        public Builder messagesPerPublisher(int messagesPerPublisher) {
            this.messagesPerPublisher = messagesPerPublisher;
            return this;
        }

        public Builder consumerPrefetch(int consumerPrefetch) {
            this.consumerPrefetch = consumerPrefetch;
            return this;
        }

        public Builder durable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public Builder waitForPublishAck(boolean waitForPublishAck) {
            this.waitForPublishAck = waitForPublishAck;
            return this;
        }

        public Builder publishAck(boolean publishAck) {
            this.waitForPublishAck = publishAck;
            return this;
        }

        public Builder waitForConsumerAck(boolean waitForConsumerAck) {
            this.waitForConsumerAck = waitForConsumerAck;
            return this;
        }

        public Builder consumerAck(boolean consumerAck) {
            this.waitForConsumerAck = consumerAck;
            return this;
        }

        public Builder fireAndForget() {
            this.waitForPublishAck = false;
            return this;
        }

        public QueueTarget build() {
            return new QueueTarget(
                    queueName,
                    publishers,
                    consumers,
                    messagesPerPublisher,
                    consumerPrefetch,
                    durable,
                    waitForPublishAck,
                    waitForConsumerAck
            );
        }
    }
}
