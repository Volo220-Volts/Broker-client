package com.volo.broker.client.consumer;

import java.util.Objects;

public record ConsumerOptions(
        String queuePath,
        int prefetchCount,
        ConsumerAckMode ackMode,
        boolean requeueOnHandlerError,
        int handlerThreads,
        MessageHandler handler,
        ConsumerEventListener eventListener
) {
    public ConsumerOptions {
        if (queuePath == null || queuePath.isBlank()) {
            throw new IllegalArgumentException("queuePath is required");
        }

        if (prefetchCount <= 0) {
            prefetchCount = 10;
        }

        ackMode = ackMode == null ? ConsumerAckMode.MANUAL : ackMode;

        if (handlerThreads <= 0) {
            handlerThreads = 1;
        }

        Objects.requireNonNull(handler, "handler is required");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String queuePath;
        private int prefetchCount = 10;
        private ConsumerAckMode ackMode = ConsumerAckMode.MANUAL;
        private boolean requeueOnHandlerError = true;
        private int handlerThreads = 1;
        private MessageHandler handler;
        private ConsumerEventListener eventListener = new ConsumerEventListener() {};

        public Builder queuePath(String queuePath) {
            this.queuePath = queuePath;
            return this;
        }

        public Builder prefetchCount(int prefetchCount) {
            this.prefetchCount = prefetchCount;
            return this;
        }

        public Builder manualAck() {
            this.ackMode = ConsumerAckMode.MANUAL;
            return this;
        }

        public Builder autoAck() {
            this.ackMode = ConsumerAckMode.AUTO;
            return this;
        }

        public Builder noAck() {
            this.ackMode = ConsumerAckMode.NONE;
            return this;
        }

        public Builder requeueOnHandlerError(boolean value) {
            this.requeueOnHandlerError = value;
            return this;
        }

        public Builder handlerThreads(int handlerThreads) {
            this.handlerThreads = handlerThreads;
            return this;
        }

        public Builder handler(MessageHandler handler) {
            this.handler = handler;
            return this;
        }

        public Builder eventListener(ConsumerEventListener eventListener){
            this.eventListener = eventListener;
            return this;
        }

        public ConsumerOptions build() {
            return new ConsumerOptions(
                    queuePath,
                    prefetchCount,
                    ackMode,
                    requeueOnHandlerError,
                    handlerThreads,
                    handler,
                    eventListener
            );
        }
    }
}