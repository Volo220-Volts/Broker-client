package com.volo.broker.client;

import java.time.Duration;
import java.util.Objects;

public record BrokerClientConfig(
        String host,
        int port,
        boolean plaintext,
        Duration keepAliveInterval,
        Duration publishResponseTimeout,
        Duration shutdownTimeout,
        int maxInboundMessageSizeBytes,
        int keepAliveThreads,
        int handlerThreads,
        int reconnectThreads
) {
    private static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 16 * 1024 * 1024;

    public BrokerClientConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host is required");
        }

        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("port is invalid");
        }

        keepAliveInterval = requirePositive(keepAliveInterval, "keepAliveInterval");
        publishResponseTimeout = requirePositive(publishResponseTimeout, "publishResponseTimeout");
        shutdownTimeout = requirePositive(shutdownTimeout, "shutdownTimeout");

        if (maxInboundMessageSizeBytes <= 0) {
            throw new IllegalArgumentException("maxInboundMessageSizeBytes must be greater than zero");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static BrokerClientConfig plaintext(String host, int port) {
        return builder()
                .host(host)
                .port(port)
                .plaintext(true)
                .build();
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name + " is required");

        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }

        return value;
    }

    public static final class Builder {
        private String host = "localhost";
        private int port = 9090;
        private boolean plaintext = true;
        private Duration keepAliveInterval = Duration.ofSeconds(10);
        private Duration publishResponseTimeout = Duration.ofSeconds(30);
        private Duration shutdownTimeout = Duration.ofSeconds(5);
        private int maxInboundMessageSizeBytes = DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
        private int keepAliveThreads = 2;
        private int handlerThreads = 2;
        private int reconnectThreads = 2;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder plaintext(boolean plaintext) {
            this.plaintext = plaintext;
            return this;
        }

        public Builder keepAliveInterval(Duration keepAliveInterval) {
            this.keepAliveInterval = keepAliveInterval;
            return this;
        }

        public Builder publishResponseTimeout(Duration publishResponseTimeout) {
            this.publishResponseTimeout = publishResponseTimeout;
            return this;
        }

        public Builder shutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        public Builder maxInboundMessageSizeBytes(int maxInboundMessageSizeBytes) {
            this.maxInboundMessageSizeBytes = maxInboundMessageSizeBytes;
            return this;
        }

        public Builder keepAliveThreads(int keepAliveThreads){
            this.keepAliveThreads = keepAliveThreads;
            return this;
        }

        public Builder handlerThreads(int handlerThreads){
            this.handlerThreads = handlerThreads;
            return this;
        }
        public Builder reconnectThreads(int reconnectThreads){
            this.reconnectThreads = reconnectThreads;
            return this;
        }

        public BrokerClientConfig build() {
            return new BrokerClientConfig(
                    host,
                    port,
                    plaintext,
                    keepAliveInterval,
                    publishResponseTimeout,
                    shutdownTimeout,
                    maxInboundMessageSizeBytes,
                    keepAliveThreads,
                    handlerThreads,
                    reconnectThreads
            );
        }
    }
}