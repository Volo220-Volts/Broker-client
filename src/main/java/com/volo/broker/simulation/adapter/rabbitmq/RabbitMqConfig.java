package com.volo.broker.simulation.adapter.rabbitmq;

public record RabbitMqConfig(
        String host,
        int port,
        String virtualHost,
        String username,
        String password
) {
    public RabbitMqConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host is required");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("port is invalid");
        }
        if (virtualHost == null || virtualHost.isBlank()) {
            virtualHost = "/";
        }
        if (username == null || username.isBlank()) {
            username = "guest";
        }
        if (password == null) {
            password = "guest";
        }
    }

    public static RabbitMqConfig defaultLocal() {
        return new RabbitMqConfig("localhost", 5672, "/", "guest", "guest");
    }

    public static RabbitMqConfig of(String host, int port) {
        return new RabbitMqConfig(host, port, "/", "guest", "guest");
    }
}

