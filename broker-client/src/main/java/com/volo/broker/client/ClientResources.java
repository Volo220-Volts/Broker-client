package com.volo.broker.client;

import com.volo.broker.client.internal.ClientExecutors;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public final class ClientResources implements AutoCloseable {
    private final ClientExecutors executors;

    private ClientResources(ClientExecutors executors) {
        this.executors = executors;
    }

    public static ClientResources create(BrokerClientConfig config) {
        Objects.requireNonNull(config, "config is required");

        return new ClientResources(new ClientExecutors(
                Math.max(1, config.keepAliveThreads()),
                Math.max(1, config.handlerThreads()),
                Math.max(1, config.reconnectThreads())
        ));
    }

    public ScheduledExecutorService scheduler() {
        return executors.keepAliveExecutor();
    }

    public ExecutorService handlerExecutor() {
        return executors.handlerExecutor();
    }

    public ExecutorService reconnectExecutor() {
        return executors.reconnectExecutor();
    }

    @Override
    public void close() {
        executors.close();
    }
}

