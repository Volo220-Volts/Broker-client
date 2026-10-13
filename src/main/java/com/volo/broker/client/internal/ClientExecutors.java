package com.volo.broker.client.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public final class ClientExecutors implements AutoCloseable {
    private final ScheduledExecutorService keepAliveExecutor;
    private final ExecutorService handlerExecutor;
    private final ExecutorService reconnectExecutor;

    public ClientExecutors(
            int keepAliveThreads,
            int handlerThreads,
            int reconnectThreads
    ) {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(
                keepAliveThreads,
                new NamedThreadFactory("broker-client-keepalive", true)
        );
        scheduler.setRemoveOnCancelPolicy(true);
        this.keepAliveExecutor = scheduler;

        this.handlerExecutor = Executors.newFixedThreadPool(
                handlerThreads,
                new NamedThreadFactory("broker-client-handler", true)
        );

        this.reconnectExecutor = Executors.newFixedThreadPool(
                reconnectThreads,
                new NamedThreadFactory("broker-client-reconnect", true)
        );
    }

    public ScheduledExecutorService keepAliveExecutor() {
        return keepAliveExecutor;
    }

    public ExecutorService handlerExecutor() {
        return handlerExecutor;
    }

    public ExecutorService reconnectExecutor() {
        return reconnectExecutor;
    }

    @Override
    public void close() {
        keepAliveExecutor.shutdownNow();
        handlerExecutor.shutdownNow();
        reconnectExecutor.shutdownNow();
    }
}