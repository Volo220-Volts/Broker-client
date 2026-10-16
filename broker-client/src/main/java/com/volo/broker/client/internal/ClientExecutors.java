package com.volo.broker.client.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import java.util.concurrent.TimeUnit;

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
        keepAliveExecutor.shutdown();
        handlerExecutor.shutdown();
        reconnectExecutor.shutdown();

        boolean interrupted = false;
        try {
            if (!handlerExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                handlerExecutor.shutdownNow();
            }
            if (!keepAliveExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                keepAliveExecutor.shutdownNow();
            }
            if (!reconnectExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                reconnectExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            interrupted = true;
            handlerExecutor.shutdownNow();
            keepAliveExecutor.shutdownNow();
            reconnectExecutor.shutdownNow();
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}