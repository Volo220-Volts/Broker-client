package com.volo.broker.client.internal;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class NamedThreadFactory implements ThreadFactory {
    private final String prefix;
    private final boolean daemon;
    private final AtomicInteger counter = new AtomicInteger();

    public NamedThreadFactory(String prefix, boolean daemon) {
        if (prefix == null || prefix.isBlank()) {
            throw new IllegalArgumentException("prefix is required");
        }

        this.prefix = prefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Objects.requireNonNull(runnable, "runnable is required");

        Thread thread = new Thread(
                runnable,
                prefix + "-" + counter.incrementAndGet()
        );

        thread.setDaemon(daemon);

        return thread;
    }
}