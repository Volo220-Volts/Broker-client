package com.volo.broker.client.publisher;

import java.util.concurrent.atomic.AtomicLong;

public final class ClientPublishIdGenerator {
    private final AtomicLong value = new AtomicLong();

    public long nextId() {
        return value.incrementAndGet();
    }
}