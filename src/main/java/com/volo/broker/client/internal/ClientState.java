package com.volo.broker.client.internal;

import com.volo.broker.client.BrokerClientException;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ClientState {
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public void markStarted() {
        if (!started.compareAndSet(false, true)) {
            throw new BrokerClientException("Client already started");
        }
    }

    public void ensureStarted() {
        if (!started.get()) {
            throw new BrokerClientException("Client is not started");
        }
    }

    public void ensureOpen(String clientName) {
        if (closed.get()) {
            throw new BrokerClientException(clientName + " is closed");
        }
    }

    public boolean closeOnce() {
        return closed.compareAndSet(false, true);
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean isStarted() {
        return started.get();
    }
}