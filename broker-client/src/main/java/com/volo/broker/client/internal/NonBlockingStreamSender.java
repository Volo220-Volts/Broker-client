package com.volo.broker.client.internal;

import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public final class NonBlockingStreamSender<T> {
    private final StreamObserver<T> target;
    private final Queue<T> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);

    public NonBlockingStreamSender(StreamObserver<T> target) {
        this.target = Objects.requireNonNull(target, "target is required");
    }

    public void send(T message) {
        Objects.requireNonNull(message, "message is required");
        if (completed.get()) {
            return;
        }
        queue.offer(message);
        drain();
    }

    public void drain() {
        if (!draining.compareAndSet(false, true)) {
            return;
        }

        try {
            T item;
            while ((item = queue.poll()) != null) {
                target.onNext(item);
            }
        } finally {
            draining.set(false);
            if (!queue.isEmpty()) {
                drain();
            }
        }
    }

    public void complete() {
        if (!completed.compareAndSet(false, true)) {
            return;
        }
        drain();
        try {
            target.onCompleted();
        } catch (Throwable ignored) {
        }
    }

    public boolean isCompleted() {
        return completed.get();
    }
}
