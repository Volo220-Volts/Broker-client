package com.volo.broker.simulation.core;

import java.util.concurrent.CompletableFuture;

public interface SimulatedPublisher extends AutoCloseable {
    CompletableFuture<Void> publish(PublishMessage message);

    @Override
    void close();
}

