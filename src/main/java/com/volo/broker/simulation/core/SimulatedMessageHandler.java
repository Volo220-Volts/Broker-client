package com.volo.broker.simulation.core;

@FunctionalInterface
public interface SimulatedMessageHandler {
    void handle(long latencyNs, int payloadSizeBytes);
}

