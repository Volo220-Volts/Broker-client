package com.volo.broker.simulation.core;

public interface PublishMetrics {
    void published(String queuePath, long publishAckLatencyNs, long payloadBytes);

    void publishFailed(String queuePath);
}

