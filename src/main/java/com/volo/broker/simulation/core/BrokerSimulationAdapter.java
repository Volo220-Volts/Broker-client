package com.volo.broker.simulation.core;

import java.util.List;

public interface BrokerSimulationAdapter extends AutoCloseable {
    void initialize();

    void declareQueues(List<QueueTarget> queues);

    AutoCloseable startConsumer(String queueName, int prefetchCount, SimulatedMessageHandler handler);

    SimulatedPublisher createPublisher();

    @Override
    void close();
}

