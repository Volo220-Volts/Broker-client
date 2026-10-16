package com.volo.broker.simulation.journal.model;

import com.volo.broker.simulation.config.BrokerType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record SimulationRunMetadata(
        String simulationId,
        BrokerType activeBroker,
        Instant startedAt,
        Map<String, Object> environment,
        Map<String, Object> brokerConnection,
        Map<String, Object> simulationParameters,
        List<QueueConfigSnapshot> queues
) {
    public SimulationRunMetadata {
        queues = queues == null ? List.of() : List.copyOf(queues);
        environment = environment == null ? Map.of() : Map.copyOf(environment);
        brokerConnection = brokerConnection == null ? Map.of() : Map.copyOf(brokerConnection);
        simulationParameters = simulationParameters == null ? Map.of() : Map.copyOf(simulationParameters);
    }
}

