package com.volo.broker.simulation.config;

import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.simulation.adapter.custom.CustomBrokerAdapter;
import com.volo.broker.simulation.adapter.rabbitmq.RabbitMqAdapter;
import com.volo.broker.simulation.adapter.rabbitmq.RabbitMqConfig;
import com.volo.broker.simulation.core.BrokerSimulationAdapter;

import java.util.Objects;

public final class SimulationEnvironmentConfig {
    private final BrokerType activeBroker;
    private final BrokerClientConfig customBrokerConfig;
    private final RabbitMqConfig rabbitMqConfig;

    private SimulationEnvironmentConfig(
            BrokerType activeBroker,
            BrokerClientConfig customBrokerConfig,
            RabbitMqConfig rabbitMqConfig
    ) {
        this.activeBroker = Objects.requireNonNull(activeBroker, "activeBroker is required");
        this.customBrokerConfig = customBrokerConfig;
        this.rabbitMqConfig = rabbitMqConfig;
    }

    public BrokerType activeBroker() {
        return activeBroker;
    }

    public BrokerClientConfig customBrokerConfig() {
        return customBrokerConfig;
    }

    public RabbitMqConfig rabbitMqConfig() {
        return rabbitMqConfig;
    }

    public BrokerSimulationAdapter createAdapter() {
        return switch (activeBroker) {
            case CUSTOM -> {
                if (customBrokerConfig == null) {
                    throw new IllegalStateException("customBrokerConfig is not configured");
                }
                yield new CustomBrokerAdapter(customBrokerConfig);
            }
            case RABBITMQ -> {
                if (rabbitMqConfig == null) {
                    throw new IllegalStateException("rabbitMqConfig is not configured");
                }
                yield new RabbitMqAdapter(rabbitMqConfig);
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private BrokerType activeBroker = BrokerType.CUSTOM;
        private BrokerClientConfig customBrokerConfig;
        private RabbitMqConfig rabbitMqConfig;

        public Builder activeBroker(BrokerType activeBroker) {
            this.activeBroker = activeBroker;
            return this;
        }

        public Builder customBrokerConfig(BrokerClientConfig customBrokerConfig) {
            this.customBrokerConfig = customBrokerConfig;
            return this;
        }

        public Builder rabbitMqConfig(RabbitMqConfig rabbitMqConfig) {
            this.rabbitMqConfig = rabbitMqConfig;
            return this;
        }

        public SimulationEnvironmentConfig build() {
            return new SimulationEnvironmentConfig(activeBroker, customBrokerConfig, rabbitMqConfig);
        }
    }
}

