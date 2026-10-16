package com.volo.broker.simulation;

import com.volo.broker.client.BrokerClientConfig;
import com.volo.broker.simulation.adapter.rabbitmq.RabbitMqConfig;
import com.volo.broker.simulation.config.BrokerType;
import com.volo.broker.simulation.config.SimulationEnvironmentConfig;
import com.volo.broker.simulation.core.*;
import com.volo.broker.simulation.report.SimulationStatsPrinter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class SimulationApp {
    public static void main(String[] args) {
        int cores = Runtime.getRuntime().availableProcessors();

        BrokerClientConfig customBrokerConfig = BrokerClientConfig.builder()
                .host("localhost")
                .port(9090)
                .plaintext(true)
                .keepAliveThreads(Math.max(2, cores / 2))
                .handlerThreads(cores)
                .reconnectThreads(cores)
                .build();

        RabbitMqConfig rabbitMqConfig = RabbitMqConfig.of("localhost", 5672);

        SimulationEnvironmentConfig envConfig = SimulationEnvironmentConfig.builder()
                .activeBroker(BrokerType.CUSTOM)
                .customBrokerConfig(customBrokerConfig)
                .rabbitMqConfig(rabbitMqConfig)
                .build();

        List<QueueTarget> queues = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            queues.add(QueueTarget.builder()
                    .queueName("orders.created." + i)
                    .publishers(20)
                    .consumers(10)
                    .messagesPerPublisher(500)
                    .consumerPrefetch(500)
                    .durable(true)
                    .waitForPublishAck(true)
                    .waitForConsumerAck(true)
                    .build());
        }

        SimulationConfig simConfig = SimulationConfig.builder()
                .queues(queues)
                .payloadSizeBytes(1024 *10)
                .publishingOptions(new PublishingOptions(64, 32))
                .consumerStartupDelay(Duration.ofSeconds(1))
                .consumptionTimeout(Duration.ofSeconds(60))
                .showProgressBar(true)
                .build();

        System.out.println("Starting simulation for: " + envConfig.activeBroker());
        BrokerSimulation simulation = new BrokerSimulation(simConfig, envConfig);
        SimulationStats stats = simulation.run();

        SimulationStatsPrinter.print(stats, simConfig.expectedMessages());
    }
}

