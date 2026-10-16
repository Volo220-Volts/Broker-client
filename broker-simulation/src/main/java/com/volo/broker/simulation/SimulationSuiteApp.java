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

public class SimulationSuiteApp {

    // ====================================================================================
    // >>> SELECT TARGET BROKER HERE <<<
    // Set to BrokerType.CUSTOM or BrokerType.RABBITMQ to execute the simulation suite
    // ====================================================================================
    public static final BrokerType SELECTED_BROKER = BrokerType.RABBITMQ;

    // Set to "ALL" to run all 12 scenarios sequentially, or specify a scenario prefix (e.g. "S1", "S2", "S4")
    public static final String SCENARIO_FILTER = "S7";

    public static Duration benchDuration = Duration.ZERO;

    public static double totalSimDuration = 0L;


    public record BenchmarkScenario(
            String name,
            String description,
            SimulationConfig config
    ) {
    }

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
                .activeBroker(SELECTED_BROKER)
                .customBrokerConfig(customBrokerConfig)
                .rabbitMqConfig(rabbitMqConfig)
                .build();

        List<BenchmarkScenario> suite = buildBenchmarkSuite();
        List<BenchmarkScenario> scenariosToRun = suite.stream()
                .filter(s -> "ALL".equalsIgnoreCase(SCENARIO_FILTER) || s.name().toUpperCase().startsWith(SCENARIO_FILTER.toUpperCase()))
                .toList();

        System.out.println();
        System.out.println("================================================================================");
        System.out.printf("  STARTING BENCHMARK SUITE FOR: %s (%d scenarios to execute)%n", SELECTED_BROKER, scenariosToRun.size());
        System.out.println("================================================================================");

        int index = 1;
        for (BenchmarkScenario scenario : scenariosToRun) {
            System.out.println();
            System.out.printf("[%d/%d] >>> Starting simulation: \"%s\"%n", index, scenariosToRun.size(), scenario.name());
            System.out.printf("      Description: %s%n", scenario.description());
            System.out.printf("      Config: %d queues | %,d expected msgs | payload: %,d bytes%n",
                    scenario.config().queues().size(),
                    scenario.config().expectedMessages(),
                    scenario.config().payloadSizeBytes());

            long startNs = System.nanoTime();
            BrokerSimulation simulation = new BrokerSimulation(scenario.config(), envConfig, scenario.name());
            SimulationStats stats = simulation.run();
            long elapsedNs = System.nanoTime() - startNs;

            double durationSec = elapsedNs / 1_000_000_000.0;
            totalSimDuration += durationSec;

            System.out.printf("<<< Finished simulation: \"%s\" in %.2f s%n", scenario.name(), durationSec);

           benchDuration = benchDuration.plus(stats.duration());

            printSummary(stats, scenario.name(), scenario.config().expectedMessages());
            index++;

            // Brief pause between benchmark scenarios to allow socket cleanup and GC stabilization
            try {
                Thread.sleep(1500);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                break;
            }
        }


        System.out.println();
        System.out.println("================================================================================");
        System.out.printf("  BENCHMARK SUITE COMPLETED FOR: %s%n", SELECTED_BROKER);
        System.out.printf("  Total benchmark duration: %.2f s%n", benchDuration.toMillis() / 1_000.0);
        System.out.printf("  Total simulation duration: %.2f s%n", totalSimDuration);
        System.out.println("================================================================================");

    }   

    public static void printSummary(SimulationStats stats, String simulationName, long expectedMessages) {
        SimulationStatsPrinter.printSummary(stats, simulationName, expectedMessages);
    }

    private static List<BenchmarkScenario> buildBenchmarkSuite() {
        List<BenchmarkScenario> scenarios = new ArrayList<>();

        // S1: Baseline Ping-Pong (1 queue, 1 pub, 1 cons, 5k msg, 1 KiB, durable=false, ack=true, prefetch=1)
        scenarios.add(new BenchmarkScenario(
                "S1_Baseline_PingPong",
                "Socket latency floor without thread contention (1 queue, 1 pub, 1 cons, prefetch=1)",
                buildConfig(createQueues("orders.s1.", 1, 1, 1, 5_000, 1, false, true, true), 1024, 64, 32, Duration.ofSeconds(30))
        ));

        // S2: Max Raw Throughput (10 queues, 25 pub, 15 cons, 250k msg, 1 KiB, durable=false, ack=false, prefetch=500)
        scenarios.add(new BenchmarkScenario(
                "S2_MaxRawThroughput",
                "Maximum fire-and-forget network throughput (10 queues, 250 pub threads, ack=false, prefetch=500)",
                buildConfig(createQueues("orders.s2.", 10, 25, 15, 1_000, 500, false, false, false), 1024, 64, 32, Duration.ofSeconds(60))
        ));

        // S3: Production Baseline (10 queues, 20 pub, 10 cons, 100k msg, 4 KiB, durable=true, ack=true, prefetch=50)
        scenarios.add(new BenchmarkScenario(
                "S3_ProductionBaseline",
                "Standard production workload with ACK and disk persistence (10 queues, 4 KiB payload, prefetch=50)",
                buildConfig(createQueues("orders.s3.", 10, 20, 10, 500, 50, true, true, true), 4096, 64, 32, Duration.ofSeconds(60))
        ));

        // S4.1: Persistence Penalty - RAM (5 queues, 20 pub, 10 cons, 50k msg, 10 KiB, durable=false, ack=true, prefetch=100)
        scenarios.add(new BenchmarkScenario(
                "S4_1_PersistencePenalty_RAM",
                "RAM-only reference run for disk penalty measurement (5 queues, 10 KiB payload, durable=false)",
                buildConfig(createQueues("orders.s4_1.", 5, 20, 10, 500, 100, false, true, true), 10 * 1024, 64, 32, Duration.ofSeconds(60))
        ));

        // S4.2: Persistence Penalty - Disk (5 queues, 20 pub, 10 cons, 50k msg, 10 KiB, durable=true, ack=true, prefetch=100)
        scenarios.add(new BenchmarkScenario(
                "S4_2_PersistencePenalty_Disk",
                "Disk persistence run for disk penalty measurement (5 queues, 10 KiB payload, durable=true)",
                buildConfig(createQueues("orders.s4_2.", 5, 20, 10, 500, 100, true, true, true), 10 * 1024, 64, 32, Duration.ofSeconds(60))
        ));

        // S5.1: Small Framing Stress (10 queues, 20 pub, 20 cons, 200k msg, 256 B, durable=false, ack=true, prefetch=200)
        scenarios.add(new BenchmarkScenario(
                "S5_1_SmallFramingStress",
                "Event-loop and protocol framing stress with small packets (10 queues, 200k msgs, 256 B payload)",
                buildConfig(createQueues("orders.s5_1.", 10, 20, 20, 1_000, 200, false, true, true), 256, 64, 32, Duration.ofSeconds(60))
        ));

        // S5.2: Large Payload Bandwidth (2 queues, 5 pub, 5 cons, 5k msg, 1 MiB, durable=false, ack=true, prefetch=10)
        scenarios.add(new BenchmarkScenario(
                "S5_2_LargePayloadBandwidth",
                "Throughput bandwidth saturation and memory copying test (2 queues, 5k msgs = 5 GB, 1 MiB payload)",
                buildConfig(createQueues("orders.s5_2.", 2, 5, 5, 250, 10, false, true, true), 1024 * 1024, 32, 16, Duration.ofSeconds(120))
        ));

        // S6.1: Hotspot Contention (1 queue, 50 pub, 20 cons, 100k msg, 2 KiB, durable=false, ack=true, prefetch=100)
        scenarios.add(new BenchmarkScenario(
                "S6_1_HotspotContention_1Q",
                "Single-queue synchronization and lock contention test (1 queue, 50 pub threads, 20 cons threads)",
                buildConfig(createQueues("orders.s6_1.", 1, 50, 20, 2_000, 100, false, true, true), 2048, 64, 32, Duration.ofSeconds(60))
        ));

        // S6.2: Multi-Queue Scale (20 queues, 5 pub, 2 cons, 100k msg, 2 KiB, durable=false, ack=true, prefetch=100)
        scenarios.add(new BenchmarkScenario(
                "S6_2_MultiQueueScale_20Q",
                "Scaling and lock striping across 20 independent queues (20 queues, 100 pub threads total)",
                buildConfig(createQueues("orders.s6_2.", 20, 5, 2, 1_000, 100, false, true, true), 2048, 64, 32, Duration.ofSeconds(60))
        ));

        // S7.1: Prefetch Minimal (5 queues, 10 pub, 5 cons, 50k msg, 2 KiB, durable=false, ack=true, prefetch=1)
        scenarios.add(new BenchmarkScenario(
                "S7_1_PrefetchMinimal_1",
                "Sensitivity test for minimal prefetch buffer (5 queues, prefetch=1, high roundtrip ACK impact)",
                buildConfig(createQueues("orders.s7_1.", 5, 10, 5, 1_000, 1, false, true, true), 2048, 64, 32, Duration.ofSeconds(90))
        ));

        // S7.2: Prefetch Batched (5 queues, 10 pub, 5 cons, 50k msg, 2 KiB, durable=false, ack=true, prefetch=500)
        scenarios.add(new BenchmarkScenario(
                "S7_2_PrefetchBatched_500",
                "Sensitivity test for batched prefetch buffer (5 queues, prefetch=500, streaming pipeline)",
                buildConfig(createQueues("orders.s7_2.", 5, 10, 5, 1_000, 500, false, true, true), 2048, 64, 32, Duration.ofSeconds(60))
        ));

        // S8: Producer Surge & Lag (10 queues, 50 pub, 2 cons, 300k msg, 2 KiB, durable=true, ack=true, prefetch=200)
        scenarios.add(new BenchmarkScenario(
                "S8_ProducerSurge_Backpressure",
                "Backpressure stress test with severe producer/consumer imbalance (10 queues, 50 pubs vs 2 cons, 300k msgs)",
                buildConfig(createQueues("orders.s8.", 10, 50, 2, 600, 200, true, true, true), 2048, 64, 32, Duration.ofSeconds(180))
        ));

        return scenarios;
    }

    private static List<QueueTarget> createQueues(
            String prefix,
            int queueCount,
            int pubsPerQueue,
            int consPerQueue,
            int msgsPerPublisher,
            int prefetch,
            boolean durable,
            boolean waitForPublishAck,
            boolean waitForConsumerAck
    ) {
        List<QueueTarget> queues = new ArrayList<>(queueCount);
        for (int i = 0; i < queueCount; i++) {
            queues.add(QueueTarget.builder()
                    .queueName(prefix + i)
                    .publishers(pubsPerQueue)
                    .consumers(consPerQueue)
                    .messagesPerPublisher(msgsPerPublisher)
                    .consumerPrefetch(prefetch)
                    .durable(durable)
                    .waitForPublishAck(waitForPublishAck)
                    .waitForConsumerAck(waitForConsumerAck)
                    .build());
        }
        return queues;
    }

    private static SimulationConfig buildConfig(
            List<QueueTarget> queues,
            int payloadSizeBytes,
            int engineMaxPending,
            int engineBatchSize,
            Duration timeout
    ) {
        return SimulationConfig.builder()
                .queues(queues)
                .payloadSizeBytes(payloadSizeBytes)
                .publishingOptions(new PublishingOptions(engineMaxPending, engineBatchSize))
                .consumerStartupDelay(Duration.ofSeconds(1))
                .consumptionTimeout(timeout)
                .showProgressBar(true)
                .enableJournal(true)
                .build();
    }
}

