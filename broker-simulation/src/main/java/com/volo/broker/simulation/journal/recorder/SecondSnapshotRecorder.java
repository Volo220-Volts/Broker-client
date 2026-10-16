package com.volo.broker.simulation.journal.recorder;

import com.volo.broker.simulation.core.QueueTarget;
import com.volo.broker.simulation.core.SimulationStats;
import com.volo.broker.simulation.journal.model.QueueSecondSnapshot;
import com.volo.broker.simulation.journal.model.SecondSnapshot;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class SecondSnapshotRecorder implements AutoCloseable {
    private final SimulationStats stats;
    private final long totalExpectedMessages;
    private final List<QueueTarget> queues;
    private final SystemMetricsSampler sampler = new SystemMetricsSampler();
    private final List<SecondSnapshot> snapshots = new CopyOnWriteArrayList<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "simulation-journal-sampler");
        thread.setDaemon(true);
        return thread;
    });

    private final AtomicInteger secondCounter = new AtomicInteger(0);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> scheduledTask;
    private volatile long startedAtNs;

    // Previous global values for delta calculation
    private long prevPublished = 0;
    private long prevPublishFailed = 0;
    private long prevPublishedBytes = 0;
    private long prevConsumed = 0;
    private long prevConsumeFailed = 0;
    private long prevConsumedBytes = 0;
    private long prevTotalPublishAckLatencyNs = 0;
    private long prevTotalEndToEndLatencyNs = 0;

    // Previous per-queue counts
    private final Map<String, PrevQueueCounters> prevQueueMap = new ConcurrentHashMap<>();

    private record PrevQueueCounters(
            long published,
            long publishFailed,
            long publishedBytes,
            long consumed,
            long consumeFailed,
            long consumedBytes
    ) {
    }

    public SecondSnapshotRecorder(
            SimulationStats stats,
            long totalExpectedMessages,
            List<QueueTarget> queues
    ) {
        this.stats = Objects.requireNonNull(stats, "stats is required");
        this.totalExpectedMessages = totalExpectedMessages;
        this.queues = queues == null ? List.of() : List.copyOf(queues);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            startedAtNs = System.nanoTime();
            scheduledTask = scheduler.scheduleAtFixedRate(this::recordSnapshot, 1, 1, TimeUnit.SECONDS);
        }
    }

    public synchronized void recordSnapshot() {
        int second = secondCounter.incrementAndGet();
        Instant now = Instant.now();
        double elapsedSeconds = startedAtNs == 0 ? second : (System.nanoTime() - startedAtNs) / 1_000_000_000.0;

        // Current cumulative counters
        long curPublished = stats.publishedCount();
        long curPublishFailed = stats.publishFailedCount();
        long curPublishedBytes = stats.publishedBytes();
        long curConsumed = stats.consumedCount();
        long curConsumeFailed = stats.consumeFailedCount();
        long curConsumedBytes = stats.consumedBytes();
        long curTotalAckNs = stats.totalPublishAckLatencyNs();
        long curTotalE2eNs = stats.totalEndToEndLatencyNs();

        // Deltas
        long publishedDelta = Math.max(0, curPublished - prevPublished);
        long publishFailedDelta = Math.max(0, curPublishFailed - prevPublishFailed);
        long publishedBytesDelta = Math.max(0, curPublishedBytes - prevPublishedBytes);
        long consumedDelta = Math.max(0, curConsumed - prevConsumed);
        long consumeFailedDelta = Math.max(0, curConsumeFailed - prevConsumeFailed);
        long consumedBytesDelta = Math.max(0, curConsumedBytes - prevConsumedBytes);
        long throughputBalance = publishedDelta - consumedDelta;

        long backlog = Math.max(0, curPublished - curConsumed);
        double completionPercent = totalExpectedMessages > 0
                ? Math.min(100.0, (curConsumed * 100.0) / totalExpectedMessages)
                : 0.0;

        // Window latencies (ACK)
        long deltaAckNs = Math.max(0, curTotalAckNs - prevTotalPublishAckLatencyNs);
        double windowPublishAckAvgMs = publishedDelta > 0 ? (deltaAckNs / (double) publishedDelta) / 1_000_000.0 : 0.0;
        long winMaxAckNs = stats.pollWindowMaxPublishAckLatencyNs();
        double windowPublishAckMaxMs = winMaxAckNs > 0 ? winMaxAckNs / 1_000_000.0 : (publishedDelta > 0 ? windowPublishAckAvgMs : 0.0);
        long winMinAckNs = stats.pollWindowMinPublishAckLatencyNs();
        double windowPublishAckMinMs = (winMinAckNs < Long.MAX_VALUE && winMinAckNs > 0)
                ? winMinAckNs / 1_000_000.0
                : (publishedDelta > 0 ? windowPublishAckAvgMs : 0.0);

        // Window latencies (E2E)
        long deltaE2eNs = Math.max(0, curTotalE2eNs - prevTotalEndToEndLatencyNs);
        double windowEndToEndAvgMs = consumedDelta > 0 ? (deltaE2eNs / (double) consumedDelta) / 1_000_000.0 : 0.0;
        long winMaxE2eNs = stats.pollWindowMaxEndToEndLatencyNs();
        double windowEndToEndMaxMs = winMaxE2eNs > 0 ? winMaxE2eNs / 1_000_000.0 : (consumedDelta > 0 ? windowEndToEndAvgMs : 0.0);
        long winMinE2eNs = stats.pollWindowMinEndToEndLatencyNs();
        double windowEndToEndMinMs = (winMinE2eNs < Long.MAX_VALUE && winMinE2eNs > 0)
                ? winMinE2eNs / 1_000_000.0
                : (consumedDelta > 0 ? windowEndToEndAvgMs : 0.0);

        // Cumulative latencies
        double cumPublishAckAvgMs = stats.averagePublishAckLatencyMs();
        double cumPublishAckMaxMs = stats.maxPublishAckLatencyMs();
        double cumEndToEndAvgMs = stats.averageEndToEndLatencyMs();
        double cumEndToEndMaxMs = stats.maxEndToEndLatencyMs();

        // System resources
        SystemMetricsSampler.SystemSnapshot sys = sampler.sample();

        // Per-queue breakdown
        Map<String, SimulationStats.QueueStats> currentQueueStats = stats.perQueueStats();
        List<QueueSecondSnapshot> queueSnapshots = new ArrayList<>(queues.size());

        for (QueueTarget target : queues) {
            String qName = target.queueName();
            SimulationStats.QueueStats qStats = currentQueueStats.get(qName);

            long qPub = qStats != null ? qStats.published() : 0;
            long qPubFail = qStats != null ? qStats.publishFailed() : 0;
            long qPubBytes = qStats != null ? qStats.publishedBytes() : 0;
            long qCons = qStats != null ? qStats.consumed() : 0;
            long qConsFail = qStats != null ? qStats.consumeFailed() : 0;
            long qConsBytes = qStats != null ? qStats.consumedBytes() : 0;

            PrevQueueCounters prevQ = prevQueueMap.getOrDefault(qName, new PrevQueueCounters(0, 0, 0, 0, 0, 0));
            long qPubDelta = Math.max(0, qPub - prevQ.published);
            long qPubFailDelta = Math.max(0, qPubFail - prevQ.publishFailed);
            long qPubBytesDelta = Math.max(0, qPubBytes - prevQ.publishedBytes);
            long qConsDelta = Math.max(0, qCons - prevQ.consumed);
            long qConsFailDelta = Math.max(0, qConsFail - prevQ.consumeFailed);
            long qConsBytesDelta = Math.max(0, qConsBytes - prevQ.consumedBytes);
            long qBacklog = Math.max(0, qPub - qCons);

            queueSnapshots.add(new QueueSecondSnapshot(
                    qName,
                    qPubDelta,
                    qPubFailDelta,
                    qPubBytesDelta,
                    qConsDelta,
                    qConsFailDelta,
                    qConsBytesDelta,
                    qPub,
                    qCons,
                    qBacklog,
                    qStats != null ? qStats.averagePublishAckLatencyMs() : 0.0,
                    qStats != null ? qStats.maxPublishAckLatencyMs() : 0.0,
                    qStats != null ? qStats.averageEndToEndLatencyMs() : 0.0,
                    qStats != null ? qStats.maxEndToEndLatencyMs() : 0.0
            ));

            prevQueueMap.put(qName, new PrevQueueCounters(qPub, qPubFail, qPubBytes, qCons, qConsFail, qConsBytes));
        }

        SecondSnapshot snapshot = new SecondSnapshot(
                second,
                now,
                elapsedSeconds,
                publishedDelta,
                publishFailedDelta,
                publishedBytesDelta,
                consumedDelta,
                consumeFailedDelta,
                consumedBytesDelta,
                throughputBalance,
                curPublished,
                curPublishFailed,
                curPublishedBytes,
                curConsumed,
                curConsumeFailed,
                curConsumedBytes,
                backlog,
                completionPercent,
                windowPublishAckAvgMs,
                windowPublishAckMaxMs,
                windowPublishAckMinMs,
                windowEndToEndAvgMs,
                windowEndToEndMaxMs,
                windowEndToEndMinMs,
                cumPublishAckAvgMs,
                cumPublishAckMaxMs,
                cumEndToEndAvgMs,
                cumEndToEndMaxMs,
                sys.heapUsedBytes(),
                sys.heapMaxBytes(),
                sys.heapUsagePercent(),
                sys.liveThreads(),
                sys.cpuLoadPercent(),
                queueSnapshots
        );

        snapshots.add(snapshot);

        // Update previous values
        prevPublished = curPublished;
        prevPublishFailed = curPublishFailed;
        prevPublishedBytes = curPublishedBytes;
        prevConsumed = curConsumed;
        prevConsumeFailed = curConsumeFailed;
        prevConsumedBytes = curConsumedBytes;
        prevTotalPublishAckLatencyNs = curTotalAckNs;
        prevTotalEndToEndLatencyNs = curTotalE2eNs;
    }

    public List<SecondSnapshot> snapshots() {
        return Collections.unmodifiableList(snapshots);
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (scheduledTask != null) {
                scheduledTask.cancel(false);
            }
            // Capture any final delta that happened since the last tick
            recordSnapshot();
        }
    }

    @Override
    public void close() {
        stop();
        scheduler.shutdownNow();
    }
}

