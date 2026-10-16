package com.volo.broker.simulation.core;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public final class SimulationStats implements PublishMetrics {
    private final LongAdder published = new LongAdder();
    private final LongAdder publishFailed = new LongAdder();
    private final LongAdder publishedBytes = new LongAdder();

    private final LongAdder consumed = new LongAdder();
    private final LongAdder consumeFailed = new LongAdder();
    private final LongAdder consumedBytes = new LongAdder();

    private final LongAdder totalPublishAckLatencyNs = new LongAdder();
    private final LongAccumulator maxPublishAckLatencyNs = new LongAccumulator(Long::max, 0);

    private final LongAdder totalEndToEndLatencyNs = new LongAdder();
    private final LongAccumulator maxEndToEndLatencyNs = new LongAccumulator(Long::max, 0);

    private final java.util.concurrent.atomic.AtomicLong windowMaxPublishAckLatencyNs = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong windowMinPublishAckLatencyNs = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    private final java.util.concurrent.atomic.AtomicLong windowMaxEndToEndLatencyNs = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong windowMinEndToEndLatencyNs = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);

    private final ConcurrentHashMap<String, QueueCounters> queueCounters = new ConcurrentHashMap<>();

    private volatile long startedAtNs;
    private volatile long finishedAtNs;

    public void markStarted() {
        startedAtNs = System.nanoTime();
        finishedAtNs = 0;
    }

    public void markFinished() {
        if (finishedAtNs == 0) {
            finishedAtNs = System.nanoTime();
        }
    }

    @Override
    public void published(String queuePath, long publishAckLatencyNs, long payloadBytes) {
        published.increment();
        publishedBytes.add(payloadBytes);
        totalPublishAckLatencyNs.add(publishAckLatencyNs);
        maxPublishAckLatencyNs.accumulate(publishAckLatencyNs);
        windowMaxPublishAckLatencyNs.accumulateAndGet(publishAckLatencyNs, Math::max);
        windowMinPublishAckLatencyNs.accumulateAndGet(publishAckLatencyNs, Math::min);

        QueueCounters counters = counters(queuePath);
        counters.published.increment();
        counters.publishedBytes.add(payloadBytes);
        counters.totalPublishAckLatencyNs.add(publishAckLatencyNs);
        counters.maxPublishAckLatencyNs.accumulate(publishAckLatencyNs);
    }

    @Override
    public void publishFailed(String queuePath) {
        publishFailed.increment();
        counters(queuePath).publishFailed.increment();
    }

    public void consumed(String queuePath, long endToEndLatencyNs, long payloadBytes) {
        consumed.increment();
        consumedBytes.add(payloadBytes);

        if (endToEndLatencyNs > 0) {
            totalEndToEndLatencyNs.add(endToEndLatencyNs);
            maxEndToEndLatencyNs.accumulate(endToEndLatencyNs);
            windowMaxEndToEndLatencyNs.accumulateAndGet(endToEndLatencyNs, Math::max);
            windowMinEndToEndLatencyNs.accumulateAndGet(endToEndLatencyNs, Math::min);
        }

        QueueCounters counters = counters(queuePath);
        counters.consumed.increment();
        counters.consumedBytes.add(payloadBytes);

        if (endToEndLatencyNs > 0) {
            counters.totalEndToEndLatencyNs.add(endToEndLatencyNs);
            counters.maxEndToEndLatencyNs.accumulate(endToEndLatencyNs);
        }
    }

    public void consumeFailed(String queuePath) {
        consumeFailed.increment();
        counters(queuePath).consumeFailed.increment();
    }

    public long publishedCount() {
        return published.sum();
    }

    public long publishFailedCount() {
        return publishFailed.sum();
    }

    public long publishedBytes() {
        return publishedBytes.sum();
    }

    public long consumedCount() {
        return consumed.sum();
    }

    public long consumeFailedCount() {
        return consumeFailed.sum();
    }

    public long consumedBytes() {
        return consumedBytes.sum();
    }

    public boolean consumedExpectedMessages(long expectedMessages) {
        return consumedCount() >= expectedMessages;
    }

    public double averagePublishAckLatencyMs() {
        long count = publishedCount();
        return count == 0 ? 0 : nanosToMillis(totalPublishAckLatencyNs.sum() / (double) count);
    }

    public double maxPublishAckLatencyMs() {
        return nanosToMillis(maxPublishAckLatencyNs.get());
    }

    public double averageEndToEndLatencyMs() {
        long count = consumedCount();
        return count == 0 ? 0 : nanosToMillis(totalEndToEndLatencyNs.sum() / (double) count);
    }

    public double maxEndToEndLatencyMs() {
        return nanosToMillis(maxEndToEndLatencyNs.get());
    }

    public double publishThroughputPerSecond() {
        return perSecond(publishedCount());
    }

    public double consumeThroughputPerSecond() {
        return perSecond(consumedCount());
    }

    public double publishBytesPerSecond() {
        return perSecond(publishedBytes());
    }

    public double consumeBytesPerSecond() {
        return perSecond(consumedBytes());
    }

    public Duration duration() {
        if (startedAtNs == 0) {
            return Duration.ZERO;
        }

        long endNs = finishedAtNs != 0 ? finishedAtNs : System.nanoTime();
        return Duration.ofNanos(Math.max(0, endNs - startedAtNs));
    }

    public long totalPublishAckLatencyNs() {
        return totalPublishAckLatencyNs.sum();
    }

    public long totalEndToEndLatencyNs() {
        return totalEndToEndLatencyNs.sum();
    }

    public long pollWindowMaxPublishAckLatencyNs() {
        return windowMaxPublishAckLatencyNs.getAndSet(0);
    }

    public long pollWindowMinPublishAckLatencyNs() {
        return windowMinPublishAckLatencyNs.getAndSet(Long.MAX_VALUE);
    }

    public long pollWindowMaxEndToEndLatencyNs() {
        return windowMaxEndToEndLatencyNs.getAndSet(0);
    }

    public long pollWindowMinEndToEndLatencyNs() {
        return windowMinEndToEndLatencyNs.getAndSet(Long.MAX_VALUE);
    }

    public Map<String, QueueStats> perQueueStats() {
        return queueCounters.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().snapshot()
                ));
    }

    private double perSecond(long value) {
        double seconds = duration().toNanos() / 1_000_000_000.0;
        return seconds <= 0 ? 0 : value / seconds;
    }

    private QueueCounters counters(String queuePath) {
        String key = queuePath == null || queuePath.isBlank()
                ? "unknown"
                : queuePath;

        return queueCounters.computeIfAbsent(key, ignored -> new QueueCounters());
    }

    private static double nanosToMillis(double nanos) {
        return nanos / 1_000_000.0;
    }

    private static final class QueueCounters {
        private final LongAdder published = new LongAdder();
        private final LongAdder publishFailed = new LongAdder();
        private final LongAdder publishedBytes = new LongAdder();

        private final LongAdder consumed = new LongAdder();
        private final LongAdder consumeFailed = new LongAdder();
        private final LongAdder consumedBytes = new LongAdder();

        private final LongAdder totalPublishAckLatencyNs = new LongAdder();
        private final LongAccumulator maxPublishAckLatencyNs = new LongAccumulator(Long::max, 0);

        private final LongAdder totalEndToEndLatencyNs = new LongAdder();
        private final LongAccumulator maxEndToEndLatencyNs = new LongAccumulator(Long::max, 0);

        private QueueStats snapshot() {
            long publishedCount = published.sum();
            long consumedCount = consumed.sum();

            return new QueueStats(
                    publishedCount,
                    publishFailed.sum(),
                    publishedBytes.sum(),
                    consumedCount,
                    consumeFailed.sum(),
                    consumedBytes.sum(),
                    publishedCount == 0 ? 0 : nanosToMillis(totalPublishAckLatencyNs.sum() / (double) publishedCount),
                    nanosToMillis(maxPublishAckLatencyNs.get()),
                    consumedCount == 0 ? 0 : nanosToMillis(totalEndToEndLatencyNs.sum() / (double) consumedCount),
                    nanosToMillis(maxEndToEndLatencyNs.get())
            );
        }
    }

    public record QueueStats(
            long published,
            long publishFailed,
            long publishedBytes,
            long consumed,
            long consumeFailed,
            long consumedBytes,
            double averagePublishAckLatencyMs,
            double maxPublishAckLatencyMs,
            double averageEndToEndLatencyMs,
            double maxEndToEndLatencyMs
    ) {
    }
}

