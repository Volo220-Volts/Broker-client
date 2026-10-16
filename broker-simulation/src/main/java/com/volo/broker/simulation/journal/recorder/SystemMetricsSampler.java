package com.volo.broker.simulation.journal.recorder;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;

public final class SystemMetricsSampler {
    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

    public record SystemSnapshot(
            long heapUsedBytes,
            long heapMaxBytes,
            double heapUsagePercent,
            int liveThreads,
            double cpuLoadPercent
    ) {
    }

    public SystemSnapshot sample() {
        long used = memoryMXBean.getHeapMemoryUsage().getUsed();
        long max = memoryMXBean.getHeapMemoryUsage().getMax();
        double usagePercent = max > 0 ? (used * 100.0) / max : 0.0;
        int threads = threadMXBean.getThreadCount();

        double cpuPercent = 0.0;
        if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
            double load = sunOsBean.getProcessCpuLoad();
            if (load >= 0) {
                cpuPercent = load * 100.0;
            }
        }

        return new SystemSnapshot(used, max, usagePercent, threads, cpuPercent);
    }
}

