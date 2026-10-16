package com.volo.broker.simulation.report;

import com.volo.broker.simulation.core.SimulationStats;

import java.io.PrintStream;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Console progress bar for tracking broker simulation progress in real time.
 * Redraws in place using cursor repositioning and line clearing (setCursor + clear)
 * to ensure previous progress characters disappear without emitting new lines.
 */
public final class SimulationProgressBar implements AutoCloseable {
    private static final int DEFAULT_BAR_WIDTH = 12;
    private static final long REFRESH_INTERVAL_MS = 80;

    // ANSI escape sequences:
    // \r      : Carriage return (move cursor to column 0)
    // \033[2K : Erase entire line (removes all existing characters on the row)
    // \033[1G : Set cursor to column 1 (absolute horizontal position)
    private static final String CURSOR_RESET_AND_CLEAR = "\r\033[2K\033[1G";

    private final SimulationStats stats;
    private final long totalMessages;
    private final boolean hasConsumers;
    private final int barWidth;
    private final PrintStream out;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private volatile ScheduledFuture<?> updateTask;
    private volatile long startedAtNs;

    private int lastPrintedLength = 0;
    private String lastPrintedContent = "";

    public SimulationProgressBar(SimulationStats stats, long totalMessages) {
        this(stats, totalMessages, true, DEFAULT_BAR_WIDTH, System.out);
    }

    public SimulationProgressBar(SimulationStats stats, long totalMessages, boolean hasConsumers) {
        this(stats, totalMessages, hasConsumers, DEFAULT_BAR_WIDTH, System.out);
    }

    public SimulationProgressBar(
            SimulationStats stats,
            long totalMessages,
            boolean hasConsumers,
            int barWidth,
            PrintStream out
    ) {
        this.stats = Objects.requireNonNull(stats, "stats is required");
        this.totalMessages = Math.max(0, totalMessages);
        this.hasConsumers = hasConsumers;
        this.barWidth = Math.max(10, barWidth);
        this.out = Objects.requireNonNull(out, "out is required");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "simulation-progress-bar");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts the periodic progress bar updates.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        this.startedAtNs = System.nanoTime();
        redraw(renderCurrent(), false);

        this.updateTask = scheduler.scheduleAtFixedRate(
                this::renderAndPrintSafely,
                REFRESH_INTERVAL_MS,
                REFRESH_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    private void renderAndPrintSafely() {
        try {
            if (finished.get()) {
                return;
            }
            redraw(renderCurrent(), false);
        } catch (Throwable ignored) {
        }
    }

    /**
     * Renders current progress snapshot string.
     */
    public String renderCurrent() {
        long published = stats.publishedCount();
        long consumed = stats.consumedCount();
        long current = hasConsumers ? consumed : published;

        return render(current, totalMessages, published, consumed, startedAtNs);
    }

    /**
     * Pure rendering function for easy testing and deterministic output.
     */
    public String render(long current, long total, long published, long consumed, long startNs) {
        double ratio = total <= 0 ? 1.0 : Math.min(1.0, Math.max(0.0, (double) current / total));
        double percent = ratio * 100.0;

        String bar = buildBar(ratio, barWidth);

        return String.format(
                Locale.ROOT,
                "Simulation: [%s] %5.1f%% (%d/%d msg) | Pub: %d | Cons: %d",
                bar,
                percent,
                Math.min(current, total),
                total,
                published,
                consumed
        );
    }

    /**
     * Repositions cursor to the start of the line and rewrites the content in place.
     * Also guarantees that any leftover characters from previous longer lines
     * are completely wiped out with spaces.
     *
     * @param content  the new progress text
     * @param complete true if this is the final draw and should end with a newline
     */
    public synchronized void redraw(String content, boolean complete) {
        if (!complete && content.equals(lastPrintedContent)) {
            return;
        }
        lastPrintedContent = content;

        StringBuilder sb = new StringBuilder();

        // 1. Move cursor to column 1 and clear the line via ANSI escape sequence
        sb.append(CURSOR_RESET_AND_CLEAR);
        sb.append(content);

        // 2. Clear any lingering old characters by padding spaces if new line is shorter
        if (content.length() < lastPrintedLength) {
            int extraSpaces = lastPrintedLength - content.length();
            sb.append(" ".repeat(extraSpaces));

            // Set cursor back to the end of the actual text
            if (!complete) {
                sb.append(CURSOR_RESET_AND_CLEAR).append(content);
            }
        } else {
            lastPrintedLength = content.length();
        }

        if (complete) {
            sb.append(System.lineSeparator());
        }

        out.print(sb.toString());
        out.flush();
    }

    private static String buildBar(double ratio, int width) {
        int filled = (int) Math.round(ratio * width);
        StringBuilder sb = new StringBuilder(width);

        for (int i = 0; i < width; i++) {
            if (i < filled) {
                sb.append('=');
            } else if (i == filled && filled < width) {
                sb.append('>');
            } else {
                sb.append('.');
            }
        }
        return sb.toString();
    }

    /**
     * Concludes progress bar display, printing the final state followed by newline.
     */
    public void finish() {
        if (!finished.compareAndSet(false, true)) {
            return;
        }

        ScheduledFuture<?> task = updateTask;
        if (task != null) {
            task.cancel(false);
        }

        long published = stats.publishedCount();
        long consumed = stats.consumedCount();
        long current = hasConsumers ? consumed : published;

        redraw(render(current, totalMessages, published, consumed, startedAtNs), true);

        scheduler.shutdown();
    }

    @Override
    public void close() {
        finish();
    }
}
