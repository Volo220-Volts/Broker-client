package com.volo.broker.simulation.report;

import com.volo.broker.simulation.core.SimulationStats;

public final class SimulationStatsPrinter {
    private static final SimulationReportRenderer DEFAULT_RENDERER = new TableSimulationReportRenderer();

    private SimulationStatsPrinter() {
    }

    public static void print(SimulationStats stats, long expectedMessages) {
        print(stats, expectedMessages, DEFAULT_RENDERER);
    }

    public static void print(
            SimulationStats stats,
            long expectedMessages,
            SimulationReportRenderer renderer
    ) {
        System.out.println(renderer.render(stats, expectedMessages));
    }
}

