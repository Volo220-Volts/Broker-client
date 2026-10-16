package com.volo.broker.simulation.report;

import com.volo.broker.simulation.core.SimulationStats;

public interface SimulationReportRenderer {
    String render(SimulationStats stats, long expectedMessages);
}

