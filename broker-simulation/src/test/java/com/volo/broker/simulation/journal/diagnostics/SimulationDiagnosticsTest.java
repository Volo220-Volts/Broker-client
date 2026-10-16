package com.volo.broker.simulation.journal.diagnostics;

import com.volo.broker.simulation.core.SimulationStats;
import com.volo.broker.simulation.journal.model.SecondSnapshot;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SimulationDiagnosticsTest {

    @Test
    void shouldDetectPublishFailures() {
        SimulationStats stats = new SimulationStats();
        stats.published("orders", 1_000_000, 100);
        stats.publishFailed("orders");

        SecondSnapshot snapshot = createDummySnapshot(1, 1, 1, 0, 0, 0);
        SimulationDiagnostics.AnalysisResult result = SimulationDiagnostics.analyze(stats, List.of(snapshot), 10);

        assertThat(result.findings()).anyMatch(f -> "CRITICAL".equals(f.severity()) && "FAILURE".equals(f.category()));
        assertThat(result.healthSummary()).containsIgnoringCase("critical");
    }

    @Test
    void shouldPassCleanSimulationWithoutWarnings() {
        SimulationStats stats = new SimulationStats();
        stats.published("orders", 1_000_000, 100);
        stats.consumed("orders", 2_000_000, 100);

        SecondSnapshot snapshot = createDummySnapshot(1, 1, 0, 1, 1.0, 2.0);
        SimulationDiagnostics.AnalysisResult result = SimulationDiagnostics.analyze(stats, List.of(snapshot), 1);

        assertThat(result.findings()).isEmpty();
        assertThat(result.healthSummary()).contains("successfully");
    }

    private SecondSnapshot createDummySnapshot(
            int second,
            long pubDelta,
            long pubFailDelta,
            long consDelta,
            double ackMaxMs,
            double e2eMaxMs
    ) {
        return new SecondSnapshot(
                second,
                Instant.now(),
                second,
                pubDelta,
                pubFailDelta,
                pubDelta * 100,
                consDelta,
                0,
                consDelta * 100,
                pubDelta - consDelta,
                pubDelta,
                pubFailDelta,
                pubDelta * 100,
                consDelta,
                0,
                consDelta * 100,
                pubDelta - consDelta,
                100.0,
                ackMaxMs,
                ackMaxMs,
                ackMaxMs,
                e2eMaxMs,
                e2eMaxMs,
                e2eMaxMs,
                ackMaxMs,
                ackMaxMs,
                e2eMaxMs,
                e2eMaxMs,
                100_000_000,
                1_000_000_000,
                10.0,
                10,
                5.0,
                List.of()
        );
    }
}

