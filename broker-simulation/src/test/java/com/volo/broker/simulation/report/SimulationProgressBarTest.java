package com.volo.broker.simulation.report;

import com.volo.broker.simulation.core.SimulationStats;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class SimulationProgressBarTest {

    @Test
    void shouldRenderZeroPercentAtStart() {
        SimulationStats stats = new SimulationStats();
        SimulationProgressBar bar = new SimulationProgressBar(stats, 1000L, true, 20, System.out);

        String output = bar.render(0, 1000L, 0, 0, 0);

        bar.close();
        
        assertThat(output).contains("0.0%");
        assertThat(output).contains("(0/1000 msg)");
        assertThat(output).contains("Pub: 0");
        assertThat(output).contains("Cons: 0");
    }

    @Test
    void shouldRenderFiftyPercentMidway() {
        SimulationStats stats = new SimulationStats();
        SimulationProgressBar bar = new SimulationProgressBar(stats, 1000L, true, 20, System.out);

        String output = bar.render(500, 1000L, 700, 500, 0);

        bar.close();

        assertThat(output).contains("50.0%");
        assertThat(output).contains("(500/1000 msg)");
        assertThat(output).contains("Pub: 700");
        assertThat(output).contains("Cons: 500");
    }

    @Test
    void shouldRenderOneHundredPercentAtFinish() {
        SimulationStats stats = new SimulationStats();
        SimulationProgressBar bar = new SimulationProgressBar(stats, 1000L, true, 20, System.out);

        String output = bar.render(1000, 1000L, 1000, 1000, 0);

        bar.close();

        assertThat(output).contains("100.0%");
        assertThat(output).contains("(1000/1000 msg)");
        assertThat(output).contains("Pub: 1000");
        assertThat(output).contains("Cons: 1000");
        assertThat(output).contains("====================");
    }

    @Test
    void shouldHandleZeroTotalMessagesWithoutDivisionByZero() {
        SimulationStats stats = new SimulationStats();
        SimulationProgressBar bar = new SimulationProgressBar(stats, 0L, true, 20, System.out);

        String output = bar.render(0, 0L, 0, 0, 0);

        bar.close();

        assertThat(output).contains("100.0%");
        assertThat(output).contains("(0/0 msg)");
    }

    @Test
    void shouldPrintToStreamAndFinishCleanly() {
        SimulationStats stats = new SimulationStats();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        try (SimulationProgressBar bar = new SimulationProgressBar(stats, 100L, true, 10, ps)) {
            bar.start();
            stats.published("test.q", 1000L, 256L);
            stats.consumed("test.q", 2000L, 256L);
            bar.finish();
        }

        String consoleOutput = baos.toString(StandardCharsets.UTF_8);
        assertThat(consoleOutput).contains("Simulation: [");
        assertThat(consoleOutput).contains("/100 msg)");
        assertThat(consoleOutput).endsWith(System.lineSeparator());
    }
}

