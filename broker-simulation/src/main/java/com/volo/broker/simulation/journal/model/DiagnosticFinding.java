package com.volo.broker.simulation.journal.model;

public record DiagnosticFinding(
        String severity,
        String category,
        int secondDetected,
        String description
) {
}

