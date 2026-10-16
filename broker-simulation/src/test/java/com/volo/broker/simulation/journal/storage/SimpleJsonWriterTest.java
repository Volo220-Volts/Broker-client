package com.volo.broker.simulation.journal.storage;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleJsonWriterTest {

    private record TestPerson(String name, int age, boolean active, List<String> tags) {}

    @Test
    void shouldSerializeBasicTypesAndRecords() {
        TestPerson person = new TestPerson("Volo \"Dev\"", 25, true, List.of("java", "grpc"));
        String json = SimpleJsonWriter.toJson(person);

        assertThat(json)
                .contains("\"name\": \"Volo \\\"Dev\\\"\"")
                .contains("\"age\": 25")
                .contains("\"active\": true")
                .contains("\"java\"")
                .contains("\"grpc\"");
    }

    @Test
    void shouldSerializeMapAndInstant() {
        Map<String, Object> map = Map.of(
                "timestamp", Instant.parse("2026-10-13T20:00:00Z"),
                "count", 42
        );
        String json = SimpleJsonWriter.toJson(map);

        assertThat(json)
                .contains("\"timestamp\": \"2026-10-13T20:00:00Z\"")
                .contains("\"count\": 42");
    }
}

