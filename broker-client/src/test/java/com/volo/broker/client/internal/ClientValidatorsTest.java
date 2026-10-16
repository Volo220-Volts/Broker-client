package com.volo.broker.client.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClientValidatorsTest {

    @ParameterizedTest
    @ValueSource(strings = {"orders", "orders.create", "app.v1.events.user_registered"})
    void shouldAcceptValidQueuePaths(String queuePath) {
        assertThatCode(() -> ClientValidators.requireQueuePath(queuePath))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "orders.*", "orders.#", "orders..create", ".orders", "orders."})
    void shouldRejectInvalidQueuePaths(String invalidPath) {
        assertThatThrownBy(() -> ClientValidators.requireQueuePath(invalidPath))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNullQueuePath() {
        assertThatThrownBy(() -> ClientValidators.requireQueuePath(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queuePath is required");
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders", "orders.create", "orders.*", "orders.#"})
    void shouldAcceptValidRoutingKeys(String routingKey) {
        assertThatCode(() -> ClientValidators.requireRoutingKey(routingKey))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "  ", "orders..create", ".orders", "orders."})
    void shouldRejectInvalidRoutingKeys(String invalidKey) {
        assertThatThrownBy(() -> ClientValidators.requireRoutingKey(invalidKey))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

