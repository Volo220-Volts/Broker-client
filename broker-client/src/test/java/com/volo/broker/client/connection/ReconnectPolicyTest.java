package com.volo.broker.client.connection;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReconnectPolicyTest {

    @Test
    void shouldRejectInvalidDelays() {
        assertThatThrownBy(() -> new ReconnectPolicy(true, 5, Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDelay must be greater than or equal to initialDelay");
    }

    @Test
    void shouldRespectMaxAttempts() {
        ReconnectPolicy policy = ReconnectPolicy.limited(3);

        assertThat(policy.shouldRetry(0)).isTrue();
        assertThat(policy.shouldRetry(1)).isTrue();
        assertThat(policy.shouldRetry(2)).isTrue();
        assertThat(policy.shouldRetry(3)).isFalse();
        assertThat(policy.shouldRetry(4)).isFalse();
    }

    @Test
    void shouldCalculateDelayWithCapping() {
        ReconnectPolicy policy = new ReconnectPolicy(
                true,
                10,
                Duration.ofMillis(100),
                Duration.ofMillis(500)
        );

        assertThat(policy.delayForAttempt(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.delayForAttempt(2)).isEqualTo(Duration.ofMillis(200));
        assertThat(policy.delayForAttempt(4)).isEqualTo(Duration.ofMillis(400));
        assertThat(policy.delayForAttempt(5)).isEqualTo(Duration.ofMillis(500));
        assertThat(policy.delayForAttempt(10)).isEqualTo(Duration.ofMillis(500)); // capped at maxDelay
    }

    @Test
    void shouldDisableRetriesWhenDisabled() {
        ReconnectPolicy policy = ReconnectPolicy.disabled();

        assertThat(policy.shouldRetry(0)).isFalse();
        assertThat(policy.shouldRetry(1)).isFalse();
    }
}

