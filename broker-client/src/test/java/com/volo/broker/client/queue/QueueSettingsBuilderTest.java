package com.volo.broker.client.queue;

import com.custombroker.grpc.FlowControlPolicy;
import com.custombroker.grpc.OverflowPolicy;
import com.custombroker.grpc.QueueSettings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueueSettingsBuilderTest {

    @Test
    void shouldBuildQueueWithFlowControlPolicy() {
        QueueSettings settings = QueueSettingsBuilder.builder()
                .flowControl(20_000, 5_000, 30_000)
                .build();

        assertThat(settings.hasFlowControlPolicy()).isTrue();
        FlowControlPolicy policy = settings.getFlowControlPolicy();
        assertThat(policy.getEnabled()).isTrue();
        assertThat(policy.getHighWatermarkMessages()).isEqualTo(20_000);
        assertThat(policy.getLowWatermarkMessages()).isEqualTo(5_000);
        assertThat(policy.getMaxBlockMs()).isEqualTo(30_000);
    }

    @Test
    void shouldBuildQueueWithByteFlowControlPolicy() {
        QueueSettings settings = QueueSettingsBuilder.builder()
                .flowControlBytes(10_000_000L, 2_000_000L, 15_000L)
                .build();

        assertThat(settings.hasFlowControlPolicy()).isTrue();
        FlowControlPolicy policy = settings.getFlowControlPolicy();
        assertThat(policy.getEnabled()).isTrue();
        assertThat(policy.getHighWatermarkBytes()).isEqualTo(10_000_000L);
        assertThat(policy.getLowWatermarkBytes()).isEqualTo(2_000_000L);
        assertThat(policy.getMaxBlockMs()).isEqualTo(15_000L);
    }

    @Test
    void shouldThrowWhenHighWatermarkLessThanOrEqualToLowWatermark() {
        assertThatThrownBy(() -> QueueSettingsBuilder.builder().flowControl(5_000, 5_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("highWatermarkMessages must be greater than lowWatermarkMessages");

        assertThatThrownBy(() -> QueueSettingsBuilder.builder().flowControl(4_000, 5_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("highWatermarkMessages must be greater than lowWatermarkMessages");
    }

    @Test
    void shouldThrowWhenWatermarksAreNegativeOrZero() {
        assertThatThrownBy(() -> QueueSettingsBuilder.builder().flowControl(0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("highWatermarkMessages must be greater than zero");

        assertThatThrownBy(() -> QueueSettingsBuilder.builder().flowControl(100, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lowWatermarkMessages cannot be negative");

        assertThatThrownBy(() -> QueueSettingsBuilder.builder().flowControl(100, 50, -10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBlockMs cannot be negative");
    }

    @Test
    void shouldThrowWhenHighWatermarkExceedsOrEqualsMaxMessages() {
        assertThatThrownBy(() -> QueueSettingsBuilder.builder()
                .maxMessages(10_000, OverflowPolicy.REJECT_PUBLISH)
                .flowControl(10_000, 2_000)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be less than maxMessages");

        assertThatThrownBy(() -> QueueSettingsBuilder.builder()
                .maxMessages(10_000, OverflowPolicy.REJECT_PUBLISH)
                .flowControl(15_000, 2_000)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be less than maxMessages");
    }

    @Test
    void shouldSupportDisablingFlowControl() {
        QueueSettings settings = QueueSettingsBuilder.builder()
                .flowControl(10_000, 2_000)
                .flowControlDisabled()
                .build();

        assertThat(settings.hasFlowControlPolicy()).isFalse();
    }
}

