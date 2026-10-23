package com.volo.broker.simulation.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueueTargetTest {

    @Test
    void shouldDefaultToTrackedAck() {
        QueueTarget target = QueueTarget.builder()
                .queueName("orders")
                .publishers(2)
                .consumers(1)
                .messagesPerPublisher(100)
                .build();

        assertThat(target.waitForPublishAck()).isTrue();
        assertThat(target.waitForConsumerAck()).isTrue();
    }

    @Test
    void shouldSupportFireAndForgetWithoutPublishAck() {
        QueueTarget target = QueueTarget.builder()
                .queueName("telemetry")
                .publishers(5)
                .consumers(0)
                .messagesPerPublisher(1000)
                .fireAndForget()
                .build();

        assertThat(target.waitForPublishAck()).isFalse();
        assertThat(target.waitForConsumerAck()).isTrue();
    }

    @Test
    void shouldSupportExplicitAckConfiguration() {
        QueueTarget target = QueueTarget.builder()
                .queueName("metrics")
                .publishers(1)
                .consumers(1)
                .messagesPerPublisher(50)
                .waitForPublishAck(false)
                .waitForConsumerAck(false)
                .build();

        assertThat(target.waitForPublishAck()).isFalse();
        assertThat(target.waitForConsumerAck()).isFalse();
    }

    @Test
    void shouldConfigureFlowControl() {
        QueueTarget target = QueueTarget.builder()
                .queueName("orders.surge")
                .publishers(10)
                .consumers(2)
                .messagesPerPublisher(1000)
                .flowControl(20_000, 5_000, 30_000)
                .build();

        assertThat(target.flowControlEnabled()).isTrue();
        assertThat(target.highWatermarkMessages()).isEqualTo(20_000);
        assertThat(target.lowWatermarkMessages()).isEqualTo(5_000);
        assertThat(target.maxBlockMs()).isEqualTo(30_000);
    }
}

