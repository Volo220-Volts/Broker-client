package com.volo.broker.client.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ClientExecutorsTest {

    @Test
    void shouldExecuteTasksAndShutdownGracefully() throws InterruptedException {
        ClientExecutors executors = new ClientExecutors(1, 2, 1);

        AtomicBoolean taskRan = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        executors.handlerExecutor().submit(() -> {
            taskRan.set(true);
            latch.countDown();
        });

        boolean executed = latch.await(2, TimeUnit.SECONDS);
        assertThat(executed).isTrue();
        assertThat(taskRan.get()).isTrue();

        executors.close();

        assertThat(executors.handlerExecutor().isShutdown()).isTrue();
        assertThat(executors.keepAliveExecutor().isShutdown()).isTrue();
        assertThat(executors.reconnectExecutor().isShutdown()).isTrue();
    }
}

