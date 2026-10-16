package com.volo.broker.client.publisher;

import com.volo.broker.client.BrokerClientException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PendingPublishRegistryTest {

    @Test
    void shouldRegisterAndCompletePublish() {
        PendingPublishRegistry registry = new PendingPublishRegistry();

        CompletableFuture<PublishReceipt> future = registry.register(101L);
        assertThat(registry.size()).isEqualTo(1);
        assertThat(future).isNotDone();

        PublishReceipt receipt = new PublishReceipt(101L, 3);
        boolean completed = registry.complete(receipt);

        assertThat(completed).isTrue();
        assertThat(future).isCompletedWithValue(receipt);
        assertThat(registry.size()).isEqualTo(0);
    }

    @Test
    void shouldFailPublishAndRemoveFromRegistry() {
        PendingPublishRegistry registry = new PendingPublishRegistry();

        CompletableFuture<PublishReceipt> future = registry.register(102L);
        assertThat(registry.size()).isEqualTo(1);

        BrokerClientException exception = new BrokerClientException("Broker rejected message");
        boolean failed = registry.fail(102L, exception);

        assertThat(failed).isTrue();
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).hasRootCause(exception);
        assertThat(registry.size()).isEqualTo(0);
    }

    @Test
    void shouldFailAllPendingPublishesOnStreamFailure() {
        PendingPublishRegistry registry = new PendingPublishRegistry();

        CompletableFuture<PublishReceipt> f1 = registry.register(1L);
        CompletableFuture<PublishReceipt> f2 = registry.register(2L);
        CompletableFuture<PublishReceipt> f3 = registry.register(3L);

        assertThat(registry.size()).isEqualTo(3);

        BrokerClientException streamError = new BrokerClientException("Stream broken");
        registry.failAll(streamError);

        assertThat(registry.size()).isEqualTo(0);
        assertThat(f1).isCompletedExceptionally();
        assertThat(f2).isCompletedExceptionally();
        assertThat(f3).isCompletedExceptionally();
    }

    @Test
    void shouldCleanUpWhenFutureCancelledOrTimedOut() {
        PendingPublishRegistry registry = new PendingPublishRegistry();

        CompletableFuture<PublishReceipt> future = registry.register(200L);
        assertThat(registry.size()).isEqualTo(1);

        future.cancel(true);
        assertThat(registry.size()).isEqualTo(0);
    }

    @Test
    void shouldHandleConcurrentRegistrationAndCompletion() throws InterruptedException {
        PendingPublishRegistry registry = new PendingPublishRegistry();
        int count = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            final long id = i;
            executor.submit(() -> {
                registry.register(id);
                registry.complete(new PublishReceipt(id, 1));
                latch.countDown();
            });
        }

        boolean done = latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(done).isTrue();
        assertThat(registry.size()).isEqualTo(0);
    }
}

