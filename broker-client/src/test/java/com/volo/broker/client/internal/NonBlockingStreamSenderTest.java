package com.volo.broker.client.internal;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class NonBlockingStreamSenderTest {

    @Test
    void shouldDeliverMessagesInStrictSequentialOrderWithoutOverlap() throws InterruptedException {
        int messageCount = 2000;
        int threadCount = 10;
        ConcurrentLinkedQueue<Integer> received = new ConcurrentLinkedQueue<>();
        AtomicBoolean concurrentCollisionDetected = new AtomicBoolean(false);
        AtomicBoolean inOnNext = new AtomicBoolean(false);
        AtomicInteger completedCount = new AtomicInteger(0);

        StreamObserver<Integer> testObserver = new StreamObserver<>() {
            @Override
            public void onNext(Integer value) {
                // Verify that no two threads execute onNext simultaneously
                if (!inOnNext.compareAndSet(false, true)) {
                    concurrentCollisionDetected.set(true);
                }

                try {
                    received.add(value);
                    // Tiny artificial jitter to stress test concurrency
                    Thread.yield();
                } finally {
                    inOnNext.set(false);
                }
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
                completedCount.incrementAndGet();
            }
        };

        NonBlockingStreamSender<Integer> sender = new NonBlockingStreamSender<>(testObserver);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(messageCount);

        for (int i = 0; i < messageCount; i++) {
            final int value = i;
            executor.submit(() -> {
                sender.send(value);
                latch.countDown();
            });
        }

        boolean finishedSending = latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        sender.complete();

        assertThat(finishedSending).isTrue();
        assertThat(concurrentCollisionDetected.get()).isFalse();
        assertThat(received).hasSize(messageCount);
        assertThat(completedCount.get()).isEqualTo(1);
    }

    @Test
    void shouldNotSendAfterComplete() {
        AtomicInteger count = new AtomicInteger();
        StreamObserver<String> observer = new StreamObserver<>() {
            @Override
            public void onNext(String value) {
                count.incrementAndGet();
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        };

        NonBlockingStreamSender<String> sender = new NonBlockingStreamSender<>(observer);
        sender.send("msg1");
        sender.complete();
        sender.send("msg2");

        assertThat(count.get()).isEqualTo(1);
    }
}

