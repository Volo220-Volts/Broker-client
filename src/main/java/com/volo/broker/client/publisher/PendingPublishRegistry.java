package com.volo.broker.client.publisher;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

final class PendingPublishRegistry {
    private final ConcurrentHashMap<Long, CompletableFuture<PublishReceipt>> pending = new ConcurrentHashMap<>();

    CompletableFuture<PublishReceipt> register(long clientPublishId) {
        CompletableFuture<PublishReceipt> future = new CompletableFuture<>();
        pending.put(clientPublishId, future);
        return future;
    }

    void complete(PublishReceipt receipt) {
        CompletableFuture<PublishReceipt> future = pending.remove(receipt.clientPublishId());

        if (future != null) {
            future.complete(receipt);
        }
    }

    void fail(long clientPublishId, Throwable throwable) {
        CompletableFuture<PublishReceipt> future = pending.remove(clientPublishId);

        if (future != null) {
            future.completeExceptionally(throwable);
        }
    }

    void failAll(Throwable throwable) {
        for (Map.Entry<Long, CompletableFuture<PublishReceipt>> entry : pending.entrySet()) {
            entry.getValue().completeExceptionally(throwable);
        }

        pending.clear();
    }

    int size() {
        return pending.size();
    }
}