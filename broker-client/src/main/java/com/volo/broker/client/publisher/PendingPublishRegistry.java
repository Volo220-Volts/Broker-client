package com.volo.broker.client.publisher;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

final class PendingPublishRegistry {
    private final ConcurrentHashMap<Long, CompletableFuture<PublishReceipt>> pending = new ConcurrentHashMap<>();

    CompletableFuture<PublishReceipt> register(long clientPublishId) {
        CompletableFuture<PublishReceipt> future = new CompletableFuture<>();
        pending.put(clientPublishId, future);
        future.whenComplete((receipt, throwable) -> pending.remove(clientPublishId));
        return future;
    }

    boolean complete(PublishReceipt receipt) {
        CompletableFuture<PublishReceipt> future = pending.remove(receipt.clientPublishId());

        if (future != null) {
            return future.complete(receipt);
        }
        return false;
    }

    boolean fail(long clientPublishId, Throwable throwable) {
        CompletableFuture<PublishReceipt> future = pending.remove(clientPublishId);

        if (future != null) {
            return future.completeExceptionally(throwable);
        }
        return false;
    }

    void failAll(Throwable throwable) {
        for (Map.Entry<Long, CompletableFuture<PublishReceipt>> entry : pending.entrySet()) {
            entry.getValue().completeExceptionally(throwable);
        }

        pending.clear();
    }

    void remove(long clientPublishId) {
        pending.remove(clientPublishId);
    }

    int size() {
        return pending.size();
    }
}