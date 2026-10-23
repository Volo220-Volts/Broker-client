package com.volo.broker.client.publisher;

public interface PublisherEventListener {
    default void onPublishSent(long clientPublishId, PublishRequest request) {
    }

    default void onPublishAck(PublishReceipt receipt) {
    }

    default void onPublishNack(long clientPublishId, Throwable throwable) {
    }

    default void onStreamClosed(Throwable reason) {
    }

    default void onFlowControl(boolean blocked, String reason, long timestamp) {
    }
}