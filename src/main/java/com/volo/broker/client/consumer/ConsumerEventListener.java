package com.volo.broker.client.consumer;

public interface ConsumerEventListener {
    default void onStarted(String queuePath) {
    }

    default void onDelivery(Delivery delivery) {
    }

    default void onHandled(Delivery delivery) {
    }

    default void onHandlerError(Delivery delivery, Throwable throwable) {
    }

    default void onClosed() {
    }
}