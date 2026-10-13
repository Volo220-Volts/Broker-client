package com.volo.broker.client.consumer;

@FunctionalInterface
public interface MessageHandler {
    void handle(Delivery delivery) throws Exception;
}