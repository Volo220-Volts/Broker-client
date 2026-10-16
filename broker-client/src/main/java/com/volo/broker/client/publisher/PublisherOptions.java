package com.volo.broker.client.publisher;

public record PublisherOptions(
        PublisherEventListener eventListener
) {
    public PublisherOptions {
        if (eventListener == null) {
            eventListener = new PublisherEventListener() {};
        }
    }

    public static PublisherOptions defaults() {
        return new PublisherOptions(new PublisherEventListener() {});
    }
}