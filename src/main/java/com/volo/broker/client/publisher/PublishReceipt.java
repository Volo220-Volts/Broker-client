package com.volo.broker.client.publisher;

public record PublishReceipt(
        long clientPublishId,
        int routedQueueCount
) {
}