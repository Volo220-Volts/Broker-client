package com.volo.broker.client.queue;

import com.custombroker.grpc.QueueSettings;

/**
 * Public administrative interface for managing broker queues.
 */
public interface QueueAdmin {

    /**
     * Declares a queue with default reliable persistent settings, failing if it already exists.
     *
     * @param queuePath the unique queue path/name
     */
    void declareQueue(String queuePath);

    /**
     * Declares a queue with the specified settings, failing if it already exists.
     *
     * @param queuePath the unique queue path/name
     * @param settings  the queue configuration settings
     */
    void declareQueue(String queuePath, QueueSettings settings);

    /**
     * Declares a queue with the specified settings and declaration policy.
     *
     * @param queuePath the unique queue path/name
     * @param settings  the queue configuration settings
     * @param options   the declaration options (e.g. fail or use existing if present)
     */
    void declareQueue(String queuePath, QueueSettings settings, QueueDeclarationOptions options);

    /**
     * Declares a queue if absent; ignores the error if it already exists.
     *
     * @param queuePath the unique queue path/name
     * @param settings  the queue configuration settings
     */
    void declareQueueIfAbsent(String queuePath, QueueSettings settings);

    /**
     * Deletes a queue from the broker.
     *
     * @param queuePath the queue path to delete
     */
    void deleteQueue(String queuePath);
}

