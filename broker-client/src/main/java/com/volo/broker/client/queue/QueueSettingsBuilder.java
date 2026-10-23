package com.volo.broker.client.queue;

import com.custombroker.grpc.*;

public final class QueueSettingsBuilder {
    private PersistenceMode persistenceMode = PersistenceMode.PERSISTENT;
    private PublishAckMode publishAckMode = PublishAckMode.PUBLISH_ACK_ENABLED;
    private ConsumerAckMode consumerAckMode = ConsumerAckMode.CONSUMER_ACK_ENABLED;

    private boolean redeliveryEnabled = true;
    private int maxAttempts = 5;
    private boolean requeueOnDisconnect = true;
    private boolean requeueOnSendFailure = true;

    private boolean ttlEnabled;
    private long ttlMs;

    private boolean autoDelete;
    private boolean idleTimeoutEnabled;
    private long idleTimeoutMs;
    private boolean durable = true;

    private boolean capacityLimited;
    private int maxMessages;
    private long maxBytes;
    private OverflowPolicy overflowPolicy = OverflowPolicy.REJECT_PUBLISH;

    private int prefetchCount = 10;
    private ConsumerSelectionStrategy consumerSelectionStrategy = ConsumerSelectionStrategy.ROUND_ROBIN;

    private boolean flowControlEnabled;
    private int highWatermarkMessages;
    private int lowWatermarkMessages;
    private long highWatermarkBytes;
    private long lowWatermarkBytes;
    private long maxBlockMs;

    private QueueSettingsBuilder() {
    }

    public static QueueSettingsBuilder builder() {
        return new QueueSettingsBuilder();
    }

    public static QueueSettings persistentReliable() {
        return builder()
                .persistent()
                .publishAckEnabled()
                .consumerAckEnabled()
                .redelivery(true, 5)
                .durable(true)
                .build();
    }

    public static QueueSettings memoryOnlyFast() {
        return builder()
                .memoryOnly()
                .publishAckEnabled()
                .consumerAckEnabled()
                .redelivery(false, 1)
                .durable(false)
                .build();
    }

    public QueueSettingsBuilder persistent() {
        persistenceMode = PersistenceMode.PERSISTENT;
        return this;
    }

    public QueueSettingsBuilder memoryOnly() {
        persistenceMode = PersistenceMode.MEMORY_ONLY;
        return this;
    }

    public QueueSettingsBuilder publishAckEnabled() {
        publishAckMode = PublishAckMode.PUBLISH_ACK_ENABLED;
        return this;
    }

    public QueueSettingsBuilder publishAckNone() {
        publishAckMode = PublishAckMode.PUBLISH_ACK_NONE;
        return this;
    }

    public QueueSettingsBuilder consumerAckEnabled() {
        consumerAckMode = ConsumerAckMode.CONSUMER_ACK_ENABLED;
        return this;
    }

    public QueueSettingsBuilder consumerAckNone() {
        consumerAckMode = ConsumerAckMode.CONSUMER_ACK_NONE;
        return this;
    }

    public QueueSettingsBuilder redelivery(boolean enabled, int maxAttempts) {
        if (enabled && maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be greater than zero");
        }

        this.redeliveryEnabled = enabled;
        this.maxAttempts = enabled ? maxAttempts : 1;

        if (!enabled) {
            this.requeueOnDisconnect = false;
            this.requeueOnSendFailure = false;
        }

        return this;
    }

    public QueueSettingsBuilder requeueOnDisconnect(boolean value) {
        this.requeueOnDisconnect = value;
        return this;
    }

    public QueueSettingsBuilder requeueOnSendFailure(boolean value) {
        this.requeueOnSendFailure = value;
        return this;
    }

    public QueueSettingsBuilder ttl(long ttlMs) {
        if (ttlMs <= 0) {
            throw new IllegalArgumentException("ttlMs must be greater than zero");
        }

        this.ttlEnabled = true;
        this.ttlMs = ttlMs;
        return this;
    }

    public QueueSettingsBuilder ttlDisabled() {
        this.ttlEnabled = false;
        this.ttlMs = 0;
        return this;
    }

    public QueueSettingsBuilder autoDelete(boolean value) {
        this.autoDelete = value;

        if (value) {
            this.durable = false;
        }

        return this;
    }

    public QueueSettingsBuilder idleTimeout(long idleTimeoutMs) {
        if (idleTimeoutMs <= 0) {
            throw new IllegalArgumentException("idleTimeoutMs must be greater than zero");
        }

        this.idleTimeoutEnabled = true;
        this.idleTimeoutMs = idleTimeoutMs;
        return this;
    }

    public QueueSettingsBuilder idleTimeoutDisabled() {
        this.idleTimeoutEnabled = false;
        this.idleTimeoutMs = 0;
        return this;
    }

    public QueueSettingsBuilder durable(boolean value) {
        if (value && autoDelete) {
            throw new IllegalArgumentException("auto-delete queue cannot be durable");
        }

        this.durable = value;
        return this;
    }

    public QueueSettingsBuilder maxMessages(int maxMessages, OverflowPolicy overflowPolicy) {
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("maxMessages must be greater than zero");
        }

        this.capacityLimited = true;
        this.maxMessages = maxMessages;
        this.overflowPolicy = overflowPolicy == null
                ? OverflowPolicy.REJECT_PUBLISH
                : overflowPolicy;

        return this;
    }

    public QueueSettingsBuilder maxBytes(long maxBytes, OverflowPolicy overflowPolicy) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be greater than zero");
        }

        this.capacityLimited = true;
        this.maxBytes = maxBytes;
        this.overflowPolicy = overflowPolicy == null
                ? OverflowPolicy.REJECT_PUBLISH
                : overflowPolicy;

        return this;
    }

    public QueueSettingsBuilder unlimitedCapacity() {
        this.capacityLimited = false;
        this.maxMessages = 0;
        this.maxBytes = 0;
        this.overflowPolicy = OverflowPolicy.REJECT_PUBLISH;
        return this;
    }

    public QueueSettingsBuilder dispatch(
            int prefetchCount,
            ConsumerSelectionStrategy strategy
    ) {
        if (prefetchCount <= 0) {
            throw new IllegalArgumentException("prefetchCount must be greater than zero");
        }

        this.prefetchCount = prefetchCount;
        this.consumerSelectionStrategy = strategy == null
                ? ConsumerSelectionStrategy.ROUND_ROBIN
                : strategy;

        return this;
    }

    public QueueSettingsBuilder flowControl(int highWatermarkMessages, int lowWatermarkMessages) {
        return flowControl(highWatermarkMessages, lowWatermarkMessages, 0);
    }

    public QueueSettingsBuilder flowControl(int highWatermarkMessages, int lowWatermarkMessages, long maxBlockMs) {
        if (highWatermarkMessages <= 0) {
            throw new IllegalArgumentException("highWatermarkMessages must be greater than zero");
        }
        if (lowWatermarkMessages < 0) {
            throw new IllegalArgumentException("lowWatermarkMessages cannot be negative");
        }
        if (highWatermarkMessages <= lowWatermarkMessages) {
            throw new IllegalArgumentException("highWatermarkMessages must be greater than lowWatermarkMessages");
        }
        if (maxBlockMs < 0) {
            throw new IllegalArgumentException("maxBlockMs cannot be negative");
        }

        this.flowControlEnabled = true;
        this.highWatermarkMessages = highWatermarkMessages;
        this.lowWatermarkMessages = lowWatermarkMessages;
        this.maxBlockMs = maxBlockMs;
        return this;
    }

    public QueueSettingsBuilder flowControlBytes(long highWatermarkBytes, long lowWatermarkBytes) {
        return flowControlBytes(highWatermarkBytes, lowWatermarkBytes, 0);
    }

    public QueueSettingsBuilder flowControlBytes(long highWatermarkBytes, long lowWatermarkBytes, long maxBlockMs) {
        if (highWatermarkBytes <= 0) {
            throw new IllegalArgumentException("highWatermarkBytes must be greater than zero");
        }
        if (lowWatermarkBytes < 0) {
            throw new IllegalArgumentException("lowWatermarkBytes cannot be negative");
        }
        if (highWatermarkBytes <= lowWatermarkBytes) {
            throw new IllegalArgumentException("highWatermarkBytes must be greater than lowWatermarkBytes");
        }
        if (maxBlockMs < 0) {
            throw new IllegalArgumentException("maxBlockMs cannot be negative");
        }

        this.flowControlEnabled = true;
        this.highWatermarkBytes = highWatermarkBytes;
        this.lowWatermarkBytes = lowWatermarkBytes;
        this.maxBlockMs = maxBlockMs;
        return this;
    }

    public QueueSettingsBuilder flowControl(FlowControlPolicy policy) {
        if (policy == null || !policy.getEnabled()) {
            return flowControlDisabled();
        }

        this.flowControlEnabled = true;
        this.highWatermarkMessages = policy.getHighWatermarkMessages();
        this.lowWatermarkMessages = policy.getLowWatermarkMessages();
        this.highWatermarkBytes = policy.getHighWatermarkBytes();
        this.lowWatermarkBytes = policy.getLowWatermarkBytes();
        this.maxBlockMs = policy.getMaxBlockMs();
        return this;
    }

    public QueueSettingsBuilder flowControlDisabled() {
        this.flowControlEnabled = false;
        this.highWatermarkMessages = 0;
        this.lowWatermarkMessages = 0;
        this.highWatermarkBytes = 0;
        this.lowWatermarkBytes = 0;
        this.maxBlockMs = 0;
        return this;
    }

    public QueueSettings build() {
        validateCompatibility();

        QueueSettings.Builder builder = QueueSettings.newBuilder()
                .setPersistenceMode(persistenceMode)
                .setPublishAckMode(publishAckMode)
                .setConsumerAckMode(consumerAckMode)
                .setRedeliveryPolicy(RedeliveryPolicy.newBuilder()
                        .setEnabled(redeliveryEnabled)
                        .setMaxAttempts(maxAttempts)
                        .setRequeueOnDisconnect(requeueOnDisconnect)
                        .setRequeueOnSendFailure(requeueOnSendFailure)
                        .build())
                .setRetentionPolicy(MessageRetentionPolicy.newBuilder()
                        .setTtlEnabled(ttlEnabled)
                        .setTtlMs(ttlMs)
                        .build())
                .setLifecyclePolicy(QueueLifecyclePolicy.newBuilder()
                        .setAutoDelete(autoDelete)
                        .setIdleTimeoutEnabled(idleTimeoutEnabled)
                        .setIdleTimeoutMs(idleTimeoutMs)
                        .setDurable(durable)
                        .build())
                .setCapacityPolicy(QueueCapacityPolicy.newBuilder()
                        .setLimited(capacityLimited)
                        .setMaxMessages(maxMessages)
                        .setMaxBytes(maxBytes)
                        .setOverflowPolicy(overflowPolicy)
                        .build())
                .setDispatchPolicy(DispatchPolicy.newBuilder()
                        .setPrefetchCount(prefetchCount)
                        .setConsumerSelectionStrategy(consumerSelectionStrategy)
                        .build());

        if (flowControlEnabled) {
            builder.setFlowControlPolicy(FlowControlPolicy.newBuilder()
                    .setEnabled(true)
                    .setHighWatermarkMessages(highWatermarkMessages)
                    .setLowWatermarkMessages(lowWatermarkMessages)
                    .setHighWatermarkBytes(highWatermarkBytes)
                    .setLowWatermarkBytes(lowWatermarkBytes)
                    .setMaxBlockMs(maxBlockMs)
                    .build());
        }

        return builder.build();
    }

    private void validateCompatibility() {
        if (persistenceMode == PersistenceMode.PERSISTENT
                && consumerAckMode == ConsumerAckMode.CONSUMER_ACK_NONE) {
            throw new IllegalStateException("persistent queue requires consumer acknowledgements");
        }

        if (consumerAckMode == ConsumerAckMode.CONSUMER_ACK_NONE && redeliveryEnabled) {
            throw new IllegalStateException("redelivery requires consumer acknowledgements");
        }

        if (autoDelete && durable) {
            throw new IllegalStateException("auto-delete queue cannot be durable");
        }

        if (flowControlEnabled) {
            if (highWatermarkMessages > 0 && highWatermarkMessages <= lowWatermarkMessages) {
                throw new IllegalStateException("highWatermarkMessages must be greater than lowWatermarkMessages");
            }
            if (highWatermarkBytes > 0 && highWatermarkBytes <= lowWatermarkBytes) {
                throw new IllegalStateException("highWatermarkBytes must be greater than lowWatermarkBytes");
            }
            if (capacityLimited && maxMessages > 0 && highWatermarkMessages > 0 && highWatermarkMessages >= maxMessages) {
                throw new IllegalStateException("highWatermarkMessages (" + highWatermarkMessages
                        + ") must be less than maxMessages (" + maxMessages + ")");
            }
            if (capacityLimited && maxBytes > 0 && highWatermarkBytes > 0 && highWatermarkBytes >= maxBytes) {
                throw new IllegalStateException("highWatermarkBytes (" + highWatermarkBytes
                        + ") must be less than maxBytes (" + maxBytes + ")");
            }
        }
    }
}