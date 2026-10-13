package com.volo.broker.client.queue;

public record QueueDeclarationOptions(
        QueueExistsPolicy existsPolicy
) {
    public QueueDeclarationOptions {
        if (existsPolicy == null) {
            existsPolicy = QueueExistsPolicy.FAIL;
        }
    }

    public static QueueDeclarationOptions failIfExists() {
        return new QueueDeclarationOptions(QueueExistsPolicy.FAIL);
    }

    public static QueueDeclarationOptions useExisting() {
        return new QueueDeclarationOptions(QueueExistsPolicy.USE_EXISTING);
    }
}