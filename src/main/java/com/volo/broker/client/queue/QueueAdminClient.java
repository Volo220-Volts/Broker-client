package com.volo.broker.client.queue;

import com.custombroker.grpc.DeclareQueueRequest;
import com.custombroker.grpc.DeleteQueueRequest;
import com.custombroker.grpc.ErrorCode;
import com.custombroker.grpc.QueueOperationResponse;
import com.custombroker.grpc.QueueServiceGrpc;
import com.custombroker.grpc.QueueSettings;
import com.volo.broker.client.BrokerRemoteException;
import com.volo.broker.client.internal.GrpcErrorMapper;

import java.util.Objects;

public final class QueueAdminClient {
    private final QueueServiceGrpc.QueueServiceBlockingStub stub;

    public QueueAdminClient(QueueServiceGrpc.QueueServiceBlockingStub stub) {
        this.stub = Objects.requireNonNull(stub, "stub is required");
    }

    public void declareQueue(String queuePath) {
        declareQueue(
                queuePath,
                QueueSettingsBuilder.persistentReliable(),
                QueueDeclarationOptions.failIfExists()
        );
    }

    public void declareQueue(String queuePath, QueueSettings settings) {
        declareQueue(
                queuePath,
                settings,
                QueueDeclarationOptions.failIfExists()
        );
    }

    public void declareQueueIfAbsent(String queuePath, QueueSettings settings) {
        declareQueue(
                queuePath,
                settings,
                QueueDeclarationOptions.useExisting()
        );
    }

    public void declareQueue(
            String queuePath,
            QueueSettings settings,
            QueueDeclarationOptions options
    ) {
        validateQueuePath(queuePath);
        Objects.requireNonNull(settings, "settings is required");

        QueueDeclarationOptions effectiveOptions = options == null
                ? QueueDeclarationOptions.failIfExists()
                : options;

        QueueOperationResponse response = stub.declareQueue(
                DeclareQueueRequest.newBuilder()
                        .setQueuePath(queuePath)
                        .setSettings(settings)
                        .build()
        );

        if (response.getSuccess()) {
            return;
        }

        BrokerRemoteException exception = GrpcErrorMapper.toException(response.getError());

        if (effectiveOptions.existsPolicy() == QueueExistsPolicy.USE_EXISTING
                && exception.errorCode() == ErrorCode.QUEUE_ALREADY_EXISTS) {
            return;
        }

        throw exception;
    }

    public void deleteQueue(String queuePath) {
        validateQueuePath(queuePath);

        QueueOperationResponse response = stub.deleteQueue(
                DeleteQueueRequest.newBuilder()
                        .setQueuePath(queuePath)
                        .build()
        );

        if (response.getSuccess()) {
            return;
        }

        throw GrpcErrorMapper.toException(response.getError());
    }

    private void validateQueuePath(String queuePath) {
        if (queuePath == null || queuePath.isBlank()) {
            throw new IllegalArgumentException("queuePath is required");
        }
    }
}