package com.volo.broker.client.queue;

import com.custombroker.grpc.DeclareQueueRequest;
import com.custombroker.grpc.DeleteQueueRequest;
import com.custombroker.grpc.ErrorCode;
import com.custombroker.grpc.QueueOperationResponse;
import com.custombroker.grpc.QueueServiceGrpc;
import com.custombroker.grpc.QueueSettings;
import com.volo.broker.client.BrokerClientException;
import com.volo.broker.client.BrokerRemoteException;
import com.volo.broker.client.internal.ClientValidators;
import com.volo.broker.client.internal.GrpcErrorMapper;
import io.grpc.StatusRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class QueueAdminClient implements QueueAdmin {
    private static final Logger log = LoggerFactory.getLogger(QueueAdminClient.class);

    private final QueueServiceGrpc.QueueServiceBlockingStub stub;
    private final Duration operationTimeout;

    public QueueAdminClient(QueueServiceGrpc.QueueServiceBlockingStub stub) {
        this(stub, Duration.ofSeconds(10));
    }

    public QueueAdminClient(QueueServiceGrpc.QueueServiceBlockingStub stub, Duration operationTimeout) {
        this.stub = Objects.requireNonNull(stub, "stub is required");
        this.operationTimeout = operationTimeout == null ? Duration.ofSeconds(10) : operationTimeout;
    }

    @Override
    public void declareQueue(String queuePath) {
        declareQueue(
                queuePath,
                QueueSettingsBuilder.persistentReliable(),
                QueueDeclarationOptions.failIfExists()
        );
    }

    @Override
    public void declareQueue(String queuePath, QueueSettings settings) {
        declareQueue(
                queuePath,
                settings,
                QueueDeclarationOptions.failIfExists()
        );
    }

    @Override
    public void declareQueueIfAbsent(String queuePath, QueueSettings settings) {
        declareQueue(
                queuePath,
                settings,
                QueueDeclarationOptions.useExisting()
        );
    }

    @Override
    public void declareQueue(
            String queuePath,
            QueueSettings settings,
            QueueDeclarationOptions options
    ) {
        ClientValidators.requireQueuePath(queuePath);
        Objects.requireNonNull(settings, "settings is required");

        QueueDeclarationOptions effectiveOptions = options == null
                ? QueueDeclarationOptions.failIfExists()
                : options;

        log.debug("Declaring queue '{}' with options: {}", queuePath, effectiveOptions);

        QueueOperationResponse response;
        try {
            response = stub
                    .withDeadlineAfter(operationTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .declareQueue(
                            DeclareQueueRequest.newBuilder()
                                    .setQueuePath(queuePath)
                                    .setSettings(settings)
                                    .build()
                    );
        } catch (StatusRuntimeException exception) {
            log.error("Failed to declare queue '{}' due to gRPC transport error: {}", queuePath, exception.getStatus());
            throw new BrokerClientException("Failed to declare queue due to gRPC error: " + exception.getStatus(), exception);
        }

        if (response.getSuccess()) {
            log.info("Queue '{}' declared successfully", queuePath);
            return;
        }

        BrokerRemoteException exception = GrpcErrorMapper.toException(response.getError());

        if (effectiveOptions.existsPolicy() == QueueExistsPolicy.USE_EXISTING
                && exception.errorCode() == ErrorCode.QUEUE_ALREADY_EXISTS) {
            log.debug("Queue '{}' already exists, using existing instance", queuePath);
            return;
        }

        log.warn("Broker rejected declaration of queue '{}': {}", queuePath, exception.getMessage());
        throw exception;
    }

    @Override
    public void deleteQueue(String queuePath) {
        ClientValidators.requireQueuePath(queuePath);

        log.debug("Deleting queue '{}'", queuePath);

        QueueOperationResponse response;
        try {
            response = stub
                    .withDeadlineAfter(operationTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .deleteQueue(
                            DeleteQueueRequest.newBuilder()
                                    .setQueuePath(queuePath)
                                    .build()
                    );
        } catch (StatusRuntimeException exception) {
            log.error("Failed to delete queue '{}' due to gRPC transport error: {}", queuePath, exception.getStatus());
            throw new BrokerClientException("Failed to delete queue due to gRPC error: " + exception.getStatus(), exception);
        }

        if (response.getSuccess()) {
            log.info("Queue '{}' deleted successfully", queuePath);
            return;
        }

        BrokerRemoteException exception = GrpcErrorMapper.toException(response.getError());
        log.warn("Broker rejected deletion of queue '{}': {}", queuePath, exception.getMessage());
        throw exception;
    }
}