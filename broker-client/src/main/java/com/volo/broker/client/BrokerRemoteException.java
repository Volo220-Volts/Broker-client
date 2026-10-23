package com.volo.broker.client;

import com.custombroker.grpc.ErrorCode;

import java.util.Map;

public class BrokerRemoteException extends BrokerClientException {
    private final ErrorCode errorCode;
    private final Map<String, String> details;

    public BrokerRemoteException(
            ErrorCode errorCode,
            String message,
            Map<String, String> details
    ) {
        super(message == null || message.isBlank() ? "Broker remote error" : message);
        this.errorCode = errorCode == null ? ErrorCode.INTERNAL_ERROR : errorCode;
        this.details = details == null ? Map.of() : Map.copyOf(details);
    }

    public ErrorCode errorCode() {
        return errorCode;
    }

    public Map<String, String> details() {
        return details;
    }

    public boolean isResourceExhausted() {
        return errorCode == ErrorCode.RESOURCE_EXHAUSTED;
    }
}