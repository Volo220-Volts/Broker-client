package com.volo.broker.client.internal;

import com.custombroker.grpc.ErrorInfo;
import com.volo.broker.client.BrokerRemoteException;

public final class GrpcErrorMapper {
    private GrpcErrorMapper() {
    }

    public static BrokerRemoteException toException(ErrorInfo errorInfo) {
        if (errorInfo == null) {
            return new BrokerRemoteException(
                    null,
                    "Unknown broker error",
                    null
            );
        }

        return new BrokerRemoteException(
                errorInfo.getCode(),
                errorInfo.getMessage(),
                errorInfo.getDetailsMap()
        );
    }
}