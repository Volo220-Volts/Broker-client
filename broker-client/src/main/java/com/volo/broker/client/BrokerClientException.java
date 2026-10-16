package com.volo.broker.client;

public class BrokerClientException extends RuntimeException {
    public BrokerClientException(String message) {
        super(message);
    }

    public BrokerClientException(String message, Throwable cause) {
        super(message, cause);
    }
}