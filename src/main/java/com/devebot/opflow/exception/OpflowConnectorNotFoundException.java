package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowConnectorNotFoundException extends OpflowOperationException {

    public OpflowConnectorNotFoundException() {
    }

    public OpflowConnectorNotFoundException(String message) {
        super(message);
    }

    public OpflowConnectorNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowConnectorNotFoundException(Throwable cause) {
        super(cause);
    }

    public OpflowConnectorNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
