package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowDiscoveryConnectionException extends OpflowOperationException {

    public OpflowDiscoveryConnectionException() {
    }

    public OpflowDiscoveryConnectionException(String message) {
        super(message);
    }

    public OpflowDiscoveryConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowDiscoveryConnectionException(Throwable cause) {
        super(cause);
    }

    public OpflowDiscoveryConnectionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
