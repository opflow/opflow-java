package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRpcRegistrationException extends OpflowConstructionException {

    public OpflowRpcRegistrationException() {
    }

    public OpflowRpcRegistrationException(String message) {
        super(message);
    }

    public OpflowRpcRegistrationException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRpcRegistrationException(Throwable cause) {
        super(cause);
    }

    public OpflowRpcRegistrationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
