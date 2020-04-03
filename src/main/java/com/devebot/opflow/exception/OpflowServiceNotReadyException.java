package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowServiceNotReadyException extends OpflowOperationException {

    public OpflowServiceNotReadyException() {
    }

    public OpflowServiceNotReadyException(String message) {
        super(message);
    }

    public OpflowServiceNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowServiceNotReadyException(Throwable cause) {
        super(cause);
    }

    public OpflowServiceNotReadyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
