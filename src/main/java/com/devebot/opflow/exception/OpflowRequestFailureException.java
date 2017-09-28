package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRequestFailureException extends OpflowOperationException {

    public OpflowRequestFailureException() {
    }

    public OpflowRequestFailureException(String message) {
        super(message);
    }

    public OpflowRequestFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRequestFailureException(Throwable cause) {
        super(cause);
    }

    public OpflowRequestFailureException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
