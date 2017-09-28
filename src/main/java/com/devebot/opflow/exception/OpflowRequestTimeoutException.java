package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRequestTimeoutException extends OpflowOperationException {

    public OpflowRequestTimeoutException() {
    }

    public OpflowRequestTimeoutException(String message) {
        super(message);
    }

    public OpflowRequestTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRequestTimeoutException(Throwable cause) {
        super(cause);
    }

    public OpflowRequestTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
