package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRequestSuspendException extends OpflowOperationException {

    public OpflowRequestSuspendException() {
    }

    public OpflowRequestSuspendException(String message) {
        super(message);
    }

    public OpflowRequestSuspendException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRequestSuspendException(Throwable cause) {
        super(cause);
    }

    public OpflowRequestSuspendException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
