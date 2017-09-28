package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRequestFailedException extends OpflowOperationException {

    public OpflowRequestFailedException() {
    }

    public OpflowRequestFailedException(String message) {
        super(message);
    }

    public OpflowRequestFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRequestFailedException(Throwable cause) {
        super(cause);
    }

    public OpflowRequestFailedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
