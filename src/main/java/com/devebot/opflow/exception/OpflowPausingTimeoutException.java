package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowPausingTimeoutException extends OpflowRestrictionException {

    public OpflowPausingTimeoutException() {
    }

    public OpflowPausingTimeoutException(String message) {
        super(message);
    }

    public OpflowPausingTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowPausingTimeoutException(Throwable cause) {
        super(cause);
    }

    public OpflowPausingTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
