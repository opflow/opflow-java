package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowSemaphoreTimeoutException extends OpflowRestrictionException {

    public OpflowSemaphoreTimeoutException() {
    }

    public OpflowSemaphoreTimeoutException(String message) {
        super(message);
    }

    public OpflowSemaphoreTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowSemaphoreTimeoutException(Throwable cause) {
        super(cause);
    }

    public OpflowSemaphoreTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
