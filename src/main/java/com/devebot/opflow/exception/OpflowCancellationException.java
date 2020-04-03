package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowCancellationException extends OpflowRestrictionException {

    public OpflowCancellationException() {
    }

    public OpflowCancellationException(String message) {
        super(message);
    }

    public OpflowCancellationException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowCancellationException(Throwable cause) {
        super(cause);
    }

    public OpflowCancellationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
