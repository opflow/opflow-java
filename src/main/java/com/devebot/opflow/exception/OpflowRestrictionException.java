package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRestrictionException extends OpflowOperationException {

    public OpflowRestrictionException() {
    }

    public OpflowRestrictionException(String message) {
        super(message);
    }

    public OpflowRestrictionException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRestrictionException(Throwable cause) {
        super(cause);
    }

    public OpflowRestrictionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
