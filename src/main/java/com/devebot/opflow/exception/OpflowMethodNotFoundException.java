package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowMethodNotFoundException extends OpflowOperationException {

    public OpflowMethodNotFoundException() {
    }

    public OpflowMethodNotFoundException(String message) {
        super(message);
    }

    public OpflowMethodNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowMethodNotFoundException(Throwable cause) {
        super(cause);
    }

    public OpflowMethodNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
