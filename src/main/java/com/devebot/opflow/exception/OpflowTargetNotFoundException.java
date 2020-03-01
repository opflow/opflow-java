package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowTargetNotFoundException extends OpflowOperationException {

    public OpflowTargetNotFoundException() {
    }

    public OpflowTargetNotFoundException(String message) {
        super(message);
    }

    public OpflowTargetNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowTargetNotFoundException(Throwable cause) {
        super(cause);
    }

    public OpflowTargetNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
